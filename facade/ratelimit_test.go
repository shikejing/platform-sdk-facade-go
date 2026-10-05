package facade

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoffModeRejectsOverLimit(t *testing.T) {
	fakeClock(t)
	logger := NewMemLogger()
	r := NewRateLimiter("b1", RatePolicy{RatePerSec: 1, Burst: 2, OverLimit: OverLimitBackoff}, logger)

	for i := 0; i < 2; i++ {
		if err := r.Acquire(context.Background()); err != nil {
			t.Fatalf("token %d should succeed: %v", i, err)
		}
	}
	if err := r.Acquire(context.Background()); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("over-limit should be ErrRateLimited, got %v", err)
	}
	if len(logger.ByReason(ReasonRateLimited)) != 1 {
		t.Fatal("missing RATE_LIMITED event")
	}
}

func TestQueueModeWaitsForToken(t *testing.T) {
	advance := fakeClock(t)
	logger := NewMemLogger()
	r := NewRateLimiter("b1", RatePolicy{RatePerSec: 100, Burst: 1, OverLimit: OverLimitQueue, QueueWait: time.Second}, logger)

	if err := r.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 后台推进时钟，模拟令牌随时间补充。
	done := make(chan error, 1)
	go func() { done <- r.Acquire(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	advance(50 * time.Millisecond) // 补充 5 个令牌
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("queued acquire should succeed after refill, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued acquire did not complete")
	}
	if len(logger.ByReason(ReasonRateQueued)) != 1 {
		t.Fatal("missing RATE_QUEUED event")
	}
}

func TestQueueModeGivesUpAfterQueueWait(t *testing.T) {
	advance := fakeClock(t)
	r := NewRateLimiter("b1", RatePolicy{RatePerSec: 0.0001, Burst: 1, OverLimit: OverLimitQueue, QueueWait: 100 * time.Millisecond}, nil)
	if err := r.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.Acquire(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	advance(time.Second) // 超过 QueueWait
	if err := <-done; !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited after queue wait, got %v", err)
	}
}

func TestLimitersAreIsolatedPerBackend(t *testing.T) {
	fakeClock(t)
	r1 := NewRateLimiter("b1", RatePolicy{RatePerSec: 1, Burst: 1, OverLimit: OverLimitBackoff}, nil)
	r2 := NewRateLimiter("b2", RatePolicy{RatePerSec: 1, Burst: 1, OverLimit: OverLimitBackoff}, nil)

	if err := r1.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r1.Acquire(context.Background()); !errors.Is(err, ErrRateLimited) {
		t.Fatal("b1 should be limited")
	}
	// b1 被限流不得牵连 b2。
	if err := r2.Acquire(context.Background()); err != nil {
		t.Fatalf("b2 must not be affected by b1 limit: %v", err)
	}
}
