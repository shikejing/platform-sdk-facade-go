package facade

import (
	"context"
	"testing"
	"time"
)

// 某后端被限流时只影响它自身：请求溢出到未限流的备用后端，且全部成功。
func TestRateLimitIsolation(t *testing.T) {
	limited := newFakeBackend("limited", time.Hour)
	free := newFakeBackend("free", time.Hour)
	sink := NewMemorySink()
	c := NewClient(Config{
		Backends: []BackendConfig{
			{Backend: limited, RateLimit: RateLimitConfig{Rate: 1, Burst: 1}},
			{Backend: free}, // 不限流
		},
		Sink: sink,
	})
	ctx := context.Background()

	// 突发 5 个请求：第 1 个用掉 limited 的唯一令牌，其余溢出到 free。
	for i := 0; i < 5; i++ {
		if _, err := c.GetAccount(ctx, "a1"); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if got := limited.reads(); got != 1 {
		t.Fatalf("limited backend served %d reads, want 1 (burst)", got)
	}
	if got := free.reads(); got != 4 {
		t.Fatalf("free backend served %d reads, want 4 (spillover)", got)
	}
	// 限流不影响健康状态：被限流后端不应被熔断。
	if got := c.Health()["limited"]; got != StateHealthy {
		t.Fatalf("limited backend state = %s, want healthy (rate limit is not a failure)", got)
	}
	if sink.Count(EventRateLimit, ReasonRateLimited) == 0 {
		t.Fatal("missing rate_limit audit event")
	}
}

// 配额超限只影响该后端自身，其他后端不受牵连。
func TestQuotaExceededOnlyAffectsOwner(t *testing.T) {
	quota := newFakeBackend("quota", time.Hour)
	other := newFakeBackend("other", time.Hour)
	sink := NewMemorySink()
	c := NewClient(Config{
		Backends: []BackendConfig{
			{Backend: quota, RateLimit: RateLimitConfig{Quota: 2, QuotaWindow: time.Hour}},
			{Backend: other},
		},
		Sink: sink,
	})
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		if _, err := c.GetAccount(ctx, "a1"); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if got := quota.reads(); got != 2 {
		t.Fatalf("quota backend served %d reads, want 2 (quota)", got)
	}
	if got := other.reads(); got != 2 {
		t.Fatalf("other backend served %d reads, want 2", got)
	}
	if sink.Count(EventRateLimit, ReasonQuotaExceeded) == 0 {
		t.Fatal("missing quota_exceeded audit event")
	}
}

// 单后端且 Wait=false 时，超限请求立即拿到带退避信息的可区分错误。
func TestRateLimitBackoffError(t *testing.T) {
	only := newFakeBackend("only", time.Hour)
	c := NewClient(Config{
		Backends: []BackendConfig{
			{Backend: only, RateLimit: RateLimitConfig{Rate: 1, Burst: 1}},
		},
	})
	ctx := context.Background()

	if _, err := c.GetAccount(ctx, "a1"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	_, err := c.GetAccount(ctx, "a1")
	if !IsCode(err, ReasonRateLimited) {
		t.Fatalf("err = %v, want code %s", err, ReasonRateLimited)
	}
	fe := findByCode(err, ReasonRateLimited)
	if fe == nil || fe.RetryAfter <= 0 {
		t.Fatalf("err = %v, want positive RetryAfter hint", err)
	}
}

// Wait=true 时超限请求排队等待令牌，而不是立即失败。
func TestRateLimitWaitQueues(t *testing.T) {
	only := newFakeBackend("only", time.Hour)
	c := NewClient(Config{
		Backends: []BackendConfig{
			{Backend: only, RateLimit: RateLimitConfig{Rate: 100, Burst: 1, Wait: true}},
		},
	})
	ctx := context.Background()

	if _, err := c.GetAccount(ctx, "a1"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	start := time.Now()
	if _, err := c.GetAccount(ctx, "a1"); err != nil {
		t.Fatalf("queued call should succeed, got: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 5*time.Millisecond {
		t.Fatalf("queued call returned after %s, expected to wait for a token", elapsed)
	}
}

// findByCode 在错误链中查找指定原因码的 *Error。
func findByCode(err error, code ReasonCode) *Error {
	for err != nil {
		if e, ok := err.(*Error); ok && e.Code == code {
			return e
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return nil
		}
		err = u.Unwrap()
	}
	return nil
}
