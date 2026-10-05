package facade

import (
	"sync"
	"testing"
	"time"
)

// fakeClock 替换包内 now（线程安全），测试结束恢复。
func fakeClock(t *testing.T) func(d time.Duration) {
	t.Helper()
	var mu sync.Mutex
	cur := time.Now()
	old := now
	now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return cur
	}
	t.Cleanup(func() { now = old })
	return func(d time.Duration) {
		mu.Lock()
		cur = cur.Add(d)
		mu.Unlock()
	}
}

func testPolicy() HealthPolicy {
	return HealthPolicy{FailureThreshold: 3, OpenDuration: 10 * time.Second, SuccessThreshold: 2}
}

func TestCircuitOpensAfterConsecutiveFailures(t *testing.T) {
	advance := fakeClock(t)
	logger := NewMemLogger()
	h := NewHealthTracker("b1", testPolicy(), logger)

	h.OnFailure("timeout")
	h.OnFailure("timeout")
	if h.State() != StateHealthy || !h.Selectable() {
		t.Fatal("should still be healthy below threshold")
	}
	h.OnFailure("timeout")
	if h.State() != StateOpen {
		t.Fatalf("expected open, got %s", h.State())
	}
	if h.Selectable() {
		t.Fatal("open circuit must not be selectable")
	}
	if len(logger.ByReason(ReasonCircuitOpen)) != 1 {
		t.Fatal("missing CIRCUIT_OPEN event")
	}
	advance(time.Second)
}

func TestHalfOpenProbeAndReopen(t *testing.T) {
	advance := fakeClock(t)
	logger := NewMemLogger()
	h := NewHealthTracker("b1", testPolicy(), logger)

	for i := 0; i < 3; i++ {
		h.OnFailure("down")
	}
	advance(10 * time.Second) // 到达 OpenDuration
	if h.State() != StateHalfOpen {
		t.Fatalf("expected half_open after open duration, got %s", h.State())
	}
	if !h.Selectable() {
		t.Fatal("half-open should allow probe traffic")
	}
	// 试探失败：立即重新熔断。
	h.OnFailure("still down")
	if h.State() != StateOpen {
		t.Fatalf("probe failure should reopen circuit, got %s", h.State())
	}
	if got := len(logger.ByReason(ReasonCircuitOpen)); got != 2 {
		t.Fatalf("expected 2 CIRCUIT_OPEN events, got %d", got)
	}
}

func TestFailbackRequiresSuccessThreshold(t *testing.T) {
	advance := fakeClock(t)
	logger := NewMemLogger()
	h := NewHealthTracker("b1", testPolicy(), logger)

	for i := 0; i < 3; i++ {
		h.OnFailure("down")
	}
	advance(10 * time.Second)
	if h.State() != StateHalfOpen {
		t.Fatal("expected half_open")
	}
	// 防抖动：单次成功不足以回切。
	h.OnSuccess()
	if h.State() != StateHalfOpen {
		t.Fatalf("single success must not failback, got %s", h.State())
	}
	// 中途一次失败清零进度并重新熔断。
	h.OnFailure("flap")
	if h.State() != StateOpen {
		t.Fatal("flap should reopen")
	}
	advance(10 * time.Second)
	if h.State() != StateHalfOpen { // 触发惰性迁移后再试探
		t.Fatal("expected half_open after reopen duration")
	}
	h.OnSuccess()
	h.OnSuccess()
	if h.State() != StateHealthy {
		t.Fatalf("expected failback after %d consecutive successes, got %s", 2, h.State())
	}
	if len(logger.ByReason(ReasonFailback)) != 1 {
		t.Fatal("missing FAILBACK event")
	}
}

func TestSuccessResetsFailureCount(t *testing.T) {
	fakeClock(t)
	h := NewHealthTracker("b1", testPolicy(), nil)
	h.OnFailure("a")
	h.OnFailure("b")
	h.OnSuccess() // 重置连续失败计数
	h.OnFailure("c")
	h.OnFailure("d")
	if h.State() != StateHealthy {
		t.Fatal("non-consecutive failures must not open circuit")
	}
}
