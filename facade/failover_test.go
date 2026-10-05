package facade

import (
	"context"
	"errors"
	"testing"
	"time"
)

func twoBackendClient(primary, secondary *fakeBackend, sink Sink) *Client {
	hc := HealthConfig{
		FailureThreshold: 2,
		OpenCooldown:     80 * time.Millisecond,
		SuccessThreshold: 2,
	}
	return NewClient(Config{
		Backends: []BackendConfig{
			{Backend: primary, Health: hc},
			{Backend: secondary, Health: hc},
		},
		RequestTimeout: 200 * time.Millisecond,
		Sink:           sink,
	})
}

// 主后端失败时切换到备用后端；连续失败达到阈值后熔断，熔断的后端不再被选中。
func TestFailoverAndCircuitOpen(t *testing.T) {
	primary := newFakeBackend("primary", time.Hour)
	secondary := newFakeBackend("secondary", time.Hour)
	sink := NewMemorySink()
	c := twoBackendClient(primary, secondary, sink)
	ctx := WithTraceID(context.Background(), "trace-failover")

	primary.setReadErr(errors.New("boom"))

	// 第 1、2 次：主失败 -> 切换备用；达到阈值后熔断。
	for i := 0; i < 2; i++ {
		info, err := c.GetAccount(ctx, "a1")
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if info.AccountID.Value != "a1" {
			t.Fatalf("call %d: unexpected account %q", i, info.AccountID.Value)
		}
	}
	if got := c.Health()["primary"]; got != StateCircuitOpen {
		t.Fatalf("primary state = %s, want %s", got, StateCircuitOpen)
	}
	readsBefore := primary.reads()

	// 第 3 次：主已熔断，不得再被选中。
	if _, err := c.GetAccount(ctx, "a1"); err != nil {
		t.Fatalf("call 3: %v", err)
	}
	if got := primary.reads(); got != readsBefore {
		t.Fatalf("circuit-open primary received %d more reads", got-readsBefore)
	}

	// 审计：熔断与切换事件都有记录，且携带 trace id。
	if sink.Count(EventCircuitOpen, "") == 0 {
		t.Fatal("missing circuit_open audit event")
	}
	if sink.Count(EventFailover, "") == 0 {
		t.Fatal("missing failover audit event")
	}
	for _, e := range sink.Events() {
		if e.TraceID != "trace-failover" {
			t.Fatalf("event %+v missing trace id", e)
		}
	}
}

// 回切需要连续成功达到阈值，半开期间的失败会立即重新熔断，避免反复横跳。
func TestFailbackWithHysteresis(t *testing.T) {
	primary := newFakeBackend("primary", time.Hour)
	secondary := newFakeBackend("secondary", time.Hour)
	sink := NewMemorySink()
	c := twoBackendClient(primary, secondary, sink)
	ctx := context.Background()

	// 先熔断主后端。
	primary.setReadErr(errors.New("boom"))
	for i := 0; i < 2; i++ {
		if _, err := c.GetAccount(ctx, "a1"); err != nil {
			t.Fatalf("setup call %d: %v", i, err)
		}
	}
	if got := c.Health()["primary"]; got != StateCircuitOpen {
		t.Fatalf("primary state = %s, want circuit_open", got)
	}

	// 冷却结束后主后端恢复：第一次成功只进入半开，不回切。
	primary.setReadErr(nil)
	time.Sleep(100 * time.Millisecond)
	if _, err := c.GetAccount(ctx, "a1"); err != nil {
		t.Fatalf("probe call: %v", err)
	}
	if got := c.Health()["primary"]; got == StateHealthy {
		t.Fatalf("primary回切过早：单次成功即恢复健康，缺少迟滞")
	}
	if sink.Count(EventFailback, ReasonFailback) != 0 {
		t.Fatal("failback emitted before success threshold reached")
	}

	// 半开期间再次失败：立即重新熔断。
	primary.setReadErr(errors.New("flapping"))
	if _, err := c.GetAccount(ctx, "a1"); err != nil {
		t.Fatalf("flap call: %v", err)
	}
	if got := c.Health()["primary"]; got != StateCircuitOpen {
		t.Fatalf("primary state = %s after half-open failure, want circuit_open", got)
	}

	// 再次冷却并稳定恢复：连续两次成功后回切，且只回切一次。
	primary.setReadErr(nil)
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 2; i++ {
		if _, err := c.GetAccount(ctx, "a1"); err != nil {
			t.Fatalf("recover call %d: %v", i, err)
		}
	}
	if got := c.Health()["primary"]; got != StateHealthy {
		t.Fatalf("primary state = %s, want healthy after %d consecutive successes", got, 2)
	}
	if n := sink.Count(EventFailback, ReasonFailback); n != 1 {
		t.Fatalf("failback events = %d, want exactly 1 (no flapping)", n)
	}
}

// 超时同样触发切换并归类为 backend_timeout。
func TestTimeoutTriggersFailover(t *testing.T) {
	primary := newFakeBackend("primary", time.Hour)
	primary.readDelay = 300 * time.Millisecond // 超过 200ms 的请求超时
	secondary := newFakeBackend("secondary", time.Hour)
	sink := NewMemorySink()
	c := twoBackendClient(primary, secondary, sink)

	if _, err := c.GetAccount(context.Background(), "a1"); err != nil {
		t.Fatalf("call: %v", err)
	}
	if sink.Count(EventFailover, ReasonBackendTimeout) == 0 {
		t.Fatal("missing failover event with reason backend_timeout")
	}
	if secondary.reads() == 0 {
		t.Fatal("secondary was not used after primary timeout")
	}
}
