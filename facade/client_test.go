package facade_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/shikejing/platform-sdk-facade-go/backends"
	"github.com/shikejing/platform-sdk-facade-go/facade"
)

func newClient(logger facade.Logger) (*facade.Client, *backends.Alpha, *backends.Beta) {
	c := facade.NewClient(logger)
	a := backends.NewAlpha(logger)
	b := backends.NewBeta(logger)
	opts := facade.BackendOptions{
		Health:      facade.HealthPolicy{FailureThreshold: 2, OpenDuration: 60 * time.Millisecond, SuccessThreshold: 2},
		Rate:        facade.RatePolicy{RatePerSec: 1000, Burst: 100},
		RenewMargin: time.Second,
	}
	c.AddBackend(a, opts)
	c.AddBackend(b, opts)
	return c, a, b
}

func getAccount(id string) facade.Request {
	return facade.Request{Op: facade.OpAccountGet, Payload: map[string]string{"account_id": id}}
}

func TestFailoverToBackupAndNormalize(t *testing.T) {
	logger := facade.NewMemLogger()
	c, a, b := newClient(logger)

	// 主后端故障：连续失败后熔断，请求切换到 beta。
	a.Server.SetDown(true)
	var res *facade.NormalizedResult
	var err error
	for i := 0; i < 4; i++ {
		res, err = c.Do(context.Background(), getAccount("u1"))
	}
	if err != nil {
		t.Fatalf("request should succeed via backup: %v", err)
	}
	if res.Backend != "beta" {
		t.Fatalf("expected beta to serve, got %s", res.Backend)
	}
	if c.Health("alpha") != facade.StateOpen {
		t.Fatalf("alpha should be circuit-open, got %s", c.Health("alpha"))
	}
	// beta 的字段差异被归一化吸收：uid->account_id 映射成功，region 缺失有标记。
	if v, ok := res.Get("account_id"); !ok || v != "u1" {
		t.Fatalf("account_id not normalized: %v %v", v, ok)
	}
	if f := res.Fields["region"]; f.Status != facade.FieldMissing {
		t.Fatalf("region should be marked missing on beta, got %+v", f)
	}
	if len(logger.ByReason(facade.ReasonFailover)) == 0 {
		t.Fatal("missing FAILOVER audit event")
	}
	if b.Server.CallCount() == 0 {
		t.Fatal("beta should have served traffic")
	}
}

func TestFailbackAfterRecovery(t *testing.T) {
	logger := facade.NewMemLogger()
	c, a, _ := newClient(logger)

	a.Server.SetDown(true)
	for i := 0; i < 3; i++ {
		c.Do(context.Background(), getAccount("u1"))
	}
	if c.Health("alpha") != facade.StateOpen {
		t.Fatal("alpha should be open")
	}
	// 主后端恢复，等待进入半开后连续成功达到阈值即回切。
	a.Server.SetDown(false)
	time.Sleep(80 * time.Millisecond)
	for i := 0; i < 3; i++ {
		res, err := c.Do(context.Background(), getAccount("u2"))
		if err != nil {
			t.Fatal(err)
		}
		_ = res
	}
	if c.Health("alpha") != facade.StateHealthy {
		t.Fatalf("alpha should failback to healthy, got %s", c.Health("alpha"))
	}
	res, err := c.Do(context.Background(), getAccount("u3"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Backend != "alpha" {
		t.Fatalf("after failback primary should serve, got %s", res.Backend)
	}
	if len(logger.ByReason(facade.ReasonFailback)) != 1 {
		t.Fatal("missing FAILBACK audit event")
	}
}

func TestWriteQueuedWhenAllDownAndReplayed(t *testing.T) {
	logger := facade.NewMemLogger()
	c, a, b := newClient(logger)

	a.Server.SetDown(true)
	b.Server.SetDown(true)
	write := facade.Request{Op: facade.OpMatchCreate, IdempotencyKey: "match-1",
		Payload: map[string]string{"account_id": "u1"}}

	_, err := c.Do(context.Background(), write)
	if !errors.Is(err, facade.ErrQueued) {
		t.Fatalf("write should be queued, got %v", err)
	}
	// 重复提交同一幂等键：拒绝，不产生重复副作用。
	if _, err := c.Do(context.Background(), write); !errors.Is(err, facade.ErrDuplicate) {
		t.Fatalf("duplicate submit should be ErrDuplicate, got %v", err)
	}
	if c.Queue().Len() != 1 {
		t.Fatalf("queue len = %d, want 1", c.Queue().Len())
	}

	// 后端恢复后按序重放，服务端只应用一次。
	// 等待熔断进入半开，允许试探流量通过。
	a.Server.SetDown(false)
	time.Sleep(80 * time.Millisecond)
	n, err := c.ReplayQueue(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("replay n=%d err=%v", n, err)
	}
	if got := a.Server.AppliedCount("match-1"); got != 1 {
		t.Fatalf("server applied %d times, want exactly 1", got)
	}
	if len(logger.ByReason(facade.ReasonReplayOK)) != 1 {
		t.Fatal("missing REPLAY_OK audit event")
	}
}

func TestConcurrentRenewalSingleFlightAtClient(t *testing.T) {
	c, a, _ := newClient(nil)
	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = c.Do(context.Background(), getAccount("u"))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
	if got := a.Server.RenewCount(); got != 1 {
		t.Fatalf("alpha renewed %d times under concurrency, want 1", got)
	}
}

func TestAuditTrailPerRequest(t *testing.T) {
	logger := facade.NewMemLogger()
	c, _, _ := newClient(logger)
	if _, err := c.Do(context.Background(), getAccount("u9")); err != nil {
		t.Fatal(err)
	}
	// 一次请求应能通过 TraceID 串起 REQUEST_START -> REQUEST_SUCCESS。
	starts := logger.ByReason(facade.ReasonRequestStart)
	succs := logger.ByReason(facade.ReasonRequestSuccess)
	if len(starts) != 1 || len(succs) != 1 {
		t.Fatalf("want 1 start + 1 success, got %d/%d", len(starts), len(succs))
	}
	if starts[0].TraceID == "" || starts[0].TraceID != succs[0].TraceID {
		t.Fatal("trace id should link start and success events")
	}
}

func TestReplayInterruptionKeepsProgressAtClient(t *testing.T) {
	logger := facade.NewMemLogger()
	c, a, b := newClient(logger)

	a.Server.SetDown(true)
	b.Server.SetDown(true)
	for _, key := range []string{"w1", "w2", "w3"} {
		_, err := c.Do(context.Background(), facade.Request{
			Op: facade.OpRoomCreate, IdempotencyKey: key,
			Payload: map[string]string{"account_id": "u1"}})
		if !errors.Is(err, facade.ErrQueued) {
			t.Fatalf("write %s should be queued, got %v", key, err)
		}
	}

	// 第一次重放：后端仍故障 -> 全部保留，不得跳号。
	if n, err := c.ReplayQueue(context.Background()); err == nil || n != 0 {
		t.Fatalf("replay should fail with 0 progress, n=%d err=%v", n, err)
	}
	if c.Queue().Len() != 3 {
		t.Fatalf("progress lost: queue len = %d, want 3", c.Queue().Len())
	}

	// 恢复后重放：按序应用且不重复。
	a.Server.SetDown(false)
	time.Sleep(80 * time.Millisecond)
	n, err := c.ReplayQueue(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("replay n=%d err=%v", n, err)
	}
	for _, key := range []string{"w1", "w2", "w3"} {
		if got := a.Server.AppliedCount(key); got != 1 {
			t.Fatalf("%s applied %d times, want 1", key, got)
		}
	}
	if len(logger.ByReason(facade.ReasonReplayFailed)) == 0 {
		t.Fatal("missing REPLAY_FAILED audit event from interrupted attempt")
	}
}
