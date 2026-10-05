package facade

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func writeClient(b *fakeBackend, sink Sink) *Client {
	return NewClient(Config{
		Backends: []BackendConfig{{
			Backend: b,
			// 写路径测试关注队列与重放，放宽熔断阈值避免干扰。
			Health: HealthConfig{FailureThreshold: 100, OpenCooldown: 50 * time.Millisecond, SuccessThreshold: 1},
		}},
		Sink: sink,
	})
}

func op(id string) WriteOp {
	return WriteOp{ID: id, Kind: WriteCreateRoom, Payload: map[string]any{"game": "g"}}
}

// 后端不可用时写请求进入本地队列，恢复后按原顺序重放。
func TestWriteQueuedThenReplayedInOrder(t *testing.T) {
	fake := newFakeBackend("b1", time.Hour)
	sink := NewMemorySink()
	c := writeClient(fake, sink)
	ctx := context.Background()

	fake.setWriteErr(errors.New("backend down"))
	for _, id := range []string{"w1", "w2", "w3"} {
		err := c.SubmitWrite(ctx, op(id))
		if !IsCode(err, ReasonWriteQueued) {
			t.Fatalf("submit %s: err = %v, want code %s", id, err, ReasonWriteQueued)
		}
	}
	if got := c.PendingWrites(); got != 3 {
		t.Fatalf("pending = %d, want 3", got)
	}
	if sink.Count(EventReplay, ReasonWriteQueued) != 3 {
		t.Fatalf("queued events = %d, want 3", sink.Count(EventReplay, ReasonWriteQueued))
	}

	fake.setWriteErr(nil)
	n, err := c.ReplayWrites(ctx)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if n != 3 {
		t.Fatalf("replayed = %d, want 3", n)
	}
	if got := fake.appliedWrites(); !reflect.DeepEqual(got, []string{"w1", "w2", "w3"}) {
		t.Fatalf("applied order = %v, want [w1 w2 w3]", got)
	}
	if got := c.PendingWrites(); got != 0 {
		t.Fatalf("pending after replay = %d, want 0", got)
	}
	if sink.Count(EventReplay, ReasonWriteReplayed) != 3 {
		t.Fatalf("replayed events = %d, want 3", sink.Count(EventReplay, ReasonWriteReplayed))
	}
}

// 重放中途失败时保留已有进度，不跳号、不重复；恢复后从断点继续。
func TestReplayInterruptedKeepsProgress(t *testing.T) {
	fake := newFakeBackend("b1", time.Hour)
	c := writeClient(fake, NewMemorySink())
	ctx := context.Background()

	fake.setWriteErr(errors.New("down"))
	for _, id := range []string{"w1", "w2", "w3"} {
		_ = c.SubmitWrite(ctx, op(id))
	}

	// 恢复但 w2 会失败：w1 应用后中断。
	fake.setWriteErr(nil)
	fake.writeErrOn["w2"] = true
	n, err := c.ReplayWrites(ctx)
	if !IsCode(err, ReasonReplayInterrupted) {
		t.Fatalf("replay err = %v, want code %s", err, ReasonReplayInterrupted)
	}
	if n != 1 {
		t.Fatalf("replayed = %d, want 1", n)
	}
	if got := c.PendingWrites(); got != 2 {
		t.Fatalf("pending = %d, want 2 (progress retained)", got)
	}
	pending := c.queue.Pending()
	if pending[0].ID != "w2" || pending[1].ID != "w3" {
		t.Fatalf("pending order = [%s %s], want [w2 w3]", pending[0].ID, pending[1].ID)
	}

	// 恢复后从断点继续，无跳号无重复。
	delete(fake.writeErrOn, "w2")
	n, err = c.ReplayWrites(ctx)
	if err != nil {
		t.Fatalf("second replay: %v", err)
	}
	if n != 2 {
		t.Fatalf("second replay n = %d, want 2", n)
	}
	if got := fake.appliedWrites(); !reflect.DeepEqual(got, []string{"w1", "w2", "w3"}) {
		t.Fatalf("applied = %v, want [w1 w2 w3]", got)
	}
	for _, id := range []string{"w1", "w2", "w3"} {
		if got := fake.writesOf(id); got != 1 {
			t.Fatalf("write %s applied %d times, want exactly 1", id, got)
		}
	}
}

// 重复提交不得产生重复副作用：排队期间去重，已应用后也去重。
func TestDuplicateSubmitDeduplicated(t *testing.T) {
	fake := newFakeBackend("b1", time.Hour)
	sink := NewMemorySink()
	c := writeClient(fake, sink)
	ctx := context.Background()

	// 后端不可用：相同幂等键重复提交只入队一次。
	fake.setWriteErr(errors.New("down"))
	_ = c.SubmitWrite(ctx, op("w1"))
	if err := c.SubmitWrite(ctx, op("w1")); !IsCode(err, ReasonWriteDeduplicated) {
		t.Fatalf("dup submit err = %v, want code %s", err, ReasonWriteDeduplicated)
	}
	if got := c.PendingWrites(); got != 1 {
		t.Fatalf("pending = %d, want 1 (deduplicated)", got)
	}

	// 恢复重放后，再次提交同键仍然去重。
	fake.setWriteErr(nil)
	if _, err := c.ReplayWrites(ctx); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if err := c.SubmitWrite(ctx, op("w1")); !IsCode(err, ReasonWriteDeduplicated) {
		t.Fatalf("post-replay dup err = %v, want code %s", err, ReasonWriteDeduplicated)
	}
	if got := fake.writesOf("w1"); got != 1 {
		t.Fatalf("w1 applied %d times, want exactly 1", got)
	}
	if sink.Count(EventReplay, ReasonWriteDeduplicated) == 0 {
		t.Fatal("missing dedup audit event")
	}
}

// 后端可用时直接写入成功，并登记幂等键防止后续重放重复执行。
func TestDirectWriteSuccess(t *testing.T) {
	fake := newFakeBackend("b1", time.Hour)
	c := writeClient(fake, NewMemorySink())
	ctx := context.Background()

	if err := c.SubmitWrite(ctx, op("w1")); err != nil {
		t.Fatalf("direct write: %v", err)
	}
	if got := fake.writesOf("w1"); got != 1 {
		t.Fatalf("w1 applied %d times, want 1", got)
	}
	if got := c.PendingWrites(); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
	if err := c.SubmitWrite(ctx, op("w1")); !IsCode(err, ReasonWriteDeduplicated) {
		t.Fatalf("dup after direct write err = %v, want code %s", err, ReasonWriteDeduplicated)
	}
}
