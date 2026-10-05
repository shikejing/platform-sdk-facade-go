package facade

import (
	"context"
	"errors"
	"testing"
)

func wr(key string) Request {
	return Request{Op: OpMatchCreate, IdempotencyKey: key, Payload: map[string]string{"account_id": "u1"}}
}

func TestEnqueueDedup(t *testing.T) {
	q := NewWriteQueue(nil)
	if err := q.Enqueue("b1", wr("k1")); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue("b1", wr("k1")); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate enqueue should fail with ErrDuplicate, got %v", err)
	}
	if q.Len() != 1 {
		t.Fatalf("queue len = %d, want 1", q.Len())
	}
}

func TestReplayInOrder(t *testing.T) {
	q := NewWriteQueue(nil)
	for _, k := range []string{"k1", "k2", "k3"} {
		if err := q.Enqueue("b1", wr(k)); err != nil {
			t.Fatal(err)
		}
	}
	var order []string
	n, err := q.Replay(context.Background(), func(ctx context.Context, b BackendID, r Request) error {
		order = append(order, r.IdempotencyKey)
		return nil
	})
	if err != nil || n != 3 {
		t.Fatalf("replay n=%d err=%v", n, err)
	}
	want := []string{"k1", "k2", "k3"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("replay order = %v, want %v", order, want)
		}
	}
	if q.Len() != 0 {
		t.Fatal("queue should be drained")
	}
}

func TestReplayInterruptionPreservesProgress(t *testing.T) {
	q := NewWriteQueue(nil)
	for _, k := range []string{"k1", "k2", "k3"} {
		if err := q.Enqueue("b1", wr(k)); err != nil {
			t.Fatal(err)
		}
	}
	failOn := map[string]bool{"k2": true}
	var applied []string
	n, err := q.Replay(context.Background(), func(ctx context.Context, b BackendID, r Request) error {
		if failOn[r.IdempotencyKey] {
			return errors.New("backend still down")
		}
		applied = append(applied, r.IdempotencyKey)
		return nil
	})
	if err == nil || n != 1 {
		t.Fatalf("expected stop after 1 replay, n=%d err=%v", n, err)
	}
	if q.Len() != 2 {
		t.Fatalf("progress lost: queue len = %d, want 2", q.Len())
	}

	// 恢复后继续：不得跳号（k2 先）不得重复（k1 不再出现）。
	n, err = q.Replay(context.Background(), func(ctx context.Context, b BackendID, r Request) error {
		applied = append(applied, r.IdempotencyKey)
		return nil
	})
	if err != nil || n != 2 {
		t.Fatalf("resume failed: n=%d err=%v", n, err)
	}
	want := []string{"k1", "k2", "k3"}
	if len(applied) != 3 {
		t.Fatalf("applied = %v, want %v", applied, want)
	}
	for i := range want {
		if applied[i] != want[i] {
			t.Fatalf("applied = %v, want %v", applied, want)
		}
	}
}

func TestAppliedKeyStaysDeduplicated(t *testing.T) {
	q := NewWriteQueue(nil)
	if err := q.Enqueue("b1", wr("k1")); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Replay(context.Background(), func(context.Context, BackendID, Request) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if !q.IsApplied("k1") {
		t.Fatal("k1 should be marked applied")
	}
	// 重放成功后相同幂等键再次提交仍被拒绝，不产生重复副作用。
	if err := q.Enqueue("b1", wr("k1")); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("re-submit of applied key should be ErrDuplicate, got %v", err)
	}
}
