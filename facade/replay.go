package facade

import (
	"context"
	"sync"
	"time"
)

// WriteQueue 是后端不可用时的本地写队列，按序保存、幂等去重、支持断点重放。
type WriteQueue struct {
	now func() time.Time

	mu      sync.Mutex
	pending []WriteOp
	applied map[string]struct{}
	queued  map[string]struct{}
	nextSeq uint64
}

func newWriteQueue() *WriteQueue {
	return &WriteQueue{
		now:     time.Now,
		applied: map[string]struct{}{},
		queued:  map[string]struct{}{},
	}
}

// Enqueue 将写操作入队并分配顺序号；幂等键已入队或已应用时去重，
// dup=true 表示本次未重复入队。
func (q *WriteQueue) Enqueue(op WriteOp) (seq uint64, dup bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.applied[op.ID]; ok {
		return 0, true
	}
	if _, ok := q.queued[op.ID]; ok {
		return 0, true
	}
	q.nextSeq++
	op.Seq = q.nextSeq
	op.EnqueuedAt = q.now()
	q.pending = append(q.pending, op)
	q.queued[op.ID] = struct{}{}
	return op.Seq, false
}

// Len 返回待重放数量。
func (q *WriteQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// Pending 返回待重放操作的快照（按入队顺序）。
func (q *WriteQueue) Pending() []WriteOp {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]WriteOp, len(q.pending))
	copy(out, q.pending)
	return out
}

// IsApplied 判断幂等键是否已应用。
func (q *WriteQueue) IsApplied(id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.applied[id]
	return ok
}

// MarkApplied 记录幂等键已应用（用于直接写入路径的登记，防止后续重放重复执行）。
func (q *WriteQueue) MarkApplied(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.applied[id] = struct{}{}
}

// complete 标记操作已应用并移出待重放队列。
func (q *WriteQueue) complete(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.applied[id] = struct{}{}
	delete(q.queued, id)
	for i, op := range q.pending {
		if op.ID == id {
			q.pending = append(q.pending[:i], q.pending[i+1:]...)
			return
		}
	}
}

// Replayer 按原顺序重放队列中的写操作。
type Replayer struct {
	queue   *WriteQueue
	auditor *Auditor
	mu      sync.Mutex // 串行化并发 Replay 调用
}

func newReplayer(q *WriteQueue, a *Auditor) *Replayer {
	return &Replayer{queue: q, auditor: a}
}

// Replay 按入队顺序逐个重放：
//   - 已应用的幂等键直接跳过（ReasonWriteDeduplicated），不产生重复副作用；
//   - apply 失败时立即停止并返回 ReasonReplayInterrupted，已完成的进度保留，
//     未完成的操作保持原顺序留在队列中，不跳号、不重复。
//
// 返回成功重放的数量。
func (r *Replayer) Replay(ctx context.Context, apply func(context.Context, WriteOp) error) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	replayed := 0
	for {
		pending := r.queue.Pending()
		if len(pending) == 0 {
			return replayed, nil
		}
		op := pending[0] // 严格按顺序取队首
		if r.queue.IsApplied(op.ID) {
			// 已通过其他路径应用：去重跳过。
			r.queue.complete(op.ID)
			r.auditor.Emit(ctx, EventReplay, ReasonWriteDeduplicated, "", string(op.Kind),
				map[string]any{"write_id": op.ID, "seq": op.Seq})
			continue
		}
		if err := apply(ctx, op); err != nil {
			r.auditor.Emit(ctx, EventReplay, ReasonReplayInterrupted, "", string(op.Kind),
				map[string]any{"write_id": op.ID, "seq": op.Seq, "replayed": replayed, "error": err.Error()})
			return replayed, &Error{
				Code: ReasonReplayInterrupted,
				Op:   string(op.Kind),
				Err:  err,
			}
		}
		r.queue.complete(op.ID)
		replayed++
		r.auditor.Emit(ctx, EventReplay, ReasonWriteReplayed, "", string(op.Kind),
			map[string]any{"write_id": op.ID, "seq": op.Seq})
	}
}
