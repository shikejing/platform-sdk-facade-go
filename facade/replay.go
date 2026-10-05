package facade

import (
	"context"
	"sync"
)

// queuedWrite 是进入本地队列的一条写请求。
type queuedWrite struct {
	seq     uint64
	backend BackendID
	req     Request
}

// WriteQueue 是后端不可用时的本地写队列：
//   - 按 IdempotencyKey 去重，重复提交不产生重复副作用；
//   - 恢复后按入队顺序（seq）重放；
//   - 重放中途失败时停在该处，保留进度，不跳号不重复。
type WriteQueue struct {
	logger Logger

	mu      sync.Mutex
	nextSeq uint64
	pending []queuedWrite       // 按 seq 升序
	seen    map[string]struct{} // 未完成的幂等键
	applied map[string]struct{} // 已成功应用的幂等键（防重放期间重复提交）
}

func NewWriteQueue(logger Logger) *WriteQueue {
	if logger == nil {
		logger = nopLogger{}
	}
	return &WriteQueue{logger: logger, seen: map[string]struct{}{}, applied: map[string]struct{}{}}
}

// Enqueue 将写请求入队；幂等键重复时返回 ErrDuplicate，不产生重复副作用。
func (q *WriteQueue) Enqueue(backend BackendID, req Request) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.seen[req.IdempotencyKey]; ok {
		return ErrDuplicate
	}
	if _, ok := q.applied[req.IdempotencyKey]; ok {
		return ErrDuplicate
	}
	q.nextSeq++
	q.pending = append(q.pending, queuedWrite{seq: q.nextSeq, backend: backend, req: req})
	q.seen[req.IdempotencyKey] = struct{}{}
	q.logger.Log(Event{Time: now(), Reason: ReasonWriteQueued, Backend: backend, Op: req.Op, IdemKey: req.IdempotencyKey})
	return nil
}

// Len 返回待重放条数。
func (q *WriteQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// applyFunc 真正执行一条写请求（由 Client 注入，含后端调用与结果处理）。
type applyFunc func(ctx context.Context, backend BackendID, req Request) error

// Replay 按顺序重放队列中的写请求。
// 任何一条失败即停止（保留该条及其后所有条目），保证不跳号、不重复；
// 返回已成功重放的条数。
func (q *WriteQueue) Replay(ctx context.Context, apply applyFunc) (int, error) {
	done := 0
	for {
		q.mu.Lock()
		if len(q.pending) == 0 {
			q.mu.Unlock()
			return done, nil
		}
		head := q.pending[0]
		q.mu.Unlock()

		q.logger.Log(Event{Time: now(), Reason: ReasonReplayStart, Backend: head.backend, Op: head.req.Op, IdemKey: head.req.IdempotencyKey})
		err := apply(ctx, head.backend, head.req)
		if err != nil {
			q.logger.Log(Event{Time: now(), Reason: ReasonReplayFailed, Backend: head.backend, Op: head.req.Op, IdemKey: head.req.IdempotencyKey, Detail: err.Error()})
			return done, err
		}

		q.mu.Lock()
		// 只有队首仍是该条目时才弹出，保证进度一致。
		if len(q.pending) > 0 && q.pending[0].seq == head.seq {
			q.pending = q.pending[1:]
			delete(q.seen, head.req.IdempotencyKey)
			q.applied[head.req.IdempotencyKey] = struct{}{}
		}
		q.mu.Unlock()
		q.logger.Log(Event{Time: now(), Reason: ReasonReplayOK, Backend: head.backend, Op: head.req.Op, IdemKey: head.req.IdempotencyKey})
		done++
	}
}

// IsApplied 报告某幂等键是否已成功应用。
func (q *WriteQueue) IsApplied(key string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.applied[key]
	return ok
}
