package facade

import (
	"context"
	"fmt"
	"sync/atomic"
)

// Client 是面向多个后端的统一客户端。
type Client struct {
	slots    []*backendSlot // 有序：前者为主，后者为备
	byID     map[BackendID]*backendSlot
	queue    *WriteQueue
	logger   Logger
	traceSeq atomic.Uint64
}

// NewClient 按优先级顺序注册后端（第一个为主后端）。
func NewClient(logger Logger) *Client {
	if logger == nil {
		logger = nopLogger{}
	}
	return &Client{byID: map[BackendID]*backendSlot{}, queue: NewWriteQueue(logger), logger: logger}
}

// AddBackend 注册一个后端及其策略。
func (c *Client) AddBackend(b Backend, opts BackendOptions) {
	slot := &backendSlot{
		backend: b,
		renewer: NewCredentialRenewer(b.ID(), b.CredentialSource(), c.logger, opts.RenewMargin),
		health:  NewHealthTracker(b.ID(), opts.Health, c.logger),
		limiter: NewRateLimiter(b.ID(), opts.Rate, c.logger),
	}
	c.slots = append(c.slots, slot)
	c.byID[b.ID()] = slot
}

// Queue 暴露写队列（用于观察与触发重放）。
func (c *Client) Queue() *WriteQueue { return c.queue }

// Health 返回某后端当前健康状态，便于可观测。
func (c *Client) Health(id BackendID) HealthState {
	if s, ok := c.byID[id]; ok {
		return s.health.State()
	}
	return ""
}

func (c *Client) newTrace() string {
	return fmt.Sprintf("trace-%d", c.traceSeq.Add(1))
}

// pick 按优先级选出第一个未被熔断的后端。
func (c *Client) pick(trace string, op OpKind) *backendSlot {
	for i, s := range c.slots {
		if s.health.Selectable() {
			if i > 0 {
				c.logger.Log(Event{Time: now(), Reason: ReasonFailover, Backend: s.backend.ID(), Op: op,
					Detail: "primary circuit open, switched to backup", TraceID: trace})
			}
			return s
		}
	}
	return nil
}

// Do 执行一次统一调用：限流 -> 选后端 -> 取凭据 -> 调用 -> 归一化。
// 写操作在所有后端均不可用时进入本地队列等待重放。
func (c *Client) Do(ctx context.Context, req Request) (*NormalizedResult, error) {
	trace := c.newTrace()
	c.logger.Log(Event{Time: now(), Reason: ReasonRequestStart, Op: req.Op, IdemKey: req.IdempotencyKey, TraceID: trace})

	slot := c.pick(trace, req.Op)
	if slot == nil {
		if req.Op.IsWrite() {
			if qerr := c.queue.Enqueue(c.primaryID(), req); qerr != nil {
				return nil, qerr
			}
			return nil, ErrQueued
		}
		return nil, ErrNoHealthyBackend
	}

	res, err := c.callOn(ctx, slot, req, trace)
	if err != nil {
		// 主后端失败时尝试按序切换备用后端。
		for _, alt := range c.slots {
			if alt == slot || !alt.health.Selectable() {
				continue
			}
			c.logger.Log(Event{Time: now(), Reason: ReasonFailover, Backend: alt.backend.ID(), Op: req.Op,
				Detail: "call failed on " + string(slot.backend.ID()) + ": " + err.Error(), TraceID: trace})
			res, err = c.callOn(ctx, alt, req, trace)
			if err == nil {
				return res, nil
			}
		}
		if req.Op.IsWrite() {
			if qerr := c.queue.Enqueue(slot.backend.ID(), req); qerr != nil {
				return nil, qerr
			}
			return nil, ErrQueued
		}
		return nil, err
	}
	return res, nil
}

// callOn 在指定后端上执行一次完整调用。
func (c *Client) callOn(ctx context.Context, slot *backendSlot, req Request, trace string) (*NormalizedResult, error) {
	if err := slot.limiter.Acquire(ctx); err != nil {
		return nil, err
	}
	cred, err := slot.renewer.Get(ctx)
	if err != nil {
		slot.health.OnFailure("credential: " + err.Error())
		return nil, err
	}
	// 任何调用都不得持过期凭据发出。
	if cred.Expired(0) {
		slot.renewer.Invalidate()
		slot.health.OnFailure("credential expired before send")
		return nil, ErrCredentialExpired
	}

	cctx, cancel := withTimeout(ctx, req.Timeout)
	raw, err := slot.backend.Call(cctx, cred, req)
	cancel()
	if err != nil {
		slot.health.OnFailure(err.Error())
		c.logger.Log(Event{Time: now(), Reason: ReasonRequestFailure, Backend: slot.backend.ID(), Op: req.Op, Detail: err.Error(), TraceID: trace})
		return nil, err
	}
	slot.health.OnSuccess()
	c.logger.Log(Event{Time: now(), Reason: ReasonRequestSuccess, Backend: slot.backend.ID(), Op: req.Op, TraceID: trace})
	return slot.backend.Normalizer().Normalize(req.Op, raw), nil
}

func (c *Client) primaryID() BackendID {
	if len(c.slots) == 0 {
		return ""
	}
	return c.slots[0].backend.ID()
}

// ReplayQueue 重放本地队列中的写请求（后端恢复后调用）。
// 中途失败会保留进度，返回已重放条数与错误。
func (c *Client) ReplayQueue(ctx context.Context) (int, error) {
	return c.queue.Replay(ctx, func(ctx context.Context, backend BackendID, req Request) error {
		slot, ok := c.byID[backend]
		if !ok || !slot.health.Selectable() {
			// 原后端仍不可用则尝试其他健康后端。
			slot = c.pick(c.newTrace(), req.Op)
			if slot == nil {
				return ErrNoHealthyBackend
			}
		}
		_, err := c.callOn(ctx, slot, req, c.newTrace())
		return err
	})
}
