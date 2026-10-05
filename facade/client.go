package facade

import (
	"context"
	"errors"
	"time"
)

// BackendConfig 单个后端的接入配置。
type BackendConfig struct {
	Backend Backend
	// CredMargin 凭据过期前的安全余量，避免拿着即将过期的凭据发请求。
	CredMargin time.Duration
	Health     HealthConfig
	RateLimit  RateLimitConfig
}

// Config 门面配置；Backends 按优先级排序，靠前的优先被选中。
type Config struct {
	Backends []BackendConfig
	// RequestTimeout 单次后端调用超时，默认 5s。
	RequestTimeout time.Duration
	// Sink 审计事件出口，可为 nil。
	Sink Sink
}

// Client 是面向多个后端平台服务的统一客户端门面。
type Client struct {
	slots    []*backendSlot
	queue    *WriteQueue
	replayer *Replayer
	auditor  *Auditor
	timeout  time.Duration
}

// backendSlot 组合一个后端及其凭据、健康、限流组件。
type backendSlot struct {
	backend Backend
	creds   *CredentialManager
	health  *healthTracker
	limiter *rateLimiter
}

// NewClient 构造门面客户端。
func NewClient(cfg Config) *Client {
	auditor := newAuditor(cfg.Sink)
	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	c := &Client{
		auditor: auditor,
		timeout: timeout,
		queue:   newWriteQueue(),
	}
	for _, bc := range cfg.Backends {
		c.slots = append(c.slots, &backendSlot{
			backend: bc.Backend,
			creds:   newCredentialManager(bc.Backend, bc.CredMargin, auditor),
			health:  newHealthTracker(bc.Health, bc.Backend.ID(), auditor),
			limiter: newRateLimiter(bc.RateLimit),
		})
	}
	c.replayer = newReplayer(c.queue, auditor)
	return c
}

// GetAccount 读取账户信息（归一化结果）。
func (c *Client) GetAccount(ctx context.Context, accountID string) (AccountInfo, error) {
	return doRead(ctx, c, "get_account", func(ctx context.Context, b Backend, cred Credential) (AccountInfo, error) {
		return b.GetAccount(ctx, cred, accountID)
	})
}

// CreateMatch 创建匹配。
func (c *Client) CreateMatch(ctx context.Context, req MatchRequest) (MatchTicket, error) {
	return doRead(ctx, c, "create_match", func(ctx context.Context, b Backend, cred Credential) (MatchTicket, error) {
		return b.CreateMatch(ctx, cred, req)
	})
}

// GetRoom 读取房间信息。
func (c *Client) GetRoom(ctx context.Context, roomID string) (RoomInfo, error) {
	return doRead(ctx, c, "get_room", func(ctx context.Context, b Backend, cred Credential) (RoomInfo, error) {
		return b.GetRoom(ctx, cred, roomID)
	})
}

// GetLeaderboard 读取榜单。
func (c *Client) GetLeaderboard(ctx context.Context, game string, limit int) ([]LeaderboardEntry, error) {
	return doRead(ctx, c, "get_leaderboard", func(ctx context.Context, b Backend, cred Credential) ([]LeaderboardEntry, error) {
		return b.GetLeaderboard(ctx, cred, game, limit)
	})
}

// doRead 在健康后端上执行一次读操作：按优先级选路，超时或失败按策略切换，
// 被熔断的后端不会被选中；限流只跳过该后端自身，不影响其他后端。
func doRead[T any](ctx context.Context, c *Client, op string, call func(context.Context, Backend, Credential) (T, error)) (T, error) {
	var zero T
	var lastErr error
	tried := 0
	for i, slot := range c.slots {
		id := string(slot.backend.ID())
		if !slot.health.Allow(ctx) {
			continue // 已熔断，不得选中
		}
		tried++
		if err := slot.limiter.Acquire(ctx); err != nil {
			// 该后端被限流：只影响它自身，跳过并尝试下一个后端。
			c.auditor.Emit(ctx, EventRateLimit, errCode(err), id, op,
				map[string]any{"retry_after": retryAfterOf(err).String()})
			lastErr = err
			continue
		}
		cred, err := slot.creds.Get(ctx)
		if err != nil {
			slot.health.OnFailure(ctx, ReasonCredentialRenewFailed)
			lastErr = err
			c.emitFailover(ctx, op, i, ReasonCredentialRenewFailed)
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, c.timeout)
		res, err := call(callCtx, slot.backend, cred)
		cancel()
		if err == nil {
			slot.health.OnSuccess(ctx)
			return res, nil
		}
		reason := classifyErr(callCtx, err)
		slot.health.OnFailure(ctx, reason)
		lastErr = &Error{Code: reason, Backend: id, Op: op, Err: err}
		c.emitFailover(ctx, op, i, reason)
	}
	if tried == 0 {
		return zero, &Error{Code: ReasonAllBackendsDown, Op: op, Err: lastErr}
	}
	return zero, &Error{Code: ReasonAllBackendsDown, Op: op, Err: lastErr}
}

// SubmitWrite 提交幂等写操作。
// 返回值约定：
//   - nil：已直接写入成功；
//   - IsCode(err, ReasonWriteQueued)：后端暂不可用，已入本地队列，恢复后将按序重放；
//   - IsCode(err, ReasonWriteDeduplicated)：相同幂等键已应用，未产生重复副作用；
//   - 其他错误：提交失败。
func (c *Client) SubmitWrite(ctx context.Context, op WriteOp) error {
	if op.ID == "" {
		return &Error{Code: ReasonBackendUnavailable, Op: string(op.Kind), Err: errors.New("facade: write op requires an idempotency ID")}
	}
	if c.queue.IsApplied(op.ID) {
		c.auditor.Emit(ctx, EventReplay, ReasonWriteDeduplicated, "", string(op.Kind),
			map[string]any{"write_id": op.ID})
		return &Error{Code: ReasonWriteDeduplicated, Op: string(op.Kind)}
	}
	err := c.tryWrite(ctx, op)
	if err == nil {
		c.queue.MarkApplied(op.ID)
		return nil
	}
	// 后端不可用：进入本地队列等待重放，保持提交顺序。
	if _, dup := c.queue.Enqueue(op); dup {
		return &Error{Code: ReasonWriteDeduplicated, Op: string(op.Kind)}
	}
	c.auditor.Emit(ctx, EventReplay, ReasonWriteQueued, "", string(op.Kind),
		map[string]any{"write_id": op.ID, "cause": err.Error()})
	return &Error{Code: ReasonWriteQueued, Op: string(op.Kind), Err: err}
}

// tryWrite 在第一个可用后端上执行写操作。
func (c *Client) tryWrite(ctx context.Context, op WriteOp) error {
	var lastErr error
	for i, slot := range c.slots {
		id := string(slot.backend.ID())
		if !slot.health.Allow(ctx) {
			continue
		}
		if err := slot.limiter.Acquire(ctx); err != nil {
			c.auditor.Emit(ctx, EventRateLimit, errCode(err), id, string(op.Kind), nil)
			lastErr = err
			continue
		}
		cred, err := slot.creds.Get(ctx)
		if err != nil {
			slot.health.OnFailure(ctx, ReasonCredentialRenewFailed)
			lastErr = err
			c.emitFailover(ctx, string(op.Kind), i, ReasonCredentialRenewFailed)
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, c.timeout)
		err = slot.backend.ApplyWrite(callCtx, cred, op)
		cancel()
		if err == nil {
			slot.health.OnSuccess(ctx)
			return nil
		}
		reason := classifyErr(callCtx, err)
		slot.health.OnFailure(ctx, reason)
		lastErr = &Error{Code: reason, Backend: id, Op: string(op.Kind), Err: err}
		c.emitFailover(ctx, string(op.Kind), i, reason)
	}
	if lastErr == nil {
		lastErr = &Error{Code: ReasonAllBackendsDown, Op: string(op.Kind)}
	}
	return lastErr
}

// ReplayWrites 按原顺序重放本地队列中的写操作。
// 中途失败时保留进度并返回 ReasonReplayInterrupted，不跳号、不重复。
func (c *Client) ReplayWrites(ctx context.Context) (int, error) {
	return c.replayer.Replay(ctx, func(ctx context.Context, op WriteOp) error {
		return c.tryWrite(ctx, op)
	})
}

// PendingWrites 返回待重放写操作数量。
func (c *Client) PendingWrites() int { return c.queue.Len() }

// Health 返回各后端的健康状态快照。
func (c *Client) Health() map[BackendID]HealthState {
	out := make(map[BackendID]HealthState, len(c.slots))
	for _, s := range c.slots {
		out[s.backend.ID()] = s.health.State()
	}
	return out
}

// emitFailover 记录一次切换事件；若存在下一个候选后端，注明切换目标。
func (c *Client) emitFailover(ctx context.Context, op string, fromIdx int, reason ReasonCode) {
	from := string(c.slots[fromIdx].backend.ID())
	to := ""
	for j := fromIdx + 1; j < len(c.slots); j++ {
		if c.slots[j].health.Allow(ctx) {
			to = string(c.slots[j].backend.ID())
			break
		}
	}
	c.auditor.Emit(ctx, EventFailover, reason, from, op, map[string]any{"to": to})
}

// classifyErr 把调用错误归类为稳定原因码。
func classifyErr(callCtx context.Context, err error) ReasonCode {
	if errors.Is(callCtx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return ReasonBackendTimeout
	}
	var fe *Error
	if errors.As(err, &fe) && fe.Code != "" {
		return fe.Code
	}
	return ReasonBackendUnavailable
}

func errCode(err error) ReasonCode {
	var fe *Error
	if errors.As(err, &fe) {
		return fe.Code
	}
	return ReasonBackendUnavailable
}

func retryAfterOf(err error) time.Duration {
	var fe *Error
	if errors.As(err, &fe) {
		return fe.RetryAfter
	}
	return 0
}
