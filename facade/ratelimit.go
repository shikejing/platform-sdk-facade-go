package facade

import (
	"context"
	"sync"
	"time"
)

// RatePolicy 描述单个后端的限流与配额策略。
type RatePolicy struct {
	RatePerSec float64       // 令牌桶速率
	Burst      int           // 桶容量
	OverLimit  OverLimitMode // 超限时的行为
	QueueWait  time.Duration // OverLimitQueue 模式下最长排队等待
}

type OverLimitMode string

const (
	OverLimitBackoff OverLimitMode = "backoff" // 立即拒绝，调用方退避重试
	OverLimitQueue   OverLimitMode = "queue"   // 排队等待令牌
)

func (p RatePolicy) withDefaults() RatePolicy {
	if p.RatePerSec <= 0 {
		p.RatePerSec = 50
	}
	if p.Burst <= 0 {
		p.Burst = 10
	}
	if p.OverLimit == "" {
		p.OverLimit = OverLimitBackoff
	}
	if p.QueueWait <= 0 {
		p.QueueWait = 2 * time.Second
	}
	return p
}

// RateLimiter 是单个后端专属的令牌桶限流器，天然与其他后端隔离。
type RateLimiter struct {
	backend BackendID
	policy  RatePolicy
	logger  Logger

	mu       sync.Mutex
	tokens   float64
	lastFill time.Time
}

func NewRateLimiter(backend BackendID, policy RatePolicy, logger Logger) *RateLimiter {
	if logger == nil {
		logger = nopLogger{}
	}
	p := policy.withDefaults()
	return &RateLimiter{backend: backend, policy: p, logger: logger,
		tokens: float64(p.Burst), lastFill: now()}
}

// Acquire 获取一个令牌；按该后端自身策略退避或排队，不影响其他后端。
func (r *RateLimiter) Acquire(ctx context.Context) error {
	r.mu.Lock()
	r.refillLocked()
	if r.tokens >= 1 {
		r.tokens--
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	switch r.policy.OverLimit {
	case OverLimitQueue:
		r.logger.Log(Event{Time: now(), Reason: ReasonRateQueued, Backend: r.backend})
		deadline := now().Add(r.policy.QueueWait)
		for now().Before(deadline) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Millisecond):
			}
			r.mu.Lock()
			r.refillLocked()
			if r.tokens >= 1 {
				r.tokens--
				r.mu.Unlock()
				return nil
			}
			r.mu.Unlock()
		}
	}
	r.logger.Log(Event{Time: now(), Reason: ReasonRateLimited, Backend: r.backend})
	return ErrRateLimited
}

func (r *RateLimiter) refillLocked() {
	elapsed := now().Sub(r.lastFill).Seconds()
	r.tokens += elapsed * r.policy.RatePerSec
	if max := float64(r.policy.Burst); r.tokens > max {
		r.tokens = max
	}
	r.lastFill = now()
}
