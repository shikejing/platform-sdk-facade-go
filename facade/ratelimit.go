package facade

import (
	"context"
	"sync"
	"time"
)

// RateLimitConfig 单个后端的限流与配额配置。
type RateLimitConfig struct {
	// Rate 令牌桶速率（个/秒），0 表示不限速。
	Rate float64
	// Burst 令牌桶容量，0 表示不限速。
	Burst int
	// Quota 配额窗口内允许的最大请求数，0 表示不限。
	Quota int64
	// QuotaWindow 配额窗口长度。
	QuotaWindow time.Duration
	// Wait 为 true 时超限请求排队等待，为 false 时立即返回退避错误。
	Wait bool
}

// rateLimiter 是单个后端独享的限流器，后端之间完全隔离：
// 某后端被限流只影响它自身的 Acquire，不与其他后端共享任何状态。
type rateLimiter struct {
	cfg RateLimitConfig
	now func() time.Time

	mu          sync.Mutex
	tokens      float64
	lastRefill  time.Time
	windowStart time.Time
	windowUsed  int64
}

func newRateLimiter(cfg RateLimitConfig) *rateLimiter {
	now := time.Now()
	return &rateLimiter{
		cfg:         cfg,
		now:         time.Now,
		tokens:      float64(cfg.Burst),
		lastRefill:  now,
		windowStart: now,
	}
}

// Acquire 获取一次调用许可；超限按策略排队（Wait=true）或返回带
// RetryAfter 的 *Error（ReasonRateLimited / ReasonQuotaExceeded）。
func (l *rateLimiter) Acquire(ctx context.Context) error {
	for {
		l.mu.Lock()
		wait, err := l.tryAcquireLocked()
		l.mu.Unlock()
		if err == nil {
			return nil
		}
		if !l.cfg.Wait {
			return err
		}
		// 排队策略：睡到预计可用时刻再重试，可被 ctx 取消。
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &Error{Code: err.Code, Op: "rate_limit_acquire", Err: ctx.Err()}
		case <-timer.C:
		}
	}
}

// tryAcquireLocked 尝试立即获取许可；失败时返回预计等待时长与错误。
func (l *rateLimiter) tryAcquireLocked() (time.Duration, *Error) {
	now := l.now()

	// 配额窗口滚动。
	if l.cfg.Quota > 0 && l.cfg.QuotaWindow > 0 && now.Sub(l.windowStart) >= l.cfg.QuotaWindow {
		l.windowStart = now
		l.windowUsed = 0
	}

	// 令牌桶补充。
	if l.cfg.Rate > 0 && l.cfg.Burst > 0 {
		elapsed := now.Sub(l.lastRefill).Seconds()
		l.tokens += elapsed * l.cfg.Rate
		if max := float64(l.cfg.Burst); l.tokens > max {
			l.tokens = max
		}
		l.lastRefill = now
	}

	// 配额检查。
	if l.cfg.Quota > 0 && l.windowUsed >= l.cfg.Quota {
		wait := l.cfg.QuotaWindow - now.Sub(l.windowStart)
		if wait < 0 {
			wait = 0
		}
		return wait, &Error{Code: ReasonQuotaExceeded, Op: "rate_limit_acquire", RetryAfter: wait}
	}

	// 限速检查。
	if l.cfg.Rate > 0 && l.cfg.Burst > 0 && l.tokens < 1 {
		wait := time.Duration((1 - l.tokens) / l.cfg.Rate * float64(time.Second))
		return wait, &Error{Code: ReasonRateLimited, Op: "rate_limit_acquire", RetryAfter: wait}
	}

	if l.cfg.Rate > 0 && l.cfg.Burst > 0 {
		l.tokens--
	}
	l.windowUsed++
	return 0, nil
}
