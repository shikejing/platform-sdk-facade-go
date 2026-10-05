package facade

import (
	"context"
	"sync"
	"time"
)

// HealthState 是后端的可观测健康状态。
type HealthState int

const (
	// StateHealthy 健康，可被选中。
	StateHealthy HealthState = iota
	// StateDegraded 出现失败但尚未熔断，仍可被选中。
	StateDegraded
	// StateCircuitOpen 已熔断，不得被选中。
	StateCircuitOpen
	// StateHalfOpen 冷却结束，允许探测流量验证恢复。
	StateHalfOpen
)

func (s HealthState) String() string {
	switch s {
	case StateHealthy:
		return "healthy"
	case StateDegraded:
		return "degraded"
	case StateCircuitOpen:
		return "circuit_open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// HealthConfig 熔断与回切策略。
type HealthConfig struct {
	// FailureThreshold 连续失败次数达到后熔断。
	FailureThreshold int
	// OpenCooldown 熔断后经过该时长进入半开，允许探测。
	OpenCooldown time.Duration
	// SuccessThreshold 半开状态下连续成功次数达到后回切为健康，
	// 用于避免健康度抖动时反复横跳。
	SuccessThreshold int
}

func (c HealthConfig) withDefaults() HealthConfig {
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = 3
	}
	if c.OpenCooldown <= 0 {
		c.OpenCooldown = 30 * time.Second
	}
	if c.SuccessThreshold <= 0 {
		c.SuccessThreshold = 2
	}
	return c
}

// healthTracker 跟踪单个后端的健康状态。
type healthTracker struct {
	cfg     HealthConfig
	backend BackendID
	auditor *Auditor
	now     func() time.Time

	mu        sync.Mutex
	state     HealthState
	consecErr int
	consecOK  int
	openedAt  time.Time
}

func newHealthTracker(cfg HealthConfig, id BackendID, a *Auditor) *healthTracker {
	return &healthTracker{cfg: cfg.withDefaults(), backend: id, auditor: a, now: time.Now}
}

// Allow 报告该后端当前是否允许被选中（熔断中的后端返回 false）。
// 熔断冷却到期后自动转入半开并放行一次探测。
func (h *healthTracker) Allow(ctx context.Context) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch h.state {
	case StateCircuitOpen:
		if h.now().Sub(h.openedAt) >= h.cfg.OpenCooldown {
			h.state = StateHalfOpen
			h.consecOK = 0
			h.auditor.Emit(ctx, EventCircuitOpen, ReasonCircuitOpen,
				string(h.backend), "health_probe", map[string]any{"state": "half_open"})
			return true
		}
		return false
	default:
		return true
	}
}

// State 返回当前健康状态。
func (h *healthTracker) State() HealthState {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state
}

// OnSuccess 记录一次成功；半开状态下需连续成功 SuccessThreshold 次才回切，
// 避免健康度抖动时在熔断与健康之间反复横跳。
func (h *healthTracker) OnSuccess(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.consecErr = 0
	switch h.state {
	case StateHalfOpen:
		h.consecOK++
		if h.consecOK >= h.cfg.SuccessThreshold {
			h.state = StateHealthy
			h.consecOK = 0
			h.auditor.Emit(ctx, EventFailback, ReasonFailback,
				string(h.backend), "health_recover",
				map[string]any{"success_threshold": h.cfg.SuccessThreshold})
		}
	case StateDegraded:
		h.state = StateHealthy
		h.consecOK = 0
	default:
		h.consecOK = 0
	}
}

// OnFailure 记录一次失败；连续失败达到阈值即熔断，半开期间任何失败立即重新熔断。
func (h *healthTracker) OnFailure(ctx context.Context, reason ReasonCode) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.consecErr++
	h.consecOK = 0
	if h.state == StateHalfOpen {
		h.openLocked(ctx, reason, "half_open_probe_failed")
		return
	}
	if h.consecErr >= h.cfg.FailureThreshold {
		h.openLocked(ctx, reason, "failure_threshold")
		return
	}
	if h.state == StateHealthy {
		h.state = StateDegraded
	}
}

func (h *healthTracker) openLocked(ctx context.Context, reason ReasonCode, trigger string) {
	h.state = StateCircuitOpen
	h.openedAt = h.now()
	h.consecErr = 0
	h.auditor.Emit(ctx, EventCircuitOpen, reason,
		string(h.backend), "health_open",
		map[string]any{"trigger": trigger, "cooldown": h.cfg.OpenCooldown.String()})
}
