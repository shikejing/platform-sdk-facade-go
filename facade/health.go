package facade

import (
	"sync"
	"time"
)

// HealthState 是后端的可观测健康状态。
type HealthState string

const (
	StateHealthy  HealthState = "healthy"
	StateOpen     HealthState = "open"      // 熔断：不再被选中
	StateHalfOpen HealthState = "half_open" // 试探恢复
)

// HealthPolicy 控制熔断与回切策略。
type HealthPolicy struct {
	FailureThreshold int           // 连续失败多少次后熔断
	OpenDuration     time.Duration // 熔断保持时长，之后进入半开
	SuccessThreshold int           // 半开状态下连续成功多少次后回切（防抖动横跳）
}

func (p HealthPolicy) withDefaults() HealthPolicy {
	if p.FailureThreshold <= 0 {
		p.FailureThreshold = 3
	}
	if p.OpenDuration <= 0 {
		p.OpenDuration = 10 * time.Second
	}
	if p.SuccessThreshold <= 0 {
		p.SuccessThreshold = 2
	}
	return p
}

// HealthTracker 跟踪单个后端的健康状态。
type HealthTracker struct {
	backend BackendID
	policy  HealthPolicy
	logger  Logger

	mu             sync.Mutex
	state          HealthState
	consecFail     int
	consecOK       int
	openedAt       time.Time
	lastFailReason string
}

func NewHealthTracker(backend BackendID, policy HealthPolicy, logger Logger) *HealthTracker {
	if logger == nil {
		logger = nopLogger{}
	}
	return &HealthTracker{backend: backend, policy: policy.withDefaults(), logger: logger, state: StateHealthy}
}

// State 返回当前健康状态（会根据时间自动从 open 过渡到 half_open）。
func (h *HealthTracker) State() HealthState {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.maybeHalfOpenLocked()
	return h.state
}

// Selectable 报告该后端当前是否可被选中处理请求。
func (h *HealthTracker) Selectable() bool {
	s := h.State()
	return s == StateHealthy || s == StateHalfOpen
}

func (h *HealthTracker) maybeHalfOpenLocked() {
	if h.state == StateOpen && now().Sub(h.openedAt) >= h.policy.OpenDuration {
		h.state = StateHalfOpen
		h.consecOK = 0
		h.logger.Log(Event{Time: now(), Reason: ReasonCircuitHalf, Backend: h.backend})
	}
}

// OnSuccess 记录一次成功调用。
func (h *HealthTracker) OnSuccess() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.consecFail = 0
	if h.state == StateHalfOpen {
		h.consecOK++
		if h.consecOK >= h.policy.SuccessThreshold {
			h.state = StateHealthy
			h.consecOK = 0
			h.logger.Log(Event{Time: now(), Reason: ReasonFailback, Backend: h.backend,
				Detail: "consecutive probe successes reached threshold"})
		}
	}
}

// OnFailure 记录一次失败调用（含超时），可能触发熔断。
func (h *HealthTracker) OnFailure(reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastFailReason = reason
	h.consecOK = 0
	h.consecFail++
	if h.state == StateHalfOpen {
		// 半开试探失败：立即重新熔断。
		h.openLocked("half-open probe failed: " + reason)
		return
	}
	if h.state == StateHealthy && h.consecFail >= h.policy.FailureThreshold {
		h.openLocked("consecutive failures: " + reason)
	}
}

func (h *HealthTracker) openLocked(why string) {
	h.state = StateOpen
	h.openedAt = now()
	h.consecFail = 0
	h.logger.Log(Event{Time: now(), Reason: ReasonCircuitOpen, Backend: h.backend, Detail: why})
}
