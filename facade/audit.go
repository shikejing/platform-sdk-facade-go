package facade

import (
	"sync"
	"time"
)

// ReasonCode 是稳定的机器可读原因码，用于还原一次请求的完整链路。
type ReasonCode string

const (
	ReasonRenewStart     ReasonCode = "RENEW_START"
	ReasonRenewOK        ReasonCode = "RENEW_OK"
	ReasonRenewFailed    ReasonCode = "RENEW_FAILED"
	ReasonRenewShared    ReasonCode = "RENEW_SHARED" // 等待中的请求共享了续期结果
	ReasonFailover       ReasonCode = "FAILOVER"
	ReasonFailback       ReasonCode = "FAILBACK"
	ReasonCircuitOpen    ReasonCode = "CIRCUIT_OPEN"
	ReasonCircuitHalf    ReasonCode = "CIRCUIT_HALF_OPEN"
	ReasonWriteQueued    ReasonCode = "WRITE_QUEUED"
	ReasonReplayStart    ReasonCode = "REPLAY_START"
	ReasonReplayOK       ReasonCode = "REPLAY_OK"
	ReasonReplayFailed   ReasonCode = "REPLAY_FAILED"
	ReasonReplayDedup    ReasonCode = "REPLAY_DEDUP"
	ReasonRateLimited    ReasonCode = "RATE_LIMITED"
	ReasonRateQueued     ReasonCode = "RATE_QUEUED"
	ReasonFieldMissing   ReasonCode = "FIELD_MISSING"
	ReasonFieldUnmapped  ReasonCode = "FIELD_UNMAPPED"
	ReasonRequestStart   ReasonCode = "REQUEST_START"
	ReasonRequestSuccess ReasonCode = "REQUEST_SUCCESS"
	ReasonRequestFailure ReasonCode = "REQUEST_FAILURE"
)

// Event 是一条结构化审计事件。
type Event struct {
	Time    time.Time
	Reason  ReasonCode
	Backend BackendID
	Op      OpKind
	IdemKey string
	Detail  string
	TraceID string // 串联一次请求的完整链路
}

// Logger 接收结构化审计事件。
type Logger interface {
	Log(Event)
}

// MemLogger 是线程安全的内存 Logger，主要用于测试与默认场景。
type MemLogger struct {
	mu     sync.Mutex
	events []Event
}

func NewMemLogger() *MemLogger { return &MemLogger{} }

func (m *MemLogger) Log(e Event) {
	m.mu.Lock()
	m.events = append(m.events, e)
	m.mu.Unlock()
}

// Events 返回已记录事件的副本。
func (m *MemLogger) Events() []Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Event, len(m.events))
	copy(out, m.events)
	return out
}

// ByReason 过滤出指定原因码的事件。
func (m *MemLogger) ByReason(r ReasonCode) []Event {
	var out []Event
	for _, e := range m.Events() {
		if e.Reason == r {
			out = append(out, e)
		}
	}
	return out
}

// nopLogger 丢弃所有事件。
type nopLogger struct{}

func (nopLogger) Log(Event) {}
