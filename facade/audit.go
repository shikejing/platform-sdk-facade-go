package facade

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// EventType 审计事件类型。
type EventType string

const (
	EventRequest      EventType = "request"
	EventRenewal      EventType = "credential_renewal"
	EventCircuitOpen  EventType = "circuit_open"
	EventCircuitClose EventType = "circuit_close"
	EventFailover     EventType = "failover"
	EventFailback     EventType = "failback"
	EventReplay       EventType = "replay"
	EventRateLimit    EventType = "rate_limit"
	EventNormalize    EventType = "normalize"
)

// Event 是一条结构化审计事件，凭 TraceID 可还原一次请求的完整链路。
type Event struct {
	Time    time.Time
	Type    EventType
	Reason  ReasonCode
	Backend string
	Op      string
	TraceID string
	Detail  map[string]any
}

// Sink 接收审计事件。
type Sink interface {
	Emit(Event)
}

// Auditor 负责发送审计事件，nil sink 时安全降级为无操作。
type Auditor struct {
	sink Sink
	now  func() time.Time
}

func newAuditor(s Sink) *Auditor { return &Auditor{sink: s, now: time.Now} }

// Emit 记录一条事件。
func (a *Auditor) Emit(ctx context.Context, typ EventType, reason ReasonCode, backend, op string, detail map[string]any) {
	if a == nil || a.sink == nil {
		return
	}
	a.sink.Emit(Event{
		Time:    a.now(),
		Type:    typ,
		Reason:  reason,
		Backend: backend,
		Op:      op,
		TraceID: traceIDFrom(ctx),
		Detail:  detail,
	})
}

// SlogSink 把审计事件写入 slog 结构化日志。
type SlogSink struct {
	l *slog.Logger
}

// NewSlogSink 构造 slog 出口；l 为 nil 时使用 slog.Default()。
func NewSlogSink(l *slog.Logger) *SlogSink {
	if l == nil {
		l = slog.Default()
	}
	return &SlogSink{l: l}
}

// Emit 实现 Sink。
func (s *SlogSink) Emit(e Event) {
	attrs := []any{
		"event_type", string(e.Type),
		"reason", string(e.Reason),
		"backend", e.Backend,
		"op", e.Op,
		"trace_id", e.TraceID,
	}
	for k, v := range e.Detail {
		attrs = append(attrs, k, v)
	}
	s.l.LogAttrs(context.Background(), slog.LevelInfo, "facade.audit",
		attrsToAttrs(attrs)...)
}

func attrsToAttrs(kv []any) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		key, _ := kv[i].(string)
		attrs = append(attrs, slog.Any(key, kv[i+1]))
	}
	return attrs
}

// MemorySink 把事件保存在内存中，主要用于测试与调试。
type MemorySink struct {
	mu     sync.Mutex
	events []Event
}

// NewMemorySink 构造内存出口。
func NewMemorySink() *MemorySink { return &MemorySink{} }

// Emit 实现 Sink。
func (m *MemorySink) Emit(e Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
}

// Events 返回已记录事件的快照。
func (m *MemorySink) Events() []Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Event, len(m.events))
	copy(out, m.events)
	return out
}

// Count 统计指定类型与原因码的事件数；reason 为空串表示不过滤。
func (m *MemorySink) Count(typ EventType, reason ReasonCode) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, e := range m.events {
		if e.Type != typ {
			continue
		}
		if reason != "" && e.Reason != reason {
			continue
		}
		n++
	}
	return n
}

// traceKey 是 context 中 trace id 的键。
type traceKey struct{}

// WithTraceID 把 trace id 放入 context，用于串联一次请求的完整链路。
func WithTraceID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceKey{}, id)
}

func traceIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(traceKey{}).(string); ok {
		return v
	}
	return ""
}
