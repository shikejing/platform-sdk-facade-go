package facade

import (
	"errors"
	"fmt"
	"time"
)

// ReasonCode 是稳定的原因码，用于错误分类与审计事件。
type ReasonCode string

const (
	ReasonCredentialRenewFailed ReasonCode = "credential_renew_failed"
	ReasonCredentialExpired     ReasonCode = "credential_expired"
	ReasonBackendTimeout        ReasonCode = "backend_timeout"
	ReasonBackendUnavailable    ReasonCode = "backend_unavailable"
	ReasonCircuitOpen           ReasonCode = "circuit_open"
	ReasonFailover              ReasonCode = "failover"
	ReasonFailback              ReasonCode = "failback"
	ReasonRateLimited           ReasonCode = "rate_limited"
	ReasonQuotaExceeded         ReasonCode = "quota_exceeded"
	ReasonWriteQueued           ReasonCode = "write_queued"
	ReasonWriteReplayed         ReasonCode = "write_replayed"
	ReasonWriteDeduplicated     ReasonCode = "write_deduplicated"
	ReasonReplayInterrupted     ReasonCode = "replay_interrupted"
	ReasonFieldMissing          ReasonCode = "field_missing"
	ReasonFieldUnmapped         ReasonCode = "field_unmapped"
	ReasonAllBackendsDown       ReasonCode = "all_backends_down"
)

// Error 是门面层对外暴露的统一错误，携带原因码以便调用方区分。
type Error struct {
	Code       ReasonCode
	Backend    string
	Op         string
	RetryAfter time.Duration
	Err        error
}

func (e *Error) Error() string {
	s := fmt.Sprintf("facade: op=%s backend=%s code=%s", e.Op, e.Backend, e.Code)
	if e.RetryAfter > 0 {
		s += fmt.Sprintf(" retry_after=%s", e.RetryAfter)
	}
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

func (e *Error) Unwrap() error { return e.Err }

// Is 支持 errors.Is(err, &Error{Code: ...}) 的按码匹配。
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return t.Code == "" || t.Code == e.Code
}

// IsCode 判断错误链中是否包含指定原因码（逐层解开 *Error 检查）。
func IsCode(err error, code ReasonCode) bool {
	for err != nil {
		if e, ok := err.(*Error); ok && e.Code == code {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}
