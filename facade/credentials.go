package facade

import (
	"context"
	"sync"
	"time"
)

// CredentialManager 管理单个后端的凭据：并发下同一凭据只续期一次，
// 等待中的请求共享续期结果，绝不返回已过期的凭据。
type CredentialManager struct {
	backend Backend
	margin  time.Duration
	auditor *Auditor
	now     func() time.Time

	mu       sync.Mutex
	cached   Credential
	inflight *renewCall
}

// renewCall 表示一次进行中的续期，等待者通过 done 共享结果。
type renewCall struct {
	done chan struct{}
	cred Credential
	err  error
}

func newCredentialManager(b Backend, margin time.Duration, a *Auditor) *CredentialManager {
	return &CredentialManager{backend: b, margin: margin, auditor: a, now: time.Now}
}

// Get 返回一个保证在 margin 余量内不过期的凭据。
// 续期失败返回可区分的 *Error（ReasonCredentialRenewFailed）。
func (m *CredentialManager) Get(ctx context.Context) (Credential, error) {
	m.mu.Lock()
	if m.cached.Valid(m.now(), m.margin) {
		cred := m.cached
		m.mu.Unlock()
		return cred, nil
	}
	if m.inflight != nil {
		// 已有续期在进行：等待并共享其结果。
		call := m.inflight
		m.mu.Unlock()
		select {
		case <-call.done:
			return call.cred, call.err
		case <-ctx.Done():
			return Credential{}, &Error{
				Code:    ReasonCredentialRenewFailed,
				Backend: string(m.backend.ID()),
				Op:      "credential_get",
				Err:     ctx.Err(),
			}
		}
	}
	// 自己成为续期发起者。
	call := &renewCall{done: make(chan struct{})}
	m.inflight = call
	m.mu.Unlock()

	cred, err := m.renew(ctx)

	m.mu.Lock()
	call.cred, call.err = cred, err
	m.inflight = nil
	if err == nil {
		m.cached = cred
	}
	m.mu.Unlock()
	close(call.done)
	return cred, err
}

// renew 执行一次真实续期并记录审计事件。
func (m *CredentialManager) renew(ctx context.Context) (Credential, error) {
	id := string(m.backend.ID())
	cred, err := m.backend.RenewCredential(ctx)
	if err != nil {
		m.auditor.Emit(ctx, EventRenewal, ReasonCredentialRenewFailed, id, "credential_renew",
			map[string]any{"error": err.Error()})
		return Credential{}, &Error{
			Code:    ReasonCredentialRenewFailed,
			Backend: id,
			Op:      "credential_renew",
			Err:     err,
		}
	}
	if !cred.Valid(m.now(), m.margin) {
		// 后端返回了已过期或即将过期的凭据，视为续期失败，绝不向下游发放。
		m.auditor.Emit(ctx, EventRenewal, ReasonCredentialExpired, id, "credential_renew",
			map[string]any{"expires_at": cred.ExpiresAt})
		return Credential{}, &Error{
			Code:    ReasonCredentialRenewFailed,
			Backend: id,
			Op:      "credential_renew",
			Err: &Error{
				Code:    ReasonCredentialExpired,
				Backend: id,
				Op:      "credential_renew",
			},
		}
	}
	m.auditor.Emit(ctx, EventRenewal, "", id, "credential_renew",
		map[string]any{"expires_at": cred.ExpiresAt})
	return cred, nil
}

// Invalidate 使缓存凭据失效（例如后端返回 401 时），下次 Get 将触发续期。
func (m *CredentialManager) Invalidate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cached = Credential{}
}
