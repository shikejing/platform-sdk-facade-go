package facade

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Credential 是某后端的访问凭据，形式因后端而异（token/签名等）。
type Credential struct {
	Value     string
	ExpiresAt time.Time
}

// Expired 判断凭据是否已过期（含安全余量）。
func (c Credential) Expired(margin time.Duration) bool {
	return !now().Add(margin).Before(c.ExpiresAt)
}

// CredentialSource 由后端适配器实现：真实执行一次续期。
type CredentialSource interface {
	Renew(ctx context.Context) (Credential, error)
}

// CredentialRenewer 保证同一后端的并发续期只真正发生一次（single-flight），
// 等待中的调用共享同一次续期结果。
type CredentialRenewer struct {
	backend BackendID
	source  CredentialSource
	logger  Logger
	margin  time.Duration // 过期安全余量

	mu       sync.Mutex
	cred     Credential
	has      bool
	inflight *renewCall
}

type renewCall struct {
	done chan struct{}
	cred Credential
	err  error
}

func NewCredentialRenewer(backend BackendID, src CredentialSource, logger Logger, margin time.Duration) *CredentialRenewer {
	if logger == nil {
		logger = nopLogger{}
	}
	return &CredentialRenewer{backend: backend, source: src, logger: logger, margin: margin}
}

// Get 返回一个保证未过期的凭据；必要时触发或等待续期。
// 续期失败返回 *RenewalError，调用方可明确区分。
func (r *CredentialRenewer) Get(ctx context.Context) (Credential, error) {
	r.mu.Lock()
	if r.has && !r.cred.Expired(r.margin) {
		c := r.cred
		r.mu.Unlock()
		return c, nil
	}
	if r.inflight != nil {
		// 已有续期在进行：等待并共享结果。
		call := r.inflight
		r.mu.Unlock()
		select {
		case <-call.done:
			if call.err != nil {
				r.logger.Log(Event{Time: now(), Reason: ReasonRenewShared, Backend: r.backend, Detail: "shared failed renewal: " + call.err.Error()})
				return Credential{}, call.err
			}
			r.logger.Log(Event{Time: now(), Reason: ReasonRenewShared, Backend: r.backend})
			return call.cred, nil
		case <-ctx.Done():
			return Credential{}, ctx.Err()
		}
	}
	call := &renewCall{done: make(chan struct{})}
	r.inflight = call
	r.mu.Unlock()

	r.logger.Log(Event{Time: now(), Reason: ReasonRenewStart, Backend: r.backend})
	cred, err := r.source.Renew(ctx)
	if err == nil && cred.Expired(r.margin) {
		// 续期结果在安全余量内已过期：视同续期失败，绝不向外发放过期凭据。
		err = errors.New("renewed credential already expired within safety margin")
	}

	r.mu.Lock()
	if err != nil {
		call.err = &RenewalError{Backend: r.backend, Cause: err}
		r.logger.Log(Event{Time: now(), Reason: ReasonRenewFailed, Backend: r.backend, Detail: err.Error()})
	} else {
		call.cred = cred
		r.cred, r.has = cred, true
		r.logger.Log(Event{Time: now(), Reason: ReasonRenewOK, Backend: r.backend})
	}
	r.inflight = nil
	close(call.done)
	r.mu.Unlock()

	if call.err != nil {
		return Credential{}, call.err
	}
	return call.cred, nil
}

// Invalidate 强制下次 Get 触发续期（例如后端返回 401 时）。
func (r *CredentialRenewer) Invalidate() {
	r.mu.Lock()
	r.has = false
	r.mu.Unlock()
}
