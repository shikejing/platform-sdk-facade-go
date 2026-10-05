// Package backends 提供两个语义不同的模拟后端适配器（alpha / beta），
// 用于演示与测试统一封装如何吸收后端差异。
package backends

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shikejing/platform-sdk-facade-go/facade"
)

// ErrBackendDown 表示后端故障（网络错误/超时等）。
var ErrBackendDown = errors.New("backend: unavailable")

// Server 是一个可注入故障的内存模拟平台服务。
type Server struct {
	ID facade.BackendID

	// 凭据形式与过期时间因后端而异。
	CredPrefix string
	CredTTL    time.Duration

	mu         sync.Mutex
	down       bool  // 置为不可用
	renewErr   error // 非空则续期失败
	renewCount int   // 真实发生的续期次数
	callCount  int
	applied    map[string]int // 服务端幂等表：幂等键 -> 应用次数
	calls      []facade.Request
}

func NewServer(id facade.BackendID, credPrefix string, credTTL time.Duration) *Server {
	return &Server{ID: id, CredPrefix: credPrefix, CredTTL: credTTL, applied: map[string]int{}}
}

// SetDown 切换后端故障状态。
func (s *Server) SetDown(down bool) { s.mu.Lock(); s.down = down; s.mu.Unlock() }

// SetRenewError 注入续期失败。
func (s *Server) SetRenewError(err error) { s.mu.Lock(); s.renewErr = err; s.mu.Unlock() }

// RenewCount 返回真实发生的续期次数（验证 single-flight）。
func (s *Server) RenewCount() int { s.mu.Lock(); defer s.mu.Unlock(); return s.renewCount }

// CallCount 返回真实到达后端的调用次数。
func (s *Server) CallCount() int { s.mu.Lock(); defer s.mu.Unlock(); return s.callCount }

// AppliedCount 返回某幂等键在服务端被应用的次数（验证去重）。
func (s *Server) AppliedCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied[key]
}

// Renew 实现 facade.CredentialSource。
func (s *Server) Renew(ctx context.Context) (facade.Credential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewCount++
	if s.renewErr != nil {
		return facade.Credential{}, s.renewErr
	}
	if s.down {
		return facade.Credential{}, ErrBackendDown
	}
	return facade.Credential{
		Value:     fmt.Sprintf("%s-%d", s.CredPrefix, s.renewCount),
		ExpiresAt: time.Now().Add(s.CredTTL),
	}, nil
}

// Handle 是服务端通用入口：校验凭据、应用幂等写、返回原始字段。
// rawFields 由具体后端适配器提供（字段命名因后端而异）。
func (s *Server) Handle(ctx context.Context, cred facade.Credential, req facade.Request, rawFields map[string]string) (facade.RawResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return facade.RawResponse{}, ErrBackendDown
	}
	if cred.Value == "" || time.Now().After(cred.ExpiresAt) {
		return facade.RawResponse{}, errors.New("backend: bad or expired credential")
	}
	s.callCount++
	s.calls = append(s.calls, req)
	if req.Op.IsWrite() {
		// 服务端幂等：同一幂等键只应用一次。
		if _, ok := s.applied[req.IdempotencyKey]; !ok {
			s.applied[req.IdempotencyKey] = 1
		} else {
			s.applied[req.IdempotencyKey]++
		}
	}
	return facade.RawResponse{Fields: rawFields}, nil
}

// credSource 适配：让 Server 直接作为 CredentialSource。
type credSource struct{ s *Server }

func (c credSource) Renew(ctx context.Context) (facade.Credential, error) { return c.s.Renew(ctx) }

// seq 用于生成各后端独立的返回值。
var seq atomic.Uint64

func nextID(prefix string) string { return fmt.Sprintf("%s-%d", prefix, seq.Add(1)) }
