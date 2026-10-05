// Package facade 提供面向多个后端平台服务的统一客户端封装。
package facade

import (
	"context"
	"errors"
	"time"
)

// BackendID 标识一个后端平台。
type BackendID string

// OpKind 统一抽象的逻辑操作类型。
type OpKind string

const (
	OpAccountGet   OpKind = "account.get"
	OpAccountBan   OpKind = "account.ban"
	OpMatchCreate  OpKind = "match.create"
	OpMatchJoin    OpKind = "match.join"
	OpRoomCreate   OpKind = "room.create"
	OpRoomClose    OpKind = "room.close"
	OpLeaderSubmit OpKind = "leaderboard.submit"
	OpLeaderQuery  OpKind = "leaderboard.query"
)

// IsWrite 判断该操作是否为写操作（需要幂等与重放保障）。
func (k OpKind) IsWrite() bool {
	switch k {
	case OpAccountBan, OpMatchCreate, OpMatchJoin, OpRoomCreate, OpRoomClose, OpLeaderSubmit:
		return true
	}
	return false
}

// Request 是归一化后的统一请求。
type Request struct {
	Op             OpKind
	IdempotencyKey string            // 写操作必填，用于去重与重放
	Payload        map[string]string // 归一化入参
	Timeout        time.Duration     // 单次后端调用超时，0 表示用默认
}

// RawResponse 是后端返回的原始结果（字段命名因后端而异）。
type RawResponse struct {
	Fields map[string]string
}

// 统一错误：调用方可通过 errors.Is/As 区分。
var (
	// ErrCredentialExpired 表示凭据已过期且未能续期。
	ErrCredentialExpired = errors.New("facade: credential expired")
	// ErrNoHealthyBackend 表示所有后端均被熔断。
	ErrNoHealthyBackend = errors.New("facade: no healthy backend")
	// ErrRateLimited 表示请求被对应后端的限流策略拒绝。
	ErrRateLimited = errors.New("facade: rate limited")
	// ErrDuplicate 表示幂等键重复的写请求。
	ErrDuplicate = errors.New("facade: duplicate idempotency key")
	// ErrQueued 表示写请求已入本地队列等待重放（后端暂不可用）。
	ErrQueued = errors.New("facade: write queued for replay")
)

// RenewalError 包装续期失败，便于调用方区分续期错误与普通调用错误。
type RenewalError struct {
	Backend BackendID
	Cause   error
}

func (e *RenewalError) Error() string {
	return "facade: credential renewal failed on " + string(e.Backend) + ": " + e.Cause.Error()
}
func (e *RenewalError) Unwrap() error { return e.Cause }

// now 便于测试替换时钟。
var now = time.Now

// ctxDeadline 辅助：根据请求超时派生 context。
func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		d = 5 * time.Second
	}
	return context.WithTimeout(ctx, d)
}
