package facade

import (
	"context"
	"time"
)

// Backend 是一个后端平台服务的适配接口。
// 各后端的语义差异（凭据形式、字段命名、限流策略）由实现方吸收。
type Backend interface {
	ID() BackendID
	// Call 携带凭据执行一次调用；返回原始响应。
	Call(ctx context.Context, cred Credential, req Request) (RawResponse, error)
	// CredentialSource 提供该后端的凭据续期能力。
	CredentialSource() CredentialSource
	// Normalizer 返回该后端的字段归一化器。
	Normalizer() *Normalizer
}

// BackendOptions 是单个后端的运行策略配置。
type BackendOptions struct {
	Health      HealthPolicy  // 熔断与回切策略
	Rate        RatePolicy    // 限流与配额策略
	RenewMargin time.Duration // 凭据过期安全余量（提前续期）
}

// backendSlot 聚合一个后端及其全部周边组件。
type backendSlot struct {
	backend Backend
	renewer *CredentialRenewer
	health  *HealthTracker
	limiter *RateLimiter
}
