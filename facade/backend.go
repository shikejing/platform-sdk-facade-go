package facade

import (
	"context"
	"time"
)

// BackendID 唯一标识一个后端平台。
type BackendID string

// Credential 是后端凭据，各后端形式与过期时间不同，统一抽象为 token + 过期时刻。
type Credential struct {
	Token     string
	ExpiresAt time.Time
}

// Valid 判断凭据在 now 时刻（预留 margin 安全余量）是否可用。
func (c Credential) Valid(now time.Time, margin time.Duration) bool {
	return c.Token != "" && now.Add(margin).Before(c.ExpiresAt)
}

// WriteKind 写操作类型。
type WriteKind string

const (
	WriteCreateRoom        WriteKind = "create_room"
	WriteSubmitMatchResult WriteKind = "submit_match_result"
	WriteUpdateAccount     WriteKind = "update_account"
)

// WriteOp 是一个可幂等重放的写操作。
type WriteOp struct {
	ID         string         // 幂等键，由调用方提供，重复提交不产生重复副作用
	Kind       WriteKind      // 写操作类型
	Payload    map[string]any // 操作参数
	Seq        uint64         // 队列内顺序号，由门面分配
	EnqueuedAt time.Time
}

// Backend 抽象单个后端平台的能力，各后端的语义差异在其实现内部吸收。
type Backend interface {
	ID() BackendID
	// RenewCredential 续期凭据，返回新凭据及其过期时间。
	RenewCredential(ctx context.Context) (Credential, error)
	// GetAccount 读取账户信息。
	GetAccount(ctx context.Context, cred Credential, accountID string) (AccountInfo, error)
	// CreateMatch 创建匹配。
	CreateMatch(ctx context.Context, cred Credential, req MatchRequest) (MatchTicket, error)
	// GetRoom 读取房间信息。
	GetRoom(ctx context.Context, cred Credential, roomID string) (RoomInfo, error)
	// GetLeaderboard 读取榜单。
	GetLeaderboard(ctx context.Context, cred Credential, game string, limit int) ([]LeaderboardEntry, error)
	// ApplyWrite 应用一个写操作，必须按 op.ID 幂等。
	ApplyWrite(ctx context.Context, cred Credential, op WriteOp) error
}
