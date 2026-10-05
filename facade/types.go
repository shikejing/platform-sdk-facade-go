package facade

import "time"

// FieldStatus 标记归一化字段的来源状态。
type FieldStatus int

const (
	// FieldOK 字段由后端正常返回并成功映射。
	FieldOK FieldStatus = iota
	// FieldMissing 后端未返回该字段。
	FieldMissing
	// FieldUnmapped 后端返回了字段但无法映射到统一模型。
	FieldUnmapped
)

// Field 是归一化后的字段，缺失或无法映射时通过 Status 区分，绝不用默认值掩盖。
type Field[T any] struct {
	Value  T
	Status FieldStatus
}

// OK 构造一个正常字段。
func OK[T any](v T) Field[T] { return Field[T]{Value: v, Status: FieldOK} }

// Missing 构造一个缺失字段。
func Missing[T any]() Field[T] { return Field[T]{Status: FieldMissing} }

// Unmapped 构造一个无法映射的字段。
func Unmapped[T any]() Field[T] { return Field[T]{Status: FieldUnmapped} }

// AccountInfo 归一化账户信息。
type AccountInfo struct {
	AccountID   Field[string]
	DisplayName Field[string]
	Region      Field[string]
	Level       Field[int]
}

// MatchRequest 创建匹配的请求。
type MatchRequest struct {
	Game     string
	Region   string
	PartyIDs []string
}

// MatchTicket 归一化匹配票据。
type MatchTicket struct {
	TicketID Field[string]
	Status   Field[string]
	ETA      Field[time.Duration]
}

// RoomInfo 归一化房间信息。
type RoomInfo struct {
	RoomID   Field[string]
	Host     Field[string]
	Capacity Field[int]
	Members  Field[int]
}

// LeaderboardEntry 归一化榜单条目。
type LeaderboardEntry struct {
	Rank      Field[int]
	AccountID Field[string]
	Score     Field[int64]
}
