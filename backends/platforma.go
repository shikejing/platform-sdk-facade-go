package backends

import (
	"context"
	"fmt"
	"time"

	facade "github.com/shikejing/platform-sdk-facade-go/facade"
)

// PlatformA 是平台 A 的适配器。
// 平台 A 的语义：
//   - 凭据：renew 返回 {"token": "...", "expires_in": <秒>}；
//   - 账户字段：account_id / nickname / region / level（数值）；
//   - 匹配票据：ticket_id / status / eta_ms（毫秒）；
//   - 房间：room_id / host / capacity / members；
//   - 榜单：entries: [{rank, account_id, score}]。
type PlatformA struct {
	id facade.BackendID
	tr Transport
}

// NewPlatformA 构造平台 A 适配器。
func NewPlatformA(id string, tr Transport) *PlatformA {
	return &PlatformA{id: facade.BackendID(id), tr: tr}
}

// ID 实现 facade.Backend。
func (p *PlatformA) ID() facade.BackendID { return p.id }

// RenewCredential 实现 facade.Backend。
func (p *PlatformA) RenewCredential(ctx context.Context) (facade.Credential, error) {
	resp, err := p.tr(ctx, RawRequest{Op: "renew"})
	if err != nil {
		return facade.Credential{}, err
	}
	token, _ := resp["token"].(string)
	if token == "" {
		return facade.Credential{}, fmt.Errorf("platform-a: renew response missing token")
	}
	expiresIn, _ := resp["expires_in"].(float64)
	if expiresIn <= 0 {
		return facade.Credential{}, fmt.Errorf("platform-a: renew response missing expires_in")
	}
	return facade.Credential{
		Token:     token,
		ExpiresAt: time.Now().Add(time.Duration(expiresIn * float64(time.Second))),
	}, nil
}

// GetAccount 实现 facade.Backend。
func (p *PlatformA) GetAccount(ctx context.Context, cred facade.Credential, accountID string) (facade.AccountInfo, error) {
	resp, err := p.tr(ctx, RawRequest{Op: "get_account", Cred: cred, Params: map[string]any{"account_id": accountID}})
	if err != nil {
		return facade.AccountInfo{}, err
	}
	return facade.AccountInfo{
		AccountID:   str(resp, "account_id"),
		DisplayName: str(resp, "nickname"),
		Region:      str(resp, "region"),
		Level:       integer(resp, "level"),
	}, nil
}

// CreateMatch 实现 facade.Backend。
func (p *PlatformA) CreateMatch(ctx context.Context, cred facade.Credential, req facade.MatchRequest) (facade.MatchTicket, error) {
	resp, err := p.tr(ctx, RawRequest{Op: "create_match", Cred: cred, Params: map[string]any{
		"game": req.Game, "region": req.Region, "party_ids": req.PartyIDs,
	}})
	if err != nil {
		return facade.MatchTicket{}, err
	}
	return facade.MatchTicket{
		TicketID: str(resp, "ticket_id"),
		Status:   str(resp, "status"),
		ETA:      durationMs(resp, "eta_ms"),
	}, nil
}

// GetRoom 实现 facade.Backend。
func (p *PlatformA) GetRoom(ctx context.Context, cred facade.Credential, roomID string) (facade.RoomInfo, error) {
	resp, err := p.tr(ctx, RawRequest{Op: "get_room", Cred: cred, Params: map[string]any{"room_id": roomID}})
	if err != nil {
		return facade.RoomInfo{}, err
	}
	return facade.RoomInfo{
		RoomID:   str(resp, "room_id"),
		Host:     str(resp, "host"),
		Capacity: integer(resp, "capacity"),
		Members:  integer(resp, "members"),
	}, nil
}

// GetLeaderboard 实现 facade.Backend。
func (p *PlatformA) GetLeaderboard(ctx context.Context, cred facade.Credential, game string, limit int) ([]facade.LeaderboardEntry, error) {
	resp, err := p.tr(ctx, RawRequest{Op: "get_leaderboard", Cred: cred, Params: map[string]any{"game": game, "limit": limit}})
	if err != nil {
		return nil, err
	}
	items, err := list(resp, "entries")
	if err != nil {
		return nil, err
	}
	out := make([]facade.LeaderboardEntry, 0, len(items))
	for _, it := range items {
		out = append(out, facade.LeaderboardEntry{
			Rank:      integer(it, "rank"),
			AccountID: str(it, "account_id"),
			Score:     integer64(it, "score"),
		})
	}
	return out, nil
}

// ApplyWrite 实现 facade.Backend；幂等键随请求上送，由平台侧去重。
func (p *PlatformA) ApplyWrite(ctx context.Context, cred facade.Credential, op facade.WriteOp) error {
	params := map[string]any{"idempotency_key": op.ID}
	for k, v := range op.Payload {
		params[k] = v
	}
	_, err := p.tr(ctx, RawRequest{Op: string(op.Kind), Cred: cred, Params: params})
	return err
}
