package backends

import (
	"context"
	"fmt"
	"time"

	facade "github.com/shikejing/platform-sdk-facade-go/facade"
)

// PlatformB 是平台 B 的适配器。
// 平台 B 的语义（与平台 A 存在差异，由本适配器吸收）：
//   - 凭据：renew 返回 {"session_key": "...", "expires_at": <unix 秒>}；
//   - 账户字段：uid / display_name / lvl（字符串数字）；不返回 region，
//     归一化结果中 Region 标记为 FieldMissing；
//   - 匹配票据：ticket / state / eta_seconds（秒）；
//   - 房间：roomId / owner / max / cur；
//   - 榜单：items: [{position, player, points}]。
type PlatformB struct {
	id facade.BackendID
	tr Transport
}

// NewPlatformB 构造平台 B 适配器。
func NewPlatformB(id string, tr Transport) *PlatformB {
	return &PlatformB{id: facade.BackendID(id), tr: tr}
}

// ID 实现 facade.Backend。
func (p *PlatformB) ID() facade.BackendID { return p.id }

// RenewCredential 实现 facade.Backend。
func (p *PlatformB) RenewCredential(ctx context.Context) (facade.Credential, error) {
	resp, err := p.tr(ctx, RawRequest{Op: "renew"})
	if err != nil {
		return facade.Credential{}, err
	}
	token, _ := resp["session_key"].(string)
	if token == "" {
		return facade.Credential{}, fmt.Errorf("platform-b: renew response missing session_key")
	}
	expiresAt, _ := resp["expires_at"].(float64)
	if expiresAt <= 0 {
		return facade.Credential{}, fmt.Errorf("platform-b: renew response missing expires_at")
	}
	return facade.Credential{
		Token:     token,
		ExpiresAt: time.Unix(int64(expiresAt), 0),
	}, nil
}

// GetAccount 实现 facade.Backend。
func (p *PlatformB) GetAccount(ctx context.Context, cred facade.Credential, accountID string) (facade.AccountInfo, error) {
	resp, err := p.tr(ctx, RawRequest{Op: "get_account", Cred: cred, Params: map[string]any{"uid": accountID}})
	if err != nil {
		return facade.AccountInfo{}, err
	}
	return facade.AccountInfo{
		AccountID:   str(resp, "uid"),
		DisplayName: str(resp, "display_name"),
		// 平台 B 不提供 region：显式标记缺失，绝不用空串冒充有效值。
		Region: facade.Missing[string](),
		Level:  integer(resp, "lvl"),
	}, nil
}

// CreateMatch 实现 facade.Backend。
func (p *PlatformB) CreateMatch(ctx context.Context, cred facade.Credential, req facade.MatchRequest) (facade.MatchTicket, error) {
	resp, err := p.tr(ctx, RawRequest{Op: "create_match", Cred: cred, Params: map[string]any{
		"game": req.Game, "region": req.Region, "party": req.PartyIDs,
	}})
	if err != nil {
		return facade.MatchTicket{}, err
	}
	return facade.MatchTicket{
		TicketID: str(resp, "ticket"),
		Status:   str(resp, "state"),
		ETA:      durationSec(resp, "eta_seconds"),
	}, nil
}

// GetRoom 实现 facade.Backend。
func (p *PlatformB) GetRoom(ctx context.Context, cred facade.Credential, roomID string) (facade.RoomInfo, error) {
	resp, err := p.tr(ctx, RawRequest{Op: "get_room", Cred: cred, Params: map[string]any{"roomId": roomID}})
	if err != nil {
		return facade.RoomInfo{}, err
	}
	return facade.RoomInfo{
		RoomID:   str(resp, "roomId"),
		Host:     str(resp, "owner"),
		Capacity: integer(resp, "max"),
		Members:  integer(resp, "cur"),
	}, nil
}

// GetLeaderboard 实现 facade.Backend。
func (p *PlatformB) GetLeaderboard(ctx context.Context, cred facade.Credential, game string, limit int) ([]facade.LeaderboardEntry, error) {
	resp, err := p.tr(ctx, RawRequest{Op: "get_leaderboard", Cred: cred, Params: map[string]any{"game": game, "size": limit}})
	if err != nil {
		return nil, err
	}
	items, err := list(resp, "items")
	if err != nil {
		return nil, err
	}
	out := make([]facade.LeaderboardEntry, 0, len(items))
	for _, it := range items {
		out = append(out, facade.LeaderboardEntry{
			Rank:      integer(it, "position"),
			AccountID: str(it, "player"),
			Score:     integer64(it, "points"),
		})
	}
	return out, nil
}

// ApplyWrite 实现 facade.Backend；平台 B 的幂等键字段名为 request_id。
func (p *PlatformB) ApplyWrite(ctx context.Context, cred facade.Credential, op facade.WriteOp) error {
	params := map[string]any{"request_id": op.ID}
	for k, v := range op.Payload {
		params[k] = v
	}
	_, err := p.tr(ctx, RawRequest{Op: string(op.Kind), Cred: cred, Params: params})
	return err
}
