package backends

import (
	"context"
	"time"

	"github.com/shikejing/platform-sdk-facade-go/facade"
)

// Beta 是备用后端模拟：签名凭据（5min 过期），驼峰/简写命名，
// 且部分字段缺失（无 region）或语义无法映射（leaderboard 无 rank 概念）。
type Beta struct {
	Server *Server
	norm   *facade.Normalizer
}

func NewBeta(logger facade.Logger) *Beta {
	s := NewServer("beta", "beta:sig", 5*time.Minute)
	b := &Beta{Server: s}
	b.norm = facade.NewNormalizer("beta", CanonicalFields, map[facade.OpKind]facade.FieldMapping{
		facade.OpAccountGet: {
			"account_id": "uid",
			"name":       "nick",
			"region":     "region", // beta 有映射但响应不返回该字段 -> FieldMissing
			"created_at": "signupTime",
		},
		facade.OpMatchCreate: {"match_id": "matchId", "status": "phase"},
		facade.OpRoomCreate:  {"room_id": "roomId", "host": "hostUid"},
		facade.OpLeaderQuery: {
			"player": "uid",
			"score":  "score",
			// rank: beta 无此概念，无映射 -> FieldUnmapped
		},
	}, logger)
	return b
}

func (b *Beta) ID() facade.BackendID                      { return "beta" }
func (b *Beta) CredentialSource() facade.CredentialSource { return credSource{b.Server} }
func (b *Beta) Normalizer() *facade.Normalizer            { return b.norm }

// Call 实现 facade.Backend：按 beta 的原始字段命名返回（字段有缺失）。
func (b *Beta) Call(ctx context.Context, cred facade.Credential, req facade.Request) (facade.RawResponse, error) {
	var raw map[string]string
	switch req.Op {
	case facade.OpAccountGet:
		raw = map[string]string{
			"uid":        req.Payload["account_id"],
			"nick":       "player-" + req.Payload["account_id"],
			"signupTime": "2023-11-14",
		}
	case facade.OpMatchCreate:
		raw = map[string]string{"matchId": nextID("bm"), "phase": "WAITING"}
	case facade.OpRoomCreate:
		raw = map[string]string{"roomId": nextID("br"), "hostUid": req.Payload["account_id"]}
	case facade.OpLeaderQuery:
		raw = map[string]string{"uid": req.Payload["account_id"], "score": "1024"}
	default:
		raw = map[string]string{}
	}
	return b.Server.Handle(ctx, cred, req, raw)
}
