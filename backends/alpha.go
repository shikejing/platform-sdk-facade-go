package backends

import (
	"context"
	"time"

	"github.com/shikejing/platform-sdk-facade-go/facade"
)

// Alpha 是主后端模拟：token 凭据（60s 过期），下划线命名，字段齐全。
type Alpha struct {
	Server *Server
	norm   *facade.Normalizer
}

// CanonicalFields 是对外承诺的统一字段集合（所有后端共用）。
var CanonicalFields = map[facade.OpKind][]string{
	facade.OpAccountGet:  {"account_id", "name", "region", "created_at"},
	facade.OpMatchCreate: {"match_id", "status"},
	facade.OpRoomCreate:  {"room_id", "host"},
	facade.OpLeaderQuery: {"player", "score", "rank"},
}

func NewAlpha(logger facade.Logger) *Alpha {
	s := NewServer("alpha", "alpha-token", 60*time.Second)
	a := &Alpha{Server: s}
	a.norm = facade.NewNormalizer("alpha", CanonicalFields, map[facade.OpKind]facade.FieldMapping{
		facade.OpAccountGet: {
			"account_id": "account_id",
			"name":       "display_name",
			"region":     "region",
			"created_at": "created_ts",
		},
		facade.OpMatchCreate: {"match_id": "match_id", "status": "state"},
		facade.OpRoomCreate:  {"room_id": "room_id", "host": "owner"},
		facade.OpLeaderQuery: {"player": "player", "score": "points", "rank": "position"},
	}, logger)
	return a
}

func (a *Alpha) ID() facade.BackendID                      { return "alpha" }
func (a *Alpha) CredentialSource() facade.CredentialSource { return credSource{a.Server} }
func (a *Alpha) Normalizer() *facade.Normalizer            { return a.norm }

// Call 实现 facade.Backend：按 alpha 的原始字段命名返回。
func (a *Alpha) Call(ctx context.Context, cred facade.Credential, req facade.Request) (facade.RawResponse, error) {
	var raw map[string]string
	switch req.Op {
	case facade.OpAccountGet:
		raw = map[string]string{
			"account_id":   req.Payload["account_id"],
			"display_name": "player-" + req.Payload["account_id"],
			"region":       "ap-east",
			"created_ts":   "1700000000",
		}
	case facade.OpMatchCreate:
		raw = map[string]string{"match_id": nextID("m"), "state": "open"}
	case facade.OpRoomCreate:
		raw = map[string]string{"room_id": nextID("r"), "owner": req.Payload["account_id"]}
	case facade.OpLeaderQuery:
		raw = map[string]string{"player": req.Payload["account_id"], "points": "1024", "position": "7"}
	default:
		raw = map[string]string{}
	}
	return a.Server.Handle(ctx, cred, req, raw)
}
