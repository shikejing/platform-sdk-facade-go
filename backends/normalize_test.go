package backends

import (
	"context"
	"testing"
	"time"

	facade "github.com/shikejing/platform-sdk-facade-go/facade"
)

// staticTransport 按 op 返回预置响应。
func staticTransport(resps map[string]map[string]any) Transport {
	return func(_ context.Context, req RawRequest) (map[string]any, error) {
		if r, ok := resps[req.Op]; ok {
			return r, nil
		}
		return map[string]any{}, nil
	}
}

func renewA() map[string]any { return map[string]any{"token": "tok-a", "expires_in": float64(3600)} }
func renewB() map[string]any {
	return map[string]any{"session_key": "tok-b", "expires_at": float64(time.Now().Add(time.Hour).Unix())}
}

// 平台 A 的完整响应：所有字段正常映射。
func TestPlatformAFullMapping(t *testing.T) {
	a := NewPlatformA("a", staticTransport(map[string]map[string]any{
		"renew": renewA(),
		"get_account": {
			"account_id": "u1", "nickname": "neo", "region": "cn", "level": float64(42),
		},
	}))
	cred, err := a.RenewCredential(context.Background())
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	info, err := a.GetAccount(context.Background(), cred, "u1")
	if err != nil {
		t.Fatalf("get_account: %v", err)
	}
	assertField(t, "AccountID", info.AccountID, facade.FieldOK, "u1")
	assertField(t, "DisplayName", info.DisplayName, facade.FieldOK, "neo")
	assertField(t, "Region", info.Region, facade.FieldOK, "cn")
	assertField(t, "Level", info.Level, facade.FieldOK, 42)
}

// 平台 B 的语义差异被适配器吸收：不同字段名、字符串数字、缺失 region。
func TestPlatformBDifferencesAbsorbed(t *testing.T) {
	b := NewPlatformB("b", staticTransport(map[string]map[string]any{
		"renew": renewB(),
		"get_account": {
			"uid": "u2", "display_name": "trinity", "lvl": "17", // region 不存在
		},
	}))
	cred, err := b.RenewCredential(context.Background())
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if cred.Token != "tok-b" {
		t.Fatalf("token = %q, want session_key mapped to unified credential", cred.Token)
	}
	info, err := b.GetAccount(context.Background(), cred, "u2")
	if err != nil {
		t.Fatalf("get_account: %v", err)
	}
	assertField(t, "AccountID", info.AccountID, facade.FieldOK, "u2")
	assertField(t, "DisplayName", info.DisplayName, facade.FieldOK, "trinity")
	assertField(t, "Level", info.Level, facade.FieldOK, 17) // 字符串数字被解析
	// region 缺失：必须显式标记，不得用空串冒充。
	if info.Region.Status != facade.FieldMissing {
		t.Fatalf("Region.Status = %v, want FieldMissing", info.Region.Status)
	}
	if info.Region.Value != "" {
		t.Fatalf("Region.Value = %q, want zero value for missing field", info.Region.Value)
	}
}

// 无法映射的字段类型标记为 FieldUnmapped，而不是静默置零。
func TestUnmappableFieldMarked(t *testing.T) {
	b := NewPlatformB("b", staticTransport(map[string]map[string]any{
		"renew":       renewB(),
		"get_account": {"uid": "u3", "display_name": "x", "lvl": "not-a-number"},
	}))
	cred, _ := b.RenewCredential(context.Background())
	info, err := b.GetAccount(context.Background(), cred, "u3")
	if err != nil {
		t.Fatalf("get_account: %v", err)
	}
	if info.Level.Status != facade.FieldUnmapped {
		t.Fatalf("Level.Status = %v, want FieldUnmapped", info.Level.Status)
	}
}

// 平台 A 缺失字段时同样显式标记。
func TestPlatformAMissingField(t *testing.T) {
	a := NewPlatformA("a", staticTransport(map[string]map[string]any{
		"renew":       renewA(),
		"get_account": {"account_id": "u4", "region": "us"}, // nickname 缺失
	}))
	cred, _ := a.RenewCredential(context.Background())
	info, err := a.GetAccount(context.Background(), cred, "u4")
	if err != nil {
		t.Fatalf("get_account: %v", err)
	}
	if info.DisplayName.Status != facade.FieldMissing {
		t.Fatalf("DisplayName.Status = %v, want FieldMissing", info.DisplayName.Status)
	}
}

// 两个平台的榜单响应归一化为同一模型。
func TestLeaderboardNormalization(t *testing.T) {
	a := NewPlatformA("a", staticTransport(map[string]map[string]any{
		"renew": renewA(),
		"get_leaderboard": {"entries": []any{
			map[string]any{"rank": float64(1), "account_id": "u1", "score": float64(900)},
		}},
	}))
	b := NewPlatformB("b", staticTransport(map[string]map[string]any{
		"renew": renewB(),
		"get_leaderboard": {"items": []any{
			map[string]any{"position": float64(1), "player": "u1", "points": float64(900)},
		}},
	}))
	ctx := context.Background()
	credA, _ := a.RenewCredential(ctx)
	credB, _ := b.RenewCredential(ctx)
	la, err := a.GetLeaderboard(ctx, credA, "g", 10)
	if err != nil {
		t.Fatalf("platform-a leaderboard: %v", err)
	}
	lb, err := b.GetLeaderboard(ctx, credB, "g", 10)
	if err != nil {
		t.Fatalf("platform-b leaderboard: %v", err)
	}
	if len(la) != 1 || len(lb) != 1 {
		t.Fatalf("unexpected leaderboard sizes: %d, %d", len(la), len(lb))
	}
	for i, e := range []facade.LeaderboardEntry{la[0], lb[0]} {
		assertField(t, "Rank", e.Rank, facade.FieldOK, 1)
		assertField(t, "AccountID", e.AccountID, facade.FieldOK, "u1")
		assertField(t, "Score", e.Score, facade.FieldOK, int64(900))
		_ = i
	}
}

func assertField[T comparable](t *testing.T, name string, f facade.Field[T], status facade.FieldStatus, want T) {
	t.Helper()
	if f.Status != status {
		t.Fatalf("%s.Status = %v, want %v", name, f.Status, status)
	}
	if f.Status == facade.FieldOK && f.Value != want {
		t.Fatalf("%s.Value = %v, want %v", name, f.Value, want)
	}
}
