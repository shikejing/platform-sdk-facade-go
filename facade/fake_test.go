package facade

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// fakeBackend 是可控的测试后端：可注入延迟与错误，记录调用与凭据使用情况。
type fakeBackend struct {
	id  BackendID
	ttl time.Duration

	mu         sync.Mutex
	renewCalls int
	renewDelay time.Duration
	renewErr   error

	readErr   error
	readDelay time.Duration
	readCalls int

	writeErr   error
	writeErrOn map[string]bool // 指定幂等键的写操作失败
	writes     []string        // 已应用写操作（按序）
	writeCount map[string]int

	tokens      map[string]time.Time // 已签发 token -> 过期时刻
	expiredUses int                  // 携带过期/未知 token 的调用次数
}

func newFakeBackend(id string, ttl time.Duration) *fakeBackend {
	return &fakeBackend{
		id:         BackendID(id),
		ttl:        ttl,
		writeErrOn: map[string]bool{},
		writeCount: map[string]int{},
		tokens:     map[string]time.Time{},
	}
}

func (f *fakeBackend) ID() BackendID { return f.id }

func (f *fakeBackend) RenewCredential(ctx context.Context) (Credential, error) {
	if f.renewDelay > 0 {
		select {
		case <-time.After(f.renewDelay):
		case <-ctx.Done():
			return Credential{}, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.renewErr != nil {
		return Credential{}, f.renewErr
	}
	f.renewCalls++
	tok := fmt.Sprintf("%s-token-%d", f.id, f.renewCalls)
	exp := time.Now().Add(f.ttl)
	f.tokens[tok] = exp
	return Credential{Token: tok, ExpiresAt: exp}, nil
}

// checkCred 校验调用方携带的凭据，过期或未知则记录违规。
func (f *fakeBackend) checkCred(c Credential) {
	f.mu.Lock()
	defer f.mu.Unlock()
	exp, ok := f.tokens[c.Token]
	if !ok || !time.Now().Before(exp) {
		f.expiredUses++
	}
}

func (f *fakeBackend) beforeRead() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readCalls++
	if f.readErr != nil {
		return f.readErr
	}
	return nil
}

func (f *fakeBackend) GetAccount(ctx context.Context, cred Credential, accountID string) (AccountInfo, error) {
	f.checkCred(cred)
	if f.readDelay > 0 {
		select {
		case <-time.After(f.readDelay):
		case <-ctx.Done():
			return AccountInfo{}, ctx.Err()
		}
	}
	if err := f.beforeRead(); err != nil {
		return AccountInfo{}, err
	}
	return AccountInfo{
		AccountID:   OK(accountID),
		DisplayName: OK("name-" + accountID),
		Region:      OK("cn"),
		Level:       OK(10),
	}, nil
}

func (f *fakeBackend) CreateMatch(ctx context.Context, cred Credential, req MatchRequest) (MatchTicket, error) {
	f.checkCred(cred)
	if err := f.beforeRead(); err != nil {
		return MatchTicket{}, err
	}
	return MatchTicket{TicketID: OK("t-1"), Status: OK("searching"), ETA: OK(time.Second)}, nil
}

func (f *fakeBackend) GetRoom(ctx context.Context, cred Credential, roomID string) (RoomInfo, error) {
	f.checkCred(cred)
	if err := f.beforeRead(); err != nil {
		return RoomInfo{}, err
	}
	return RoomInfo{RoomID: OK(roomID), Host: OK("h"), Capacity: OK(8), Members: OK(1)}, nil
}

func (f *fakeBackend) GetLeaderboard(ctx context.Context, cred Credential, game string, limit int) ([]LeaderboardEntry, error) {
	f.checkCred(cred)
	if err := f.beforeRead(); err != nil {
		return nil, err
	}
	return []LeaderboardEntry{{Rank: OK(1), AccountID: OK("a1"), Score: OK(int64(100))}}, nil
}

func (f *fakeBackend) ApplyWrite(ctx context.Context, cred Credential, op WriteOp) error {
	f.checkCred(cred)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writeErr != nil || f.writeErrOn[op.ID] {
		if f.writeErr != nil {
			return f.writeErr
		}
		return fmt.Errorf("injected write failure on %s", op.ID)
	}
	f.writes = append(f.writes, op.ID)
	f.writeCount[op.ID]++
	return nil
}

// 测试辅助读取方法。

func (f *fakeBackend) renewCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.renewCalls
}

func (f *fakeBackend) reads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.readCalls
}

func (f *fakeBackend) appliedWrites() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.writes))
	copy(out, f.writes)
	return out
}

func (f *fakeBackend) writesOf(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writeCount[id]
}

func (f *fakeBackend) expiredCredUses() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.expiredUses
}

func (f *fakeBackend) setReadErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readErr = err
}

func (f *fakeBackend) setWriteErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeErr = err
}
