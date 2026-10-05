package facade

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// 并发请求下同一凭据的续期只能真正发生一次，且所有等待者共享同一结果。
func TestConcurrentRenewalHappensOnce(t *testing.T) {
	fake := newFakeBackend("b1", time.Hour)
	fake.renewDelay = 50 * time.Millisecond // 放大并发窗口
	mgr := newCredentialManager(fake, time.Minute, newAuditor(nil))

	const n = 32
	var wg sync.WaitGroup
	tokens := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cred, err := mgr.Get(context.Background())
			tokens[i] = cred.Token
			errs[i] = err
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d got error: %v", i, errs[i])
		}
		if tokens[i] != tokens[0] {
			t.Fatalf("goroutine %d got different token %q, want shared %q", i, tokens[i], tokens[0])
		}
	}
	if got := fake.renewCount(); got != 1 {
		t.Fatalf("renewal happened %d times, want exactly 1", got)
	}
}

// 续期失败时所有等待者拿到同一个可区分错误。
func TestConcurrentRenewalFailureShared(t *testing.T) {
	fake := newFakeBackend("b1", time.Hour)
	fake.renewDelay = 30 * time.Millisecond
	fake.renewErr = errors.New("idp unreachable")
	mgr := newCredentialManager(fake, time.Minute, newAuditor(nil))

	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = mgr.Get(context.Background())
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if !IsCode(errs[i], ReasonCredentialRenewFailed) {
			t.Fatalf("goroutine %d error %v is not distinguishable as %s", i, errs[i], ReasonCredentialRenewFailed)
		}
	}
	if got := fake.renewCount(); got != 0 {
		t.Fatalf("renew count = %d, want 0 (renewal failed before issuing)", got)
	}
}

// 后端返回已过期凭据时，视为续期失败，绝不向下游发放。
func TestExpiredCredentialFromBackendRejected(t *testing.T) {
	fake := newFakeBackend("b1", -time.Minute) // 签发的凭据立刻过期
	mgr := newCredentialManager(fake, time.Minute, newAuditor(nil))

	_, err := mgr.Get(context.Background())
	if !IsCode(err, ReasonCredentialRenewFailed) {
		t.Fatalf("err = %v, want code %s", err, ReasonCredentialRenewFailed)
	}
	if !IsCode(err, ReasonCredentialExpired) {
		t.Fatalf("err = %v, want inner code %s", err, ReasonCredentialExpired)
	}
}

// 凭据过期后再次获取会触发新的续期，且任何请求都不会携带过期凭据。
func TestNoRequestWithExpiredCredential(t *testing.T) {
	fake := newFakeBackend("b1", 120*time.Millisecond)
	sink := NewMemorySink()
	c := NewClient(Config{
		Backends: []BackendConfig{{
			Backend:    fake,
			CredMargin: 20 * time.Millisecond,
		}},
		Sink: sink,
	})
	ctx := context.Background()

	if _, err := c.GetAccount(ctx, "a1"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // 等待凭据过期
	if _, err := c.GetAccount(ctx, "a2"); err != nil {
		t.Fatalf("second call: %v", err)
	}

	if got := fake.renewCount(); got != 2 {
		t.Fatalf("renew count = %d, want 2 (renew after expiry)", got)
	}
	if got := fake.expiredCredUses(); got != 0 {
		t.Fatalf("backend received %d calls with expired credential, want 0", got)
	}
}
