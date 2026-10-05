package facade

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeSource 统计真实续期次数，可控制阻塞与失败。
type fakeSource struct {
	calls   atomic.Int32
	release chan struct{}
	err     error
	ttl     time.Duration
}

func (f *fakeSource) Renew(ctx context.Context) (Credential, error) {
	f.calls.Add(1)
	if f.release != nil {
		select {
		case <-f.release:
		case <-ctx.Done():
			return Credential{}, ctx.Err()
		}
	}
	if f.err != nil {
		return Credential{}, f.err
	}
	return Credential{Value: "tok", ExpiresAt: now().Add(f.ttl)}, nil
}

func TestConcurrentRenewalHappensOnce(t *testing.T) {
	src := &fakeSource{release: make(chan struct{}), ttl: time.Minute}
	r := NewCredentialRenewer("b1", src, nil, 0)

	const n = 32
	var wg sync.WaitGroup
	creds := make([]Credential, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			creds[i], errs[i] = r.Get(context.Background())
		}(i)
	}
	// 等所有 goroutine 都进入等待后放行续期。
	time.Sleep(50 * time.Millisecond)
	close(src.release)
	wg.Wait()

	if got := src.calls.Load(); got != 1 {
		t.Fatalf("renewal happened %d times, want 1", got)
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d got error: %v", i, errs[i])
		}
		if creds[i].Value != "tok" {
			t.Fatalf("goroutine %d did not share renewal result", i)
		}
	}
}

func TestRenewalFailureIsDistinguishable(t *testing.T) {
	src := &fakeSource{err: errors.New("idp unreachable")}
	r := NewCredentialRenewer("b1", src, nil, 0)

	_, err := r.Get(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	var re *RenewalError
	if !errors.As(err, &re) {
		t.Fatalf("error %T is not *RenewalError", err)
	}
	if re.Backend != "b1" {
		t.Fatalf("wrong backend in RenewalError: %s", re.Backend)
	}
	// 共享续期失败的等待者也拿到同一可区分错误。
	src2 := &fakeSource{release: make(chan struct{}), err: errors.New("boom")}
	r2 := NewCredentialRenewer("b1", src2, nil, 0)
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) { defer wg.Done(); _, errs[i] = r2.Get(context.Background()) }(i)
	}
	time.Sleep(20 * time.Millisecond)
	close(src2.release)
	wg.Wait()
	for i, e := range errs {
		var re *RenewalError
		if !errors.As(e, &re) {
			t.Fatalf("waiter %d got non-RenewalError: %v", i, e)
		}
	}
}

func TestExpiredCredentialNeverReturned(t *testing.T) {
	// 凭据签发生即过期（在安全余量内）：续期器必须拒绝发放，报可区分的续期错误。
	src := &fakeSource{ttl: 0}
	r := NewCredentialRenewer("b1", src, nil, time.Second)
	_, err := r.Get(context.Background())
	var re *RenewalError
	if !errors.As(err, &re) {
		t.Fatalf("expected *RenewalError for stillborn credential, got %v", err)
	}

	// 正常 TTL：缓存期内不重复续期，且返回值绝不过期。
	src2 := &fakeSource{ttl: time.Hour}
	r2 := NewCredentialRenewer("b1", src2, nil, time.Second)
	for i := 0; i < 3; i++ {
		c, err := r2.Get(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if c.Expired(0) {
			t.Fatal("returned an expired credential")
		}
	}
	if got := src2.calls.Load(); got != 1 {
		t.Fatalf("expected 1 renewal with cache, got %d", got)
	}
}

func TestInvalidateForcesRenewal(t *testing.T) {
	src := &fakeSource{ttl: time.Hour}
	r := NewCredentialRenewer("b1", src, nil, 0)
	if _, err := r.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.Invalidate()
	if _, err := r.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := src.calls.Load(); got != 2 {
		t.Fatalf("expected 2 renewals after invalidate, got %d", got)
	}
}
