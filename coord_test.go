// Seam-based unit tests. These use in-package fakes (a fault-injecting etcdKV,
// a fake concurrency session + election backend) and controllable time/marshal
// seams to drive every branch — including the etcd error paths — deterministically
// and without any network or embedded server. They carry no build tag, so they
// build and run on every CI lane, including the cross-arch qemu lanes where the
// heavy embedded-etcd integration tests are skipped.
package coord

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var errInjected = errors.New("injected")

// ---- fake etcdKV ---------------------------------------------------

type fakeKV struct {
	grantFn  func(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	putFn    func(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	kaFn     func(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error)
	revokeFn func(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
	getFn    func(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	watchFn  func(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan

	mu          sync.Mutex
	grantCalls  int
	revokeCalls int
	nextLease   int64
}

func (f *fakeKV) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	f.mu.Lock()
	f.grantCalls++
	f.nextLease++
	id := f.nextLease
	f.mu.Unlock()
	if f.grantFn != nil {
		return f.grantFn(ctx, ttl)
	}
	return &clientv3.LeaseGrantResponse{ID: clientv3.LeaseID(id), TTL: ttl}, nil
}

func (f *fakeKV) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	if f.putFn != nil {
		return f.putFn(ctx, key, val, opts...)
	}
	return &clientv3.PutResponse{}, nil
}

func (f *fakeKV) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	if f.kaFn != nil {
		return f.kaFn(ctx, id)
	}
	ch := make(chan *clientv3.LeaseKeepAliveResponse) // never closed; drain blocks until keepCtx done
	return ch, nil
}

func (f *fakeKV) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	f.mu.Lock()
	f.revokeCalls++
	f.mu.Unlock()
	if f.revokeFn != nil {
		return f.revokeFn(ctx, id)
	}
	return &clientv3.LeaseRevokeResponse{}, nil
}

func (f *fakeKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if f.getFn != nil {
		return f.getFn(ctx, key, opts...)
	}
	return &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 1}}, nil
}

func (f *fakeKV) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	if f.watchFn != nil {
		return f.watchFn(ctx, key, opts...)
	}
	ch := make(chan clientv3.WatchResponse)
	close(ch) // empty watch: goroutine exits immediately
	return ch
}

// ---- fake session + election backend -------------------------------

type fakeSession struct {
	lease    clientv3.LeaseID
	closeErr error
	closes   int
}

func (s *fakeSession) Lease() clientv3.LeaseID { return s.lease }
func (s *fakeSession) Close() error            { s.closes++; return s.closeErr }

type fakeBackend struct {
	campaignErr error
	leaderResp  *clientv3.GetResponse
	leaderErr   error
	resignErr   error
	observeCh   chan clientv3.GetResponse
	campaigns   int
}

func (b *fakeBackend) Campaign(ctx context.Context, val string) error {
	b.campaigns++
	return b.campaignErr
}
func (b *fakeBackend) Leader(ctx context.Context) (*clientv3.GetResponse, error) {
	return b.leaderResp, b.leaderErr
}
func (b *fakeBackend) Resign(ctx context.Context) error { return b.resignErr }
func (b *fakeBackend) Observe(ctx context.Context) <-chan clientv3.GetResponse {
	return b.observeCh
}

// swapSeams saves the session/election seams and restores them on cleanup,
// pointing them at the supplied fakes.
func swapSeams(t *testing.T, sess session, be electionBackend) {
	t.Helper()
	oS, oE := newSession, newElectionFor
	t.Cleanup(func() { newSession, newElectionFor = oS, oE })
	newSession = func(cli *clientv3.Client, ttl int, ctx context.Context) (session, error) {
		return sess, nil
	}
	newElectionFor = func(s session, pfx string) electionBackend { return be }
}

// ---- HostLiveness --------------------------------------------------

func TestRegisterHostLiveness_NilClient(t *testing.T) {
	if _, err := RegisterHostLiveness(context.Background(), nil, HostMetadata{HostUUID: "x"}, LivenessOptions{}); err == nil {
		t.Fatal("want error for nil client")
	}
}

func TestRegisterHostLiveness_EmptyUUID(t *testing.T) {
	if _, err := registerHostLiveness(context.Background(), &fakeKV{}, HostMetadata{}, LivenessOptions{}); err == nil {
		t.Fatal("want error for empty HostUUID")
	}
}

func TestRegisterHostLiveness_MarshalError(t *testing.T) {
	old := marshalMeta
	t.Cleanup(func() { marshalMeta = old })
	marshalMeta = func(any) ([]byte, error) { return nil, errInjected }
	if _, err := registerHostLiveness(context.Background(), &fakeKV{}, HostMetadata{HostUUID: "x"}, LivenessOptions{}); err == nil {
		t.Fatal("want marshal error")
	}
}

func TestRegisterHostLiveness_GrantError(t *testing.T) {
	kv := &fakeKV{grantFn: func(context.Context, int64) (*clientv3.LeaseGrantResponse, error) {
		return nil, errInjected
	}}
	if _, err := registerHostLiveness(context.Background(), kv, HostMetadata{HostUUID: "x"}, LivenessOptions{}); err == nil {
		t.Fatal("want grant error")
	}
}

func TestRegisterHostLiveness_PutError(t *testing.T) {
	kv := &fakeKV{putFn: func(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error) {
		return nil, errInjected
	}}
	if _, err := registerHostLiveness(context.Background(), kv, HostMetadata{HostUUID: "x"}, LivenessOptions{}); err == nil {
		t.Fatal("want put error")
	}
	if kv.revokeCalls != 1 {
		t.Errorf("put error must best-effort revoke; revokeCalls=%d", kv.revokeCalls)
	}
}

func TestRegisterHostLiveness_KeepAliveError(t *testing.T) {
	kv := &fakeKV{kaFn: func(context.Context, clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
		return nil, errInjected
	}}
	if _, err := registerHostLiveness(context.Background(), kv, HostMetadata{HostUUID: "x"}, LivenessOptions{}); err == nil {
		t.Fatal("want keepalive error")
	}
	if kv.revokeCalls != 1 {
		t.Errorf("keepalive error must best-effort revoke; revokeCalls=%d", kv.revokeCalls)
	}
}

func TestRegisterHostLiveness_SuccessDefaultsAndStop(t *testing.T) {
	kv := &fakeKV{}
	// StartedAt=0 exercises the nowNanos default; zero opts exercise the
	// default prefix/ttl/logger.
	hl, err := registerHostLiveness(context.Background(), kv, HostMetadata{HostUUID: "host-a"}, LivenessOptions{})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if hl.Key() != HostsPrefix+"host-a" {
		t.Errorf("Key()=%q; want %q", hl.Key(), HostsPrefix+"host-a")
	}
	if hl.LeaseID() == 0 {
		t.Error("LeaseID() should be non-zero after register")
	}
	if err := hl.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	// Second Stop is a no-op and returns nil.
	if err := hl.Stop(context.Background()); err != nil {
		t.Errorf("second Stop: %v", err)
	}
}

func TestRegisterHostLiveness_CustomOptionsAndStopError(t *testing.T) {
	kv := &fakeKV{revokeFn: func(context.Context, clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
		return nil, errInjected
	}}
	hl, err := registerHostLiveness(context.Background(), kv, HostMetadata{HostUUID: "h", StartedAt: 42},
		LivenessOptions{Prefix: "/fleet/", LeaseTTLSec: 7, Logger: slog.Default()})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if hl.Key() != "/fleet/h" {
		t.Errorf("Key()=%q; want /fleet/h", hl.Key())
	}
	if err := hl.Stop(context.Background()); err == nil {
		t.Fatal("want revoke error from Stop")
	}
}

// TestKeepAliveLoop_SelfHeal drives the drain->close->re-register path with the
// real afterFunc seam (covers its default body). The first KeepAlive channel is
// fed one refresh then closed; the second registerOnce succeeds with a new lease.
func TestKeepAliveLoop_SelfHeal(t *testing.T) {
	var mu sync.Mutex
	call := 0
	firstCh := make(chan *clientv3.LeaseKeepAliveResponse)
	secondCh := make(chan *clientv3.LeaseKeepAliveResponse) // stays open

	kv := &fakeKV{}
	kv.kaFn = func(context.Context, clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		call++
		if call == 1 {
			return firstCh, nil
		}
		return secondCh, nil
	}
	hl, err := registerHostLiveness(context.Background(), kv, HostMetadata{HostUUID: "h"}, LivenessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	first := hl.LeaseID()
	// One successful refresh (ok==true branch), then close to trigger re-register.
	firstCh <- &clientv3.LeaseKeepAliveResponse{}
	close(firstCh)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if hl.LeaseID() != first && hl.LeaseID() != 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if hl.LeaseID() == first {
		t.Fatalf("expected re-registration to change lease from %d", first)
	}
	hl.cancel()
	<-hl.done
}

// TestKeepAliveLoop_BackoffGrowthAndCap forces many failed re-register attempts
// (with an instant afterFunc) so the backoff doubles past maxBackoff and hits
// the cap branch, then finally succeeds.
func TestKeepAliveLoop_BackoffGrowthAndCap(t *testing.T) {
	oldAfter := afterFunc
	t.Cleanup(func() { afterFunc = oldAfter })
	afterFunc = func(time.Duration) <-chan time.Time {
		c := make(chan time.Time, 1)
		c <- time.Now()
		return c
	}

	var mu sync.Mutex
	grantCall := 0
	firstCh := make(chan *clientv3.LeaseKeepAliveResponse)
	kv := &fakeKV{}
	kv.kaFn = func(context.Context, clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
		return firstCh, nil // only used for the initial registration
	}
	kv.grantFn = func(context.Context, int64) (*clientv3.LeaseGrantResponse, error) {
		mu.Lock()
		grantCall++
		n := grantCall
		mu.Unlock()
		if n == 1 {
			return &clientv3.LeaseGrantResponse{ID: 1}, nil // initial ok
		}
		if n <= 13 { // fail 12 re-register attempts -> backoff grows past the 30s cap
			return nil, errInjected
		}
		return &clientv3.LeaseGrantResponse{ID: clientv3.LeaseID(n)}, nil
	}
	// After the initial registration, a fresh KeepAlive channel that stays open.
	secondCh := make(chan *clientv3.LeaseKeepAliveResponse)
	var kaCall int
	kv.kaFn = func(context.Context, clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
		mu.Lock()
		kaCall++
		n := kaCall
		mu.Unlock()
		if n == 1 {
			return firstCh, nil
		}
		return secondCh, nil
	}
	hl, err := registerHostLiveness(context.Background(), kv, HostMetadata{HostUUID: "h"}, LivenessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	close(firstCh) // trigger re-register storm

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := grantCall >= 14
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	gc := grantCall
	mu.Unlock()
	if gc < 14 {
		t.Fatalf("expected >=14 grant calls (backoff cap path); got %d", gc)
	}
	hl.cancel()
	<-hl.done
}

// TestKeepAliveLoop_CancelDuringBackoff blocks in the backoff wait, then cancels
// keepCtx, covering the keepCtx.Done() return inside the re-register loop.
func TestKeepAliveLoop_CancelDuringBackoff(t *testing.T) {
	oldAfter := afterFunc
	t.Cleanup(func() { afterFunc = oldAfter })
	blocked := make(chan struct{})
	afterFunc = func(time.Duration) <-chan time.Time {
		select {
		case blocked <- struct{}{}:
		default:
		}
		return make(chan time.Time) // never fires
	}

	firstCh := make(chan *clientv3.LeaseKeepAliveResponse)
	var kaCall int
	var mu sync.Mutex
	kv := &fakeKV{}
	kv.kaFn = func(context.Context, clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
		mu.Lock()
		kaCall++
		n := kaCall
		mu.Unlock()
		if n == 1 {
			return firstCh, nil
		}
		return make(chan *clientv3.LeaseKeepAliveResponse), nil
	}
	// Force re-register attempts to fail so we stay in the backoff loop.
	kv.grantFn = func(context.Context, int64) (*clientv3.LeaseGrantResponse, error) {
		mu.Lock()
		n := kaCall
		mu.Unlock()
		if n == 0 {
			return &clientv3.LeaseGrantResponse{ID: 1}, nil
		}
		return nil, errInjected
	}
	hl, err := registerHostLiveness(context.Background(), kv, HostMetadata{HostUUID: "h"}, LivenessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	close(firstCh) // enter backoff loop
	<-blocked      // ensure we're parked in afterFunc wait
	hl.cancel()    // covers the keepCtx.Done() branch inside the backoff select
	<-hl.done
}

// ---- HostWatcher ---------------------------------------------------

func TestNewHostWatcher_NilClient(t *testing.T) {
	if _, err := NewHostWatcher(context.Background(), nil, WatcherOptions{}); err == nil {
		t.Fatal("want error for nil client")
	}
}

func TestNewHostWatcher_GetError(t *testing.T) {
	kv := &fakeKV{getFn: func(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
		return nil, errInjected
	}}
	if _, err := newHostWatcher(context.Background(), kv, WatcherOptions{}); err == nil {
		t.Fatal("want get error")
	}
}

func TestNewHostWatcher_SnapshotAndWatch(t *testing.T) {
	good, _ := json.Marshal(HostMetadata{HostUUID: "host-x", Hostname: "hx"})
	watchCh := make(chan clientv3.WatchResponse, 4)
	kv := &fakeKV{
		getFn: func(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			return &clientv3.GetResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 5},
				Kvs: []*mvccpb.KeyValue{
					{Key: []byte("/coord/hosts/host-x"), Value: good},        // valid json
					{Key: []byte("/coord/hosts/host-y"), Value: []byte("{")}, // bad json -> zero meta
					{Key: []byte("/coord/hosts/")},                           // base "hosts" but empty value ok; still an event
				},
			}, nil
		},
		watchFn: func(context.Context, string, ...clientv3.OpOption) clientv3.WatchChan {
			return watchCh
		},
	}
	w, err := newHostWatcher(context.Background(), kv, WatcherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Feed watch events: a compaction error (wr.Err()!=nil, skipped), a PUT,
	// a DELETE with PrevKv, and a DELETE without PrevKv.
	watchCh <- clientv3.WatchResponse{CompactRevision: 1}
	watchCh <- clientv3.WatchResponse{Events: []*clientv3.Event{
		{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/coord/hosts/host-new"), Value: good}},
		{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: []byte("/coord/hosts/host-x")},
			PrevKv: &mvccpb.KeyValue{Key: []byte("/coord/hosts/host-x"), Value: good}},
		{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: []byte("/coord/hosts/host-z")}},
	}}
	close(watchCh)

	var kinds []HostEventKind
	for ev := range w.Events() {
		kinds = append(kinds, ev.Kind)
	}
	w.Wait()
	// 3 snapshot Ups + 1 PUT Up + 2 DELETE Downs = 6 events.
	if len(kinds) != 6 {
		t.Fatalf("got %d events; want 6 (%v)", len(kinds), kinds)
	}
}

func TestMakeEvent_Branches(t *testing.T) {
	w := &HostWatcher{opts: WatcherOptions{IncludeSelf: "me"}}
	cases := []struct {
		key    string
		val    string
		wantOK bool
	}{
		{"", "", false},                  // path.Base("")="." -> filtered
		{"/", "", false},                 // "/" -> filtered
		{"/coord/hosts/me", "", false},   // self -> filtered
		{"/coord/hosts/a", "", true},     // empty value, valid uuid
		{"/coord/hosts/b", "{bad", true}, // bad json -> zero meta, still ok
	}
	for _, c := range cases {
		if _, ok := w.makeEvent([]byte(c.key), []byte(c.val), HostUp); ok != c.wantOK {
			t.Errorf("makeEvent(%q,%q) ok=%v; want %v", c.key, c.val, ok, c.wantOK)
		}
	}
	// Valid value populates metadata.
	good, _ := json.Marshal(HostMetadata{Hostname: "h"})
	ev, ok := w.makeEvent([]byte("/coord/hosts/c"), good, HostUp)
	if !ok || ev.Metadata.Hostname != "h" {
		t.Errorf("makeEvent good meta: ok=%v meta=%+v", ok, ev.Metadata)
	}
}

func TestDeliver_ContextDone(t *testing.T) {
	w := &HostWatcher{ch: make(chan HostEvent)} // unbuffered, no reader
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Must return via ctx.Done() rather than block forever.
	done := make(chan struct{})
	go func() { w.deliver(ctx, HostEvent{}); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("deliver did not return on ctx.Done()")
	}
}

func TestDeliver_Success(t *testing.T) {
	w := &HostWatcher{ch: make(chan HostEvent, 1)}
	w.deliver(context.Background(), HostEvent{HostUUID: "a"})
	if got := <-w.ch; got.HostUUID != "a" {
		t.Errorf("deliver: got %+v", got)
	}
}

// ---- Election ------------------------------------------------------

func TestElection_CampaignSuccessAndError(t *testing.T) {
	e := &Election{election: &fakeBackend{}, session: &fakeSession{}, log: discardLogger()}
	if err := e.Campaign(context.Background(), "id"); err != nil {
		t.Errorf("campaign success: %v", err)
	}
	e2 := &Election{election: &fakeBackend{campaignErr: errInjected}, session: &fakeSession{}, log: discardLogger()}
	if err := e2.Campaign(context.Background(), "id"); err == nil {
		t.Error("want campaign error")
	}
}

func TestElection_TryCampaign(t *testing.T) {
	ctx := context.Background()
	// (a) existing leader matches identity + lease -> true
	be := &fakeBackend{leaderResp: &clientv3.GetResponse{
		Kvs: []*mvccpb.KeyValue{{Value: []byte("me"), Lease: 7}},
	}}
	e := &Election{election: be, session: &fakeSession{lease: 7}, log: discardLogger()}
	if won, err := e.TryCampaign(ctx, "me"); !won || err != nil {
		t.Errorf("match: won=%v err=%v; want true,nil", won, err)
	}
	// (b) existing leader, different holder -> false
	be2 := &fakeBackend{leaderResp: &clientv3.GetResponse{
		Kvs: []*mvccpb.KeyValue{{Value: []byte("other"), Lease: 7}},
	}}
	e2 := &Election{election: be2, session: &fakeSession{lease: 7}, log: discardLogger()}
	if won, err := e2.TryCampaign(ctx, "me"); won || err != nil {
		t.Errorf("nomatch: won=%v err=%v; want false,nil", won, err)
	}
	// (c) Leader() errors -> falls through to Campaign (success) -> true
	be3 := &fakeBackend{leaderErr: errInjected}
	e3 := &Election{election: be3, session: &fakeSession{}, log: discardLogger()}
	if won, err := e3.TryCampaign(ctx, "me"); !won || err != nil {
		t.Errorf("leader-err: won=%v err=%v; want true,nil", won, err)
	}
	// (d) Leader() empty -> Campaign success -> true
	be4 := &fakeBackend{leaderResp: &clientv3.GetResponse{}}
	e4 := &Election{election: be4, session: &fakeSession{}, log: discardLogger()}
	if won, err := e4.TryCampaign(ctx, "me"); !won || err != nil {
		t.Errorf("empty-leader: won=%v err=%v; want true,nil", won, err)
	}
	// (e) Campaign errors -> false, err
	be5 := &fakeBackend{leaderResp: &clientv3.GetResponse{}, campaignErr: errInjected}
	e5 := &Election{election: be5, session: &fakeSession{}, log: discardLogger()}
	if won, err := e5.TryCampaign(ctx, "me"); won || err == nil {
		t.Errorf("campaign-err: won=%v err=%v; want false,err", won, err)
	}
}

func TestElection_ResignSuccessAndError(t *testing.T) {
	e := &Election{election: &fakeBackend{}, log: discardLogger()}
	if err := e.Resign(context.Background()); err != nil {
		t.Errorf("resign success: %v", err)
	}
	e2 := &Election{election: &fakeBackend{resignErr: errInjected}, log: discardLogger()}
	if err := e2.Resign(context.Background()); err == nil {
		t.Error("want resign error")
	}
}

func TestElection_Observe(t *testing.T) {
	obs := make(chan clientv3.GetResponse, 4)
	be := &fakeBackend{observeCh: obs}
	e := &Election{election: be, log: discardLogger()}
	obs <- clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{Value: []byte("leader-1")}}} // delivered
	obs <- clientv3.GetResponse{}                                                     // empty kvs -> skipped
	close(obs)
	out := e.Observe(context.Background())
	got, ok := <-out
	if !ok || got != "leader-1" {
		t.Errorf("observe: got %q ok=%v; want leader-1", got, ok)
	}
	// Channel closes once the backend channel drains.
	for range out {
	}
}

func TestElection_ObserveContextDone(t *testing.T) {
	obs := make(chan clientv3.GetResponse, 8)
	be := &fakeBackend{observeCh: obs}
	e := &Election{election: be, log: discardLogger()}
	ctx, cancel := context.WithCancel(context.Background())
	// Fill the out buffer (cap 4) + one more so the goroutine parks on send.
	for i := 0; i < 6; i++ {
		obs <- clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{Value: []byte("x")}}}
	}
	_ = e.Observe(ctx) // no reader; goroutine fills buffer then blocks on send
	time.Sleep(50 * time.Millisecond)
	cancel() // covers the ctx.Done() branch on the blocked send
	time.Sleep(50 * time.Millisecond)
}

func TestElection_Close(t *testing.T) {
	// owned + session -> Close called
	s := &fakeSession{}
	e := &Election{owned: true, session: s, log: discardLogger()}
	if err := e.Close(); err != nil || s.closes != 1 {
		t.Errorf("owned close: err=%v closes=%d", err, s.closes)
	}
	// not owned -> no-op
	s2 := &fakeSession{}
	e2 := &Election{owned: false, session: s2, log: discardLogger()}
	if err := e2.Close(); err != nil || s2.closes != 0 {
		t.Errorf("borrowed close: err=%v closes=%d", err, s2.closes)
	}
	// nil session -> no-op
	e3 := &Election{owned: true, session: nil, log: discardLogger()}
	if err := e3.Close(); err != nil {
		t.Errorf("nil-session close: %v", err)
	}
}

func TestNewElection_NilAndEmptyKey(t *testing.T) {
	if _, err := NewElection(context.Background(), nil, ElectionOptions{Key: "/x"}); err == nil {
		t.Error("nil client: want error")
	}
	if _, err := NewElection(context.Background(), &clientv3.Client{}, ElectionOptions{}); err == nil {
		t.Error("empty key: want error")
	}
}

func TestNewElection_SessionErrorAndSuccess(t *testing.T) {
	// session creation error
	oS := newSession
	t.Cleanup(func() { newSession = oS })
	newSession = func(*clientv3.Client, int, context.Context) (session, error) { return nil, errInjected }
	if _, err := NewElection(context.Background(), &clientv3.Client{}, ElectionOptions{Key: "/k"}); err == nil {
		t.Error("want session error")
	}
	// success with defaults (TTL=0 -> 10, Logger=nil -> discard)
	be := &fakeBackend{}
	swapSeams(t, &fakeSession{lease: 3}, be)
	el, err := NewElection(context.Background(), &clientv3.Client{}, ElectionOptions{Key: "/k"})
	if err != nil {
		t.Fatalf("success: %v", err)
	}
	if !el.owned {
		t.Error("NewElection result should be owned")
	}
	if err := el.Campaign(context.Background(), "id"); err != nil {
		t.Errorf("wired campaign: %v", err)
	}
	// success with explicit TTL + Logger (non-default branches)
	el2, err := NewElection(context.Background(), &clientv3.Client{}, ElectionOptions{Key: "/k", TTL: 20, Logger: slog.Default()})
	if err != nil || el2 == nil {
		t.Errorf("explicit opts: err=%v", err)
	}
}

// ---- ElectionPool --------------------------------------------------

func TestNewElectionPool_DefaultsAndCustom(t *testing.T) {
	p := NewElectionPool(nil, PoolOptions{}) // TTLSec 0 -> 30, Logger nil -> discard
	if got := p.Stats().TTLSec; got != 30 {
		t.Errorf("default TTLSec=%d; want 30", got)
	}
	p2 := NewElectionPool(nil, PoolOptions{TTLSec: 15, Logger: slog.Default()})
	if got := p2.Stats().TTLSec; got != 15 {
		t.Errorf("custom TTLSec=%d; want 15", got)
	}
}

func TestElectionPool_EmptyKey(t *testing.T) {
	p := NewElectionPool(nil, PoolOptions{})
	if _, err := p.Election(context.Background(), ""); err == nil {
		t.Error("empty key: want error")
	}
}

func TestElectionPool_ReuseDistinctAndSessionError(t *testing.T) {
	sessions := []*fakeSession{{lease: 1}, {lease: 2}}
	var idx int
	oS, oE := newSession, newElectionFor
	t.Cleanup(func() { newSession, newElectionFor = oS, oE })
	newElectionFor = func(session, string) electionBackend { return &fakeBackend{} }
	newSession = func(*clientv3.Client, int, context.Context) (session, error) {
		s := sessions[idx]
		idx++
		return s, nil
	}
	p := NewElectionPool(&clientv3.Client{}, PoolOptions{TTLSec: 10})
	ctx := context.Background()

	e1, err := p.Election(ctx, "k1")
	if err != nil {
		t.Fatal(err)
	}
	if err := e1.Close(); err != nil { // borrowed -> no-op
		t.Errorf("borrowed close: %v", err)
	}
	e1b, _ := p.Election(ctx, "k1") // reuse
	if e1.session != e1b.session {
		t.Error("same key must reuse session")
	}
	if got := p.Stats().SessionCount; got != 1 {
		t.Errorf("SessionCount=%d; want 1", got)
	}
	e2, _ := p.Election(ctx, "k2") // distinct
	if e2.session == e1.session {
		t.Error("distinct keys must not share a session")
	}
	if got := p.Stats().SessionCount; got != 2 {
		t.Errorf("SessionCount=%d; want 2", got)
	}

	// session creation error path
	newSession = func(*clientv3.Client, int, context.Context) (session, error) { return nil, errInjected }
	if _, err := p.Election(ctx, "k3"); err == nil {
		t.Error("want pooled session error")
	}
}

func TestElectionPool_CloseErrorAndClosed(t *testing.T) {
	failing := &fakeSession{closeErr: errInjected}
	ok := &fakeSession{}
	seq := []*fakeSession{failing, ok}
	var idx int
	oS, oE := newSession, newElectionFor
	t.Cleanup(func() { newSession, newElectionFor = oS, oE })
	newElectionFor = func(session, string) electionBackend { return &fakeBackend{} }
	newSession = func(*clientv3.Client, int, context.Context) (session, error) {
		s := seq[idx]
		idx++
		return s, nil
	}
	p := NewElectionPool(&clientv3.Client{}, PoolOptions{})
	ctx := context.Background()
	p.Election(ctx, "a")
	p.Election(ctx, "b")
	if err := p.Close(); err == nil {
		t.Error("Close should surface the failing session's error")
	}
	if failing.closes != 1 || ok.closes != 1 {
		t.Errorf("all sessions must be closed; failing=%d ok=%d", failing.closes, ok.closes)
	}
	// Second Close is a no-op.
	if err := p.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	// Election after Close errors.
	if _, err := p.Election(ctx, "c"); err == nil {
		t.Error("Election after Close: want error")
	}
}

// ---- helpers -------------------------------------------------------

func TestDiscardWriter(t *testing.T) {
	n, err := discardW{}.Write([]byte("hello"))
	if n != 5 || err != nil {
		t.Errorf("discardW.Write = %d,%v; want 5,nil", n, err)
	}
}
