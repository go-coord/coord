//go:build etcd_integration

// Package coord's integration tests boot a real embedded etcd server and
// exercise the liveness / watcher / election paths end-to-end against genuine
// etcd v3 semantics (leases, watches, elections). They are heavy and bind
// loopback TCP ports, so they are gated behind the `etcd_integration` build
// tag: the full-featured CI lane runs them (and measures 100% coverage there),
// while the cross-arch qemu lanes skip them and run only the seam-based unit
// tests in coord_test.go.
package coord

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// embeddedEtcd boots an embed.Etcd whose client + peer listeners bind to free
// loopback ports under t.TempDir(). It lets the suite exercise real etcd v3
// semantics without an external dependency.
func embeddedEtcd(t *testing.T) *clientv3.Client {
	t.Helper()
	root := filepath.Join(t.TempDir(), "etcd")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	clientURL := pickURL(t)
	peerURL := pickURL(t)

	cfg := embed.NewConfig()
	cfg.Name = "test"
	cfg.Dir = root
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.InitialCluster = cfg.Name + "=" + peerURL.String()
	cfg.InitialClusterToken = "coord-test"
	cfg.LogLevel = "warn"
	cfg.LogOutputs = []string{"stderr"}

	srv, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("embed etcd: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	select {
	case <-srv.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		t.Fatal("etcd not ready in 30s")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientURL.String()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("dial etcd: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

func pickURL(t *testing.T) *url.URL {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port
	u, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestIntegration_HostLiveness_RegisterAndStop(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := context.Background()
	hl, err := RegisterHostLiveness(ctx, cli, HostMetadata{
		HostUUID: "host-a", Hostname: "dc1-h1", Hypervisor: "qemu",
	}, LivenessOptions{LeaseTTLSec: 5})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	resp, err := cli.Get(ctx, hl.Key())
	if err != nil || resp.Count != 1 {
		t.Fatalf("get after register: count=%d err=%v", resp.Count, err)
	}
	if err := hl.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	resp, err = cli.Get(ctx, hl.Key())
	if err != nil {
		t.Fatalf("get after stop: %v", err)
	}
	if resp.Count != 0 {
		t.Errorf("key still present after Stop; count=%d", resp.Count)
	}
}

func TestIntegration_HostLiveness_LeaseExpiresAfterClientCrash(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := context.Background()
	hl, err := RegisterHostLiveness(ctx, cli, HostMetadata{HostUUID: "host-b"}, LivenessOptions{LeaseTTLSec: 1})
	if err != nil {
		t.Fatal(err)
	}
	hl.cancel() // simulate crash: cancel keepalive without revoking
	<-hl.done

	time.Sleep(3 * time.Second)
	resp, err := cli.Get(ctx, hl.Key())
	if err != nil {
		t.Fatal(err)
	}
	if resp.Count != 0 {
		t.Errorf("lease did not expire after TTL; count=%d", resp.Count)
	}
}

func TestIntegration_HostLiveness_SelfHealsAfterKeepAliveClose(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := context.Background()
	hl, err := RegisterHostLiveness(ctx, cli, HostMetadata{HostUUID: "host-heal"}, LivenessOptions{LeaseTTLSec: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer hl.Stop(ctx)
	oldLease := hl.LeaseID()
	if _, err := cli.Revoke(ctx, oldLease); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := cli.Get(ctx, hl.Key())
		if err == nil && resp.Count == 1 && hl.LeaseID() != oldLease {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	resp, _ := cli.Get(ctx, hl.Key())
	t.Errorf("self-heal did not run; key count=%d lease=%d oldLease=%d", resp.Count, hl.LeaseID(), oldLease)
}

func TestIntegration_HostWatcher_InitialSnapshot(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hl, err := RegisterHostLiveness(ctx, cli, HostMetadata{HostUUID: "host-existing", Hostname: "early"}, LivenessOptions{LeaseTTLSec: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer hl.Stop(ctx)

	w, err := NewHostWatcher(ctx, cli, WatcherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-w.Events():
		if ev.Kind != HostUp || ev.HostUUID != "host-existing" {
			t.Errorf("got %+v; want HostUp/host-existing", ev)
		}
		if ev.Metadata.Hostname != "early" {
			t.Errorf("metadata hostname = %q; want early", ev.Metadata.Hostname)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no synthetic HostUp event for pre-existing host")
	}
}

func TestIntegration_HostWatcher_FiresDownOnLeaseExpiry(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewHostWatcher(ctx, cli, WatcherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	hl, err := RegisterHostLiveness(ctx, cli, HostMetadata{HostUUID: "host-c"}, LivenessOptions{LeaseTTLSec: 1})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-w.Events():
		if ev.Kind != HostUp || ev.HostUUID != "host-c" {
			t.Errorf("first event = %+v; want HostUp/host-c", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no Up event")
	}
	hl.cancel()
	<-hl.done

	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev := <-w.Events():
			if ev.Kind == HostDown && ev.HostUUID == "host-c" {
				return
			}
		case <-deadline:
			t.Fatal("no HostDown event within 5s of lease expiry")
		}
	}
}

func TestIntegration_HostWatcher_SuppressesSelf(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hl, err := RegisterHostLiveness(ctx, cli, HostMetadata{HostUUID: "me"}, LivenessOptions{LeaseTTLSec: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer hl.Stop(ctx)
	w, err := NewHostWatcher(ctx, cli, WatcherOptions{IncludeSelf: "me"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-w.Events():
		t.Errorf("got event for self: %+v", ev)
	case <-time.After(300 * time.Millisecond):
	}
	_ = w
}

func TestIntegration_Election_FirstCampaignWinsAndOthersBlock(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key := "/test/election/rule-1"
	winner, err := NewElection(ctx, cli, ElectionOptions{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	defer winner.Close()
	if err := winner.Campaign(ctx, "host-a"); err != nil {
		t.Fatalf("winner campaign: %v", err)
	}
	loser, err := NewElection(ctx, cli, ElectionOptions{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	defer loser.Close()
	cctx, ccancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer ccancel()
	if err := loser.Campaign(cctx, "host-b"); err == nil {
		t.Error("loser campaign returned nil; want ctx-deadline-exceeded")
	}
}

func TestIntegration_Election_TryCampaignAndObserve(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key := "/test/election/try-1"

	e, err := NewElection(ctx, cli, ElectionOptions{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	won, err := e.TryCampaign(ctx, "host-a")
	if err != nil || !won {
		t.Fatalf("TryCampaign #1: won=%v err=%v; want true,nil", won, err)
	}
	// Second try by the same election observes itself as leader.
	won, err = e.TryCampaign(ctx, "host-a")
	if err != nil || !won {
		t.Fatalf("TryCampaign #2 (self): won=%v err=%v; want true,nil", won, err)
	}

	observer, err := NewElection(ctx, cli, ElectionOptions{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	// A different session must NOT win while host-a holds it.
	won, err = observer.TryCampaign(ctx, "host-b")
	if err != nil || won {
		t.Fatalf("TryCampaign (other): won=%v err=%v; want false,nil", won, err)
	}
	select {
	case identity := <-observer.Observe(ctx):
		if identity != "host-a" {
			t.Errorf("Observe yielded %q; want host-a", identity)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no leader observed in 3s")
	}
}

func TestIntegration_Election_ResignAllowsSuccession(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key := "/test/election/rule-2"

	first, err := NewElection(ctx, cli, ElectionOptions{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Campaign(ctx, "host-a"); err != nil {
		t.Fatal(err)
	}
	if err := first.Resign(ctx); err != nil {
		t.Fatal(err)
	}
	first.Close()

	second, err := NewElection(ctx, cli, ElectionOptions{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	cctx, ccancel := context.WithTimeout(ctx, 2*time.Second)
	defer ccancel()
	if err := second.Campaign(cctx, "host-b"); err != nil {
		t.Fatalf("second campaign after resign: %v", err)
	}
}

func TestIntegration_Election_NilAndEmptyKey(t *testing.T) {
	ctx := context.Background()
	if _, err := NewElection(ctx, nil, ElectionOptions{Key: "/x"}); err == nil {
		t.Error("nil client: want error")
	}
	cli := embeddedEtcd(t)
	if _, err := NewElection(ctx, cli, ElectionOptions{}); err == nil {
		t.Error("empty key: want error")
	}
}

func TestIntegration_ElectionPool_ReusesSessionAcrossElections(t *testing.T) {
	cli := embeddedEtcd(t)
	pool := NewElectionPool(cli, PoolOptions{TTLSec: 10})
	defer pool.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	key := "/test/pool/rule-1"
	e1, err := pool.Election(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := e1.Close(); err != nil {
		t.Errorf("e1.Close() returned %v; want nil (no-op)", err)
	}
	if got := pool.Stats().SessionCount; got != 1 {
		t.Errorf("after e1.Close: SessionCount=%d; want 1", got)
	}
	e2, err := pool.Election(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if e1.session != e2.session {
		t.Error("Election#2 for same key got a different session")
	}
	e3, err := pool.Election(ctx, "/test/pool/rule-2")
	if err != nil {
		t.Fatal(err)
	}
	if e1.session == e3.session {
		t.Error("different keys share a session")
	}
	if got := pool.Stats().SessionCount; got != 2 {
		t.Errorf("after rule-2: SessionCount=%d; want 2", got)
	}
}

func TestIntegration_ElectionPool_CampaignAndResignWork(t *testing.T) {
	cli := embeddedEtcd(t)
	pool := NewElectionPool(cli, PoolOptions{TTLSec: 10})
	defer pool.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key := "/test/pool/elect-1"

	e1, _ := pool.Election(ctx, key)
	if err := e1.Campaign(ctx, "host-a"); err != nil {
		t.Fatal(err)
	}
	loser, _ := NewElection(ctx, cli, ElectionOptions{Key: key})
	defer loser.Close()
	cctx, ccancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer ccancel()
	if err := loser.Campaign(cctx, "host-b"); err == nil {
		t.Error("loser campaign returned nil; want deadline-exceeded")
	}
	if err := e1.Resign(ctx); err != nil {
		t.Fatal(err)
	}
	cctx2, ccancel2 := context.WithTimeout(ctx, 3*time.Second)
	defer ccancel2()
	if err := loser.Campaign(cctx2, "host-b"); err != nil {
		t.Errorf("loser campaign after resign: %v", err)
	}
}

func TestIntegration_ElectionPool_CloseAndErrors(t *testing.T) {
	cli := embeddedEtcd(t)
	pool := NewElectionPool(cli, PoolOptions{TTLSec: 30})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 3; i++ {
		if _, err := pool.Election(ctx, fmt.Sprintf("/test/pool/key-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if got := pool.Stats().SessionCount; got != 3 {
		t.Fatalf("SessionCount=%d; want 3", got)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if got := pool.Stats(); !got.Closed || got.SessionCount != 0 {
		t.Errorf("Close didn't clear pool: %+v", got)
	}
	if err := pool.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if _, err := pool.Election(ctx, "/test/pool/post-close"); err == nil {
		t.Error("Election after Close should error")
	}
}
