// Package coord is a pure-Go (CGO=0) cross-host coordination layer built on
// etcd v3. It provides three building blocks that a fleet of agents can use to
// coordinate high-availability behaviour across hosts:
//
//   - HostLiveness registers a short-TTL lease at /coord/hosts/<host_uuid> and
//     refreshes it periodically. Lease expiry signals the host is down (process
//     died, network partition, kernel panic); other agents watching the prefix
//     pick up the DELETE event and react. The keep-alive loop self-heals if
//     etcd drops the renewal stream, so a transient hiccup does not silently
//     retire a healthy host forever.
//
//   - HostWatcher subscribes to the prefix and emits HostEvent {Kind: Up|Down,
//     HostUUID, Metadata} on a channel. A freshly-started watcher first replays
//     every existing host as a synthetic Up event so it has a baseline view of
//     the cluster without waiting for renews.
//
//   - Election / ElectionPool wrap etcd-concurrency leader election scoped to a
//     key prefix, so cross-host work (e.g. claiming an orphaned host's
//     resources) coalesces to a single leader per key instead of every agent
//     racing.
//
// All three are pure-Go, CGO=0; the only dependency is go.etcd.io/etcd/client/v3
// and its /concurrency package. The package leaves the etcd dial to the caller
// (which already holds an open *clientv3.Client) so it does not fan out extra
// connections.
package coord

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	// HostsPrefix is the default etcd key prefix under which each agent
	// registers its liveness lease. Callers can override it via
	// LivenessOptions.Prefix / WatcherOptions.Prefix when running multiple
	// independent fleets against a single etcd cluster.
	HostsPrefix = "/coord/hosts/"

	// DefaultLeaseTTLSec is the default etcd lease TTL. 10s gives a 7-13s
	// window between a host dying and other agents observing it — short
	// enough to drive HA failover, long enough to absorb a single missed
	// refresh due to a GC pause.
	DefaultLeaseTTLSec = 10

	// DefaultRefreshInterval is how often a healthy HostLiveness renews its
	// lease. Set to roughly TTL/3 so a single dropped renew does not expire
	// the lease (the same half-period logic as an sd_notify watchdog).
	DefaultRefreshInterval = 3 * time.Second
)

// ---- test seams ----------------------------------------------------
//
// These package-level variables isolate the non-deterministic and
// hard-to-fault-inject edges (JSON marshalling, the wall clock, the timer, and
// the etcd-concurrency session/election constructors) so the full behaviour —
// including every error branch — can be exercised deterministically. Production
// code never reassigns them.
var (
	marshalMeta = json.Marshal
	nowNanos    = func() int64 { return time.Now().UnixNano() }
	afterFunc   = func(d time.Duration) <-chan time.Time { return time.After(d) }

	newSession = func(cli *clientv3.Client, ttl int, ctx context.Context) (session, error) {
		return concurrency.NewSession(cli, concurrency.WithTTL(ttl), concurrency.WithContext(ctx))
	}
	newElectionFor = func(s session, pfx string) electionBackend {
		return concurrency.NewElection(s.(*concurrency.Session), pfx)
	}
)

// etcdKV is the subset of *clientv3.Client the liveness + watcher paths use.
// *clientv3.Client satisfies it; tests substitute a fault-injecting fake.
type etcdKV interface {
	Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error)
	Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
}

// session is the subset of *concurrency.Session the election paths use.
type session interface {
	Lease() clientv3.LeaseID
	Close() error
}

// electionBackend is the subset of *concurrency.Election the Election wrapper
// uses.
type electionBackend interface {
	Campaign(ctx context.Context, val string) error
	Leader(ctx context.Context) (*clientv3.GetResponse, error)
	Resign(ctx context.Context) error
	Observe(ctx context.Context) <-chan *clientv3.GetResponse
}

// HostMetadata is the JSON value stored under each host's liveness key. Keep it
// small — every agent watching the prefix decodes it.
type HostMetadata struct {
	HostUUID   string `json:"host_uuid"`
	Hostname   string `json:"hostname"`
	Hypervisor string `json:"hypervisor"`
	Version    string `json:"version,omitempty"`
	StartedAt  int64  `json:"started_at_unix_ns"`
}

// ---- HostLiveness --------------------------------------------------

// LivenessOptions configures a HostLiveness registration. All fields are
// optional; the zero value uses package defaults.
type LivenessOptions struct {
	Prefix      string        // defaults to HostsPrefix
	LeaseTTLSec int64         // defaults to DefaultLeaseTTLSec
	Refresh     time.Duration // defaults to DefaultRefreshInterval
	Logger      *slog.Logger  // defaults to a discard handler
}

// HostLiveness holds the lease that announces "this agent is alive" to the
// cluster. Stop() is idempotent and deregisters immediately (revokes the lease)
// instead of waiting for the TTL to expire.
//
// The goroutine that owns the KeepAlive channel self-heals if etcd drops the
// stream (network blip, leader change, server restart): blob + ttl are cached
// so the goroutine can re-Grant + re-Put + re-KeepAlive with exponential
// backoff, and leaseID is updated under mu so Stop() always revokes the current
// lease. Without this, a single transient etcd hiccup would silently retire the
// host from the cluster's view forever.
type HostLiveness struct {
	cli      etcdKV
	key      string
	blob     string          // marshalled HostMetadata, cached for re-registration
	ttl      int64           // lease TTL, cached for re-registration
	keepCtx  context.Context // long-lived ctx every KeepAlive call binds to; cancel() ends them
	cancel   context.CancelFunc
	stopOnce sync.Once
	log      *slog.Logger
	done     chan struct{}

	mu      sync.Mutex
	leaseID clientv3.LeaseID // protected by mu (updated on self-heal)
}

// RegisterHostLiveness grants an etcd lease, attaches the host metadata, then
// launches a goroutine that keeps the lease alive until ctx is cancelled or
// Stop() is called. It returns the live HostLiveness handle.
//
// Failure modes:
//   - a nil client or empty HostUUID return immediately;
//   - etcd grant / put / keepalive errors during initial registration return
//     immediately (the partial lease is best-effort revoked);
//   - a keepalive stream close mid-flight triggers transparent
//     re-registration with exponential backoff.
func RegisterHostLiveness(ctx context.Context, cli *clientv3.Client, meta HostMetadata, opts LivenessOptions) (*HostLiveness, error) {
	if cli == nil {
		return nil, fmt.Errorf("coord: nil etcd client")
	}
	return registerHostLiveness(ctx, cli, meta, opts)
}

func registerHostLiveness(ctx context.Context, cli etcdKV, meta HostMetadata, opts LivenessOptions) (*HostLiveness, error) {
	if meta.HostUUID == "" {
		return nil, fmt.Errorf("coord: HostUUID is required")
	}
	prefix := opts.Prefix
	if prefix == "" {
		prefix = HostsPrefix
	}
	ttl := opts.LeaseTTLSec
	if ttl <= 0 {
		ttl = DefaultLeaseTTLSec
	}
	log := opts.Logger
	if log == nil {
		log = discardLogger()
	}
	if meta.StartedAt == 0 {
		// A caller-provided start time wins; default to "now" only when unset.
		meta.StartedAt = nowNanos()
	}
	blob, err := marshalMeta(meta)
	if err != nil {
		return nil, fmt.Errorf("coord: marshal metadata: %w", err)
	}
	key := path.Join(prefix, meta.HostUUID)

	keepCtx, cancel := context.WithCancel(context.Background())
	hl := &HostLiveness{
		cli:     cli,
		key:     key,
		blob:    string(blob),
		ttl:     ttl,
		keepCtx: keepCtx,
		cancel:  cancel,
		log:     log,
		done:    make(chan struct{}),
	}

	// Initial registration binds Grant + Put to the caller's ctx so this call
	// fails fast on a broken etcd. KeepAlive itself binds to keepCtx (not the
	// caller's ctx) so Stop()/cancel() can shut the renewal stream down.
	keepCh, err := hl.registerOnce(ctx)
	if err != nil {
		cancel()
		close(hl.done)
		return nil, err
	}
	go hl.keepAliveLoop(keepCh)
	log.Info("coord: host liveness registered", "key", key, "ttl_sec", ttl, "lease_id", hl.LeaseID())
	return hl, nil
}

// registerOnce performs a single Grant + Put + KeepAlive triple and updates
// h.leaseID on success. It is used both for the initial registration (setupCtx
// = caller's ctx) and for self-heal retries (setupCtx = keepCtx). KeepAlive
// always binds to h.keepCtx so Stop()/cancel() halts the underlying RPCs. On
// any error the partially-granted lease is best-effort revoked so etcd does not
// leak the orphaned key until TTL expiry.
func (h *HostLiveness) registerOnce(setupCtx context.Context) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	grant, err := h.cli.Grant(setupCtx, h.ttl)
	if err != nil {
		return nil, fmt.Errorf("coord: grant lease: %w", err)
	}
	if _, err := h.cli.Put(setupCtx, h.key, h.blob, clientv3.WithLease(grant.ID)); err != nil {
		_, _ = h.cli.Revoke(setupCtx, grant.ID)
		return nil, fmt.Errorf("coord: put liveness key: %w", err)
	}
	keepCh, err := h.cli.KeepAlive(h.keepCtx, grant.ID)
	if err != nil {
		_, _ = h.cli.Revoke(setupCtx, grant.ID)
		return nil, fmt.Errorf("coord: keepalive: %w", err)
	}
	h.mu.Lock()
	h.leaseID = grant.ID
	h.mu.Unlock()
	return keepCh, nil
}

// keepAliveLoop owns the lease for the life of the HostLiveness. It drains the
// active KeepAlive channel; on close (network blip, leader change, restart) it
// re-registers with exponential backoff until keepCtx cancels or etcd recovers.
func (h *HostLiveness) keepAliveLoop(initial <-chan *clientv3.LeaseKeepAliveResponse) {
	defer close(h.done)
	const (
		minBackoff = 100 * time.Millisecond
		maxBackoff = 30 * time.Second
	)
	keepCh := initial
	for {
		closed := false
		for !closed {
			select {
			case <-h.keepCtx.Done():
				return
			case _, ok := <-keepCh:
				if !ok {
					closed = true
				}
				// A successful refresh is uneventful at the default level.
			}
		}
		h.log.Warn("coord: keepalive channel closed; re-registering", "key", h.key)
		backoff := minBackoff
		for {
			select {
			case <-h.keepCtx.Done():
				return
			case <-afterFunc(backoff):
			}
			newCh, err := h.registerOnce(h.keepCtx)
			if err != nil {
				h.log.Warn("coord: re-register attempt failed", "key", h.key, "err", err, "backoff", backoff)
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
			h.log.Info("coord: host liveness re-registered after channel close", "key", h.key, "lease_id", h.LeaseID())
			keepCh = newCh
			break
		}
	}
}

// Stop revokes the lease (immediate deregister, no TTL wait) and shuts the
// keepalive goroutine down. It is idempotent and safe to call from a defer.
func (h *HostLiveness) Stop(ctx context.Context) error {
	var rerr error
	h.stopOnce.Do(func() {
		h.cancel()
		h.mu.Lock()
		lid := h.leaseID
		h.mu.Unlock()
		if _, err := h.cli.Revoke(ctx, lid); err != nil {
			rerr = fmt.Errorf("coord: revoke lease: %w", err)
			return
		}
		h.log.Info("coord: host liveness deregistered", "key", h.key)
	})
	<-h.done
	return rerr
}

// LeaseID returns the etcd lease the registration is currently bound to. It is
// updated by the keep-alive loop on re-registration so the returned value
// tracks the live lease. Callers should not revoke it directly (use Stop()).
func (h *HostLiveness) LeaseID() clientv3.LeaseID {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.leaseID
}

// Key returns the etcd key under which the liveness lease was put.
func (h *HostLiveness) Key() string { return h.key }

// ---- HostWatcher ---------------------------------------------------

// HostEventKind is the discriminator on a HostEvent.
type HostEventKind int

const (
	HostUp   HostEventKind = iota // new agent registered (PUT on its key)
	HostDown                      // lease expired or agent revoked (DELETE)
)

// HostEvent is one observation about a cluster member.
type HostEvent struct {
	Kind     HostEventKind
	HostUUID string
	Metadata HostMetadata // populated on Up; zero on Down
}

// WatcherOptions configures a HostWatcher.
type WatcherOptions struct {
	Prefix      string       // defaults to HostsPrefix
	Logger      *slog.Logger // defaults to discard
	IncludeSelf string       // if non-empty, suppress events for this HostUUID
}

// HostWatcher emits HostEvents on its channel until ctx is cancelled. It closes
// the channel cleanly on exit so consumers can `for range` it. The initial
// Get-with-prefix returns every existing host as a synthetic HostUp event so a
// freshly-started agent gets a baseline view without waiting for renews.
type HostWatcher struct {
	cli  etcdKV
	opts WatcherOptions
	ch   chan HostEvent
	wg   sync.WaitGroup
	log  *slog.Logger
}

// NewHostWatcher starts watching; the returned watcher's channel receives events
// until ctx is cancelled. The channel is buffered to 32 — enough for fleets
// where the events of interest are sparse.
func NewHostWatcher(ctx context.Context, cli *clientv3.Client, opts WatcherOptions) (*HostWatcher, error) {
	if cli == nil {
		return nil, fmt.Errorf("coord: nil etcd client")
	}
	return newHostWatcher(ctx, cli, opts)
}

func newHostWatcher(ctx context.Context, cli etcdKV, opts WatcherOptions) (*HostWatcher, error) {
	if opts.Prefix == "" {
		opts.Prefix = HostsPrefix
	}
	if opts.Logger == nil {
		opts.Logger = discardLogger()
	}
	w := &HostWatcher{
		cli:  cli,
		opts: opts,
		ch:   make(chan HostEvent, 32),
		log:  opts.Logger,
	}

	// Phase 1: snapshot via prefix Get, emitting synthetic Up events for each
	// existing host. WithSerializable keeps the call cheap; the watch below
	// picks up anything missed.
	resp, err := cli.Get(ctx, opts.Prefix, clientv3.WithPrefix(), clientv3.WithSerializable())
	if err != nil {
		return nil, fmt.Errorf("coord: initial get: %w", err)
	}
	for _, kv := range resp.Kvs {
		if ev, ok := w.makeEvent(kv.Key, kv.Value, HostUp); ok {
			w.deliver(ctx, ev)
		}
	}

	// Phase 2: watch the prefix from the snapshot's revision so nothing between
	// the Get and the Watch is missed.
	rev := resp.Header.Revision + 1
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer close(w.ch)
		ch := cli.Watch(ctx, opts.Prefix, clientv3.WithPrefix(), clientv3.WithRev(rev), clientv3.WithPrevKV())
		for wr := range ch {
			if wr.Err() != nil {
				w.log.Warn("coord: watch error", "err", wr.Err())
				continue
			}
			for _, ev := range wr.Events {
				kind := HostUp
				keySrc := ev.Kv.Key
				valSrc := ev.Kv.Value
				if ev.Type == clientv3.EventTypeDelete {
					kind = HostDown
					if ev.PrevKv != nil {
						// Deliver Down with the last-seen metadata so the
						// consumer can name the host without a separate lookup.
						valSrc = ev.PrevKv.Value
					} else {
						valSrc = nil
					}
				}
				if hev, ok := w.makeEvent(keySrc, valSrc, kind); ok {
					w.deliver(ctx, hev)
				}
			}
		}
	}()

	return w, nil
}

// Events returns the channel that fires HostEvents. It closes when the watcher's
// context is cancelled.
func (w *HostWatcher) Events() <-chan HostEvent { return w.ch }

// Wait blocks until the watcher's goroutine exits.
func (w *HostWatcher) Wait() { w.wg.Wait() }

func (w *HostWatcher) makeEvent(key, val []byte, kind HostEventKind) (HostEvent, bool) {
	uuid := path.Base(string(key))
	if uuid == "" || uuid == "/" || uuid == "." {
		return HostEvent{}, false
	}
	if uuid == w.opts.IncludeSelf {
		return HostEvent{}, false
	}
	ev := HostEvent{Kind: kind, HostUUID: uuid}
	if len(val) > 0 {
		var meta HostMetadata
		if err := json.Unmarshal(val, &meta); err == nil {
			ev.Metadata = meta
		}
	}
	return ev, true
}

func (w *HostWatcher) deliver(ctx context.Context, ev HostEvent) {
	select {
	case w.ch <- ev:
	case <-ctx.Done():
	}
}

// ---- Leader election ----------------------------------------------

// Election wraps etcd-concurrency's election with helpers that time out
// cleanly. One Election per coordination key means work coalesces to a single
// leader per key, avoiding an N-way thrash on a host-down event.
type Election struct {
	session  session
	election electionBackend
	owned    bool
	log      *slog.Logger
	key      string
}

// ElectionOptions configures a leader election.
type ElectionOptions struct {
	Key      string       // etcd prefix the election locks on, e.g. "/coord/elect/<rule_uuid>"
	TTL      int          // session TTL in seconds; default 10
	Identity string       // value written to the leader key (defaults to host UUID)
	Logger   *slog.Logger // defaults to a discard handler
}

// NewElection creates a fresh election bound to a new concurrency session. The
// session is owned by the Election — call Close() to release it. Pass the host
// UUID as the campaign identity so the leader is human-readable in etcd.
func NewElection(ctx context.Context, cli *clientv3.Client, opts ElectionOptions) (*Election, error) {
	if cli == nil {
		return nil, fmt.Errorf("coord: nil etcd client")
	}
	if opts.Key == "" {
		return nil, fmt.Errorf("coord: election key is required")
	}
	ttl := opts.TTL
	if ttl <= 0 {
		ttl = 10
	}
	log := opts.Logger
	if log == nil {
		log = discardLogger()
	}
	sess, err := newSession(cli, ttl, ctx)
	if err != nil {
		return nil, fmt.Errorf("coord: new session: %w", err)
	}
	return &Election{
		session:  sess,
		election: newElectionFor(sess, opts.Key),
		owned:    true,
		log:      log,
		key:      opts.Key,
	}, nil
}

// Campaign blocks until the caller becomes leader OR ctx is cancelled. It
// returns nil on victory, an error wrapping ctx.Err() on cancellation.
func (e *Election) Campaign(ctx context.Context, identity string) error {
	if err := e.election.Campaign(ctx, identity); err != nil {
		return fmt.Errorf("coord: campaign: %w", err)
	}
	e.log.Info("coord: became leader", "key", e.key, "identity", identity)
	return nil
}

// TryCampaign tries to become leader without blocking. It returns (true, nil)
// on victory; (false, nil) when another agent already holds leadership;
// (false, err) on transport errors.
func (e *Election) TryCampaign(ctx context.Context, identity string) (bool, error) {
	resp, err := e.election.Leader(ctx)
	if err == nil && resp != nil && len(resp.Kvs) > 0 {
		// Someone already holds it; it is only ours if the value + lease match.
		if string(resp.Kvs[0].Value) == identity && resp.Kvs[0].Lease == int64(e.session.Lease()) {
			return true, nil
		}
		return false, nil
	}
	if err := e.election.Campaign(ctx, identity); err != nil {
		return false, fmt.Errorf("coord: try-campaign: %w", err)
	}
	return true, nil
}

// Resign relinquishes leadership without closing the session, so a follower
// watching Observe() picks up the change.
func (e *Election) Resign(ctx context.Context) error {
	if err := e.election.Resign(ctx); err != nil {
		return fmt.Errorf("coord: resign: %w", err)
	}
	return nil
}

// Observe returns a channel that fires the current leader's identity on every
// transition. It lets followers learn who is leading without campaigning.
func (e *Election) Observe(ctx context.Context) <-chan string {
	out := make(chan string, 4)
	go func() {
		defer close(out)
		ch := e.election.Observe(ctx)
		for resp := range ch {
			// etcd v3.7 changed Observe to a channel of POINTERS, so a nil
			// element is now representable where it was not before.
			if resp != nil && len(resp.Kvs) > 0 {
				select {
				case out <- string(resp.Kvs[0].Value):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// Close releases the underlying session + lease. After Close() the Election is
// unusable. It is idempotent, and a no-op when the Election was borrowed from an
// ElectionPool (the pool owns the session lifetime).
func (e *Election) Close() error {
	if !e.owned || e.session == nil {
		return nil
	}
	e.owned = false
	return e.session.Close()
}

// ---- ElectionPool --------------------------------------------------

// ElectionPool keeps one long-lived etcd session per election key, avoiding the
// grant/revoke cycle a per-call NewElection pattern pays on every host-down ×
// key combination. The first encounter of a key grants a session; subsequent
// borrows reuse it until the pool is closed. A crashed agent's sessions are
// auto-revoked by etcd within one TTL.
type ElectionPool struct {
	cli    *clientv3.Client
	ttlSec int
	log    *slog.Logger

	mu     sync.Mutex
	closed bool
	keyed  map[string]session
}

// PoolOptions configures the pool. Zero values pick sensible defaults:
// TTLSec=30, Logger=discard.
type PoolOptions struct {
	TTLSec int          // session lease TTL; defaults to 30s
	Logger *slog.Logger // defaults to a discard handler
}

// NewElectionPool builds a fresh pool. It owns no state until the first
// Election call; sessions are created lazily.
func NewElectionPool(cli *clientv3.Client, opts PoolOptions) *ElectionPool {
	ttl := opts.TTLSec
	if ttl <= 0 {
		ttl = 30
	}
	log := opts.Logger
	if log == nil {
		log = discardLogger()
	}
	return &ElectionPool{
		cli:    cli,
		ttlSec: ttl,
		log:    log,
		keyed:  make(map[string]session),
	}
}

// Election returns a non-owning Election bound to the pool's session for key.
// The first call for a key grants a session; subsequent calls reuse it. The
// returned Election's Close() is a no-op — the pool keeps the session alive.
func (p *ElectionPool) Election(ctx context.Context, key string) (*Election, error) {
	if key == "" {
		return nil, fmt.Errorf("coord: election key is required")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, fmt.Errorf("coord: pool is closed")
	}
	sess, ok := p.keyed[key]
	if !ok {
		newSess, err := newSession(p.cli, p.ttlSec, ctx)
		if err != nil {
			return nil, fmt.Errorf("coord: new pooled session: %w", err)
		}
		p.keyed[key] = newSess
		sess = newSess
	}
	return &Election{
		session:  sess,
		election: newElectionFor(sess, key),
		owned:    false, // pool retains ownership
		log:      p.log,
		key:      key,
	}, nil
}

// Close releases every pooled session. It is safe to call multiple times.
// Closing a session revokes its lease, so any active leader's hold on its key
// disappears immediately, freeing a successor to elect.
func (p *ElectionPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	keyed := p.keyed
	p.keyed = nil
	p.mu.Unlock()
	var rerr error
	for _, s := range keyed {
		if err := s.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}
	return rerr
}

// Stats returns diagnostics about the pool — useful for tests and /metrics
// surfacing. SessionCount is the number of long-lived sessions currently held.
func (p *ElectionPool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PoolStats{
		SessionCount: len(p.keyed),
		Closed:       p.closed,
		TTLSec:       p.ttlSec,
	}
}

// PoolStats is a read-only snapshot of an ElectionPool's state.
type PoolStats struct {
	SessionCount int
	Closed       bool
	TTLSec       int
}

// ---- helpers -------------------------------------------------------

type discardW struct{}

func (discardW) Write(b []byte) (int, error) { return len(b), nil }

// discardLogger returns a logger whose records are dropped. It is used as the
// default when a caller supplies no *slog.Logger.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardW{}, &slog.HandlerOptions{Level: slog.LevelError}))
}
