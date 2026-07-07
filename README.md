<p align="center"><img src="https://raw.githubusercontent.com/go-coord/brand/main/social/go-coord.png" alt="go-coord/coord" width="720"></p>

# coord — go-coord

[![Docs](https://img.shields.io/badge/docs-mkdocs--material-0079A8)](https://go-coord.github.io/docs/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](https://go.dev/dl/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-1a7f37)](#tests--coverage)

**Pure-Go (no cgo) cross-host coordination on etcd v3** — host liveness with
TTL leases, a host up/down watcher, and leader election (single and pooled),
extracted as a standalone, reusable module.

`coord` gives a fleet of agents the primitives they need to coordinate
high-availability behaviour across hosts:

- **HostLiveness** registers a short-TTL lease at `/coord/hosts/<host_uuid>` and
  refreshes it in the background. Lease expiry signals the host is down (process
  died, network partition, kernel panic). The keep-alive loop **self-heals** if
  etcd drops the renewal stream, so a transient hiccup doesn't silently retire a
  healthy host forever.
- **HostWatcher** subscribes to the prefix and emits `HostEvent {Kind: Up|Down,
  HostUUID, Metadata}`. A freshly-started watcher first replays every existing
  host as a synthetic `Up` event, so it has a baseline view of the cluster
  without waiting for renews. `Down` events carry the host's last-seen metadata.
- **Election / ElectionPool** wrap etcd-concurrency leader election scoped to a
  key prefix, so cross-host work coalesces to a single leader per key instead of
  every agent racing. `ElectionPool` keeps one long-lived session per key,
  avoiding a grant/revoke cycle on every event.

The package leaves the etcd dial to the caller (which already holds an open
`*clientv3.Client`), so it doesn't fan out extra connections. The etcd client is
a normal module dependency — nothing is vendored.

CGO-free, **100% test coverage** (including every error branch), `gofmt` +
`go vet` clean, and green across the six 64-bit Go targets (amd64, arm64,
riscv64, loong64, ppc64le, s390x).

## Install

```sh
go get github.com/go-coord/coord
```

## Usage

```go
package main

import (
	"context"
	"log"

	"github.com/go-coord/coord"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	ctx := context.Background()

	// 1. Announce this host is alive (10s lease, refreshed automatically).
	hl, err := coord.RegisterHostLiveness(ctx, cli, coord.HostMetadata{
		HostUUID: "host-uuid-1", Hostname: "dc1-h1", Hypervisor: "qemu",
	}, coord.LivenessOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer hl.Stop(ctx) // immediate deregister, no TTL wait

	// 2. Watch the cluster for hosts coming up and going down.
	w, err := coord.NewHostWatcher(ctx, cli, coord.WatcherOptions{IncludeSelf: "host-uuid-1"})
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for ev := range w.Events() {
			switch ev.Kind {
			case coord.HostUp:
				log.Printf("host up:   %s (%s)", ev.HostUUID, ev.Metadata.Hostname)
			case coord.HostDown:
				log.Printf("host down: %s", ev.HostUUID)
			}
		}
	}()

	// 3. Elect a single leader per coordination key.
	el, err := coord.NewElection(ctx, cli, coord.ElectionOptions{Key: "/coord/elect/reconcile"})
	if err != nil {
		log.Fatal(err)
	}
	defer el.Close()
	if err := el.Campaign(ctx, "host-uuid-1"); err != nil { // blocks until leader
		log.Fatal(err)
	}
	log.Println("we are the leader; do the coordinated work")
}
```

For many keys, reuse one session per key with an `ElectionPool`:

```go
pool := coord.NewElectionPool(cli, coord.PoolOptions{TTLSec: 30})
defer pool.Close()

el, _ := pool.Election(ctx, "/coord/elect/rule-42")
won, err := el.TryCampaign(ctx, "host-uuid-1") // non-blocking
```

## API

```go
// Host liveness
func RegisterHostLiveness(ctx context.Context, cli *clientv3.Client, meta HostMetadata, opts LivenessOptions) (*HostLiveness, error)
func (h *HostLiveness) Stop(ctx context.Context) error
func (h *HostLiveness) LeaseID() clientv3.LeaseID
func (h *HostLiveness) Key() string

// Host watcher
func NewHostWatcher(ctx context.Context, cli *clientv3.Client, opts WatcherOptions) (*HostWatcher, error)
func (w *HostWatcher) Events() <-chan HostEvent
func (w *HostWatcher) Wait()

// Leader election
func NewElection(ctx context.Context, cli *clientv3.Client, opts ElectionOptions) (*Election, error)
func (e *Election) Campaign(ctx context.Context, identity string) error
func (e *Election) TryCampaign(ctx context.Context, identity string) (bool, error)
func (e *Election) Resign(ctx context.Context) error
func (e *Election) Observe(ctx context.Context) <-chan string
func (e *Election) Close() error

// Pooled election (one long-lived session per key)
func NewElectionPool(cli *clientv3.Client, opts PoolOptions) *ElectionPool
func (p *ElectionPool) Election(ctx context.Context, key string) (*Election, error)
func (p *ElectionPool) Close() error
func (p *ElectionPool) Stats() PoolStats
```

## Tests & coverage

The suite has two layers:

- **Seam-based unit tests** drive every branch — including the etcd grant / put /
  keepalive / revoke / watch error paths, the keep-alive self-heal + backoff, and
  the election/pool logic — deterministically via in-package fakes, with no
  network. They carry no build tag, so they build and run on **every** arch,
  including the cross-arch qemu lanes.
- **Embedded-etcd integration tests** (behind the `etcd_integration` build tag)
  boot a real `embed.Etcd` on loopback and exercise the whole thing end-to-end
  against genuine etcd v3 semantics. They run on the ubuntu/macos lanes, where
  **100% coverage** is measured.

```sh
# full suite + coverage (needs a Go toolchain that can build the embedded server)
COVERPKG=$(go list ./... | paste -sd, -)
go test -tags etcd_integration -race -coverpkg="$COVERPKG" -coverprofile=cover.out ./...
go tool cover -func=cover.out | tail -1   # 100.0%

# pure-logic, network-free lane (what the qemu arches run)
go test ./...
```

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright the go-coord/coord authors.
