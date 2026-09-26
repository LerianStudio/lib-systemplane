//go:build integration

package client_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	tmevent "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/event"
	"github.com/LerianStudio/lib-observability/v4/log"
	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	"github.com/LerianStudio/lib-systemplane/v4/internal/client"
)

// backend is one store as the scenarios both stores share drive it: build
// opens a tenant-managed Client on it, not yet started, feeds is its own
// census (P3-6).
type backend struct {
	build func(t *testing.T, opts ...client.Option) *tenantEnv
	feeds census
}

var (
	pgBackend = backend{
		build: func(t *testing.T, opts ...client.Option) *tenantEnv {
			return newPGTenantEnv(t, opts...).tenantEnv
		},
		feeds: listenBackends,
	}
	mongoBackend = backend{
		build: func(t *testing.T, opts ...client.Option) *tenantEnv {
			return newMongoTenantEnv(t, replicaSet, opts...).tenantEnv
		},
		feeds: changeStreams,
	}
)

// open is a started build.
func (b backend) open(t *testing.T, opts ...client.Option) *tenantEnv {
	t.Helper()

	env := b.build(t, opts...)
	env.start(t)

	return env
}

// A tenant's first read activates its scope and nobody else's; from then on
// the cache answers for it.
func firstReadActivatesOnlyThatTenant(t *testing.T, b backend) {
	env := b.open(t)
	t1, t2 := env.add(t, "t1"), env.add(t, "t2")
	row1, row2 := t1.seed(t, "t1-value"), t2.seed(t, "t2-value")

	requireEntry(t, env.c, t1.ctx, row1, "t1 first read")
	env.log.waitFor(t, log.LevelInfo, msgScopeActivated, t1.id)
	requireEntry(t, env.c, t1.ctx, row1, "t1 after activation")

	requireFeeds(t, b.feeds, t1.dbName, 1)
	requireFeeds(t, b.feeds, t2.dbName, 0)

	requireEntry(t, env.c, t1.cacheOnly(t), row1, "t1 from the cache")

	requireEntry(t, env.c, t2.ctx, row2, "t2 first read")
}

func TestIntegration_PostgresFirstReadActivatesOnlyThatTenant(t *testing.T) {
	firstReadActivatesOnlyThatTenant(t, pgBackend)
}

func TestIntegration_MongoFirstReadActivatesOnlyThatTenant(t *testing.T) {
	firstReadActivatesOnlyThatTenant(t, mongoBackend)
}

// changeRecorder keeps every delivery an OnChange subscriber receives.
type changeRecorder struct {
	mu      sync.Mutex
	changes []client.Change
}

func (r *changeRecorder) record(_ context.Context, ch client.Change) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.changes = append(r.changes, ch)
}

// written returns tenant's deliveries backed by a row. With no row seeded, an
// activation announces the default at revision 0, so anything above is a write.
func (r *changeRecorder) written(tenant string) []client.Change {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []client.Change

	for _, ch := range r.changes {
		if ch.Tenant == tenant && ch.Revision > 0 {
			out = append(out, ch)
		}
	}

	return out
}

// await asserts tenant's row-backed deliveries become exactly want, in order,
// and stay so through three debounce periods: room for an echo or a leak to land.
func (r *changeRecorder) await(t *testing.T, tenant string, want ...client.Entry) {
	t.Helper()

	eventually(t, fmt.Sprintf("%d deliveries for %s", len(want), tenant), func() bool { return len(r.written(tenant)) >= len(want) })
	time.Sleep(3 * deliveryDebounce)

	exp := make([]client.Change, len(want))
	for i, e := range want {
		exp[i] = client.Change{Tenant: tenant, Namespace: tenantNS, Key: tenantKey, Revision: e.Revision, Value: e.Value}
	}

	if got := r.written(tenant); !slices.Equal(got, exp) {
		t.Fatalf("%s deliveries = %+v, want %+v", tenant, got, exp)
	}
}

// One subscription hears every tenant, each write once and tagged with its own
// tenant: a Set is in its tenant's cache when it returns and in no other's, its
// feed echo is not a second change, and a write behind the Client reaches t1's
// cached scope through t1's feed.
func writeDeliversOnceToItsTenantOnly(t *testing.T, b backend) {
	env := b.open(t, client.WithDebounce(deliveryDebounce))
	t1, t2 := env.add(t, "t1"), env.add(t, "t2")

	var rec changeRecorder
	if _, err := env.c.OnChange(tenantNS, tenantKey, rec.record); err != nil {
		t.Fatalf("OnChange: %v", err)
	}

	env.activate(t, t1, t2)

	if err := env.c.Set(t1.ctx, tenantNS, tenantKey, "written", "it"); err != nil {
		t.Fatalf("Set on t1: %v", err)
	}

	written := t1.stored(t)
	if got := readEntry(t, env.c, t1.cacheOnly(t), "t1 read-back from the cache"); got.Value != written.Value ||
		got.Revision != written.Revision || got.UpdatedBy != written.UpdatedBy || got.Stale {
		t.Fatalf("t1 read-back from the cache = %+v, want the stored %+v", got, written)
	}

	requireEntry(t, env.c, t2.cacheOnly(t), client.Entry{Value: "default"}, "t2 after t1's write")
	rec.await(t, t1.id, written)

	remote := t1.seed(t, "written-elsewhere")
	awaitEntry(t, env.c, t1.cacheOnly(t), remote, "t1 cached scope serves the write made elsewhere")

	if err := env.c.Set(t2.ctx, tenantNS, tenantKey, "written-t2", "it"); err != nil {
		t.Fatalf("Set on t2: %v", err)
	}

	rec.await(t, t2.id, t2.stored(t))
	rec.await(t, t1.id, written, remote)
}

func TestIntegration_PostgresWriteDeliversOnceToItsTenantOnly(t *testing.T) {
	writeDeliversOnceToItsTenantOnly(t, pgBackend)
}

func TestIntegration_MongoWriteDeliversOnceToItsTenantOnly(t *testing.T) {
	writeDeliversOnceToItsTenantOnly(t, mongoBackend)
}

// Concurrent first reads of one tenant activate it once and open one feed.
func concurrentFirstReadsOpenOneFeed(t *testing.T, b backend) {
	env := b.open(t)
	tn := env.add(t, "t1")

	release := make(chan struct{})

	var wg sync.WaitGroup

	for range 16 {
		wg.Go(func() {
			<-release

			if _, _, err := env.c.Get(tn.ctx, tenantNS, tenantKey); err != nil {
				t.Errorf("concurrent first read: %v", err)
			}
		})
	}

	close(release)
	wg.Wait()

	env.log.waitFor(t, log.LevelInfo, msgScopeActivated, tn.id)
	requireFeeds(t, b.feeds, tn.dbName, 1)

	// The store shares one feed among a tenant's subscriptions, so the census
	// cannot tell one activation from two; the engine logs each one.
	if n := env.log.count(log.LevelInfo, msgScopeActivated, tn.id); n != 1 {
		t.Fatalf("%s activated %d times, want once", tn.id, n)
	}
}

func TestIntegration_PostgresConcurrentFirstReadsOpenOneFeed(t *testing.T) {
	concurrentFirstReadsOpenOneFeed(t, pgBackend)
}

func TestIntegration_MongoConcurrentFirstReadsOpenOneFeed(t *testing.T) {
	concurrentFirstReadsOpenOneFeed(t, mongoBackend)
}

// A write racing a tenant's first read is in the cache once the scope is up,
// with no second write to repair it.
func writeRacingFirstReadSurvivesActivation(t *testing.T, b backend) {
	env := b.open(t)

	tenants := make([]liveTenant, 5)
	for i := range tenants {
		tenants[i] = env.add(t, fmt.Sprintf("r%d", i))
	}

	release := make(chan struct{})

	var wg sync.WaitGroup

	for _, tn := range tenants {
		wg.Go(func() {
			<-release

			if err := env.c.Set(tn.ctx, tenantNS, tenantKey, "written-"+tn.id, "it"); err != nil {
				t.Errorf("%s racing Set: %v", tn.id, err)
			}
		})
		wg.Go(func() {
			<-release

			if _, _, err := env.c.Get(tn.ctx, tenantNS, tenantKey); err != nil {
				t.Errorf("%s racing first read: %v", tn.id, err)
			}
		})
	}

	close(release)
	wg.Wait()

	for _, tn := range tenants {
		env.log.waitFor(t, log.LevelInfo, msgScopeActivated, tn.id)
		awaitEntry(t, env.c, tn.cacheOnly(t), tn.stored(t), tn.id+" cached after activation")
	}
}

func TestIntegration_PostgresWriteRacingFirstReadSurvivesActivation(t *testing.T) {
	writeRacingFirstReadSurvivesActivation(t, pgBackend)
}

func TestIntegration_MongoWriteRacingFirstReadSurvivesActivation(t *testing.T) {
	writeRacingFirstReadSurvivesActivation(t, mongoBackend)
}

// A suspended tenant loses its feed and its scope. t2, suspended before any
// read, has no retry cooldown: only the blocked marker keeps its reads from
// activating it, and they answer per request with no feed.
func suspendedTenantKeepsNoFeed(t *testing.T, b backend) {
	env := b.open(t)
	t1, t2 := env.add(t, "t1"), env.add(t, "t2")

	env.activate(t, t1)
	eventually(t, "t1's feed in the census", func() bool { return b.feeds(t, t1.dbName) == 1 })

	for _, tn := range []liveTenant{t1, t2} {
		if err := env.c.HandleTenantLifecycle(t.Context(), tmevent.TenantLifecycleEvent{
			EventType: tmevent.EventTenantSuspended,
			TenantID:  tn.id,
		}); err != nil {
			t.Fatalf("HandleTenantLifecycle(suspended) for %s: %v", tn.id, err)
		}
	}

	requireFeeds(t, b.feeds, t1.dbName, 0)

	if _, _, err := env.c.GetEntry(t1.cacheOnly(t), tenantNS, tenantKey); !errors.Is(err, client.ErrTenantConnectionMissing) {
		t.Fatalf("t1 read with no request database after suspension: err %v, want %v", err, client.ErrTenantConnectionMissing)
	}

	requireEntry(t, env.c, t2.ctx, client.Entry{Value: "default"}, "t2 read while suspended")
	requireFeeds(t, b.feeds, t2.dbName, 0)
}

func TestIntegration_PostgresSuspendedTenantKeepsNoFeed(t *testing.T) {
	suspendedTenantKeepsNoFeed(t, pgBackend)
}

func TestIntegration_MongoSuspendedTenantKeepsNoFeed(t *testing.T) {
	suspendedTenantKeepsNoFeed(t, mongoBackend)
}

// limits is the document the group test binds to groupKey.
type limits struct {
	Max int `json:"max"`
}

const groupKey = "limits"

// applyRecorder keeps the last snapshot an OnApply function received per tenant.
type applyRecorder struct {
	mu   sync.Mutex
	last map[string]systemplane.Snapshot[limits]
}

func (r *applyRecorder) apply(_ context.Context, a systemplane.Applied[limits]) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.last == nil {
		r.last = make(map[string]systemplane.Snapshot[limits])
	}

	r.last[a.Tenant] = a.Snapshot

	return nil
}

func (r *applyRecorder) of(tenant string) systemplane.Snapshot[limits] {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.last[tenant]
}

// A group applies each tenant's own document at its own revision: the applier
// ends holding each tenant's write, t2's leaves t1's alone, and Status reports
// both applied. State only: a delivery count is not part of the contract.
func groupAppliesEachTenant(t *testing.T, b backend) {
	env := b.build(t, client.WithDebounce(deliveryDebounce))

	g, err := systemplane.Bind((*systemplane.Client)(env.c), tenantNS, groupKey, limits{}, nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	var rec applyRecorder
	if _, err := g.OnApply(rec.apply); err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	env.start(t)

	t1, t2 := env.add(t, "t1"), env.add(t, "t2")
	env.activate(t, t1, t2)

	snaps := make([]systemplane.Snapshot[limits], 0, 2)
	want := make([]systemplane.ApplyStatus, 0, 2)

	for i, tn := range []liveTenant{t1, t2} {
		if err := g.Set(tn.ctx, limits{Max: i + 1}, "it"); err != nil {
			t.Fatalf("group Set on %s: %v", tn.id, err)
		}

		snap, err := g.Snapshot(tn.cacheOnly(t))
		if err != nil || snap.Value != (limits{Max: i + 1}) || snap.Revision == 0 {
			t.Fatalf("%s Snapshot after its Set = %+v, err %v; want its document at a non-zero revision", tn.id, snap, err)
		}

		eventually(t, tn.id+" applies its document", func() bool { return rec.of(tn.id) == snap })

		snaps = append(snaps, snap)
		want = append(want, systemplane.ApplyStatus{Tenant: tn.id, Desired: snap.Revision, Applied: snap.Revision})
	}

	eventually(t, "Status reports both tenants applied", func() bool { return slices.Equal(g.Status(), want) })
	time.Sleep(3 * deliveryDebounce) // room for t2's write to leak into t1

	if got := rec.of(t1.id); got != snaps[0] {
		t.Fatalf("t1 holds %+v after t2's write, want its own %+v", got, snaps[0])
	}
}

func TestIntegration_PostgresGroupAppliesEachTenant(t *testing.T) {
	groupAppliesEachTenant(t, pgBackend)
}

func TestIntegration_MongoGroupAppliesEachTenant(t *testing.T) {
	groupAppliesEachTenant(t, mongoBackend)
}
