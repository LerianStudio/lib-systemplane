//go:build integration

package client_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	tmevent "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/event"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/client"
)

// backend is one store as the scenarios both stores share drive it: open
// starts a tenant-managed Client on it, feeds is its own census (P3-6).
type backend struct {
	open  func(t *testing.T, opts ...client.Option) *tenantEnv
	feeds census
}

var (
	pgBackend = backend{
		open: func(t *testing.T, opts ...client.Option) *tenantEnv {
			return newPGTenantClient(t, opts...).tenantEnv
		},
		feeds: listenBackends,
	}
	mongoBackend = backend{
		open: func(t *testing.T, opts ...client.Option) *tenantEnv {
			return newMongoTenantClient(t, opts...).tenantEnv
		},
		feeds: changeStreams,
	}
)

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

// A write delivers exactly once, to its own tenant: the Set's publication and
// the feed's echo of the same revision are one change, and t2 hears nothing. A
// later write behind the Client, as another replica makes it, reaches t1's
// cached scope through t1's feed, which carried the Set's echo before it.
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

	eventually(t, "t1 delivery of the write", func() bool { return len(rec.written(t1.id)) > 0 })

	remote := t1.seed(t, "written-elsewhere")
	awaitEntry(t, env.c, t1.cacheOnly(t), remote, "t1 cached scope serves the write made elsewhere")
	time.Sleep(3 * deliveryDebounce) // the no-delivery window: room for an echo to land

	got := rec.written(t1.id)
	if len(got) != 2 || got[0].Value != "written" || got[1].Value != remote.Value || got[1].Revision != remote.Revision {
		t.Fatalf("t1 deliveries = %+v, want one carrying %q, then one carrying %+v", got, "written", remote)
	}

	if leaked := rec.written(t2.id); len(leaked) != 0 {
		t.Fatalf("t2 received t1's writes: %+v", leaked)
	}

	requireEntry(t, env.c, t2.cacheOnly(t), client.Entry{Value: "default"}, "t2 from its cache after t1's writes")
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

// Only a getMore counts here: a stopped Mongo feed leaves its cursor idle on
// the server until the cursor times out, a store defect this lane does not own.
func TestIntegration_MongoSuspendedTenantKeepsNoFeed(t *testing.T) {
	suspendedTenantKeepsNoFeed(t, backend{open: mongoBackend.open, feeds: inFlightChangeStreams})
}
