//go:build integration

package client_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	"github.com/LerianStudio/lib-systemplane/v4/internal/client"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Activation materializes a tenant's missing collection instead of watching
// and reading an empty namespace, and the scope then serves the tenant's writes.
func TestIntegration_MongoActivationMaterializesTheCollection(t *testing.T) {
	env := newMongoTenantClient(t)
	tn := env.tenant(t, "t1")
	cacheOnly := tn.cacheOnly(t)

	requireCollection(t, tn, false)

	// No database rides on this read, so the per-request path fails before
	// touching the tenant's database: only the activation it starts can create
	// the collection.
	if _, _, err := env.c.Get(cacheOnly, tenantNS, tenantKey); err == nil {
		t.Fatal("first read without a request database: want the per-request error, got nil")
	}

	env.log.waitFor(t, log.LevelInfo, msgScopeActivated, tn.id)
	requireCollection(t, tn, true)
	requireEntry(t, env.c, cacheOnly, client.Entry{Value: "default"}, "default from the cache")

	if err := env.c.Set(tn.ctx, tenantNS, tenantKey, "written", "it"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, ok, err := env.c.GetEntry(cacheOnly, tenantNS, tenantKey)
	if err != nil || !ok || got.Value != "written" || got.Revision == 0 || got.UpdatedBy != "it" || got.Stale {
		t.Fatalf("read-back from the cache = %+v, ok %v, err %v; want %q at a non-zero revision", got, ok, err, "written")
	}
}

// requireCollection asserts whether the tenant's database holds the entries collection.
func requireCollection(t *testing.T, tn mongoTenant, want bool) {
	t.Helper()

	names, err := tn.db.ListCollectionNames(t.Context(), bson.D{})
	if err != nil {
		t.Fatalf("list collections on %s: %v", tn.dbName, err)
	}

	if got := slices.Contains(names, entriesColl); got != want {
		t.Fatalf("%s holds %s = %v, want %v (collections %v)", tn.dbName, entriesColl, got, want, names)
	}
}

// A write made while a tenant's change stream is down reaches that tenant's
// cache after the reopen with no second write; the other tenant never notices.
func TestIntegration_MongoCursorLossConverges(t *testing.T) {
	env := newMongoTenantClient(t)
	t1, t2 := env.tenant(t, "t1"), env.tenant(t, "t2")
	row1, row2 := t1.write(t, tenantKey, "before-loss", 1), t2.write(t, tenantKey, "t2-value", 1)

	env.activate(t, t1, t2)
	requireFeeds(t, changeStreams, t1.dbName, 1)

	killChangeStream(t, env.log, t1.tenantRef)
	written := t1.write(t, tenantKey, "written-in-gap", row1.Revision+1)

	eventually(t, "t1 serves the write made in the gap", func() bool {
		requireEntry(t, env.c, t2.cacheOnly(t), row2, "t2 while t1's feed recovers")

		return sameEntry(readEntry(t, env.c, t1.cacheOnly(t), "t1 while its feed recovers"), written)
	})

	requireFeeds(t, changeStreams, t1.dbName, 1)
	requireEntry(t, env.c, t2.cacheOnly(t), row2, "t2 after t1's feed recovered")
}

// A tenant the tenant manager answers 404 for still answers per request
// through its request database, logs the failed activation and opens no
// change stream. It fails before Subscribe, so no opened feed is torn down.
func TestIntegration_MongoUnresolvableTenantAnswersPerRequest(t *testing.T) {
	env := newMongoTenantClient(t)
	healthy, unknown := env.tenant(t, "t1"), env.tenant(t, "t2")

	env.tm.mu.Lock()
	delete(env.tm.tenants, unknown.id)
	env.tm.mu.Unlock()

	env.activate(t, healthy)
	requireFeeds(t, changeStreams, healthy.dbName, 1)

	if v, ok, err := env.c.Get(unknown.ctx, tenantNS, tenantKey); err != nil || !ok || v != "default" {
		t.Fatalf("per-request read of %s = %v, ok %v, err %v; want the default", unknown.id, v, ok, err)
	}

	env.log.waitFor(t, log.LevelWarn, msgActivationFailed, unknown.id)
	requireFeeds(t, changeStreams, unknown.dbName, 0)
}

// limits is the document the group test binds to groupKey.
type limits struct {
	Max int `json:"max"`
}

const groupKey = "limits"

// applyRecorder keeps every snapshot an OnApply function receives.
type applyRecorder struct {
	mu   sync.Mutex
	seen []systemplane.Snapshot[limits]
}

func (r *applyRecorder) apply(_ context.Context, a systemplane.Applied[limits]) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.seen = append(r.seen, a.Snapshot)

	return nil
}

func (r *applyRecorder) of(tenant string) []systemplane.Snapshot[limits] {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []systemplane.Snapshot[limits]

	for _, s := range r.seen {
		if s.Tenant == tenant {
			out = append(out, s)
		}
	}

	return out
}

// A group applies each tenant's own document at its own revision, and a write
// through one tenant reaches the applier for that tenant only.
func TestIntegration_MongoGroupAppliesEachTenant(t *testing.T) {
	env := newMongoTenantEnv(t, replicaSet, client.WithDebounce(deliveryDebounce))

	g, err := systemplane.Bind((*systemplane.Client)(env.c), tenantNS, groupKey, limits{}, nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	var rec applyRecorder
	if _, err := g.OnApply(rec.apply); err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	env.start(t)

	t1, t2 := env.tenant(t, "t1"), env.tenant(t, "t2")
	row1, row2 := t1.write(t, groupKey, limits{Max: 1}, 5), t2.write(t, groupKey, limits{Max: 2}, 9)

	env.activate(t, t1, t2)

	both := []systemplane.ApplyStatus{
		{Tenant: t1.id, Desired: row1.Revision, Applied: row1.Revision},
		{Tenant: t2.id, Desired: row2.Revision, Applied: row2.Revision},
	}
	eventually(t, "both tenants applied at their row revisions", func() bool { return slices.Equal(g.Status(), both) })

	first1 := []systemplane.Snapshot[limits]{{Value: limits{Max: 1}, Revision: row1.Revision, Tenant: t1.id}}
	if got := rec.of(t1.id); !slices.Equal(got, first1) {
		t.Fatalf("t1 applied %+v, want %+v", got, first1)
	}

	first2 := []systemplane.Snapshot[limits]{{Value: limits{Max: 2}, Revision: row2.Revision, Tenant: t2.id}}

	if got := rec.of(t2.id); !slices.Equal(got, first2) {
		t.Fatalf("t2 applied %+v, want %+v", got, first2)
	}

	if err := g.Set(t1.ctx, limits{Max: 10}, "it"); err != nil {
		t.Fatalf("group Set on t1: %v", err)
	}

	eventually(t, "t1 applies its write", func() bool { return len(rec.of(t1.id)) > 1 })
	time.Sleep(3 * deliveryDebounce) // the no-delivery window: room for an echo or a leak to land

	got1 := rec.of(t1.id)
	if len(got1) != 2 || got1[1].Value != (limits{Max: 10}) || got1[1].Revision <= row1.Revision {
		t.Fatalf("t1 applied %+v, want its write once, above revision %d", got1, row1.Revision)
	}

	if got := rec.of(t2.id); !slices.Equal(got, first2) {
		t.Fatalf("t2 applied %+v after t1's write, want only %+v", got, first2)
	}

	both[0].Desired, both[0].Applied = got1[1].Revision, got1[1].Revision
	if got := g.Status(); !slices.Equal(got, both) {
		t.Fatalf("Status after t1's write = %+v, want %+v", got, both)
	}
}

// A tenant on a server with no change streams activates through the polling
// feed, and a write made behind the Client reaches its cached scope.
func TestIntegration_MongoPollingTenantConverges(t *testing.T) {
	env := newMongoTenantEnv(t, standalone, client.WithPollInterval(200*time.Millisecond))
	env.start(t)
	tn := env.tenant(t, "t1")

	env.activate(t, tn)
	requireEntry(t, env.c, tn.cacheOnly(t), client.Entry{Value: "default"}, "default from the cache")

	written := tn.write(t, tenantKey, "polled", 1)
	awaitEntry(t, env.c, tn.cacheOnly(t), written, "t1 serves the polled write")
}
