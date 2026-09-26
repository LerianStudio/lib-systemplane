//go:build integration

package client_test

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/client"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// A tenant's first read activates its scope and nobody else's; from then on
// the cache answers for it.
func TestIntegration_MongoFirstReadActivatesOnlyThatTenant(t *testing.T) {
	env := newMongoTenantClient(t)
	t1, t2 := env.tenant(t, "t1"), env.tenant(t, "t2")
	row1, row2 := t1.seed(t, "t1-value"), t2.seed(t, "t2-value")

	requireEntry(t, env.c, t1.ctx, row1, "t1 first read")
	env.log.waitFor(t, log.LevelInfo, msgScopeActivated, t1.id)
	requireEntry(t, env.c, t1.ctx, row1, "t1 after activation")

	requireChangeStreams(t, t1.dbName, 1)
	requireChangeStreams(t, t2.dbName, 0)

	requireEntry(t, env.c, t1.cacheOnly(t), row1, "t1 from the cache")

	requireEntry(t, env.c, t2.ctx, row2, "t2 first read")
}

// A write delivers exactly once, to its own tenant: the Set's publication and
// the change-stream echo of the same revision are one change, and t2 hears nothing.
func TestIntegration_MongoWriteDeliversOnceToItsTenantOnly(t *testing.T) {
	const debounce = 50 * time.Millisecond

	env := newMongoTenantClient(t, client.WithDebounce(debounce))
	t1, t2 := env.tenant(t, "t1"), env.tenant(t, "t2")

	var rec changeRecorder
	if _, err := env.c.OnChange(tenantNS, tenantKey, rec.record); err != nil {
		t.Fatalf("OnChange: %v", err)
	}

	env.activate(t, t1, t2)

	if err := env.c.Set(t1.ctx, tenantNS, tenantKey, "written", "it"); err != nil {
		t.Fatalf("Set on t1: %v", err)
	}

	eventually(t, "t1 delivery of the write", func() bool { return len(rec.written(t1.id)) > 0 })
	time.Sleep(3 * debounce) // the no-delivery window: room for an echo to land

	got := rec.written(t1.id)
	if len(got) != 1 || got[0].Value != "written" {
		t.Fatalf("t1 deliveries of the write = %+v, want exactly one carrying %q", got, "written")
	}

	if leaked := rec.written(t2.id); len(leaked) != 0 {
		t.Fatalf("t2 received t1's write: %+v", leaked)
	}
}

// Concurrent first reads of one tenant open exactly one feed for it.
func TestIntegration_MongoConcurrentFirstReadsOpenOneFeed(t *testing.T) {
	env := newMongoTenantClient(t)
	tn := env.tenant(t, "t1")

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
	requireChangeStreams(t, tn.dbName, 1)
}

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
	row1, row2 := t1.seed(t, "before-loss"), t2.seed(t, "t2-value")

	env.activate(t, t1, t2)
	requireChangeStreams(t, t1.dbName, 1)

	killChangeStream(t, t1.dbName)
	written := t1.write(t, "written-in-gap", row1.Revision+1)
	env.log.waitFor(t, log.LevelWarn, "change stream disconnected, reconnecting", t1.id)

	eventually(t, "t1 serves the write made in the gap", func() bool {
		requireEntry(t, env.c, t2.cacheOnly(t), row2, "t2 while t1's feed recovers")

		return env.servesCached(t, t1, written)
	})

	requireChangeStreams(t, t1.dbName, 1)
	requireEntry(t, env.c, t2.cacheOnly(t), row2, "t2 after t1's feed recovered")
}

// A write racing a tenant's first read is in the cache once the scope is up,
// with no second write to repair it.
func TestIntegration_MongoWriteRacingFirstReadSurvivesActivation(t *testing.T) {
	env := newMongoTenantClient(t)

	tenants := make([]mongoTenant, 5)
	for i := range tenants {
		tenants[i] = env.tenant(t, fmt.Sprintf("r%d", i))
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

		want := tn.stored(t)
		eventually(t, tn.id+" cached at its write", func() bool { return env.servesCached(t, tn, want) })
	}
}

// A tenant the tenant manager does not know keeps no change stream while its
// request database still answers; a healthy tenant's census of 1 gives the zero meaning.
func TestIntegration_MongoFailedActivationLeavesNoFeed(t *testing.T) {
	env := newMongoTenantClient(t)
	healthy, unknown := env.tenant(t, "t1"), env.tenant(t, "t2")

	env.tm.mu.Lock()
	delete(env.tm.tenants, unknown.id) // the connections route answers 404 for it
	env.tm.mu.Unlock()

	env.activate(t, healthy)
	requireChangeStreams(t, healthy.dbName, 1)

	if v, ok, err := env.c.Get(unknown.ctx, tenantNS, tenantKey); err != nil || !ok || v != "default" {
		t.Fatalf("per-request read of %s = %v, ok %v, err %v; want the default", unknown.id, v, ok, err)
	}

	env.log.waitFor(t, log.LevelWarn, "scope activation failed; reads stay per-request until a later attempt", unknown.id)
	requireChangeStreams(t, unknown.dbName, 0)
}
