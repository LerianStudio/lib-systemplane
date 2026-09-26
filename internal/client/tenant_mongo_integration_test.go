//go:build integration

package client_test

import (
	"slices"
	"sync"
	"testing"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
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

	// No database rides on this ctx, so a per-request read would fail with
	// ErrTenantConnectionMissing: only t1's cached scope can answer it.
	requireEntry(t, env.c, tmcore.ContextWithTenantID(t.Context(), t1.id), row1, "t1 from the cache")

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

	for _, tn := range []mongoTenant{t1, t2} {
		if _, _, err := env.c.Get(tn.ctx, tenantNS, tenantKey); err != nil {
			t.Fatalf("%s first read: %v", tn.id, err)
		}

		env.log.waitFor(t, log.LevelInfo, msgScopeActivated, tn.id)
	}

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
	cacheOnly := tmcore.ContextWithTenantID(t.Context(), tn.id)

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
