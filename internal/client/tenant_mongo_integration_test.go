//go:build integration

package client_test

import (
	"slices"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/client"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Activation materializes a tenant's missing collection instead of watching
// and reading an empty namespace.
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

// While a tenant's change stream is down its scope serves the last value
// marked Stale, and a write made in the gap lands after the reopen with no
// second write, delivered once; the other tenant never notices.
func TestIntegration_MongoFeedGapServesStaleThenConverges(t *testing.T) {
	env := newMongoTenantClient(t, client.WithDebounce(deliveryDebounce))
	t1, t2 := env.tenant(t, "t1"), env.tenant(t, "t2")
	row1, row2 := t1.write(t, "before-gap", 1), t2.write(t, "t2-value", 1)

	var rec changeRecorder
	if _, err := env.c.OnChange(tenantNS, tenantKey, rec.record); err != nil {
		t.Fatalf("OnChange: %v", err)
	}

	env.activate(t, t1, t2)
	requireFeeds(t, changeStreams, t1.dbName, 1)

	// Closing t1's client kills its stream, and every reopen re-resolves t1
	// through a tenant manager that no longer knows it: the gap holds until
	// t1 is back.
	env.tm.mu.Lock()
	cfg := env.tm.tenants[t1.id]
	delete(env.tm.tenants, t1.id)
	env.tm.mu.Unlock()

	if err := env.mgr.CloseConnection(t.Context(), t1.id); err != nil {
		t.Fatalf("close t1's client: %v", err)
	}

	stale1 := row1
	stale1.Stale = true

	awaitEntry(t, env.c, t1.cacheOnly(t), stale1, "t1 marked stale")
	requireEntry(t, env.c, t2.cacheOnly(t), row2, "t2 while t1's feed is down")

	gapRow := t1.write(t, "written-in-gap", row1.Revision+1)
	holdEntry(t, env.c, t1.cacheOnly(t), stale1, "t1 during the gap")

	env.tm.mu.Lock()
	env.tm.tenants[t1.id] = cfg
	env.tm.mu.Unlock()

	awaitEntry(t, env.c, t1.cacheOnly(t), gapRow, "t1 after the reopen")
	requireEntry(t, env.c, t2.cacheOnly(t), row2, "t2 after t1's reopen")
	rec.await(t, t1.id, row1, gapRow)
	rec.await(t, t2.id, row2)
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

// A tenant on a server with no change streams activates through the polling
// feed, and a write made behind the Client reaches its cached scope.
func TestIntegration_MongoPollingTenantConverges(t *testing.T) {
	env := newMongoTenantEnv(t, standalone, client.WithPollInterval(200*time.Millisecond))
	env.start(t)
	tn := env.tenant(t, "t1")

	env.activate(t, tn)
	requireEntry(t, env.c, tn.cacheOnly(t), client.Entry{Value: "default"}, "default from the cache")

	written := tn.write(t, "polled", 1)
	awaitEntry(t, env.c, tn.cacheOnly(t), written, "t1 serves the polled write")
}
