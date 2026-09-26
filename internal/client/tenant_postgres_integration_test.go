//go:build integration

package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/client"
	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/pggap"
)

// holdEntry asserts the read through ctx serves want for censusHold, which
// outlasts a feed's first reconnect delay: a reconnect that gets through shows.
func holdEntry(t *testing.T, c *client.Client, ctx context.Context, want client.Entry, what string) {
	t.Helper()

	for deadline := time.Now().Add(censusHold); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		requireEntry(t, c, ctx, want, what)
	}
}

// While a tenant's feed is down its scope serves the last value marked Stale,
// and a write made in the gap lands after the reconnect with no second write,
// delivered once; the other tenant never notices.
func TestIntegration_PostgresFeedGapServesStaleThenConverges(t *testing.T) {
	env := newPGTenantClient(t, client.WithDebounce(deliveryDebounce))
	t1, t2 := env.tenant(t, "t1"), env.tenant(t, "t2")
	row1, row2 := writeRow(t, t1.db, "before-gap"), writeRow(t, t2.db, "t2-value")

	var rec changeRecorder
	if _, err := env.c.OnChange(tenantNS, tenantKey, rec.record); err != nil {
		t.Fatalf("OnChange: %v", err)
	}

	env.activate(t, t1, t2)
	requireFeeds(t, listenBackends, t1.dbName, 1)

	held, reopen := pggap.Hold(t, pgAdmin, t1.db)

	stale1 := row1
	stale1.Stale = true

	awaitEntry(t, env.c, t1.cacheOnly(t), stale1, "t1 marked stale")
	requireEntry(t, env.c, t2.cacheOnly(t), row2, "t2 while t1's feed is down")

	gapRow := writeRow(t, held, "written-in-gap")
	holdEntry(t, env.c, t1.cacheOnly(t), stale1, "t1 during the gap")

	reopen()

	awaitEntry(t, env.c, t1.cacheOnly(t), gapRow, "t1 after the reconnect")
	requireFeeds(t, listenBackends, t1.dbName, 1)
	requireEntry(t, env.c, t2.cacheOnly(t), row2, "t2 after t1's reconnect")
	rec.await(t, t1.id, row1, gapRow)
	rec.await(t, t2.id, row2)
}

// A tenant whose first reconcile fails keeps no feed open; the census reading
// 1 for a healthy tenant first is what makes its zero mean something.
func TestIntegration_PostgresFailedActivationLeavesNoFeed(t *testing.T) {
	env := newPGTenantClient(t)
	healthy, broken := env.tenant(t, "t1"), env.tenant(t, "t2")

	env.activate(t, healthy)
	requireFeeds(t, listenBackends, healthy.dbName, 1)

	// A database nobody migrated: the runtime provisions no schema, so its reconcile fails.
	if _, err := broken.db.ExecContext(t.Context(), "DROP TABLE systemplane_entries"); err != nil {
		t.Fatalf("drop the table on %s: %v", broken.dbName, err)
	}

	if _, _, err := env.c.Get(broken.ctx, tenantNS, tenantKey); err == nil {
		t.Fatalf("first read of %s without a table: want an error", broken.id)
	}

	env.log.waitFor(t, log.LevelWarn, msgActivationFailed, broken.id)
	requireFeeds(t, listenBackends, broken.dbName, 0)
}
