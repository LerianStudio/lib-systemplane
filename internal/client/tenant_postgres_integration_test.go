//go:build integration

package client_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	tmevent "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/event"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/client"
)

const msgActivationFailed = "scope activation failed; reads stay per-request until a later attempt"

// readEntry is the read through ctx; the key is registered, so it answers or fails.
func readEntry(t *testing.T, c *client.Client, ctx context.Context, what string) client.Entry {
	t.Helper()

	got, ok, err := c.GetEntry(ctx, tenantNS, tenantKey)
	if err != nil || !ok {
		t.Fatalf("%s: GetEntry = ok %v, err %v", what, ok, err)
	}

	return got
}

func sameEntry(got, want client.Entry) bool {
	return got.Value == want.Value && got.Revision == want.Revision && got.UpdatedAt.Equal(want.UpdatedAt) &&
		got.UpdatedBy == want.UpdatedBy && got.Stale == want.Stale
}

// requireEntry asserts the read through ctx serves want, provenance and Stale included.
func requireEntry(t *testing.T, c *client.Client, ctx context.Context, want client.Entry, what string) {
	t.Helper()

	if got := readEntry(t, c, ctx, what); !sameEntry(got, want) {
		t.Fatalf("%s: GetEntry = %+v, want %+v", what, got, want)
	}
}

// awaitEntry polls the read through ctx until it serves want.
func awaitEntry(t *testing.T, c *client.Client, ctx context.Context, want client.Entry, what string) {
	t.Helper()

	var got client.Entry

	defer func() {
		if t.Failed() {
			t.Logf("%s: last GetEntry = %+v, want %+v", what, got, want)
		}
	}()

	eventually(t, what, func() bool {
		got = readEntry(t, c, ctx, what)

		return sameEntry(got, want)
	})
}

// holdEntry asserts the read through ctx serves want for censusHold, which
// outlasts a feed's first reconnect delay: a reconnect that gets through shows.
func holdEntry(t *testing.T, c *client.Client, ctx context.Context, want client.Entry, what string) {
	t.Helper()

	for deadline := time.Now().Add(censusHold); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		requireEntry(t, c, ctx, want, what)
	}
}

// cacheOnly carries the tenant id and no database: a per-request read through
// it fails with ErrTenantConnectionMissing, so only the tenant's cached scope answers.
func (p pgTenant) cacheOnly(t *testing.T) context.Context {
	return tmcore.ContextWithTenantID(t.Context(), p.id)
}

// activate reads once through each tenant's ctx and waits for its scope to come up.
func (e *pgTenantEnv) activate(t *testing.T, tenants ...pgTenant) {
	t.Helper()

	for _, tn := range tenants {
		if _, _, err := e.c.Get(tn.ctx, tenantNS, tenantKey); err != nil {
			t.Fatalf("%s first read: %v", tn.id, err)
		}

		e.log.waitFor(t, log.LevelInfo, msgScopeActivated, tn.id)
	}
}

// writeRow upserts value on conn as the store does, behind the Client's back,
// and returns the row a read should report; the trigger draws the revision.
func writeRow(t *testing.T, conn *sql.Conn, value string) client.Entry {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %q: %v", value, err)
	}

	want := client.Entry{Value: value, UpdatedBy: "direct"}

	if err := conn.QueryRowContext(t.Context(),
		`INSERT INTO systemplane_entries (namespace, "key", value, updated_by) VALUES ($1, $2, $3::jsonb, $4)
		 ON CONFLICT (namespace, "key") DO UPDATE SET value = EXCLUDED.value, updated_at = now(), updated_by = EXCLUDED.updated_by
		 RETURNING revision, updated_at`,
		tenantNS, tenantKey, string(raw), want.UpdatedBy,
	).Scan(&want.Revision, &want.UpdatedAt); err != nil {
		t.Fatalf("write %q: %v", value, err)
	}

	return want
}

// stored reads the key's row from the tenant's database as a read should report it.
func (p pgTenant) stored(t *testing.T) client.Entry {
	t.Helper()

	var (
		raw []byte
		e   client.Entry
	)

	if err := p.db.QueryRowContext(t.Context(),
		`SELECT value, revision, updated_at, updated_by FROM systemplane_entries WHERE namespace = $1 AND "key" = $2`,
		tenantNS, tenantKey,
	).Scan(&raw, &e.Revision, &e.UpdatedAt, &e.UpdatedBy); err != nil {
		t.Fatalf("read row on %s: %v", p.dbName, err)
	}

	if err := json.Unmarshal(raw, &e.Value); err != nil {
		t.Fatalf("decode row on %s: %v", p.dbName, err)
	}

	return e
}

// A tenant's first read activates its scope and nobody else's; from then on
// the cache answers for it.
func TestIntegration_PostgresFirstReadActivatesOnlyThatTenant(t *testing.T) {
	env := newPGTenantClient(t)
	t1, t2 := env.tenant(t, "t1"), env.tenant(t, "t2")
	row1, row2 := t1.seed(t, "t1-value"), t2.seed(t, "t2-value")

	requireEntry(t, env.c, t1.ctx, row1, "t1 first read")
	env.log.waitFor(t, log.LevelInfo, msgScopeActivated, t1.id)
	requireEntry(t, env.c, t1.ctx, row1, "t1 after activation")

	requireListenBackends(t, t1.dbName, 1)
	requireListenBackends(t, t2.dbName, 0)

	requireEntry(t, env.c, t1.cacheOnly(t), row1, "t1 from the cache")

	requireEntry(t, env.c, t2.ctx, row2, "t2 first read")
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
// the NOTIFY echo of the same revision are one change, and t2 hears nothing.
func TestIntegration_PostgresWriteDeliversOnceToItsTenantOnly(t *testing.T) {
	const debounce = 50 * time.Millisecond

	env := newPGTenantClient(t, client.WithDebounce(debounce))
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
func TestIntegration_PostgresConcurrentFirstReadsOpenOneFeed(t *testing.T) {
	env := newPGTenantClient(t)
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
	requireListenBackends(t, tn.dbName, 1)
}

// alterAllowConnections opens or closes dbName to every new connection,
// superuser included; connections already open are left alone.
func alterAllowConnections(ctx context.Context, dbName string, allow bool) error {
	_, err := pgAdmin.ExecContext(ctx, fmt.Sprintf("ALTER DATABASE %s ALLOW_CONNECTIONS %t", dbName, allow))

	return err
}

// While a tenant's feed is down its scope serves the last value marked Stale,
// and a write made in the gap lands after the reconnect with no second write;
// the other tenant never notices.
func TestIntegration_PostgresFeedGapServesStaleThenConverges(t *testing.T) {
	env := newPGTenantClient(t)
	t1, t2 := env.tenant(t, "t1"), env.tenant(t, "t2")
	row1, row2 := t1.seed(t, "before-gap"), t2.seed(t, "t2-value")

	env.activate(t, t1, t2)
	requireListenBackends(t, t1.dbName, 1)

	held, err := t1.db.Conn(t.Context())
	if err != nil {
		t.Fatalf("hold a connection on %s: %v", t1.dbName, err)
	}

	t.Cleanup(func() { _ = held.Close() })

	// Registered last, so it runs first: a failed test cannot leave teardown locked out.
	t.Cleanup(func() {
		if err := alterAllowConnections(context.Background(), t1.dbName, true); err != nil {
			t.Errorf("reopen %s: %v", t1.dbName, err)
		}
	})

	if err := alterAllowConnections(t.Context(), t1.dbName, false); err != nil {
		t.Fatalf("close %s to new connections: %v", t1.dbName, err)
	}

	var killed int
	if err := pgAdmin.QueryRowContext(t.Context(),
		`SELECT count(*) FILTER (WHERE pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname = $1 AND query LIKE 'LISTEN%'`,
		t1.dbName,
	).Scan(&killed); err != nil || killed != 1 {
		t.Fatalf("terminate t1's LISTEN backend: killed %d, err %v", killed, err)
	}

	stale1 := row1
	stale1.Stale = true

	awaitEntry(t, env.c, t1.cacheOnly(t), stale1, "t1 marked stale")
	requireEntry(t, env.c, t2.cacheOnly(t), row2, "t2 while t1's feed is down")

	gapRow := writeRow(t, held, "written-in-gap")
	holdEntry(t, env.c, t1.cacheOnly(t), stale1, "t1 during the gap")

	if err := alterAllowConnections(t.Context(), t1.dbName, true); err != nil {
		t.Fatalf("reopen %s: %v", t1.dbName, err)
	}

	awaitEntry(t, env.c, t1.cacheOnly(t), gapRow, "t1 after the reconnect")
	requireListenBackends(t, t1.dbName, 1)
}

// A write racing a tenant's first read is in the cache once the scope is up,
// with no second write to repair it.
func TestIntegration_PostgresWriteRacingFirstReadSurvivesActivation(t *testing.T) {
	env := newPGTenantClient(t)

	tenants := make([]pgTenant, 5)
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
		awaitEntry(t, env.c, tn.cacheOnly(t), tn.stored(t), tn.id+" cached after activation")
	}
}

// A suspended tenant loses its feed, and its later reads answer per request
// without bringing the feed back.
func TestIntegration_PostgresSuspendedTenantKeepsNoFeed(t *testing.T) {
	env := newPGTenantClient(t)
	t1 := env.tenant(t, "t1")
	row := t1.seed(t, "t1-value")

	env.activate(t, t1)
	requireListenBackends(t, t1.dbName, 1)

	if err := env.c.HandleTenantLifecycle(t.Context(), tmevent.TenantLifecycleEvent{
		EventType: tmevent.EventTenantSuspended,
		TenantID:  t1.id,
	}); err != nil {
		t.Fatalf("HandleTenantLifecycle(suspended): %v", err)
	}

	requireListenBackends(t, t1.dbName, 0)

	requireEntry(t, env.c, t1.ctx, row, "t1 read while suspended")
	requireListenBackends(t, t1.dbName, 0)
}

// A tenant whose first reconcile fails keeps no feed open; the census reading
// 1 for a healthy tenant first is what makes its zero mean something.
func TestIntegration_PostgresFailedActivationLeavesNoFeed(t *testing.T) {
	env := newPGTenantClient(t)
	healthy, broken := env.tenant(t, "t1"), env.tenant(t, "t2")

	env.activate(t, healthy)
	requireListenBackends(t, healthy.dbName, 1)

	// A database nobody migrated: the runtime provisions no schema, so its reconcile fails.
	if _, err := broken.db.ExecContext(t.Context(), "DROP TABLE systemplane_entries"); err != nil {
		t.Fatalf("drop the table on %s: %v", broken.dbName, err)
	}

	if _, _, err := env.c.Get(broken.ctx, tenantNS, tenantKey); err == nil {
		t.Fatalf("first read of %s without a table: want an error", broken.id)
	}

	env.log.waitFor(t, log.LevelWarn, msgActivationFailed, broken.id)
	requireListenBackends(t, broken.dbName, 0)
}
