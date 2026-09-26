//go:build integration

package client_test

import (
	"context"
	"sync"
	"testing"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/client"
)

// requireEntry asserts the read through ctx serves want, provenance included.
func requireEntry(t *testing.T, c *client.Client, ctx context.Context, want client.Entry, what string) {
	t.Helper()

	got, ok, err := c.GetEntry(ctx, tenantNS, tenantKey)
	if err != nil || !ok {
		t.Fatalf("%s: GetEntry = ok %v, err %v", what, ok, err)
	}

	if got.Value != want.Value || got.Revision != want.Revision || !got.UpdatedAt.Equal(want.UpdatedAt) ||
		got.UpdatedBy != want.UpdatedBy || got.Stale {
		t.Fatalf("%s: GetEntry = %+v, want %+v", what, got, want)
	}
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

	// No database rides on this ctx, so a per-request read would fail with
	// ErrTenantConnectionMissing: only t1's cached scope can answer it.
	requireEntry(t, env.c, tmcore.ContextWithTenantID(t.Context(), t1.id), row1, "t1 from the cache")

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

	for _, tn := range []pgTenant{t1, t2} {
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
