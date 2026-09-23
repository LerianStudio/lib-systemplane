//go:build unit

package client

import (
	"context"
	"testing"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"

	"github.com/LerianStudio/lib-systemplane/v4/internal/manager"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// activatedManagerFor binds a Manager to c and gives tenant t1 a per-tenant
// cache. Activation is expected to fail at warm-load: the per-tenant state
// exists and is non-stale by then, which is all the cache needs, and no LISTEN
// connection is opened so the test stays goroutine-free.
func activatedManagerFor(t *testing.T, c *Client) *manager.Manager {
	t.Helper()

	mgr := manager.New(nil)
	c.BindManager(mgr)
	mgr.SetConnector(warmLoadFailsConnector{})

	if err := mgr.OnTenantActivated(context.Background(), "t1"); err == nil {
		t.Fatal("expected activation to fail at warm-load")
	}

	return mgr
}

// TestSetWritesThroughSoReadsNeverSeeTheDefault pins the invariant the admin
// PUT path depends on: a write must never make an in-process reader observe
// the registered default for a key that already had a value. Readers see the
// previous value until the new one loads, or the new one immediately — never
// the default in between.
func TestSetWritesThroughSoReadsNeverSeeTheDefault(t *testing.T) {
	t.Run("cold per-tenant cache and a read that cannot see the fresh row", func(t *testing.T) {
		m := newMemStore(true)
		c := startedClient(t, newMultiTenantClient(t, m))
		activatedManagerFor(t, c)

		ctx := tmcore.ContextWithTenantID(context.Background(), "t1")

		// A value existed before the write.
		m.mu.Lock()
		m.entries[memKey("ns", "k")] = store.Entry{
			Namespace: "ns",
			Key:       "k",
			Value:     []byte(`"previous"`),
		}
		// The reader's read does not yet see the row it is about to write —
		// snapshot skew or replica lag. This is the window in which a write
		// used to be reported as the registered default.
		m.getHook = func(_, _ string) (store.Entry, bool, bool) {
			return store.Entry{}, false, true
		}
		m.mu.Unlock()

		if err := c.Set(ctx, "ns", "k", "new", "operator"); err != nil {
			t.Fatalf("set: %v", err)
		}

		got, ok, err := c.Get(ctx, "ns", "k")
		if err != nil || !ok {
			t.Fatalf("get: value=%v ok=%v err=%v", got, ok, err)
		}

		if got == "default" {
			t.Fatal("Get reported the registered default right after a successful write")
		}

		if got != "new" {
			t.Errorf("got %v, want the value just written", got)
		}
	})

	t.Run("warm per-tenant cache serves the value just written", func(t *testing.T) {
		m := newMemStore(true)
		c := startedClient(t, newMultiTenantClient(t, m))
		mgr := activatedManagerFor(t, c)

		ctx := tmcore.ContextWithTenantID(context.Background(), "t1")
		mgr.Populate(ctx, "t1", "ns", "k", "previous")

		if err := c.Set(ctx, "ns", "k", "new", "operator"); err != nil {
			t.Fatalf("set: %v", err)
		}

		got, ok, err := c.Get(ctx, "ns", "k")
		if err != nil || !ok {
			t.Fatalf("get: value=%v ok=%v err=%v", got, ok, err)
		}

		if got != "new" {
			t.Errorf("got %v, want the value just written", got)
		}
	})

	t.Run("delete drops the per-tenant cache entry", func(t *testing.T) {
		m := newMemStore(true)
		c := startedClient(t, newMultiTenantClient(t, m))
		mgr := activatedManagerFor(t, c)

		ctx := tmcore.ContextWithTenantID(context.Background(), "t1")
		mgr.Populate(ctx, "t1", "ns", "k", "previous")

		if err := c.Delete(ctx, "ns", "k", "operator"); err != nil {
			t.Fatalf("delete: %v", err)
		}

		got, ok, err := c.Get(ctx, "ns", "k")
		if err != nil || !ok {
			t.Fatalf("get: value=%v ok=%v err=%v", got, ok, err)
		}

		if got != "default" {
			t.Errorf("got %v, want the registered default after a delete", got)
		}
	})
}
