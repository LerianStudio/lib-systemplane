//go:build unit

package client

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-systemplane/v4/internal/manager"
)

// TestBackwardCompat_MTWithoutManager_OnChangeReturnsErr pins the v1.4.0
// invariant that Client.OnChange in multi-tenant mode without a bound
// Manager continues to return ErrNotSupportedInMultiTenant.
func TestBackwardCompat_MTWithoutManager_OnChangeReturnsErr(t *testing.T) {
	t.Parallel()

	c := newMultiTenantClient(t, newMemStore(true))

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	_, err := c.OnChange("ns", "k", func(_ context.Context, _ Change) {})
	if !errors.Is(err, ErrNotSupportedInMultiTenant) {
		t.Errorf("expected ErrNotSupportedInMultiTenant, got %v", err)
	}
}

// TestBindManager_IsIdempotent_FirstBindWins verifies that BindManager
// rejects subsequent binds, preserving the single-binding invariant.
func TestBindManager_IsIdempotent_FirstBindWins(t *testing.T) {
	t.Parallel()

	c := newMultiTenantClient(t, newMemStore(true))

	m1 := manager.New(nil)
	m2 := manager.New(nil)

	c.BindManager(m1)
	c.BindManager(m2)

	if c.boundManager() != m1 {
		t.Fatal("second BindManager should be a no-op; first bind wins")
	}
}

// TestBindManager_NilArgs_NoOp confirms nil safety for the binding call.
func TestBindManager_NilArgs_NoOp(t *testing.T) {
	t.Parallel()

	c := newMultiTenantClient(t, newMemStore(true))

	c.BindManager(nil)

	if c.boundManager() != nil {
		t.Fatal("BindManager(nil) must not set a Manager")
	}

	// And nil receiver is safe.
	var nilC *Client

	nilC.BindManager(manager.New(nil))
}

// TestMT_WithManager_OnChangeRegistersAndUnsubscribes pins that when a
// Manager is bound to an MT Client, OnChange returns a working unsubscribe
// closure instead of ErrNotSupportedInMultiTenant. The dispatch wiring is
// covered by the integration tests (slice 8); here we verify the binding.
func TestMT_WithManager_OnChangeRegistersAndUnsubscribes(t *testing.T) {
	t.Parallel()

	c := newMultiTenantClient(t, newMemStore(true))

	mgr := manager.New(nil)
	c.BindManager(mgr)

	called := 0
	unsub, err := c.OnChange("ns", "k", func(_ context.Context, _ Change) {
		called++
	})
	if err != nil {
		t.Fatalf("OnChange with Manager bound: %v", err)
	}

	if unsub == nil {
		t.Fatal("OnChange returned nil unsubscribe")
	}

	unsub()
	unsub() // idempotent

	if called != 0 {
		t.Fatalf("callback fired without NOTIFY: called %d", called)
	}
}

// TestBackwardCompat_ST_Get_BypassesManager pins that single-tenant Get
// never consults the Manager even when one is bound. ST mode keeps its own
// in-process cache; the Manager is strictly an MT-mode primitive.
func TestBackwardCompat_ST_Get_BypassesManager(t *testing.T) {
	t.Parallel()

	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Bind a Manager. ST mode should NOT route through it.
	c.BindManager(manager.New(nil))

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	defer func() { _ = c.Close() }()

	v, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("Get: %v ok=%v", err, ok)
	}

	if v != "default" {
		t.Errorf("Get = %v, want default", v)
	}
}

// TestMT_ManagerCallback_CarriesTenant pins FC-4: one subscriber bound to a
// Manager observes a distinct Change.Tenant for every tenant whose row
// changed, so a consumer can tell two tenants' deliveries apart.
func TestMT_ManagerCallback_CarriesTenant(t *testing.T) {
	t.Parallel()

	c := newMultiTenantClient(t, newMemStore(true))

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	var got []Change

	cb := c.managerCallback(func(_ context.Context, ch Change) {
		got = append(got, ch)
	})

	cb(context.Background(), "t1", "ns", "k", 7, "v1")
	cb(context.Background(), "t2", "ns", "k", 9, "v2")

	if len(got) != 2 {
		t.Fatalf("got %d changes, want 2", len(got))
	}

	if got[0].Tenant != "t1" || got[1].Tenant != "t2" {
		t.Errorf("tenants = %q, %q; want t1, t2", got[0].Tenant, got[1].Tenant)
	}

	if got[0].Revision != 7 || got[1].Revision != 9 {
		t.Errorf("revisions = %d, %d; want 7, 9", got[0].Revision, got[1].Revision)
	}

	if got[0].Value != "v1" || got[1].Value != "v2" {
		t.Errorf("values = %v, %v; want v1, v2", got[0].Value, got[1].Value)
	}
}

// TestMT_ManagerCallback_DeleteDeliversDefault pins FC-4: a delete publishes
// the registered default, never a nil value.
func TestMT_ManagerCallback_DeleteDeliversDefault(t *testing.T) {
	t.Parallel()

	c := newMultiTenantClient(t, newMemStore(true))

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	var got Change

	cb := c.managerCallback(func(_ context.Context, ch Change) {
		got = ch
	})

	cb(context.Background(), "t1", "ns", "k", 0, nil)

	if got.Value != "default" {
		t.Errorf("delete delivered %v, want the registered default", got.Value)
	}

	if got.Revision != 0 {
		t.Errorf("delete Revision = %d, want 0", got.Revision)
	}
}

// TestMT_ManagerCallback_ClonesValue pins FC-4's "the receiver owns this
// copy": a subscriber that mutates the delivered Change.Value must not reach
// the Manager's live cached object, nor the registered default that every
// later delete republishes.
func TestMT_ManagerCallback_ClonesValue(t *testing.T) {
	t.Parallel()

	c := newMultiTenantClient(t, newMemStore(true))

	if err := c.Register("ns", "k", map[string]any{"limit": 1}); err != nil {
		t.Fatalf("register: %v", err)
	}

	var observed []any

	cb := c.managerCallback(func(_ context.Context, ch Change) {
		m, ok := ch.Value.(map[string]any)
		if !ok {
			t.Errorf("delivered value is %T, want map[string]any", ch.Value)

			return
		}

		observed = append(observed, m["limit"])
		m["limit"] = 999
	})

	// Upsert: the map the Manager holds in its cache must survive the
	// subscriber's mutation.
	cached := map[string]any{"limit": 1}
	cb(context.Background(), "t1", "ns", "k", 7, cached)

	if cached["limit"] != 1 {
		t.Errorf("subscriber mutation reached the cached map: limit = %v, want 1", cached["limit"])
	}

	// Delete publishes the registered default; mutating one delivery must not
	// corrupt the default handed to the next one.
	cb(context.Background(), "t1", "ns", "k", 0, nil)
	cb(context.Background(), "t2", "ns", "k", 0, nil)

	want := []any{1, 1, 1}
	if len(observed) != len(want) {
		t.Fatalf("observed %d deliveries, want %d", len(observed), len(want))
	}

	for i, w := range want {
		if observed[i] != w {
			t.Errorf("delivery %d carried limit = %v, want %v (a previous subscriber mutated the source)", i, observed[i], w)
		}
	}
}
