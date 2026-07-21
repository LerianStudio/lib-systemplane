//go:build unit

package client

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-systemplane/v2/internal/manager"
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

	_, err := c.OnChange("ns", "k", func(_ context.Context, _, _ string, _ any) {})
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
	unsub, err := c.OnChange("ns", "k", func(_ context.Context, _, _ string, _ any) {
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
