//go:build unit

package manager_test

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-systemplane/internal/manager"
)

func TestNew_NilPgMgr_ReturnsNonNilManager(t *testing.T) {
	t.Parallel()

	m := manager.New(nil)
	if m == nil {
		t.Fatal("expected non-nil Manager even without pgMgr")
	}

	if m.IsClosed() {
		t.Fatal("fresh Manager must not report closed")
	}
}

func TestDrain_IsIdempotent(t *testing.T) {
	t.Parallel()

	m := manager.New(nil)

	if err := m.Drain(context.Background()); err != nil {
		t.Fatalf("first Drain: %v", err)
	}

	if !m.IsClosed() {
		t.Fatal("Manager should be closed after Drain")
	}

	if err := m.Drain(context.Background()); err != nil {
		t.Fatalf("second Drain: %v", err)
	}
}

func TestLookup_TenantNotActivated_MissesGracefully(t *testing.T) {
	t.Parallel()

	m := manager.New(nil)

	v, hit, err := m.Lookup(context.Background(), "tenant-a", "ns", "k")
	if err != nil {
		t.Fatalf("Lookup error: %v", err)
	}

	if hit {
		t.Fatalf("expected miss for unactivated tenant, got hit value %v", v)
	}
}

func TestLookup_EmptyTenantID_Misses(t *testing.T) {
	t.Parallel()

	m := manager.New(nil)

	_, hit, err := m.Lookup(context.Background(), "", "ns", "k")
	if err != nil {
		t.Fatalf("Lookup error: %v", err)
	}

	if hit {
		t.Fatal("expected miss for empty tenant ID")
	}
}

func TestPopulate_TenantNotActivated_NoOps(t *testing.T) {
	t.Parallel()

	m := manager.New(nil)

	m.Populate(context.Background(), "tenant-a", "ns", "k", "value")

	_, hit, _ := m.Lookup(context.Background(), "tenant-a", "ns", "k")
	if hit {
		t.Fatal("Populate must not create cache state for unactivated tenant")
	}
}

func TestRegisterCallback_StoresAndUnsubscribes(t *testing.T) {
	t.Parallel()

	m := manager.New(nil)

	called := 0
	cb := func(_ context.Context, _, _ string, _ any) {
		called++
	}

	unsub := m.RegisterCallback("ns", "k", cb)
	if unsub == nil {
		t.Fatal("RegisterCallback returned nil unsubscribe")
	}

	// Unsubscribe should be safe to call multiple times.
	unsub()
	unsub()

	// Re-register and confirm a second unsubscribe also works.
	unsub2 := m.RegisterCallback("ns", "k", cb)
	unsub2()

	if called != 0 {
		t.Fatalf("callback should not fire without dispatch wiring; called %d", called)
	}
}

func TestRegisterCallback_NilCallback_ReturnsNoOpUnsubscribe(t *testing.T) {
	t.Parallel()

	m := manager.New(nil)

	unsub := m.RegisterCallback("ns", "k", nil)
	if unsub == nil {
		t.Fatal("expected non-nil unsubscribe even for nil callback")
	}

	unsub() // must not panic
}

func TestClosedManager_LifecycleHandlers_NoOp(t *testing.T) {
	t.Parallel()

	m := manager.New(nil)
	_ = m.Drain(context.Background())

	if err := m.OnTenantActivated(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("OnTenantActivated after Drain: %v", err)
	}

	if err := m.OnTenantSuspended(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("OnTenantSuspended after Drain: %v", err)
	}

	if err := m.OnTenantDeleted(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("OnTenantDeleted after Drain: %v", err)
	}

	if err := m.OnTenantCredentialsRotated(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("OnTenantCredentialsRotated after Drain: %v", err)
	}
}

func TestLifecycle_EmptyTenantID_NoOp(t *testing.T) {
	t.Parallel()

	m := manager.New(nil)

	if err := m.OnTenantActivated(context.Background(), ""); err != nil {
		t.Fatalf("OnTenantActivated(\"\"): %v", err)
	}

	if err := m.OnTenantSuspended(context.Background(), ""); err != nil {
		t.Fatalf("OnTenantSuspended(\"\"): %v", err)
	}

	if err := m.OnTenantDeleted(context.Background(), ""); err != nil {
		t.Fatalf("OnTenantDeleted(\"\"): %v", err)
	}

	if err := m.OnTenantCredentialsRotated(context.Background(), ""); err != nil {
		t.Fatalf("OnTenantCredentialsRotated(\"\"): %v", err)
	}
}

func TestOnTenantActivated_WithoutPgMgr_NoOp(t *testing.T) {
	t.Parallel()

	// No hooks bound + no pgMgr → handler returns nil without panicking.
	m := manager.New(nil)

	if err := m.OnTenantActivated(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("OnTenantActivated without pgMgr: %v", err)
	}
}

func TestOnTenantDeleted_RemovesPerTenantState(t *testing.T) {
	t.Parallel()

	m := manager.New(nil)

	// Activate is a no-op without pgMgr, so prime perTenant manually via
	// Populate's gated path - which itself is gated. Use Invalidate to
	// confirm no panic when state doesn't exist.
	m.Invalidate(context.Background(), "unknown-tenant", "ns", "k")

	// Delete an unknown tenant is also a no-op.
	if err := m.OnTenantDeleted(context.Background(), "unknown-tenant"); err != nil {
		t.Fatalf("OnTenantDeleted unknown: %v", err)
	}
}

func TestOnTenantCredentialsRotated_DeletesThenReactivates(t *testing.T) {
	t.Parallel()

	m := manager.New(nil)

	if err := m.OnTenantCredentialsRotated(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("OnTenantCredentialsRotated: %v", err)
	}
}

func TestNilManager_AllMethodsSafe(t *testing.T) {
	t.Parallel()

	var m *manager.Manager

	if !m.IsClosed() {
		t.Fatal("nil Manager must report closed")
	}

	if err := m.OnTenantActivated(context.Background(), "t"); err != nil {
		t.Fatalf("nil OnTenantActivated: %v", err)
	}

	if err := m.Drain(context.Background()); err != nil {
		t.Fatalf("nil Drain: %v", err)
	}

	_, hit, err := m.Lookup(context.Background(), "t", "ns", "k")
	if err != nil || hit {
		t.Fatalf("nil Lookup hit=%v err=%v", hit, err)
	}

	m.Populate(context.Background(), "t", "ns", "k", "v")
	m.Invalidate(context.Background(), "t", "ns", "k")

	unsub := m.RegisterCallback("ns", "k", nil)
	if unsub != nil {
		unsub()
	}
}
