//go:build unit

package systemplane_test

import (
	"context"
	"testing"

	tmevent "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/event"
	systemplane "github.com/LerianStudio/lib-systemplane"
)

// These tests exercise the public HandleTenantLifecycle proxy. Routing
// correctness is verified at the internal package boundary
// (internal/manager/handle_lifecycle_test.go) where the On* seams can be
// spied; here we assert the public surface compiles, delegates, and is
// safe across the no-op paths (no live tenant DB available).

func TestManager_HandleTenantLifecycle_KnownEvent_NoError(t *testing.T) {
	t.Parallel()

	m := systemplane.NewManager(nil, nil)

	for _, et := range []string{
		tmevent.EventTenantActivated,
		tmevent.EventTenantSuspended,
		tmevent.EventTenantDeleted,
		tmevent.EventTenantCredentialsRotated,
	} {
		err := m.HandleTenantLifecycle(context.Background(), tmevent.TenantLifecycleEvent{
			EventType: et,
			TenantID:  "tenant-a",
		})
		if err != nil {
			t.Fatalf("HandleTenantLifecycle(%q): unexpected error %v", et, err)
		}
	}
}

func TestManager_HandleTenantLifecycle_UnrelatedEvent_NoOp(t *testing.T) {
	t.Parallel()

	m := systemplane.NewManager(nil, nil)

	err := m.HandleTenantLifecycle(context.Background(), tmevent.TenantLifecycleEvent{
		EventType: tmevent.EventTenantCreated,
		TenantID:  "tenant-a",
	})
	if err != nil {
		t.Fatalf("unrelated event must no-op, got %v", err)
	}
}

func TestManager_HandleTenantLifecycle_NilReceiver_NoPanic(t *testing.T) {
	t.Parallel()

	var m *systemplane.Manager

	err := m.HandleTenantLifecycle(context.Background(), tmevent.TenantLifecycleEvent{
		EventType: tmevent.EventTenantActivated,
		TenantID:  "tenant-a",
	})
	if err != nil {
		t.Fatalf("nil receiver must return nil, got %v", err)
	}
}

// TestManager_HandleTenantLifecycle_SatisfiesEventHandler is a compile-time
// assertion that the method can be registered directly as a
// tmevent.EventHandler — the whole point of the signature.
func TestManager_HandleTenantLifecycle_SatisfiesEventHandler(t *testing.T) {
	t.Parallel()

	m := systemplane.NewManager(nil, nil)

	var _ tmevent.EventHandler = m.HandleTenantLifecycle
}

func TestManager_PublicLifecycleWrappers_NoOpPaths(t *testing.T) {
	t.Parallel()

	m := systemplane.NewManager(nil, nil,
		systemplane.WithManagerLogger(nil),
		systemplane.WithManagerTelemetry(nil),
		systemplane.WithManagerAggregateTenantThreshold(10),
	)
	ctx := context.Background()

	if err := m.OnTenantActivated(ctx, "tenant-a"); err != nil {
		t.Fatalf("OnTenantActivated: %v", err)
	}
	if err := m.OnTenantSuspended(ctx, "tenant-a"); err != nil {
		t.Fatalf("OnTenantSuspended: %v", err)
	}
	if err := m.OnTenantDeleted(ctx, "tenant-a"); err != nil {
		t.Fatalf("OnTenantDeleted: %v", err)
	}
	if err := m.OnTenantCredentialsRotated(ctx, "tenant-a"); err != nil {
		t.Fatalf("OnTenantCredentialsRotated: %v", err)
	}
	if m.IsClosed() {
		t.Fatal("IsClosed before Drain = true, want false")
	}
	if err := m.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if !m.IsClosed() {
		t.Fatal("IsClosed after Drain = false, want true")
	}
}
