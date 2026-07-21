//go:build integration

// Per-lifecycle-path goroutine-cleanup assertions for the Manager. The
// package-level TestMain is the global leak guard; these tests pin the
// visible contract that every lifecycle handler releases the LISTEN
// goroutines it owns.
package manager_test

import (
	"context"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v2/internal/manager"
)

func TestIntegration_Manager_OnTenantDeleted_ReleasesGoroutine(t *testing.T) {
	baseDSN, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	keys := []manager.RegisteredKey{{Namespace: "ns", Key: "k", DefaultValue: "v"}}

	m, _, _, mClean := setup(t, baseDSN, "tenant-leak-d", keys)
	t.Cleanup(mClean)

	if err := m.OnTenantActivated(context.Background(), "tenant-leak-d"); err != nil {
		t.Fatalf("activate: %v", err)
	}

	if err := m.OnTenantDeleted(context.Background(), "tenant-leak-d"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Give the goroutine a moment to exit so the package-level goleak guard
	// sees it gone.
	time.Sleep(200 * time.Millisecond)
}

func TestIntegration_Manager_OnTenantSuspended_ReleasesGoroutine(t *testing.T) {
	baseDSN, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	keys := []manager.RegisteredKey{{Namespace: "ns", Key: "k", DefaultValue: "v"}}

	m, _, _, mClean := setup(t, baseDSN, "tenant-leak-s", keys)
	t.Cleanup(mClean)

	if err := m.OnTenantActivated(context.Background(), "tenant-leak-s"); err != nil {
		t.Fatalf("activate: %v", err)
	}

	if err := m.OnTenantSuspended(context.Background(), "tenant-leak-s"); err != nil {
		t.Fatalf("suspend: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
}

func TestIntegration_Manager_CredentialsRotated_ReleasesOldGoroutine(t *testing.T) {
	baseDSN, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	keys := []manager.RegisteredKey{{Namespace: "ns", Key: "k", DefaultValue: "v"}}

	m, _, _, mClean := setup(t, baseDSN, "tenant-leak-r", keys)
	t.Cleanup(mClean)

	if err := m.OnTenantActivated(context.Background(), "tenant-leak-r"); err != nil {
		t.Fatalf("activate: %v", err)
	}

	if err := m.OnTenantCredentialsRotated(context.Background(), "tenant-leak-r"); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	// Drain to clean up the post-rotation goroutine before the package-level
	// goleak guard runs.
	if err := m.Drain(context.Background()); err != nil {
		t.Fatalf("drain: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
}

func TestIntegration_Manager_Drain_HonoursCtxCancellation(t *testing.T) {
	baseDSN, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	keys := []manager.RegisteredKey{{Namespace: "ns", Key: "k", DefaultValue: "v"}}

	m, _, _, mClean := setup(t, baseDSN, "tenant-drain-ctx", keys)
	t.Cleanup(mClean)

	if err := m.OnTenantActivated(context.Background(), "tenant-drain-ctx"); err != nil {
		t.Fatalf("activate: %v", err)
	}

	// Drain with an already-canceled ctx must return promptly.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()

	if err := m.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Drain ignored ctx cancellation, took %v", elapsed)
	}

	// Subsequent Drain is idempotent.
	if err := m.Drain(context.Background()); err != nil {
		t.Fatalf("second Drain: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
}

func TestIntegration_Manager_Drain_ReleasesAllGoroutines(t *testing.T) {
	baseDSN, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	keys := []manager.RegisteredKey{{Namespace: "ns", Key: "k", DefaultValue: "v"}}

	m, _, _, mClean := setup(t, baseDSN, "tenant-leak-x", keys)
	t.Cleanup(mClean)

	if err := m.OnTenantActivated(context.Background(), "tenant-leak-x"); err != nil {
		t.Fatalf("activate: %v", err)
	}

	if err := m.Drain(context.Background()); err != nil {
		t.Fatalf("drain: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
}
