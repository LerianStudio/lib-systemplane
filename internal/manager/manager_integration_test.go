//go:build integration

package manager_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-systemplane/internal/manager"
	"github.com/bxcodec/dbresolver/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// fakeConnector resolves tenants to ad-hoc DSN/dbresolver.DB pairs without
// invoking the tenant-manager gRPC client. Tests prime it with one entry per
// tenant.
type fakeConnector struct {
	mu    sync.RWMutex
	dsns  map[string]string
	dbs   map[string]dbresolver.DB
	resErr map[string]error
}

func newFakeConnector() *fakeConnector {
	return &fakeConnector{
		dsns:   make(map[string]string),
		dbs:    make(map[string]dbresolver.DB),
		resErr: make(map[string]error),
	}
}

func (f *fakeConnector) setTenant(tenantID, dsn string, db dbresolver.DB) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dsns[tenantID] = dsn
	f.dbs[tenantID] = db
}

func (f *fakeConnector) ResolveDB(_ context.Context, tenantID string) (dbresolver.DB, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if err, ok := f.resErr[tenantID]; ok {
		return nil, err
	}

	db, ok := f.dbs[tenantID]
	if !ok {
		return nil, fmt.Errorf("fake connector: unknown tenant %s", tenantID)
	}

	return db, nil
}

func (f *fakeConnector) ResolveDSN(_ context.Context, tenantID string) (string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if err, ok := f.resErr[tenantID]; ok {
		return "", err
	}

	dsn, ok := f.dsns[tenantID]
	if !ok {
		return "", fmt.Errorf("fake connector: unknown tenant %s", tenantID)
	}

	return dsn, nil
}

// stubHooks satisfies manager.ClientHooks for integration tests so the
// Manager can read the registered keys and a lifecycle ctx.
type stubHooks struct {
	keys []manager.RegisteredKey
}

func (s *stubHooks) RegisteredKeys() []manager.RegisteredKey { return s.keys }
func (s *stubHooks) LifecycleContext() context.Context       { return context.Background() }

func startContainer(t *testing.T) (string, func()) {
	t.Helper()

	ctx := context.Background()

	container, err := pgcontainer.Run(ctx, "postgres:16-alpine",
		pgcontainer.WithDatabase("postgres"),
		pgcontainer.WithUsername("postgres"),
		pgcontainer.WithPassword("postgres"),
		pgcontainer.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = testcontainers.TerminateContainer(container)

		t.Fatalf("connection string: %v", err)
	}

	cleanup := func() {
		_ = testcontainers.TerminateContainer(container)
	}

	return dsn, cleanup
}

func adminDSN(t *testing.T, base string) *sql.DB {
	t.Helper()

	db, err := sql.Open("pgx", base)
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}

	return db
}

func freshDB(t *testing.T, admin *sql.DB, name string) {
	t.Helper()

	if _, err := admin.Exec(fmt.Sprintf(`CREATE DATABASE %s`, name)); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
}

func dsnFor(base, dbName string) string {
	for i := len(base) - 1; i >= 0; i-- {
		if base[i] == '/' {
			head := base[:i+1]
			tail := base[i+1:]

			for j := 0; j < len(tail); j++ {
				if tail[j] == '?' {
					return head + dbName + tail[j:]
				}
			}

			return head + dbName
		}
	}

	return base
}

// setup constructs a Manager wired to one tenant DB inside the container.
// It returns the Manager, the tenant ID, the live dbresolver, and a cleanup.
func setup(t *testing.T, baseDSN string, tenantID string, keys []manager.RegisteredKey) (
	*manager.Manager, string, dbresolver.DB, func(),
) {
	t.Helper()

	admin := adminDSN(t, baseDSN)
	defer admin.Close()

	dbName := strings.ReplaceAll(fmt.Sprintf("mgr_%s_%d", tenantID, time.Now().UnixNano()), "-", "_")
	freshDB(t, admin, dbName)

	tdsn := dsnFor(baseDSN, dbName)

	tenantSQL, err := sql.Open("pgx", tdsn)
	if err != nil {
		t.Fatalf("open tenant: %v", err)
	}

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(tenantSQL))

	fc := newFakeConnector()
	fc.setTenant(tenantID, tdsn, resolver)

	m := manager.New(nil)
	m.SetConnector(fc)
	m.Bind(&stubHooks{keys: keys})

	cleanup := func() {
		_ = m.Drain(context.Background())
		_ = tenantSQL.Close()
	}

	return m, tdsn, resolver, cleanup
}

// TestIntegration_Manager_OnTenantActivated_BootstrapsSchemaAndSeeds verifies
// that activation runs DDL, seeds defaults, opens LISTEN and warm-loads.
func TestIntegration_Manager_OnTenantActivated_BootstrapsSchemaAndSeeds(t *testing.T) {
	baseDSN, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	keys := []manager.RegisteredKey{
		{Namespace: "ns", Key: "retries", DefaultValue: 3.0},
		{Namespace: "ns", Key: "enabled", DefaultValue: true},
	}

	m, _, resolver, mClean := setup(t, baseDSN, "tenant-a", keys)
	defer mClean()

	if err := m.OnTenantActivated(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("OnTenantActivated: %v", err)
	}

	// Verify defaults were seeded.
	row := resolver.QueryRowContext(context.Background(),
		`SELECT value FROM systemplane_entries WHERE namespace=$1 AND key=$2`,
		"ns", "retries")

	var raw []byte
	if err := row.Scan(&raw); err != nil {
		t.Fatalf("scan seeded row: %v", err)
	}

	var v float64
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode seeded value: %v", err)
	}

	if v != 3.0 {
		t.Fatalf("seeded retries = %v, want 3", v)
	}

	// Verify cache was warm-loaded.
	got, hit, err := m.Lookup(context.Background(), "tenant-a", "ns", "retries")
	if err != nil || !hit {
		t.Fatalf("Lookup retries: err=%v hit=%v", err, hit)
	}

	if got.(float64) != 3.0 {
		t.Fatalf("Lookup retries = %v, want 3", got)
	}
}

// TestIntegration_Manager_NotifyEndToEnd verifies a NOTIFY arriving at the
// LISTEN goroutine updates the cache and fires OnChange callbacks.
func TestIntegration_Manager_NotifyEndToEnd(t *testing.T) {
	baseDSN, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	keys := []manager.RegisteredKey{
		{Namespace: "ns", Key: "k", DefaultValue: "original"},
	}

	m, _, resolver, mClean := setup(t, baseDSN, "tenant-a", keys)
	defer mClean()

	if err := m.OnTenantActivated(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("activate: %v", err)
	}

	received := make(chan any, 1)

	unsub := m.RegisterCallback("ns", "k", func(_ context.Context, _, _ string, newValue any) {
		received <- newValue
	})
	defer unsub()

	// Write through a parallel SQL connection to trigger NOTIFY.
	if _, err := resolver.ExecContext(context.Background(),
		`INSERT INTO systemplane_entries (namespace, key, value, updated_at, updated_by)
		VALUES ($1, $2, $3, now(), $4)
		ON CONFLICT (namespace, key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at`,
		"ns", "k", []byte(`"updated"`), "test"); err != nil {
		t.Fatalf("write update: %v", err)
	}

	select {
	case v := <-received:
		if v != "updated" {
			t.Fatalf("callback got %v, want updated", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for NOTIFY callback")
	}

	// Cache should reflect the update.
	got, hit, err := m.Lookup(context.Background(), "tenant-a", "ns", "k")
	if err != nil || !hit {
		t.Fatalf("post-notify Lookup: err=%v hit=%v", err, hit)
	}

	if got != "updated" {
		t.Fatalf("cache value = %v, want updated", got)
	}
}

// TestIntegration_Manager_MultiTenantIsolation verifies that activating two
// tenants creates two independent caches that never cross-contaminate.
func TestIntegration_Manager_MultiTenantIsolation(t *testing.T) {
	baseDSN, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	admin := adminDSN(t, baseDSN)
	defer admin.Close()

	dbA := fmt.Sprintf("iso_a_%d", time.Now().UnixNano())
	dbB := fmt.Sprintf("iso_b_%d", time.Now().UnixNano())

	freshDB(t, admin, dbA)
	freshDB(t, admin, dbB)

	tenantA, err := sql.Open("pgx", dsnFor(baseDSN, dbA))
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	defer tenantA.Close()

	tenantB, err := sql.Open("pgx", dsnFor(baseDSN, dbB))
	if err != nil {
		t.Fatalf("open B: %v", err)
	}
	defer tenantB.Close()

	rA := dbresolver.New(dbresolver.WithPrimaryDBs(tenantA))
	rB := dbresolver.New(dbresolver.WithPrimaryDBs(tenantB))

	fc := newFakeConnector()
	fc.setTenant("a", dsnFor(baseDSN, dbA), rA)
	fc.setTenant("b", dsnFor(baseDSN, dbB), rB)

	m := manager.New(nil)
	m.SetConnector(fc)
	m.Bind(&stubHooks{keys: []manager.RegisteredKey{{Namespace: "ns", Key: "k", DefaultValue: "default"}}})
	t.Cleanup(func() { _ = m.Drain(context.Background()) })

	if err := m.OnTenantActivated(context.Background(), "a"); err != nil {
		t.Fatalf("activate A: %v", err)
	}

	if err := m.OnTenantActivated(context.Background(), "b"); err != nil {
		t.Fatalf("activate B: %v", err)
	}

	// Write distinct values to each tenant.
	if _, err := rA.ExecContext(context.Background(),
		`UPDATE systemplane_entries SET value=$1 WHERE namespace=$2 AND key=$3`,
		[]byte(`"value-A"`), "ns", "k"); err != nil {
		t.Fatalf("write A: %v", err)
	}

	if _, err := rB.ExecContext(context.Background(),
		`UPDATE systemplane_entries SET value=$1 WHERE namespace=$2 AND key=$3`,
		[]byte(`"value-B"`), "ns", "k"); err != nil {
		t.Fatalf("write B: %v", err)
	}

	// Let NOTIFY propagate.
	if err := waitForCacheValue(m, "a", "ns", "k", "value-A", 5*time.Second); err != nil {
		t.Fatalf("tenant A cache: %v", err)
	}

	if err := waitForCacheValue(m, "b", "ns", "k", "value-B", 5*time.Second); err != nil {
		t.Fatalf("tenant B cache: %v", err)
	}

	// Cross-check: tenant A must still see A's value, not B's.
	got, _, _ := m.Lookup(context.Background(), "a", "ns", "k")
	if got != "value-A" {
		t.Fatalf("tenant A leaked from B: got %v", got)
	}
}

// TestIntegration_Manager_ReconnectAfterTerminateBackend verifies that
// pg_terminate_backend on the LISTEN connection triggers a reconnect.
func TestIntegration_Manager_ReconnectAfterTerminateBackend(t *testing.T) {
	baseDSN, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	keys := []manager.RegisteredKey{{Namespace: "ns", Key: "k", DefaultValue: "x"}}

	m, _, resolver, mClean := setup(t, baseDSN, "tenant-r", keys)
	defer mClean()

	if err := m.OnTenantActivated(context.Background(), "tenant-r"); err != nil {
		t.Fatalf("activate: %v", err)
	}

	// Kill any backend that has LISTEN on the channel.
	if _, err := resolver.ExecContext(context.Background(), `
		SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE state IS NOT NULL AND query LIKE 'LISTEN%'`); err != nil {
		t.Logf("pg_terminate_backend (may be noisy): %v", err)
	}

	// After reconnect, a fresh NOTIFY should still be delivered.
	received := make(chan any, 1)

	unsub := m.RegisterCallback("ns", "k", func(_ context.Context, _, _ string, newValue any) {
		select {
		case received <- newValue:
		default:
		}
	})
	defer unsub()

	// Give the manager a moment to reconnect.
	time.Sleep(1 * time.Second)

	if _, err := resolver.ExecContext(context.Background(),
		`UPDATE systemplane_entries SET value=$1 WHERE namespace=$2 AND key=$3`,
		[]byte(`"after-reconnect"`), "ns", "k"); err != nil {
		t.Fatalf("write after terminate: %v", err)
	}

	select {
	case v := <-received:
		if v != "after-reconnect" {
			t.Fatalf("post-reconnect callback got %v, want after-reconnect", v)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for post-reconnect NOTIFY")
	}
}

// TestIntegration_Manager_DeleteThenReactivate verifies clean recycling of
// per-tenant state for the same tenantID across a delete/activate cycle.
func TestIntegration_Manager_DeleteThenReactivate(t *testing.T) {
	baseDSN, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	keys := []manager.RegisteredKey{{Namespace: "ns", Key: "k", DefaultValue: "default"}}

	m, _, _, mClean := setup(t, baseDSN, "tenant-c", keys)
	defer mClean()

	if err := m.OnTenantActivated(context.Background(), "tenant-c"); err != nil {
		t.Fatalf("activate 1: %v", err)
	}

	if err := m.OnTenantDeleted(context.Background(), "tenant-c"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Reactivation should succeed.
	if err := m.OnTenantActivated(context.Background(), "tenant-c"); err != nil {
		t.Fatalf("activate 2: %v", err)
	}

	got, hit, err := m.Lookup(context.Background(), "tenant-c", "ns", "k")
	if err != nil || !hit {
		t.Fatalf("Lookup after reactivate: err=%v hit=%v", err, hit)
	}

	if got != "default" {
		t.Fatalf("warm-load = %v, want default", got)
	}
}

// TestIntegration_Manager_CredentialsRotated verifies tear-down + re-bring-up
// via the rotation handler.
func TestIntegration_Manager_CredentialsRotated(t *testing.T) {
	baseDSN, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	keys := []manager.RegisteredKey{{Namespace: "ns", Key: "k", DefaultValue: "v"}}

	m, _, resolver, mClean := setup(t, baseDSN, "tenant-rot", keys)
	defer mClean()

	if err := m.OnTenantActivated(context.Background(), "tenant-rot"); err != nil {
		t.Fatalf("activate: %v", err)
	}

	if err := m.OnTenantCredentialsRotated(context.Background(), "tenant-rot"); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	// After rotation, NOTIFYs should still flow.
	received := make(chan any, 1)

	unsub := m.RegisterCallback("ns", "k", func(_ context.Context, _, _ string, newValue any) {
		select {
		case received <- newValue:
		default:
		}
	})
	defer unsub()

	if _, err := resolver.ExecContext(context.Background(),
		`UPDATE systemplane_entries SET value=$1 WHERE namespace=$2 AND key=$3`,
		[]byte(`"post-rotation"`), "ns", "k"); err != nil {
		t.Fatalf("write after rotation: %v", err)
	}

	select {
	case v := <-received:
		if v != "post-rotation" {
			t.Fatalf("post-rotation callback got %v", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for post-rotation NOTIFY")
	}
}

// TestIntegration_Manager_SeedDoesNotOverwriteExisting verifies that
// re-activation does not overwrite operator-set values.
func TestIntegration_Manager_SeedDoesNotOverwriteExisting(t *testing.T) {
	baseDSN, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	keys := []manager.RegisteredKey{{Namespace: "ns", Key: "k", DefaultValue: "default"}}

	m, _, resolver, mClean := setup(t, baseDSN, "tenant-noov", keys)
	defer mClean()

	if err := m.OnTenantActivated(context.Background(), "tenant-noov"); err != nil {
		t.Fatalf("activate 1: %v", err)
	}

	// Operator-set value.
	if _, err := resolver.ExecContext(context.Background(),
		`UPDATE systemplane_entries SET value=$1 WHERE namespace=$2 AND key=$3`,
		[]byte(`"operator-set"`), "ns", "k"); err != nil {
		t.Fatalf("operator write: %v", err)
	}

	// Drop the per-tenant state so re-activation runs a fresh seed.
	if err := m.OnTenantDeleted(context.Background(), "tenant-noov"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if err := m.OnTenantActivated(context.Background(), "tenant-noov"); err != nil {
		t.Fatalf("activate 2: %v", err)
	}

	got, hit, err := m.Lookup(context.Background(), "tenant-noov", "ns", "k")
	if err != nil || !hit {
		t.Fatalf("post-re-activate Lookup: err=%v hit=%v", err, hit)
	}

	if got != "operator-set" {
		t.Fatalf("seed overwrote operator value: got %v, want operator-set", got)
	}
}

func waitForCacheValue(m *manager.Manager, tenantID, ns, key string, want any, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		got, hit, _ := m.Lookup(context.Background(), tenantID, ns, key)

		if hit && got == want {
			return nil
		}

		time.Sleep(50 * time.Millisecond)
	}

	return errors.New("timeout waiting for cache value")
}

// guard imports
var _ = tmcore.ContextWithTenantID
