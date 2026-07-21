//go:build unit

// Production-path coverage for pgMgrConnector. The nil-receiver / nil-Manager
// edges are pinned in connector_test.go; this file drives the live
// tmpostgres.Manager so the wrapper's success-path / GetConnection-error /
// GetDB-error / empty-DSN branches all execute.
//
// We build the upstream *tmpostgres.Manager directly with two construction
// shapes that don't require a real gRPC tenant-config client:
//
//   - NewManager(nil, "svc")            → no client; GetConnection routes to
//     createConnection which short-circuits with "tenant manager client is
//     required for multi-tenant connections". Exercises the GetConnection
//     error branch of both ResolveDB and ResolveDSN.
//
//   - NewManager(nil, "svc", WithTestConnections("t"))
//     → preloads connections["t"] = &PostgresConnection{} (zero-value).
//     GetConnection returns this entry; conn.GetDB() then errors because
//     ConnectionDB is nil, and ConnectionStringPrimary is empty for the
//     DSN branch. Exercises the second error branches.
//
// Success-path coverage (ConnectionDB non-nil + non-empty DSN) is provided by
// the testcontainers-backed integration test in connector_pgmgr_integration_test.go.
package manager

import (
	"context"
	"errors"
	"strings"
	"testing"

	tmpostgres "github.com/LerianStudio/lib-commons/v6/commons/tenant-manager/postgres"
)

func TestPgMgrConnector_ResolveDB_GetConnectionFails(t *testing.T) {
	t.Parallel()

	// NewManager(nil, ...) → upstream sees a nil client and returns
	// "tenant manager client is required for multi-tenant connections" from
	// createConnection, which is the GetConnection error path.
	pg := tmpostgres.NewManager(nil, "systemplane.manager.test")
	c := &pgMgrConnector{mgr: pg}

	_, err := c.ResolveDB(context.Background(), "tenant-A")
	if err == nil {
		t.Fatal("expected error when GetConnection fails")
	}

	if !strings.Contains(err.Error(), "systemplane/manager: get tenant connection") {
		t.Fatalf("expected wrapped GetConnection error, got %v", err)
	}

	if !strings.Contains(err.Error(), "tenant-A") {
		t.Fatalf("error must include tenant id, got %v", err)
	}
}

func TestPgMgrConnector_ResolveDSN_GetConnectionFails(t *testing.T) {
	t.Parallel()

	pg := tmpostgres.NewManager(nil, "systemplane.manager.test")
	c := &pgMgrConnector{mgr: pg}

	_, err := c.ResolveDSN(context.Background(), "tenant-B")
	if err == nil {
		t.Fatal("expected error when GetConnection fails")
	}

	if !strings.Contains(err.Error(), "systemplane/manager: get tenant connection") {
		t.Fatalf("expected wrapped GetConnection error, got %v", err)
	}

	if !strings.Contains(err.Error(), "tenant-B") {
		t.Fatalf("error must include tenant id, got %v", err)
	}
}

func TestPgMgrConnector_ResolveDB_GetDBFails(t *testing.T) {
	t.Parallel()

	// WithTestConnections preloads connections["t"] with a zero-value
	// PostgresConnection. GetConnection returns the cached entry, and the
	// subsequent conn.GetDB() returns "postgres resolver not initialized"
	// because ConnectionDB is nil. That drives the second error branch in
	// ResolveDB.
	pg := tmpostgres.NewManager(nil, "systemplane.manager.test",
		tmpostgres.WithTestConnections("tenant-C"),
	)
	c := &pgMgrConnector{mgr: pg}

	_, err := c.ResolveDB(context.Background(), "tenant-C")
	if err == nil {
		t.Fatal("expected error when GetDB fails")
	}

	if !strings.Contains(err.Error(), "systemplane/manager: get tenant DB") {
		t.Fatalf("expected wrapped GetDB error, got %v", err)
	}

	if !strings.Contains(err.Error(), "tenant-C") {
		t.Fatalf("error must include tenant id, got %v", err)
	}
}

func TestPgMgrConnector_ResolveDSN_EmptyPrimary(t *testing.T) {
	t.Parallel()

	// WithTestConnections sets ConnectionStringPrimary="" so ResolveDSN hits
	// the dedicated "empty primary DSN" branch.
	pg := tmpostgres.NewManager(nil, "systemplane.manager.test",
		tmpostgres.WithTestConnections("tenant-D"),
	)
	c := &pgMgrConnector{mgr: pg}

	_, err := c.ResolveDSN(context.Background(), "tenant-D")
	if err == nil {
		t.Fatal("expected error when ConnectionStringPrimary is empty")
	}

	if !strings.Contains(err.Error(), "has empty primary DSN") {
		t.Fatalf("expected empty-primary-DSN error, got %v", err)
	}

	if !strings.Contains(err.Error(), "tenant-D") {
		t.Fatalf("error must include tenant id, got %v", err)
	}
}

func TestPgMgrConnector_New_WiresPgMgrConnector(t *testing.T) {
	t.Parallel()

	// Pin the constructor's pgMgr→connector wiring: New(pgMgr) MUST install a
	// *pgMgrConnector that talks to the supplied manager. Confirms a real
	// production path (not a SetConnector test seam) is exercised end-to-end
	// without needing a live tenant-manager.
	pg := tmpostgres.NewManager(nil, "systemplane.manager.test")
	m := New(pg)

	if m.connector == nil {
		t.Fatal("expected New to wire a connector for non-nil pgMgr")
	}

	c, ok := m.connector.(*pgMgrConnector)
	if !ok {
		t.Fatalf("expected *pgMgrConnector, got %T", m.connector)
	}

	if c.mgr != pg {
		t.Fatal("pgMgrConnector must reference the supplied tmpostgres.Manager")
	}

	// And it must fail predictably (no gRPC client) — flows through the same
	// error branches as the standalone connector tests, confirming wiring.
	if _, err := m.connector.ResolveDB(context.Background(), "x"); err == nil ||
		!strings.Contains(err.Error(), "systemplane/manager: get tenant connection") {
		t.Fatalf("wired connector must surface GetConnection error, got %v", err)
	}

	// Make sure the manager isn't accidentally marked closed.
	if m.IsClosed() {
		t.Fatal("New must not return a closed Manager")
	}

	// Use errors.Is to assert ErrPgMgrUnavailable is NOT returned (the wired
	// connector talks to a real tmpostgres.Manager, so the sentinel applies
	// only to the nil-mgr case).
	_, err := m.connector.ResolveDSN(context.Background(), "x")
	if errors.Is(err, ErrPgMgrUnavailable) {
		t.Fatal("ErrPgMgrUnavailable must not surface from a wired connector")
	}
}
