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
package postgres

import (
	"context"
	"strings"
	"testing"

	tmpostgres "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/postgres"
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

	if !strings.Contains(err.Error(), "systemplane/postgres: get tenant connection") {
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

	if !strings.Contains(err.Error(), "systemplane/postgres: get tenant connection") {
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

	if !strings.Contains(err.Error(), "systemplane/postgres: get tenant DB") {
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
