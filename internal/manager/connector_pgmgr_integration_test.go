//go:build integration

// Success-path coverage for pgMgrConnector. Drives a real *tmpostgres.Manager
// pre-loaded with a PostgresConnection backed by a live testcontainers
// Postgres so ResolveDB / ResolveDSN return non-nil DSN and non-nil
// dbresolver.DB without invoking the gRPC tenant-config client.
//
// This file is internal (package manager) so the pgMgrConnector type stays
// unexported.
package manager

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	tmpostgres "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/postgres"
	"github.com/bxcodec/dbresolver/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func startPGForConnector(t *testing.T) (string, func()) {
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

	cleanup := func() { _ = testcontainers.TerminateContainer(container) }

	return dsn, cleanup
}

// TestPgMgrConnector_SuccessPath drives both ResolveDB and ResolveDSN against
// a live tmpostgres.Manager that has been pre-populated with a healthy
// PostgresConnection (real *sql.DB inside dbresolver). This exercises every
// non-error branch of the production connector wrapper:
//
//   - mgr.GetConnection returns the cached PostgresConnection
//   - conn.GetDB() returns the live dbresolver.DB (ResolveDB success)
//   - conn.ConnectionStringPrimary is non-empty (ResolveDSN success)
func TestPgMgrConnector_SuccessPath(t *testing.T) {
	baseDSN, cleanup := startPGForConnector(t)
	t.Cleanup(cleanup)

	tenantSQL, err := sql.Open("pgx", baseDSN)
	if err != nil {
		t.Fatalf("open tenant: %v", err)
	}
	t.Cleanup(func() { _ = tenantSQL.Close() })

	if err := tenantSQL.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(tenantSQL))

	const tenantID = "tenant-success"

	// Pre-populate the upstream Manager's internal cache so GetConnection
	// returns this entry without touching a gRPC client.
	pg := tmpostgres.NewManager(nil, "systemplane.manager.test",
		tmpostgres.WithTestConnections(tenantID),
	)

	// WithTestConnections only sets a zero-value PostgresConnection. We need
	// to fill in ConnectionStringPrimary + ConnectionDB to exercise the
	// success path. Use Reset/Replace through the upstream type by reaching
	// into the preloaded entry: we cannot mutate it directly through the
	// API, so we instead build a connection manually and use the dedicated
	// upstream seam via the Connector wrapper.
	//
	// The upstream's GetConnection short-circuits the ping when
	// ConnectionDB.Ping fails, so we must keep the resolver healthy for the
	// duration of the test.
	conn, err := pg.GetConnection(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("seed GetConnection: %v", err)
	}

	conn.ConnectionStringPrimary = baseDSN
	conn.ConnectionDB = &resolver

	c := &pgMgrConnector{mgr: pg}

	db, err := c.ResolveDB(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("ResolveDB: %v", err)
	}

	if db == nil {
		t.Fatal("ResolveDB returned nil dbresolver.DB")
	}

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("returned DB unhealthy: %v", err)
	}

	dsn, err := c.ResolveDSN(context.Background(), tenantID)
	if err != nil {
		t.Fatalf("ResolveDSN: %v", err)
	}

	if dsn == "" {
		t.Fatal("ResolveDSN returned empty DSN")
	}

	// Sanity: the DSN should look like the seeded baseDSN (modulo encoding).
	if !strings.Contains(dsn, "@") {
		t.Fatalf("DSN missing @ separator: %q", dsn)
	}
}
