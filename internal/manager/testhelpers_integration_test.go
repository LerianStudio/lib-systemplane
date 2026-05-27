//go:build integration

// Shared integration-test helpers for the package-internal manager tests.
//
// These previously lived alongside the schema-bootstrap tests, which were
// removed when runtime schema provisioning was dropped. The Manager no longer
// creates its schema, so tests provision systemplane_entries (plus the NOTIFY
// trigger) the way a consumer's migration pipeline would — by executing the
// published DDL read from ddl/schema.sql (the single source of truth, also
// surfaced by the root package's SchemaSQL()).
package manager

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/bxcodec/dbresolver/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// schemaDDL loads ddl/schema.sql from the repository root. The root package's
// SchemaSQL() embeds the same file; this package cannot import the root
// (import cycle), so it reads the file directly to keep a single source of
// truth.
func schemaDDL(t *testing.T) string {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}

	// thisFile = <repo>/internal/manager/testhelpers_integration_test.go
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")

	raw, err := os.ReadFile(filepath.Join(repoRoot, "ddl", "schema.sql"))
	if err != nil {
		t.Fatalf("read ddl/schema.sql: %v", err)
	}

	return string(raw)
}

// provisionTestSchema applies the published DDL to db, simulating the
// consumer's external migration pipeline.
func provisionTestSchema(t *testing.T, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}) {
	t.Helper()

	if _, err := db.ExecContext(context.Background(), schemaDDL(t)); err != nil {
		t.Fatalf("provision schema: %v", err)
	}
}

func startPGForSchema(t *testing.T) (string, func()) {
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

func freshTenantDB(t *testing.T, baseDSN string) (dbresolver.DB, *sql.DB, string, func()) {
	t.Helper()

	admin, err := sql.Open("pgx", baseDSN)
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	defer admin.Close()

	dbName := strings.ReplaceAll(fmt.Sprintf("mgr_schema_%d", time.Now().UnixNano()), "-", "_")
	if _, err := admin.Exec(`CREATE DATABASE ` + dbName); err != nil {
		t.Fatalf("create db: %v", err)
	}

	tDSN := strings.Replace(baseDSN, "/postgres?", "/"+dbName+"?", 1)
	if tDSN == baseDSN {
		// no '?' suffix
		tDSN = strings.Replace(baseDSN, "/postgres", "/"+dbName, 1)
	}

	tenantSQL, err := sql.Open("pgx", tDSN)
	if err != nil {
		t.Fatalf("open tenant: %v", err)
	}

	if err := tenantSQL.Ping(); err != nil {
		_ = tenantSQL.Close()
		t.Fatalf("ping tenant: %v", err)
	}

	resolver := dbresolver.New(dbresolver.WithPrimaryDBs(tenantSQL))

	cleanup := func() {
		_ = tenantSQL.Close()
		// Drop DB in background; it's OK if it fails on teardown.
		admin2, _ := sql.Open("pgx", baseDSN)
		if admin2 != nil {
			_, _ = admin2.Exec(`DROP DATABASE IF EXISTS ` + dbName + ` WITH (FORCE)`)
			_ = admin2.Close()
		}
	}

	return resolver, tenantSQL, tDSN, cleanup
}
