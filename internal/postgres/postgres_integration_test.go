//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	systemplane "github.com/LerianStudio/lib-systemplane"
	"github.com/LerianStudio/lib-systemplane/internal/postgres"
	"github.com/LerianStudio/lib-systemplane/internal/store"
	"github.com/LerianStudio/lib-systemplane/systemplanetest"
	"github.com/bxcodec/dbresolver/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	pgcontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// startContainer launches a single Postgres testcontainer reused across the
// integration tests in this file.
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

// adminDSN returns a connection string for the postgres admin database used
// to create per-test databases.
func adminDSN(t *testing.T, base string) *sql.DB {
	t.Helper()

	db, err := sql.Open("pgx", base)
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}

	return db
}

// freshDB creates a uniquely-named database and returns its DSN.
func freshDB(t *testing.T, admin *sql.DB, name string) string {
	t.Helper()

	if _, err := admin.Exec(fmt.Sprintf(`CREATE DATABASE %s`, name)); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}

	return name
}

// provisionSchema applies the published systemplane DDL to db. The Store no
// longer creates its schema at runtime, so the consumer (here, the test acting
// as the consumer's migration pipeline) provisions systemplane_entries plus
// the NOTIFY trigger before exercising reads/writes.
func provisionSchema(t *testing.T, db *sql.DB) {
	t.Helper()

	if _, err := db.Exec(systemplane.SchemaSQL()); err != nil {
		t.Fatalf("provision schema: %v", err)
	}
}

// dsnFor reshapes the admin DSN to point at db.
func dsnFor(base, dbName string) string {
	// testcontainers gives us a fully-formed URL of the shape
	// postgres://user:pass@host:port/postgres?sslmode=disable; we swap the
	// database segment with dbName.
	for i := len(base) - 1; i >= 0; i-- {
		if base[i] == '/' {
			head := base[:i+1]
			tail := base[i+1:]

			// Drop the existing dbname (everything up to the first '?').
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

func TestIntegration_PostgresSingleTenant(t *testing.T) {
	dsn, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	factory := func(t *testing.T) (store.Store, func()) {
		t.Helper()

		admin := adminDSN(t, dsn)
		dbName := fmt.Sprintf("st_%d", time.Now().UnixNano())
		freshDB(t, admin, dbName)

		_ = admin.Close()

		tenantDSN := dsnFor(dsn, dbName)

		db, err := sql.Open("pgx", tenantDSN)
		if err != nil {
			t.Fatalf("open: %v", err)
		}

		// The Store no longer auto-creates its schema; provision it the way a
		// consumer's migration pipeline would.
		provisionSchema(t, db)

		s, err := postgres.New(postgres.Config{
			DB:        db,
			ListenDSN: tenantDSN,
		})
		if err != nil {
			t.Fatalf("postgres.New: %v", err)
		}

		return s, func() {
			_ = s.Close()
			_ = db.Close()
		}
	}

	systemplanetest.Run(t, factory, systemplanetest.RunOptions{EventWait: 5 * time.Second})
}

// TestIntegration_PostgresMultiTenantIsolation verifies that writes against
// one tenant's database are invisible to another tenant in multi-tenant mode,
// using schema that the consumer provisioned externally (the Store performs no
// runtime DDL).
func TestIntegration_PostgresMultiTenantIsolation(t *testing.T) {
	dsn, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	admin := adminDSN(t, dsn)
	defer admin.Close()

	dbA := fmt.Sprintf("tenant_a_%d", time.Now().UnixNano())
	dbB := fmt.Sprintf("tenant_b_%d", time.Now().UnixNano())

	freshDB(t, admin, dbA)
	freshDB(t, admin, dbB)

	tenantA, err := sql.Open("pgx", dsnFor(dsn, dbA))
	if err != nil {
		t.Fatalf("open A: %v", err)
	}

	defer tenantA.Close()

	tenantB, err := sql.Open("pgx", dsnFor(dsn, dbB))
	if err != nil {
		t.Fatalf("open B: %v", err)
	}

	defer tenantB.Close()

	// Provision each tenant DB externally — the Store no longer creates its
	// schema at runtime.
	provisionSchema(t, tenantA)
	provisionSchema(t, tenantB)

	resolverA := dbresolver.New(dbresolver.WithPrimaryDBs(tenantA))
	resolverB := dbresolver.New(dbresolver.WithPrimaryDBs(tenantB))

	s, err := postgres.New(postgres.Config{
		MultiTenantEnabled: true,
		Module:             "systemplane",
	})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	defer s.Close()

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	ctxA := tmcore.ContextWithPG(context.Background(), resolverA, "systemplane")
	ctxB := tmcore.ContextWithPG(context.Background(), resolverB, "systemplane")

	mustSet(t, s, ctxA, "ns", "k", "value-A")
	mustSet(t, s, ctxB, "ns", "k", "value-B")

	gotA := mustGet(t, s, ctxA, "ns", "k")
	if gotA != "value-A" {
		t.Errorf("tenant A read = %q, want value-A", gotA)
	}

	gotB := mustGet(t, s, ctxB, "ns", "k")
	if gotB != "value-B" {
		t.Errorf("tenant B read = %q, want value-B", gotB)
	}

	// Confirm tenant A cannot see tenant B's value or vice-versa.
	listA, err := s.List(ctxA)
	if err != nil {
		t.Fatalf("listA: %v", err)
	}

	for _, e := range listA {
		var v string
		_ = json.Unmarshal(e.Value, &v)

		if v == "value-B" {
			t.Errorf("tenant A list leaked tenant B value")
		}
	}

	// Drop a trigger out-of-band, then issue another write. The Store must NOT
	// recreate it — it performs no runtime DDL. The trigger STAYS gone,
	// proving the store never issues CREATE statements at runtime.
	if _, err := tenantA.Exec(`DROP TRIGGER IF EXISTS systemplane_notify_trigger ON systemplane_entries`); err != nil {
		t.Fatalf("drop trigger: %v", err)
	}

	if err := s.Set(ctxA, store.Entry{Namespace: "ns", Key: "k2", Value: jsonBytes(t, "again")}); err != nil {
		t.Fatalf("second set on A: %v", err)
	}

	var triggerExists bool

	if err := tenantA.QueryRow(`SELECT EXISTS (
		SELECT 1 FROM information_schema.triggers
		WHERE event_object_table = 'systemplane_entries'
		  AND trigger_name = 'systemplane_notify_trigger'
	)`).Scan(&triggerExists); err != nil {
		t.Fatalf("check trigger: %v", err)
	}

	if triggerExists {
		t.Errorf("store issued runtime DDL — dropped trigger was recreated")
	}

	// Multi-tenant mode disables Subscribe.
	if _, err := s.Subscribe(ctxA, func(_ store.Event) {}); err != store.ErrNotSupportedInMultiTenant {
		t.Errorf("subscribe should fail with ErrNotSupportedInMultiTenant, got %v", err)
	}
}

func TestIntegration_PostgresMultiTenantMissingCtx(t *testing.T) {
	dsn, cleanup := startContainer(t)
	t.Cleanup(cleanup)
	_ = dsn

	s, err := postgres.New(postgres.Config{
		MultiTenantEnabled: true,
		Module:             "systemplane",
	})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	defer s.Close()

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	_, _, err = s.Get(context.Background(), "ns", "k")
	if err != store.ErrTenantConnectionMissing {
		t.Errorf("expected ErrTenantConnectionMissing, got %v", err)
	}
}

// TestIntegration_PostgresLeastPrivilegeRole_NoRuntimeDDL pins the core
// contract of this change: when the schema is provisioned externally and the
// runtime role has only DML (SELECT/INSERT/UPDATE/DELETE) — no CREATE on the
// schema — Get/Set/Delete still succeed. If the Store attempted any runtime
// DDL it would fail with "permission denied for schema" (42501).
func TestIntegration_PostgresLeastPrivilegeRole_NoRuntimeDDL(t *testing.T) {
	dsn, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	admin := adminDSN(t, dsn)
	defer admin.Close()

	dbName := fmt.Sprintf("lp_%d", time.Now().UnixNano())
	freshDB(t, admin, dbName)

	// Provision the schema as a privileged role (the owner of the fresh DB).
	owner, err := sql.Open("pgx", dsnFor(dsn, dbName))
	if err != nil {
		t.Fatalf("open owner: %v", err)
	}

	defer owner.Close()

	provisionSchema(t, owner)

	// Create a least-privilege role with DML only — explicitly NO CREATE on
	// the public schema. This mirrors the role the tenant-manager hands the
	// runtime.
	roleName := fmt.Sprintf("sp_dml_%d", time.Now().UnixNano())
	rolePass := "dmlpass"

	if _, err := owner.Exec(fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, roleName, rolePass)); err != nil {
		t.Fatalf("create role: %v", err)
	}

	stmts := []string{
		fmt.Sprintf(`REVOKE CREATE ON SCHEMA public FROM %s`, roleName),
		fmt.Sprintf(`GRANT USAGE ON SCHEMA public TO %s`, roleName),
		fmt.Sprintf(`GRANT SELECT, INSERT, UPDATE, DELETE ON systemplane_entries TO %s`, roleName),
	}
	for _, stmt := range stmts {
		if _, err := owner.Exec(stmt); err != nil {
			t.Fatalf("grant stmt %q: %v", stmt, err)
		}
	}

	// Build a DSN for the least-privilege role.
	lpDSN := dsnWithUser(dsnFor(dsn, dbName), roleName, rolePass)

	lpDB, err := sql.Open("pgx", lpDSN)
	if err != nil {
		t.Fatalf("open least-privilege: %v", err)
	}

	defer lpDB.Close()

	s, err := postgres.New(postgres.Config{DB: lpDB, ListenDSN: lpDSN})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	defer s.Close()

	// Start must succeed without issuing any DDL (it only opens LISTEN now).
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start with least-privilege role: %v", err)
	}

	ctx := context.Background()

	if err := s.Set(ctx, store.Entry{Namespace: "ns", Key: "k", Value: jsonBytes(t, "value")}); err != nil {
		t.Fatalf("Set with least-privilege role: %v", err)
	}

	got := mustGet(t, s, ctx, "ns", "k")
	if got != "value" {
		t.Fatalf("Get = %q, want value", got)
	}

	if err := s.Delete(ctx, "ns", "k", "actor"); err != nil {
		t.Fatalf("Delete with least-privilege role: %v", err)
	}
}

// dsnWithUser rewrites the userinfo segment of a postgres URL DSN to user:pass.
func dsnWithUser(base, user, pass string) string {
	// base is postgres://olduser:oldpass@host:port/db?params
	const scheme = "postgres://"

	if !strings.HasPrefix(base, scheme) {
		return base
	}

	rest := base[len(scheme):]

	at := strings.IndexByte(rest, '@')
	if at < 0 {
		return base
	}

	return scheme + user + ":" + pass + "@" + rest[at+1:]
}

func mustSet(t *testing.T, s store.Store, ctx context.Context, ns, key, value string) {
	t.Helper()

	if err := s.Set(ctx, store.Entry{Namespace: ns, Key: key, Value: jsonBytes(t, value)}); err != nil {
		t.Fatalf("set: %v", err)
	}
}

func mustGet(t *testing.T, s store.Store, ctx context.Context, ns, key string) string {
	t.Helper()

	entry, found, err := s.Get(ctx, ns, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !found {
		t.Fatalf("get: not found")
	}

	var v string

	if err := json.Unmarshal(entry.Value, &v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	return v
}

func jsonBytes(t *testing.T, v any) []byte {
	t.Helper()

	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	return raw
}

// ensure the sync/sync imports are used when only some sub-tests run.
var _ = sync.Mutex{}
