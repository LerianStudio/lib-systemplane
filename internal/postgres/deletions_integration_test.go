//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	"github.com/LerianStudio/lib-systemplane/v4/internal/postgres"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// provisionDeletionHistory applies the opt-in deletion history DDL, the way a
// consumer that builds its Client with WithDeletionHistory does after
// SchemaSQL().
func provisionDeletionHistory(t *testing.T, db *sql.DB) {
	t.Helper()

	if _, err := db.Exec(systemplane.DeletionHistorySQL()); err != nil {
		t.Fatalf("provision deletion history: %v", err)
	}
}

// recordingTenantStore is tenantStore with RecordDeletions on.
func recordingTenantStore(t *testing.T, conn postgres.Connector) *postgres.Store {
	t.Helper()

	s, err := postgres.New(postgres.Config{MultiTenantEnabled: true, Connector: conn, RecordDeletions: true})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	return s
}

// TestIntegration_DeletionHistoryTableMissingFailsClosed pins the fail-closed
// half of the opt-in: a Store recording deletes over a database that never got
// DeletionHistorySQL() refuses the delete, and because the removal and the
// record are one statement the value is still there.
func TestIntegration_DeletionHistoryTableMissingFailsClosed(t *testing.T) {
	base := startContainer(t)

	admin := adminDSN(t, base)
	defer admin.Close()

	_, _, db := provisionTenantDB(t, admin, base, "del_missing")

	conn := newFakeConnector()
	conn.set("t1", db, "")

	s := recordingTenantStore(t, conn)
	ctx := context.Background()
	scope := store.Scope{Tenant: "t1"}

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`"v"`), UpdatedBy: "writer"}); err != nil {
		t.Fatalf("set: %v", err)
	}

	if err := s.Delete(ctx, scope, "ns", "k", "alice"); err == nil {
		t.Fatal("Delete with RecordDeletions over a database without systemplane_deletions succeeded; want an error")
	}

	if _, found, err := s.Get(ctx, scope, "ns", "k"); err != nil || !found {
		t.Fatalf("after the refused delete: found = %v, err = %v; want the value still stored", found, err)
	}
}

// TestIntegration_DeletionHistoryDMLOnlyRole pins the grant the docs promise:
// once the owner applied DeletionHistorySQL(), a runtime role with DML on
// systemplane_entries and only INSERT and SELECT on systemplane_deletions can
// delete, record and list.
func TestIntegration_DeletionHistoryDMLOnlyRole(t *testing.T) {
	base := startContainer(t)

	admin := adminDSN(t, base)
	defer admin.Close()

	dbName := fmt.Sprintf("del_dml_%d", time.Now().UnixNano())
	freshDB(t, admin, dbName)

	owner, err := sql.Open("pgx", dsnFor(base, dbName))
	if err != nil {
		t.Fatalf("open owner: %v", err)
	}

	defer owner.Close()

	provisionSchema(t, owner)
	provisionDeletionHistory(t, owner)

	roleName := fmt.Sprintf("sp_del_%d", time.Now().UnixNano())

	for _, stmt := range []string{
		fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD 'dmlpass'`, roleName),
		fmt.Sprintf(`REVOKE CREATE ON SCHEMA public FROM %s`, roleName),
		fmt.Sprintf(`GRANT USAGE ON SCHEMA public TO %s`, roleName),
		fmt.Sprintf(`GRANT SELECT, INSERT, UPDATE, DELETE ON systemplane_entries TO %s`, roleName),
		fmt.Sprintf(`GRANT INSERT, SELECT ON systemplane_deletions TO %s`, roleName),
	} {
		if _, err := owner.Exec(stmt); err != nil {
			t.Fatalf("grant %q: %v", stmt, err)
		}
	}

	roleDSN := dsnWithUser(dsnFor(base, dbName), roleName, "dmlpass")

	roleDB, err := sql.Open("pgx", roleDSN)
	if err != nil {
		t.Fatalf("open as %s: %v", roleName, err)
	}

	defer roleDB.Close()

	s, err := postgres.New(postgres.Config{DB: roleDB, ListenDSN: roleDSN, RecordDeletions: true})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	defer s.Close()

	ctx := context.Background()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	rev, err := s.Set(ctx, store.Scope{}, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`"v"`), UpdatedBy: "writer"})
	if err != nil {
		t.Fatalf("set: %v", err)
	}

	if err := s.Delete(ctx, store.Scope{}, "ns", "k", "alice"); err != nil {
		t.Fatalf("delete as the DML-only role: %v", err)
	}

	got, err := s.ListDeletions(ctx, store.Scope{}, "ns", "k", 10)
	if err != nil {
		t.Fatalf("list deletions as the DML-only role: %v", err)
	}

	if len(got) != 1 || got[0].DeletedBy != "alice" || got[0].Revision != rev {
		t.Errorf("history = %+v, want one record by alice at revision %d", got, rev)
	}
}

// TestIntegration_DeletionHistoryIsTenantScoped pins that a history lives in
// its tenant's own database: a delete in one tenant is invisible to another.
func TestIntegration_DeletionHistoryIsTenantScoped(t *testing.T) {
	base := startContainer(t)

	admin := adminDSN(t, base)
	defer admin.Close()

	_, _, dbA := provisionTenantDB(t, admin, base, "del_ta")
	_, _, dbB := provisionTenantDB(t, admin, base, "del_tb")

	provisionDeletionHistory(t, dbA)
	provisionDeletionHistory(t, dbB)

	conn := newFakeConnector()
	conn.set("a", dbA, "")
	conn.set("b", dbB, "")

	s := recordingTenantStore(t, conn)
	ctx := context.Background()

	for _, tenant := range []string{"a", "b"} {
		if _, err := s.Set(ctx, store.Scope{Tenant: tenant}, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`)}); err != nil {
			t.Fatalf("set in %s: %v", tenant, err)
		}
	}

	if err := s.Delete(ctx, store.Scope{Tenant: "a"}, "ns", "k", "alice"); err != nil {
		t.Fatalf("delete in a: %v", err)
	}

	gotA, err := s.ListDeletions(ctx, store.Scope{Tenant: "a"}, "ns", "k", 10)
	if err != nil || len(gotA) != 1 {
		t.Fatalf("history in a = (%+v, %v), want one record", gotA, err)
	}

	gotB, err := s.ListDeletions(ctx, store.Scope{Tenant: "b"}, "ns", "k", 10)
	if err != nil || len(gotB) != 0 {
		t.Fatalf("history in b = (%+v, %v), want none", gotB, err)
	}
}
