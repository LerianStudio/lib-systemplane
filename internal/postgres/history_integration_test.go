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

// provisionChangeHistory applies the opt-in change history DDL, the way a
// consumer that builds its Client with WithChangeHistory does after
// SchemaSQL().
func provisionChangeHistory(t *testing.T, db *sql.DB) {
	t.Helper()

	if _, err := db.Exec(systemplane.ChangeHistorySQL()); err != nil {
		t.Fatalf("provision change history: %v", err)
	}
}

// recordingTenantStore is tenantStore with RecordChanges on.
func recordingTenantStore(t *testing.T, conn postgres.Connector) *postgres.Store {
	t.Helper()

	s, err := postgres.New(postgres.Config{MultiTenantEnabled: true, Connector: conn, RecordChanges: true})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	return s
}

// TestIntegration_ChangeHistoryTableMissingFailsClosed pins the fail-closed
// half of the opt-in: a Store recording changes over a database that never got
// ChangeHistorySQL() refuses both writes, and because each write and its record
// commit together the stored value is untouched.
func TestIntegration_ChangeHistoryTableMissingFailsClosed(t *testing.T) {
	base := startContainer(t)

	admin := adminDSN(t, base)
	defer admin.Close()

	_, _, db := provisionTenantDB(t, admin, base, "hist_missing")

	conn := newFakeConnector()
	conn.set("t1", db, "")

	plain := tenantStore(t, conn)
	s := recordingTenantStore(t, conn)
	ctx := context.Background()
	scope := store.Scope{Tenant: "t1"}

	if _, err := plain.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`"v"`), UpdatedBy: "writer"}); err != nil {
		t.Fatalf("set without the history: %v", err)
	}

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`"v2"`), UpdatedBy: "alice"}); err == nil {
		t.Fatal("Set with RecordChanges over a database without systemplane_history succeeded; want an error")
	}

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "fresh", Value: []byte(`1`), UpdatedBy: "alice"}); err == nil {
		t.Fatal("creating Set with RecordChanges over a database without systemplane_history succeeded; want an error")
	}

	if err := s.Delete(ctx, scope, "ns", "k", "alice"); err == nil {
		t.Fatal("Delete with RecordChanges over a database without systemplane_history succeeded; want an error")
	}

	got, found, err := s.Get(ctx, scope, "ns", "k")
	if err != nil || !found || string(got.Value) != `"v"` || got.UpdatedBy != "writer" {
		t.Fatalf("after the refused writes: Get = (%+v, %v, %v), want the value writer stored", got, found, err)
	}

	if _, found, err := s.Get(ctx, scope, "ns", "fresh"); err != nil || found {
		t.Fatalf("after the refused create: found = %v, err = %v; want no row", found, err)
	}
}

// TestIntegration_ChangeHistoryDMLOnlyRole pins the grant the docs promise:
// once the owner applied ChangeHistorySQL(), a runtime role with DML on
// systemplane_entries and only INSERT and SELECT on systemplane_history can
// create, update, delete, record and list. The history's identity column draws
// its numbers without any grant on its sequence.
func TestIntegration_ChangeHistoryDMLOnlyRole(t *testing.T) {
	base := startContainer(t)

	admin := adminDSN(t, base)
	defer admin.Close()

	dbName := fmt.Sprintf("hist_dml_%d", time.Now().UnixNano())
	freshDB(t, admin, dbName)

	owner, err := sql.Open("pgx", dsnFor(base, dbName))
	if err != nil {
		t.Fatalf("open owner: %v", err)
	}

	defer owner.Close()

	provisionSchema(t, owner)
	provisionChangeHistory(t, owner)

	roleName := fmt.Sprintf("sp_hist_%d", time.Now().UnixNano())

	for _, stmt := range []string{
		fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD 'dmlpass'`, roleName),
		fmt.Sprintf(`REVOKE CREATE ON SCHEMA public FROM %s`, roleName),
		fmt.Sprintf(`GRANT USAGE ON SCHEMA public TO %s`, roleName),
		fmt.Sprintf(`GRANT SELECT, INSERT, UPDATE, DELETE ON systemplane_entries TO %s`, roleName),
		fmt.Sprintf(`GRANT INSERT, SELECT ON systemplane_history TO %s`, roleName),
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

	s, err := postgres.New(postgres.Config{DB: roleDB, ListenDSN: roleDSN, RecordChanges: true})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	defer s.Close()

	ctx := context.Background()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	if _, err := s.Set(ctx, store.Scope{}, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`"v1"`), UpdatedBy: "writer"}); err != nil {
		t.Fatalf("create as the DML-only role: %v", err)
	}

	rev, err := s.Set(ctx, store.Scope{}, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`"v2"`), UpdatedBy: "editor"})
	if err != nil {
		t.Fatalf("update as the DML-only role: %v", err)
	}

	if err := s.Delete(ctx, store.Scope{}, "ns", "k", "alice"); err != nil {
		t.Fatalf("delete as the DML-only role: %v", err)
	}

	got, err := s.ListHistory(ctx, store.Scope{}, "ns", "k", 10)
	if err != nil {
		t.Fatalf("list history as the DML-only role: %v", err)
	}

	if len(got) != 3 || got[0].Operation != store.ChangeDelete || got[0].ChangedBy != "alice" || got[0].Revision != rev ||
		string(got[0].PreviousValue) != `"v2"` || got[1].Operation != store.ChangeUpdate || got[2].Operation != store.ChangeCreate {
		t.Errorf("history = %+v, want delete by alice of \"v2\" at revision %d, then update, then create", got, rev)
	}
}

// TestIntegration_ChangeHistoryIsTenantScoped pins that a history lives in its
// tenant's own database: a write in one tenant is invisible to another.
func TestIntegration_ChangeHistoryIsTenantScoped(t *testing.T) {
	base := startContainer(t)

	admin := adminDSN(t, base)
	defer admin.Close()

	_, _, dbA := provisionTenantDB(t, admin, base, "hist_ta")
	_, _, dbB := provisionTenantDB(t, admin, base, "hist_tb")

	provisionChangeHistory(t, dbA)
	provisionChangeHistory(t, dbB)

	conn := newFakeConnector()
	conn.set("a", dbA, "")
	conn.set("b", dbB, "")

	s := recordingTenantStore(t, conn)
	ctx := context.Background()

	if _, err := s.Set(ctx, store.Scope{Tenant: "a"}, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`), UpdatedBy: "alice"}); err != nil {
		t.Fatalf("set in a: %v", err)
	}

	if err := s.Delete(ctx, store.Scope{Tenant: "a"}, "ns", "k", "alice"); err != nil {
		t.Fatalf("delete in a: %v", err)
	}

	gotA, err := s.ListHistory(ctx, store.Scope{Tenant: "a"}, "ns", "k", 10)
	if err != nil || len(gotA) != 2 {
		t.Fatalf("history in a = (%+v, %v), want a create and a delete", gotA, err)
	}

	gotB, err := s.ListHistory(ctx, store.Scope{Tenant: "b"}, "ns", "k", 10)
	if err != nil || len(gotB) != 0 {
		t.Fatalf("history in b = (%+v, %v), want none", gotB, err)
	}
}
