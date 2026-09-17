//go:build integration

package postgres_test

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

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	systemplane "github.com/LerianStudio/lib-systemplane/v4"
	"github.com/LerianStudio/lib-systemplane/v4/internal/postgres"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"github.com/LerianStudio/lib-systemplane/v4/systemplanetest"
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
	listA, err := s.List(ctxA, store.Scope{})
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

	if _, err := s.Set(ctxA, store.Scope{}, store.Entry{Namespace: "ns", Key: "k2", Value: jsonBytes(t, "again")}); err != nil {
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
	if _, err := s.Subscribe(ctxA, store.Scope{}, func(_ store.Event) {}); err != store.ErrNotSupportedInMultiTenant {
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

	_, _, err = s.Get(context.Background(), store.Scope{}, "ns", "k")
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

	if _, err := s.Set(ctx, store.Scope{}, store.Entry{Namespace: "ns", Key: "k", Value: jsonBytes(t, "value")}); err != nil {
		t.Fatalf("Set with least-privilege role: %v", err)
	}

	got := mustGet(t, s, ctx, "ns", "k")
	if got != "value" {
		t.Fatalf("Get = %q, want value", got)
	}

	if err := s.Delete(ctx, store.Scope{}, "ns", "k", "actor"); err != nil {
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

	if _, err := s.Set(ctx, store.Scope{}, store.Entry{Namespace: ns, Key: key, Value: jsonBytes(t, value)}); err != nil {
		t.Fatalf("set: %v", err)
	}
}

func mustGet(t *testing.T, s store.Store, ctx context.Context, ns, key string) string {
	t.Helper()

	entry, found, err := s.Get(ctx, store.Scope{}, ns, key)
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

// freshStore provisions a container-backed database with the published schema
// and returns a Store over it. CRUD needs no Start: only the changefeed does.
func freshStore(t *testing.T, prefix string) *postgres.Store {
	t.Helper()

	dsn, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	admin := adminDSN(t, dsn)
	dbName := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	freshDB(t, admin, dbName)
	_ = admin.Close()

	tenantDSN := dsnFor(dsn, dbName)

	db, err := sql.Open("pgx", tenantDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	t.Cleanup(func() { _ = db.Close() })

	provisionSchema(t, db)

	s, err := postgres.New(postgres.Config{DB: db, ListenDSN: tenantDSN})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	return s
}

// TestIntegration_PostgresSetReturnsRevision pins FC-2's revision contract on
// the Postgres write path: the first write stores revision 1, a write of a
// DIFFERENT value advances it, a write of the SAME value does not (the BEFORE
// UPDATE trigger is gated on OLD.value IS DISTINCT FROM NEW.value and the
// ON CONFLICT DO UPDATE set-list deliberately omits revision), and both read
// paths report exactly the number Set reported.
func TestIntegration_PostgresSetReturnsRevision(t *testing.T) {
	s := freshStore(t, "rev")
	ctx := context.Background()

	set := func(value string) int64 {
		t.Helper()

		rev, err := s.Set(ctx, store.Scope{}, store.Entry{Namespace: "ns", Key: "k", Value: jsonBytes(t, value)})
		if err != nil {
			t.Fatalf("set %q: %v", value, err)
		}

		return rev
	}

	if got := set("v1"); got != 1 {
		t.Fatalf("first Set revision = %d, want 1", got)
	}

	assertRevision(t, s, ctx, 1)

	if got := set("v2"); got != 2 {
		t.Fatalf("Set of a different value revision = %d, want 2", got)
	}

	assertRevision(t, s, ctx, 2)

	if got := set("v2"); got != 2 {
		t.Fatalf("Set of an identical value revision = %d, want 2 (unchanged)", got)
	}

	assertRevision(t, s, ctx, 2)
}

// assertRevision checks that Get and the matching List entry both report want.
func assertRevision(t *testing.T, s store.Store, ctx context.Context, want int64) {
	t.Helper()

	entry, found, err := s.Get(ctx, store.Scope{}, "ns", "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !found {
		t.Fatalf("get: not found")
	}

	if entry.Revision != want {
		t.Errorf("Get revision = %d, want %d", entry.Revision, want)
	}

	entries, err := s.List(ctx, store.Scope{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	for _, e := range entries {
		if e.Namespace == "ns" && e.Key == "k" {
			if e.Revision != want {
				t.Errorf("List revision = %d, want %d", e.Revision, want)
			}

			return
		}
	}

	t.Fatalf("list: entry ns/k not found")
}

// TestIntegration_PostgresDollarPrefixedStringsStoredVerbatim is the reference
// behaviour that the MongoDB update pipeline must match: a namespace, key and
// actor beginning with "$" round-trip byte-identical. Postgres gets this for
// free — each one travels as a bind parameter and is never interpreted — and
// the test exists so no future MongoDB fix can "solve" $-prefixed strings by
// rejecting them.
func TestIntegration_PostgresDollarPrefixedStringsStoredVerbatim(t *testing.T) {
	s := freshStore(t, "dollar")
	ctx := context.Background()

	const (
		ns    = "$ns"
		key   = "$key"
		actor = "$value"
	)

	if _, err := s.Set(ctx, store.Scope{}, store.Entry{
		Namespace: ns,
		Key:       key,
		Value:     jsonBytes(t, "payload"),
		UpdatedBy: actor,
	}); err != nil {
		t.Fatalf("set: %v", err)
	}

	entry, found, err := s.Get(ctx, store.Scope{}, ns, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !found {
		t.Fatalf("get: not found")
	}

	if entry.Namespace != ns {
		t.Errorf("namespace = %q, want %q", entry.Namespace, ns)
	}

	if entry.Key != key {
		t.Errorf("key = %q, want %q", entry.Key, key)
	}

	if entry.UpdatedBy != actor {
		t.Errorf("updated_by = %q, want %q", entry.UpdatedBy, actor)
	}
}

// fakeConnector resolves tenants from static maps, standing in for a
// tenant-manager Postgres Manager without a tenant-config gRPC client. It is
// mutable under a mutex so a test can teach it a tenant it previously did not
// know, and it counts DSN resolutions so a test can prove that a re-subscribed
// tenant resolves again — which is how a credentials rotation is picked up.
type fakeConnector struct {
	mu       sync.Mutex
	dbs      map[string]*sql.DB
	dsns     map[string]string
	dsnCalls int
}

func newFakeConnector() *fakeConnector {
	return &fakeConnector{dbs: map[string]*sql.DB{}, dsns: map[string]string{}}
}

func (c *fakeConnector) set(tenantID string, db *sql.DB, dsn string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.dbs[tenantID] = db
	c.dsns[tenantID] = dsn
}

func (c *fakeConnector) resolveDSNCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.dsnCalls
}

func (c *fakeConnector) ResolveDB(_ context.Context, tenantID string) (dbresolver.DB, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	db, ok := c.dbs[tenantID]
	if !ok {
		return nil, fmt.Errorf("fakeConnector: unknown tenant %q", tenantID)
	}

	return dbresolver.New(dbresolver.WithPrimaryDBs(db)), nil
}

func (c *fakeConnector) ResolveDSN(_ context.Context, tenantID string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.dsnCalls++

	dsn, ok := c.dsns[tenantID]
	if !ok {
		return "", fmt.Errorf("fakeConnector: unknown tenant %q", tenantID)
	}

	return dsn, nil
}

// TestIntegration_PostgresScopedCRUDIsolation pins FC-2's scoped resolution on
// the Postgres CRUD path: a named Scope.Tenant resolves its database through
// the connector and nothing else. Every call travels on a plain
// context.Background() carrying no tenant at all, so a passing test proves the
// handle came from the connector rather than from ctx, and each tenant sees
// only its own rows.
func TestIntegration_PostgresScopedCRUDIsolation(t *testing.T) {
	dsn, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	admin := adminDSN(t, dsn)
	defer admin.Close()

	conn := newFakeConnector()

	for _, tenant := range []string{"t1", "t2"} {
		dbName := fmt.Sprintf("scoped_%s_%d", tenant, time.Now().UnixNano())
		freshDB(t, admin, dbName)

		tenantDSN := dsnFor(dsn, dbName)

		db, err := sql.Open("pgx", tenantDSN)
		if err != nil {
			t.Fatalf("open %s: %v", tenant, err)
		}

		t.Cleanup(func() { _ = db.Close() })

		provisionSchema(t, db)

		conn.set(tenant, db, tenantDSN)
	}

	// No DB, no ListenDSN: every handle must come from the connector.
	s, err := postgres.New(postgres.Config{MultiTenantEnabled: true, Connector: conn})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()
	scope1 := store.Scope{Tenant: "t1"}
	scope2 := store.Scope{Tenant: "t2"}

	set := func(scope store.Scope, value string) int64 {
		t.Helper()

		rev, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: jsonBytes(t, value)})
		if err != nil {
			t.Fatalf("set %s: %v", scope.Tenant, err)
		}

		return rev
	}

	get := func(scope store.Scope) (string, bool) {
		t.Helper()

		entry, found, err := s.Get(ctx, scope, "ns", "k")
		if err != nil {
			t.Fatalf("get %s: %v", scope.Tenant, err)
		}

		if !found {
			return "", false
		}

		var v string
		if err := json.Unmarshal(entry.Value, &v); err != nil {
			t.Fatalf("unmarshal %s: %v", scope.Tenant, err)
		}

		return v, true
	}

	if rev := set(scope1, "value-t1"); rev != 1 {
		t.Errorf("t1 first Set revision = %d, want 1", rev)
	}

	if rev := set(scope2, "value-t2"); rev != 1 {
		t.Errorf("t2 first Set revision = %d, want 1", rev)
	}

	if got, found := get(scope1); !found || got != "value-t1" {
		t.Errorf("t1 Get = %q (found=%v), want value-t1", got, found)
	}

	if got, found := get(scope2); !found || got != "value-t2" {
		t.Errorf("t2 Get = %q (found=%v), want value-t2", got, found)
	}

	// t2-only write must stay invisible from t1's scope.
	if _, err := s.Set(ctx, scope2, store.Entry{Namespace: "ns", Key: "only-t2", Value: jsonBytes(t, "x")}); err != nil {
		t.Fatalf("set only-t2: %v", err)
	}

	list1, err := s.List(ctx, scope1)
	if err != nil {
		t.Fatalf("list t1: %v", err)
	}

	if len(list1) != 1 || list1[0].Key != "k" {
		t.Fatalf("t1 List = %#v, want exactly ns/k", list1)
	}

	list2, err := s.List(ctx, scope2)
	if err != nil {
		t.Fatalf("list t2: %v", err)
	}

	if len(list2) != 2 {
		t.Fatalf("t2 List has %d entries, want 2", len(list2))
	}

	// Deleting in t1 must not touch t2.
	if err := s.Delete(ctx, scope1, "ns", "k", "actor"); err != nil {
		t.Fatalf("delete t1: %v", err)
	}

	if _, found := get(scope1); found {
		t.Errorf("t1 Get after delete still found the entry")
	}

	if got, found := get(scope2); !found || got != "value-t2" {
		t.Errorf("t2 Get after t1 delete = %q (found=%v), want value-t2", got, found)
	}

	// A tenant the connector does not know surfaces the resolution failure.
	if _, err := s.List(ctx, store.Scope{Tenant: "unknown"}); err == nil {
		t.Fatal("List for an unknown tenant succeeded, want a resolution error")
	} else if !strings.Contains(err.Error(), "resolve tenant unknown") {
		t.Errorf("unknown tenant error = %v, want it to name the tenant", err)
	}
}

// TestIntegration_PostgresSubscribeAfterStartGetsResyncFirst pins the engine's
// real sequence: Start connects the zero-scope feed, and only then does the
// engine subscribe. The joining subscriber must still be told OpResync first —
// otherwise it would never reconcile and a quiet scope would stay stale
// forever — and key events from that same connection must arrive after it.
func TestIntegration_PostgresSubscribeAfterStartGetsResyncFirst(t *testing.T) {
	s := freshStore(t, "joinresync")
	ctx := context.Background()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Start returns once LISTEN is installed; the feed marks itself connected
	// from its reader goroutine a moment later.
	time.Sleep(500 * time.Millisecond)

	// Buffered: the joining resync is delivered synchronously inside Subscribe.
	events := make(chan store.Event, 8)

	unsub, err := s.Subscribe(ctx, store.Scope{}, func(evt store.Event) {
		select {
		case events <- evt:
		default:
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	defer unsub()

	first := recvEvent(t, events, "joining resync")
	if first.Op != store.OpResync || first.Scope != (store.Scope{}) {
		t.Fatalf("first event = %+v, want {Scope:{} Op:%q}", first, store.OpResync)
	}

	if _, err := s.Set(ctx, store.Scope{}, store.Entry{
		Namespace: "ns",
		Key:       "k",
		Value:     jsonBytes(t, "v1"),
	}); err != nil {
		t.Fatalf("set: %v", err)
	}

	next := recvEvent(t, events, "upsert after the joining resync")
	if next.Op != store.OpUpsert || next.Namespace != "ns" || next.Key != "k" {
		t.Fatalf("event after resync = %+v, want an upsert of ns/k", next)
	}
}

// recvEvent waits for one changefeed event or fails the test.
func recvEvent(t *testing.T, events <-chan store.Event, what string) store.Event {
	t.Helper()

	select {
	case evt := <-events:
		return evt
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)

		return store.Event{}
	}
}

// provisionTenantDB creates a fresh database carrying the published schema and
// returns its name, its DSN and an open handle. The caller decides when — or
// whether — the connector learns about it.
func provisionTenantDB(t *testing.T, admin *sql.DB, baseDSN, prefix string) (string, string, *sql.DB) {
	t.Helper()

	dbName := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	freshDB(t, admin, dbName)

	tenantDSN := dsnFor(baseDSN, dbName)

	db, err := sql.Open("pgx", tenantDSN)
	if err != nil {
		t.Fatalf("open %s: %v", dbName, err)
	}

	t.Cleanup(func() { _ = db.Close() })

	provisionSchema(t, db)

	return dbName, tenantDSN, db
}

// tenantStore builds a Store with no database of its own: every scope resolves
// through the connector, which is the shape the engine uses for tenant scopes.
func tenantStore(t *testing.T, conn postgres.Connector) *postgres.Store {
	t.Helper()

	s, err := postgres.New(postgres.Config{MultiTenantEnabled: true, Connector: conn})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	return s
}

// subscribeScope subscribes to scope and returns the delivered events. The
// channel is buffered because a joining subscriber's OpResync is delivered
// synchronously inside Subscribe.
func subscribeScope(t *testing.T, s *postgres.Store, scope store.Scope) (<-chan store.Event, func()) {
	t.Helper()

	events := make(chan store.Event, 32)

	unsub, err := s.Subscribe(context.Background(), scope, func(evt store.Event) {
		select {
		case events <- evt:
		default:
		}
	})
	if err != nil {
		t.Fatalf("subscribe tenant %q: %v", scope.Tenant, err)
	}

	return events, unsub
}

// assertNoEvent fails when anything is delivered within wait.
func assertNoEvent(t *testing.T, events <-chan store.Event, wait time.Duration, what string) {
	t.Helper()

	select {
	case evt := <-events:
		t.Fatalf("%s: unexpected event %+v", what, evt)
	case <-time.After(wait):
	}
}

// listenBackends counts the dedicated LISTEN connections open on dbName. A feed
// parks its connection in `LISTEN "..."` for its whole life, so that query text
// isolates it from the test's own pooled handles.
func listenBackends(t *testing.T, admin *sql.DB, dbName string) int {
	t.Helper()

	var n int

	if err := admin.QueryRow(
		`SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND query LIKE 'LISTEN%'`,
		dbName,
	).Scan(&n); err != nil {
		t.Fatalf("count LISTEN backends on %s: %v", dbName, err)
	}

	return n
}

// waitForListenBackends polls until dbName holds exactly want LISTEN
// connections. Postgres reaps a closed backend asynchronously, so a teardown
// assertion has to wait rather than sample once.
func waitForListenBackends(t *testing.T, admin *sql.DB, dbName string, want int, what string) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)

	for {
		got := listenBackends(t, admin, dbName)
		if got == want {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("%s: %s holds %d LISTEN connections, want %d", what, dbName, got, want)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// TestIntegration_PostgresTwoTenantFeedsAreIsolated pins the per-tenant feed:
// each tenant's subscriber is told OpResync for ITS OWN scope, and a write in
// one tenant's database never reaches the other tenant's subscriber.
func TestIntegration_PostgresTwoTenantFeedsAreIsolated(t *testing.T) {
	base, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	admin := adminDSN(t, base)
	defer admin.Close()

	conn := newFakeConnector()

	for _, tenant := range []string{"t1", "t2"} {
		_, tenantDSN, db := provisionTenantDB(t, admin, base, "feed_"+tenant)
		conn.set(tenant, db, tenantDSN)
	}

	s := tenantStore(t, conn)

	scope1 := store.Scope{Tenant: "t1"}
	scope2 := store.Scope{Tenant: "t2"}

	events1, unsub1 := subscribeScope(t, s, scope1)
	defer unsub1()

	events2, unsub2 := subscribeScope(t, s, scope2)
	defer unsub2()

	if first := recvEvent(t, events1, "t1 resync"); first.Op != store.OpResync || first.Scope != scope1 {
		t.Fatalf("t1 first event = %+v, want {Scope:%+v Op:%q}", first, scope1, store.OpResync)
	}

	if first := recvEvent(t, events2, "t2 resync"); first.Op != store.OpResync || first.Scope != scope2 {
		t.Fatalf("t2 first event = %+v, want {Scope:%+v Op:%q}", first, scope2, store.OpResync)
	}

	ctx := context.Background()

	if _, err := s.Set(ctx, scope1, store.Entry{Namespace: "ns", Key: "only-t1", Value: jsonBytes(t, "v1")}); err != nil {
		t.Fatalf("set t1: %v", err)
	}

	if got := recvEvent(t, events1, "t1 upsert"); got.Op != store.OpUpsert || got.Namespace != "ns" || got.Key != "only-t1" {
		t.Fatalf("t1 event = %+v, want an upsert of ns/only-t1", got)
	}

	assertNoEvent(t, events2, 2*time.Second, "t2 must never see t1's write")

	if _, err := s.Set(ctx, scope2, store.Entry{Namespace: "ns", Key: "only-t2", Value: jsonBytes(t, "v2")}); err != nil {
		t.Fatalf("set t2: %v", err)
	}

	if got := recvEvent(t, events2, "t2 upsert"); got.Op != store.OpUpsert || got.Namespace != "ns" || got.Key != "only-t2" {
		t.Fatalf("t2 event = %+v, want an upsert of ns/only-t2", got)
	}

	assertNoEvent(t, events1, 2*time.Second, "t1 must never see t2's write")
}

// TestIntegration_PostgresTenantFeedTornDownOnLastUnsubscribe pins the shared,
// reference-counted lifetime of a tenant feed: two subscribers ride ONE LISTEN
// connection, dropping the first keeps it alive for the second, the last one to
// leave closes it, and a later Subscribe resolves the tenant's DSN again — which
// is how a credentials rotation is picked up.
func TestIntegration_PostgresTenantFeedTornDownOnLastUnsubscribe(t *testing.T) {
	base, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	admin := adminDSN(t, base)
	defer admin.Close()

	conn := newFakeConnector()
	dbName, tenantDSN, db := provisionTenantDB(t, admin, base, "teardown")
	conn.set("t1", db, tenantDSN)

	s := tenantStore(t, conn)
	scope := store.Scope{Tenant: "t1"}
	ctx := context.Background()

	eventsA, unsubA := subscribeScope(t, s, scope)
	eventsB, unsubB := subscribeScope(t, s, scope)

	if first := recvEvent(t, eventsA, "A resync"); first.Op != store.OpResync || first.Scope != scope {
		t.Fatalf("A first event = %+v, want {Scope:%+v Op:%q}", first, scope, store.OpResync)
	}

	if first := recvEvent(t, eventsB, "B resync"); first.Op != store.OpResync || first.Scope != scope {
		t.Fatalf("B first event = %+v, want {Scope:%+v Op:%q}", first, scope, store.OpResync)
	}

	waitForListenBackends(t, admin, dbName, 1, "two subscribers share one feed")

	if calls := conn.resolveDSNCalls(); calls != 1 {
		t.Errorf("ResolveDSN calls = %d, want 1: the second subscriber must join the existing feed", calls)
	}

	// Dropping one subscriber keeps the connection alive for the other.
	unsubA()

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: jsonBytes(t, "v1")}); err != nil {
		t.Fatalf("set: %v", err)
	}

	if got := recvEvent(t, eventsB, "upsert after A left"); got.Op != store.OpUpsert || got.Key != "k" {
		t.Fatalf("B event = %+v, want an upsert of ns/k", got)
	}

	assertNoEvent(t, eventsA, time.Second, "A unsubscribed and must receive nothing")

	if n := listenBackends(t, admin, dbName); n != 1 {
		t.Fatalf("%s holds %d LISTEN connections while one subscriber remains, want 1", dbName, n)
	}

	// The last subscriber closes the tenant's connection.
	unsubB()
	waitForListenBackends(t, admin, dbName, 0, "the last unsubscribe tears the feed down")

	// A later Subscribe rebuilds the feed from a freshly resolved DSN.
	eventsC, unsubC := subscribeScope(t, s, scope)
	defer unsubC()

	if first := recvEvent(t, eventsC, "resync after re-subscribe"); first.Op != store.OpResync || first.Scope != scope {
		t.Fatalf("re-subscribe first event = %+v, want {Scope:%+v Op:%q}", first, scope, store.OpResync)
	}

	waitForListenBackends(t, admin, dbName, 1, "re-subscribing opens a fresh feed")

	if calls := conn.resolveDSNCalls(); calls != 2 {
		t.Errorf("ResolveDSN calls = %d, want 2: a re-subscribed tenant must resolve its DSN again", calls)
	}
}

// TestIntegration_PostgresConcurrentFirstSubscribeOpensOneConnection covers both
// halves of the creation handshake. A tenant the connector cannot resolve fails
// every concurrent caller — none of them blocks, and nothing is left running —
// and once the connector knows the tenant, the same burst of callers shares
// exactly ONE LISTEN connection, which is what "one live subscription per
// activated tenant" rests on.
func TestIntegration_PostgresConcurrentFirstSubscribeOpensOneConnection(t *testing.T) {
	base, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	admin := adminDSN(t, base)
	defer admin.Close()

	conn := newFakeConnector()
	dbName, tenantDSN, db := provisionTenantDB(t, admin, base, "concurrent")

	s := tenantStore(t, conn)
	scope := store.Scope{Tenant: "t1"}

	const callers = 8

	type result struct {
		unsub func()
		err   error
	}

	subscribeAll := func() []result {
		t.Helper()

		results := make(chan result, callers)

		var wg sync.WaitGroup

		for range callers {
			wg.Add(1)

			go func() {
				defer wg.Done()

				unsub, err := s.Subscribe(context.Background(), scope, func(store.Event) {})
				results <- result{unsub: unsub, err: err}
			}()
		}

		done := make(chan struct{})

		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(60 * time.Second):
			t.Fatal("concurrent Subscribe calls blocked instead of returning")
		}

		close(results)

		out := make([]result, 0, callers)
		for r := range results {
			out = append(out, r)
		}

		return out
	}

	for i, r := range subscribeAll() {
		if r.err == nil {
			r.unsub()
			t.Fatalf("Subscribe %d succeeded for a tenant the connector cannot resolve", i)
		}

		if !strings.Contains(r.err.Error(), "unknown tenant") {
			t.Errorf("Subscribe %d error = %v, want the connector's resolution failure", i, r.err)
		}
	}

	if n := listenBackends(t, admin, dbName); n != 0 {
		t.Fatalf("a failed feed creation left %d LISTEN connections on %s, want 0", n, dbName)
	}

	// The connector learns the tenant: every caller now succeeds on one feed.
	conn.set("t1", db, tenantDSN)

	unsubs := make([]func(), 0, callers)

	for i, r := range subscribeAll() {
		if r.err != nil {
			t.Fatalf("Subscribe %d after the connector learned the tenant: %v", i, r.err)
		}

		unsubs = append(unsubs, r.unsub)
	}

	waitForListenBackends(t, admin, dbName, 1, "concurrent subscribers share one connection")

	for _, unsub := range unsubs {
		unsub()
	}

	waitForListenBackends(t, admin, dbName, 0, "the last unsubscribe closes the shared connection")
}

// blockingConnector parks the FIRST ResolveDSN call inside the connector until
// the test releases it. That is the window Close has to survive: a creator
// still connecting, a reserved slot in the feeds map that carries no stop
// channel, and a second caller already waiting on it.
type blockingConnector struct {
	db  *sql.DB
	dsn string

	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingConnector) ResolveDB(context.Context, string) (dbresolver.DB, error) {
	return dbresolver.New(dbresolver.WithPrimaryDBs(c.db)), nil
}

func (c *blockingConnector) ResolveDSN(ctx context.Context, _ string) (string, error) {
	c.once.Do(func() { close(c.entered) })

	select {
	case <-c.release:
		return c.dsn, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Close must not be able to leave a live feed, a live goroutine or an open
// connection behind when it lands while a tenant feed is still being created.
// Walking the feeds map is not enough on its own: the entry Close finds there
// is a reserved slot with nothing to stop yet, so the creator itself has to
// notice the shutdown after it connects and throw the connection away.
func TestIntegration_PostgresCloseDuringFeedCreationLeavesNothingRunning(t *testing.T) {
	base, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	admin := adminDSN(t, base)
	defer admin.Close()

	dbName, tenantDSN, db := provisionTenantDB(t, admin, base, "close_race")

	conn := &blockingConnector{
		db:      db,
		dsn:     tenantDSN,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}

	s := tenantStore(t, conn)
	scope := store.Scope{Tenant: "t1"}

	const callers = 2

	results := make(chan error, callers)

	var wg sync.WaitGroup

	subscribe := func() {
		defer wg.Done()

		unsub, err := s.Subscribe(context.Background(), scope, func(store.Event) {})
		if err == nil {
			unsub()
		}

		results <- err
	}

	wg.Add(1)

	go subscribe()

	select {
	case <-conn.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("Subscribe never reached the connector")
	}

	// The second caller parks on the creator's reserved slot. Waiting for the
	// reference count keeps the race deterministic: once it is there, Close
	// provably runs against a slot that already has a waiter on it.
	wg.Add(1)

	go subscribe()

	waitForFeedRefs(t, s, scope.Tenant, callers)

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	close(conn.release)

	// Join before asserting: Close does not wait for an in-flight creator, and
	// the package goleak guard is only meaningful once the creator has returned.
	wg.Wait()
	close(results)

	for err := range results {
		if !errors.Is(err, store.ErrClosed) {
			t.Fatalf("Subscribe racing Close = %v, want store.ErrClosed", err)
		}
	}

	if total, _ := s.FeedsSnapshot(scope.Tenant); total != 0 {
		t.Fatalf("feeds map holds %d entries after Close, want 0", total)
	}

	waitForListenBackends(t, admin, dbName, 0, "Close during feed creation")
}

// waitForFeedRefs blocks until tenant's reserved slot has taken want
// references — one per caller parked on it.
func waitForFeedRefs(t *testing.T, s *postgres.Store, tenant string, want int) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)

	for {
		if _, refs := s.FeedsSnapshot(tenant); refs >= want {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("reserved slot for tenant %q never took %d references", tenant, want)
		}

		time.Sleep(time.Millisecond)
	}
}
