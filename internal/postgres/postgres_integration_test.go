//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
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
)

// startContainer returns the admin DSN of the package-wide Postgres server
// (see shared_container_integration_test.go). Tests isolate themselves with a
// uniquely-named database on that server, so none of them needs its own.
func startContainer(t *testing.T) string {
	t.Helper()

	return postgres.SharedContainerDSN(t)
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
	dsn := startContainer(t)

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
	dsn := startContainer(t)

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
	dsn := startContainer(t)
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
	dsn := startContainer(t)

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

	dsn := startContainer(t)

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
// the Postgres write path: the first write stores a non-zero revision, a write
// of a DIFFERENT value advances it, a write of the SAME value does not (the
// bump trigger draws a new revision only when OLD.value IS DISTINCT FROM
// NEW.value and otherwise puts the stored one back), and both
// read paths report exactly the number Set reported. The numbers themselves
// come from the table-level systemplane_revision_seq, so the test asserts the
// relations between them and never a literal — revisions may skip.
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

	first := set("v1")
	if first <= 0 {
		t.Fatalf("first Set revision = %d, want greater than 0", first)
	}

	assertRevision(t, s, ctx, first)

	changed := set("v2")
	if changed <= first {
		t.Fatalf("Set of a different value revision = %d, want greater than %d", changed, first)
	}

	assertRevision(t, s, ctx, changed)

	if got := set("v2"); got != changed {
		t.Fatalf("Set of an identical value revision = %d, want the unchanged %d", got, changed)
	}

	assertRevision(t, s, ctx, changed)
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
	extras   map[string][]*sql.DB
	replicas map[string]*sql.DB
	dsns     map[string]string
	dsnCalls int
}

func newFakeConnector() *fakeConnector {
	return &fakeConnector{
		dbs:      map[string]*sql.DB{},
		extras:   map[string][]*sql.DB{},
		replicas: map[string]*sql.DB{},
		dsns:     map[string]string{},
	}
}

func (c *fakeConnector) set(tenantID string, db *sql.DB, dsn string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.dbs[tenantID] = db
	c.dsns[tenantID] = dsn
}

// setReplica gives the tenant a read replica, the way lib-commons registers one
// for any tenant whose config declares a secondary connection string. The
// resolver then routes anything that does not look like a write to it.
func (c *fakeConnector) setReplica(tenantID string, replica *sql.DB) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.replicas[tenantID] = replica
}

// addPrimary gives the tenant a second primary, the way lib-commons registers
// one for a tenant whose config declares several writable connection strings.
func (c *fakeConnector) addPrimary(tenantID string, db *sql.DB) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.extras[tenantID] = append(c.extras[tenantID], db)
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

	primaries := append([]*sql.DB{db}, c.extras[tenantID]...)

	if replica, ok := c.replicas[tenantID]; ok {
		return dbresolver.New(dbresolver.WithPrimaryDBs(primaries...), dbresolver.WithReplicaDBs(replica)), nil
	}

	return dbresolver.New(dbresolver.WithPrimaryDBs(primaries...)), nil
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

// TestIntegration_PostgresScopedReadsStayOnThePrimary pins that a tenant with a
// read replica still reads what it just wrote.
//
// dbresolver decides where a statement goes from its text, and its default
// checker calls a statement a write only when it contains "RETURNING": Set
// ends in RETURNING revision and Delete goes through ExecContext, so both land
// on the primary, while the plain SELECTs in Get and List would be served by a
// standby — and lib-commons registers a replica for every tenant whose config
// declares one. A tenant could therefore read a revision older than the one
// Set just returned, and older than the NOTIFY the changefeed is reconciling
// against, since the feed LISTENs on the primary DSN.
//
// The replica here is a decoy rather than a streaming standby: a separate,
// permanently empty database provisioned with the same schema. That removes
// replication lag from the test entirely — any read routed to it comes back
// missing, which is a harder signal than a stale one and fails deterministically.
func TestIntegration_PostgresScopedReadsStayOnThePrimary(t *testing.T) {
	base := startContainer(t)

	admin := adminDSN(t, base)
	t.Cleanup(func() { _ = admin.Close() })

	open := func(role string) (*sql.DB, string) {
		t.Helper()

		dbName := fmt.Sprintf("replica_%s_%d", role, time.Now().UnixNano())
		freshDB(t, admin, dbName)

		dsn := dsnFor(base, dbName)

		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open %s: %v", role, err)
		}

		t.Cleanup(func() { _ = db.Close() })

		provisionSchema(t, db)

		return db, dsn
	}

	primary, primaryDSN := open("primary")
	standby, _ := open("standby")

	conn := newFakeConnector()
	conn.set("t1", primary, primaryDSN)
	conn.setReplica("t1", standby)

	s, err := postgres.New(postgres.Config{MultiTenantEnabled: true, Connector: conn})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()
	scope := store.Scope{Tenant: "t1"}

	written, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: jsonBytes(t, "value-t1")})
	if err != nil {
		t.Fatalf("set: %v", err)
	}

	entry, found, err := s.Get(ctx, scope, "ns", "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !found {
		t.Fatal("Get missed the value Set just stored: the read was served by the replica")
	}

	if entry.Revision != written {
		t.Errorf("Get revision = %d, want the %d Set returned", entry.Revision, written)
	}

	entries, err := s.List(ctx, scope)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("List returned %d entries, want 1: the read was served by the replica", len(entries))
	}

	if entries[0].Revision != written {
		t.Errorf("List revision = %d, want the %d Set returned", entries[0].Revision, written)
	}

	// The decoy is live and was never written to, so a passing test above means
	// the reads went to the primary rather than that the replica was unusable.
	var onStandby int

	if err := standby.QueryRow(`SELECT count(*) FROM systemplane_entries`).Scan(&onStandby); err != nil {
		t.Fatalf("count rows on the standby: %v", err)
	}

	if onStandby != 0 {
		t.Errorf("standby holds %d rows, want 0: it is a decoy and nothing should write to it", onStandby)
	}
}

// TestIntegration_PostgresPrimaryPinIsDeterministic pins that a tenant whose
// connector reports several writable nodes sends every read AND every write to
// the SAME one, so a value Set returns is the value the next Get reads (D4).
//
// It deliberately makes no failover claim. Spreading the calls over the
// primaries instead would buy none: dbresolver retries only on a net.Error and
// a dead pool reports "sql: database is closed", which is not one. All it
// would buy is a read that misses what the last write stored.
//
// The two primaries are separate databases holding DIFFERENT values and the
// replica is an empty decoy, so each possible destination answers distinctly:
// the standby comes back missing, the second primary comes back with its own
// value, and only the first primary comes back with the one asserted here.
func TestIntegration_PostgresPrimaryPinIsDeterministic(t *testing.T) {
	base := startContainer(t)

	admin := adminDSN(t, base)
	t.Cleanup(func() { _ = admin.Close() })

	_, primaryDSN, primary1 := provisionTenantDB(t, admin, base, "multiprimary_one")
	_, _, primary2 := provisionTenantDB(t, admin, base, "multiprimary_two")
	_, _, standby := provisionTenantDB(t, admin, base, "multiprimary_standby")

	for i, db := range []*sql.DB{primary1, primary2} {
		value := fmt.Sprintf("primary-%d", i+1)

		if _, err := db.Exec(
			`INSERT INTO systemplane_entries (namespace, key, value, updated_at, updated_by)
			 VALUES ('ns', 'k', $1::jsonb, now(), 'seed')`,
			fmt.Sprintf("%q", value),
		); err != nil {
			t.Fatalf("seed primary %d: %v", i+1, err)
		}
	}

	conn := newFakeConnector()
	conn.set("t1", primary1, primaryDSN)
	conn.addPrimary("t1", primary2)
	conn.setReplica("t1", standby)

	s := tenantStore(t, conn)

	ctx := context.Background()
	scope := store.Scope{Tenant: "t1"}

	// Several reads, because a pin that drifts between calls is the failure
	// this guards: every one must answer from the first primary.
	for i := range 4 {
		entry, found, err := s.Get(ctx, scope, "ns", "k")
		if err != nil {
			t.Fatalf("get #%d: %v", i+1, err)
		}

		if !found {
			t.Fatalf("get #%d missed the seeded row: the read was served by the empty replica", i+1)
		}

		var got string
		if err := json.Unmarshal(entry.Value, &got); err != nil {
			t.Fatalf("get #%d: decode value: %v", i+1, err)
		}

		if got != "primary-1" {
			t.Fatalf("get #%d returned %q, want %q: reads must pin to the first primary", i+1, got, "primary-1")
		}

		if entry.Revision == 0 {
			t.Errorf("get #%d revision = 0, want the revision the insert trigger assigned", i+1)
		}
	}

	// Read-your-write across several primaries: the write must land where the
	// reads look, or the value a caller just stored comes back missing.
	written, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "fresh", Value: jsonBytes(t, "written")})
	if err != nil {
		t.Fatalf("set: %v", err)
	}

	entry, found, err := s.Get(ctx, scope, "ns", "fresh")
	if err != nil {
		t.Fatalf("get after set: %v", err)
	}

	if !found {
		t.Fatal("Get missed the value Set just stored: the write and the read chose different primaries")
	}

	if entry.Revision != written {
		t.Errorf("Get revision = %d, want the %d Set returned", entry.Revision, written)
	}

	var onSecondPrimary int

	if err := primary2.QueryRow(`SELECT count(*) FROM systemplane_entries WHERE key = 'fresh'`).Scan(&onSecondPrimary); err != nil {
		t.Fatalf("count rows on the second primary: %v", err)
	}

	if onSecondPrimary != 0 {
		t.Errorf("the second primary holds the written row, want 0: writes must pin to the first primary")
	}

	var onStandby int

	if err := standby.QueryRow(`SELECT count(*) FROM systemplane_entries`).Scan(&onStandby); err != nil {
		t.Fatalf("count rows on the standby: %v", err)
	}

	if onStandby != 0 {
		t.Errorf("standby holds %d rows, want 0: it is a decoy and nothing should write to it", onStandby)
	}
}

// TestIntegration_PostgresScopedCRUDIsolation pins FC-2's scoped resolution on
// the Postgres CRUD path: a named Scope.Tenant resolves its database through
// the connector and nothing else. Every call travels on a plain
// context.Background() carrying no tenant at all, so a passing test proves the
// handle came from the connector rather than from ctx, and each tenant sees
// only its own rows.
func TestIntegration_PostgresScopedCRUDIsolation(t *testing.T) {
	dsn := startContainer(t)

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

	// Each tenant database carries its own revision sequence, so the only
	// portable claim about a first write is that it stored something.
	if rev := set(scope1, "value-t1"); rev <= 0 {
		t.Errorf("t1 first Set revision = %d, want greater than 0", rev)
	}

	if rev := set(scope2, "value-t2"); rev <= 0 {
		t.Errorf("t2 first Set revision = %d, want greater than 0", rev)
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
	// from its reader goroutine a moment later. A probe subscriber is how that
	// moment is observed from outside the package: the probe is told OpResync
	// either synchronously, because the reader is already through its first
	// resync, or by that resync's broadcast when it lands — so once the probe
	// holds its marker the feed IS connected, and the subscriber below takes
	// the joining path. Waiting on the feed instead of on the clock is what
	// keeps this test from quietly drifting onto the broadcast path.
	probe, unsubProbe := subscribeScope(t, s, store.Scope{})
	defer unsubProbe()

	if first := recvEvent(t, probe, "the probe's resync"); first.Op != store.OpResync {
		t.Fatalf("probe first event = %+v, want %q", first, store.OpResync)
	}

	events, unsub := subscribeScope(t, s, store.Scope{})
	defer unsub()

	// Which path delivered it, asserted rather than assumed: the joining resync
	// is emitted synchronously INSIDE Subscribe, so it is already buffered by
	// the time Subscribe returns. A marker broadcast by the reader goroutine
	// instead would still be in flight, and this non-blocking receive is what
	// tells the two apart.
	select {
	case first := <-events:
		if first.Op != store.OpResync || first.Scope != (store.Scope{}) {
			t.Fatalf("first event = %+v, want {Scope:{} Op:%q}", first, store.OpResync)
		}
	default:
		t.Fatal("Subscribe returned without delivering the joining resync: the subscriber is waiting on the reader's broadcast instead")
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
//
// A full buffer FAILS the test rather than dropping the event: assertNoEvent
// asks this channel to prove that nothing was delivered, and a sink that
// silently discards what it cannot hold would let a leaked cross-tenant event
// vanish and the negative assertion pass for the wrong reason. Every caller
// unsubscribes before it returns, so no delivery — and no t.Errorf — can land
// after the test completes.
func subscribeScope(t *testing.T, s *postgres.Store, scope store.Scope) (<-chan store.Event, func()) {
	t.Helper()

	events := make(chan store.Event, 32)

	unsub, err := s.Subscribe(context.Background(), scope, func(evt store.Event) {
		select {
		case events <- evt:
		default:
			t.Errorf("subscriber sink for scope %+v overflowed and dropped %+v", scope, evt)
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
// A feed's backend is recognised by its last statement, so LISTEN must remain
// the last thing openListen runs on the connection.
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
	base := startContainer(t)

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
	base := startContainer(t)

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

// TestIntegration_PostgresSelfUnsubscribeInCallbackDoesNotStall pins the one
// teardown path that runs on the changefeed's OWN reader goroutine: a callback
// that drops the last subscription of its tenant — the engine deactivating a
// tenant from inside its own change handler, and equally a callback that closes
// the store. Waiting there for the reader to exit is waiting for the goroutine
// doing the waiting, so it can only ever end at the closeTimeout, and the
// tenant's dispatch is frozen for those five seconds.
func TestIntegration_PostgresSelfUnsubscribeInCallbackDoesNotStall(t *testing.T) {
	base := startContainer(t)

	admin := adminDSN(t, base)
	defer admin.Close()

	conn := newFakeConnector()
	dbName, tenantDSN, db := provisionTenantDB(t, admin, base, "selfunsub")
	conn.set("t1", db, tenantDSN)

	s := tenantStore(t, conn)
	scope := store.Scope{Tenant: "t1"}
	ctx := context.Background()

	var (
		mu    sync.Mutex
		unsub func()
		once  sync.Once
	)

	took := make(chan time.Duration, 1)

	// The joining resync is delivered inside Subscribe, on THIS goroutine,
	// before unsub exists — so only a key event, which arrives on the reader
	// goroutine, can exercise the self-teardown.
	sub, err := s.Subscribe(ctx, scope, func(evt store.Event) {
		if evt.Op != store.OpUpsert {
			return
		}

		once.Do(func() {
			mu.Lock()
			fn := unsub
			mu.Unlock()

			start := time.Now()

			fn()

			took <- time.Since(start)
		})
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	mu.Lock()
	unsub = sub
	mu.Unlock()

	defer sub()

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: jsonBytes(t, "v1")}); err != nil {
		t.Fatalf("set: %v", err)
	}

	select {
	case elapsed := <-took:
		if elapsed > time.Second {
			t.Fatalf("unsubscribing from inside the callback took %v; the last subscriber of a tenant feed must not wait on the reader goroutine it is running on", elapsed)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the callback to unsubscribe itself")
	}

	// The feed is still torn down for real: skipping the self-wait must not
	// leave the tenant's LISTEN connection behind.
	waitForListenBackends(t, admin, dbName, 0, "the self-unsubscribing last subscriber tears the feed down")
}

// TestIntegration_PostgresConcurrentFirstSubscribeOpensOneConnection covers both
// halves of the creation handshake. A tenant the connector cannot resolve fails
// every concurrent caller — none of them blocks, and nothing is left running —
// and once the connector knows the tenant, the same burst of callers shares
// exactly ONE LISTEN connection, which is what "one live subscription per
// activated tenant" rests on.
func TestIntegration_PostgresConcurrentFirstSubscribeOpensOneConnection(t *testing.T) {
	base := startContainer(t)

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
	base := startContainer(t)

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

// eventLog records every delivered event in arrival order, so a test can
// assert on the SEQUENCE as a whole — "exactly one disconnect, then exactly
// one resync, then key events" is a claim about order, not about any single
// event.
type eventLog struct {
	mu     sync.Mutex
	events []store.Event
}

func (l *eventLog) record(evt store.Event) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.events = append(l.events, evt)
}

func (l *eventLog) snapshot() []store.Event {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]store.Event(nil), l.events...)
}

// waitFor polls the recorded sequence until pred accepts it, then returns that
// snapshot. Polling, not a channel, because the assertions are about the whole
// sequence including what must NOT be in it.
func (l *eventLog) waitFor(t *testing.T, what string, pred func([]store.Event) bool) []store.Event {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)

	for {
		got := l.snapshot()
		if pred(got) {
			return got
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; recorded %+v", what, got)
		}

		time.Sleep(20 * time.Millisecond)
	}
}

// listenBackendPID returns the backend pid of the single LISTEN connection on
// dbName. A feed parks its dedicated connection in `LISTEN "..."` for its whole
// life, so that query text isolates it from the test's own pooled handles.
func listenBackendPID(t *testing.T, admin *sql.DB, dbName string) int {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)

	for {
		var pid int

		err := admin.QueryRow(
			`SELECT pid FROM pg_stat_activity WHERE datname = $1 AND query LIKE 'LISTEN%'`,
			dbName,
		).Scan(&pid)
		if err == nil {
			return pid
		}

		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("find LISTEN backend on %s: %v", dbName, err)
		}

		if time.Now().After(deadline) {
			t.Fatalf("no LISTEN backend on %s", dbName)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// TestIntegration_PostgresResyncAfterListenGap is the audit's headline defect,
// pinned: a value written while the LISTEN connection was down used to be lost
// in silence — the feed reconnected, announced nothing in either direction, and
// the cache stayed stale but looked fresh until someone wrote again.
//
// The kill is a real one (pg_terminate_backend on the feed's own backend from a
// separate admin connection), and the assertion is the SEQUENCE: exactly one
// OpDisconnect, then exactly one OpResync, then key events — never a key event
// of the new connection ahead of the resync, never a second disconnect for one
// loss — with both markers naming the feed's scope and carrying no key.
//
// The gap write's NOTIFY is deliberately NOT asserted to be redelivered: it is
// not, and that is precisely why the resync exists. What IS asserted is that
// the value is recoverable afterwards, at a higher revision.
func TestIntegration_PostgresResyncAfterListenGap(t *testing.T) {
	base := startContainer(t)

	admin := adminDSN(t, base)
	defer admin.Close()

	conn := newFakeConnector()
	dbName, tenantDSN, db := provisionTenantDB(t, admin, base, "gap")
	conn.set("t1", db, tenantDSN)

	s := tenantStore(t, conn)
	scope := store.Scope{Tenant: "t1"}
	ctx := context.Background()

	log := &eventLog{}

	unsub, err := s.Subscribe(ctx, scope, log.record)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	defer unsub()

	// The prefix everything else is measured against: the joining resync, then
	// the pre-gap write's own upsert — which also pins scope stamping on a
	// NOTIFY-derived event, the one thing the payload itself cannot carry.
	preRev, err := s.Set(ctx, scope, store.Entry{
		Namespace: "ns",
		Key:       "gap",
		Value:     jsonBytes(t, "before-the-gap"),
	})
	if err != nil {
		t.Fatalf("pre-gap set: %v", err)
	}

	prefix := log.waitFor(t, "the joining resync and the pre-gap upsert", func(evts []store.Event) bool {
		return len(evts) >= 2
	})

	if prefix[0].Op != store.OpResync || prefix[0].Scope != scope {
		t.Fatalf("first event = %+v, want {Scope:%+v Op:%q}", prefix[0], scope, store.OpResync)
	}

	if prefix[1].Op != store.OpUpsert || prefix[1].Namespace != "ns" || prefix[1].Key != "gap" {
		t.Fatalf("pre-gap event = %+v, want an upsert of ns/gap", prefix[1])
	}

	if prefix[1].Scope != scope {
		t.Fatalf("pre-gap event scope = %+v, want %+v", prefix[1].Scope, scope)
	}

	if prefix[1].Revision != preRev {
		t.Fatalf("pre-gap event revision = %d, want %d (what Set reported)", prefix[1].Revision, preRev)
	}

	mark := 2

	// Kill the feed's own backend from a connection it does not own.
	pid := listenBackendPID(t, admin, dbName)

	var terminated bool

	if err := admin.QueryRow(`SELECT pg_terminate_backend($1)`, pid).Scan(&terminated); err != nil {
		t.Fatalf("terminate LISTEN backend %d: %v", pid, err)
	}

	if !terminated {
		t.Fatalf("pg_terminate_backend(%d) reported false", pid)
	}

	// The gap write: a separate *sql.DB, so the value lands in the database
	// through a path the store under test has no connection to.
	writerDB, err := sql.Open("pgx", tenantDSN)
	if err != nil {
		t.Fatalf("open gap writer: %v", err)
	}

	t.Cleanup(func() { _ = writerDB.Close() })

	// ListenDSN only satisfies the constructor: the writer is never started, so
	// it opens no LISTEN connection and the feed under test stays the only one.
	writer, err := postgres.New(postgres.Config{DB: writerDB, ListenDSN: tenantDSN})
	if err != nil {
		t.Fatalf("postgres.New (gap writer): %v", err)
	}

	gapRev, err := writer.Set(ctx, store.Scope{}, store.Entry{
		Namespace: "ns",
		Key:       "gap",
		Value:     jsonBytes(t, "written-during-the-gap"),
	})
	if err != nil {
		t.Fatalf("gap set: %v", err)
	}

	if gapRev <= preRev {
		t.Fatalf("gap write revision = %d, want greater than the pre-gap %d", gapRev, preRev)
	}

	log.waitFor(t, "the reconnect's OpResync", func(evts []store.Event) bool {
		for _, evt := range evts[mark:] {
			if evt.Op == store.OpResync {
				return true
			}
		}

		return false
	})

	// A write AFTER the reconnect proves key events flow again on the new
	// connection, and gives the sequence assertion something to land behind
	// the resync.
	if _, err := s.Set(ctx, scope, store.Entry{
		Namespace: "ns",
		Key:       "after",
		Value:     jsonBytes(t, "after-the-reconnect"),
	}); err != nil {
		t.Fatalf("post-reconnect set: %v", err)
	}

	seq := log.waitFor(t, "the post-reconnect upsert of ns/after", func(evts []store.Event) bool {
		for _, evt := range evts[mark:] {
			if evt.Op == store.OpUpsert && evt.Key == "after" {
				return true
			}
		}

		return false
	})[mark:]

	if len(seq) < 2 {
		t.Fatalf("sequence after the kill = %+v; want at least the OpDisconnect/OpResync pair", seq)
	}

	if seq[0].Op != store.OpDisconnect {
		t.Fatalf("sequence after the kill = %+v; first event must be OpDisconnect", seq)
	}

	if seq[1].Op != store.OpResync {
		t.Fatalf("sequence after the kill = %+v; second event must be OpResync", seq)
	}

	for i, marker := range seq[:2] {
		if marker.Scope != scope {
			t.Errorf("marker %d scope = %+v, want %+v", i, marker.Scope, scope)
		}

		if marker.Namespace != "" || marker.Key != "" || marker.Revision != 0 {
			t.Errorf("marker %d = %+v, want empty Namespace/Key and Revision 0", i, marker)
		}
	}

	for _, evt := range seq[2:] {
		if evt.Op == store.OpDisconnect {
			t.Fatalf("second OpDisconnect for a single connection loss: %+v in %+v", evt, seq)
		}

		if evt.Op == store.OpResync {
			t.Fatalf("second OpResync for a single reconnect: %+v in %+v", evt, seq)
		}
	}

	// The gap write is recoverable from the resync alone. Its NOTIFY was never
	// redelivered; the value is simply there, at a higher revision than the one
	// the subscriber last saw.
	entry, found, err := s.Get(ctx, scope, "ns", "gap")
	if err != nil {
		t.Fatalf("get after the gap: %v", err)
	}

	if !found {
		t.Fatal("get after the gap: ns/gap not found")
	}

	if entry.Revision != gapRev {
		t.Fatalf("ns/gap revision after the gap = %d, want %d", entry.Revision, gapRev)
	}

	if entry.Revision <= preRev {
		t.Fatalf("ns/gap revision after the gap = %d, want greater than the pre-gap %d", entry.Revision, preRev)
	}
}

// TestIntegration_PostgresCleanCloseEmitsNoDisconnect is the other half of the
// "exactly one disconnect per connection LOSS" contract. Close tears the
// connection down deliberately, so the reader's wait fails exactly as it would
// in a real outage; only the closing flag keeps that from being announced. A
// disconnect here would leave every scope permanently Stale in the engine after
// an ordinary shutdown.
func TestIntegration_PostgresCleanCloseEmitsNoDisconnect(t *testing.T) {
	s := freshStore(t, "cleanclose")
	ctx := context.Background()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	log := &eventLog{}

	unsub, err := s.Subscribe(ctx, store.Scope{}, log.record)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	defer unsub()

	log.waitFor(t, "the joining resync", func(evts []store.Event) bool {
		return len(evts) >= 1
	})

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Close waits for the reader to exit, so nothing more can be delivered; the
	// grace only catches an emission racing that exit.
	time.Sleep(500 * time.Millisecond)

	for _, evt := range log.snapshot() {
		if evt.Op == store.OpDisconnect {
			t.Fatalf("clean Close delivered %+v; want no OpDisconnect at all", evt)
		}
	}
}

// TestIntegration_PostgresEventCarriesRevision pins FC-2's revision on the
// changefeed: what Set reports and what the subscriber is told are the same
// number, an identical rewrite reports it again (the trigger still fires on the
// updated_at change — deduplicating that is the engine's job, not the store's),
// a delete arrives as OpDelete with revision 0, the "no row, registered default
// in force" marker, and a key recreated after that delete comes back ABOVE
// every revision it ever carried — the counter is a table-level sequence, not
// a per-row counter, so a delete resets nothing and a subscriber that missed
// both events still accepts the recreated value instead of fencing it out as
// stale. assertV4Shape pins the catalog side of that mechanism.
func TestIntegration_PostgresEventCarriesRevision(t *testing.T) {
	s := freshStore(t, "evtrev")
	ctx := context.Background()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	events, unsub := subscribeScope(t, s, store.Scope{})
	defer unsub()

	if first := recvEvent(t, events, "the joining resync"); first.Op != store.OpResync {
		t.Fatalf("first event = %+v, want %q", first, store.OpResync)
	}

	set := func(what string, value string) int64 {
		t.Helper()

		rev, err := s.Set(ctx, store.Scope{}, store.Entry{
			Namespace: "ns",
			Key:       "k",
			Value:     jsonBytes(t, value),
		})
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}

		return rev
	}

	rev := set("first set", "v1")

	upsert := recvEvent(t, events, "the upsert of ns/k")
	if upsert.Op != store.OpUpsert || upsert.Namespace != "ns" || upsert.Key != "k" {
		t.Fatalf("event = %+v, want an upsert of ns/k", upsert)
	}

	if upsert.Revision != rev {
		t.Fatalf("upsert revision = %d, want %d (what Set reported)", upsert.Revision, rev)
	}

	if again := set("identical rewrite", "v1"); again != rev {
		t.Fatalf("identical rewrite returned revision %d, want the unchanged %d", again, rev)
	}

	rewrite := recvEvent(t, events, "the identical rewrite's upsert")
	if rewrite.Op != store.OpUpsert || rewrite.Revision != rev {
		t.Fatalf("identical rewrite event = %+v, want an upsert carrying revision %d", rewrite, rev)
	}

	// Climb above 1 before deleting, so the recreate below demonstrably
	// announces a revision the subscriber has already seen surpassed.
	climbed := set("changed value", "v2")
	if climbed <= rev {
		t.Fatalf("changed value: Set reported revision %d, want greater than %d", climbed, rev)
	}

	bumped := recvEvent(t, events, "the upsert of the changed value")
	if bumped.Op != store.OpUpsert || bumped.Revision != climbed {
		t.Fatalf("changed value event = %+v, want an upsert carrying revision %d", bumped, climbed)
	}

	if err := s.Delete(ctx, store.Scope{}, "ns", "k", "tester"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	deleted := recvEvent(t, events, "the delete of ns/k")
	if deleted.Op != store.OpDelete || deleted.Namespace != "ns" || deleted.Key != "k" {
		t.Fatalf("event = %+v, want a delete of ns/k", deleted)
	}

	if deleted.Revision != 0 {
		t.Fatalf("delete event revision = %d, want 0", deleted.Revision)
	}

	// Recreate. The revision column carries no DEFAULT at all: the BEFORE
	// INSERT OR UPDATE trigger draws nextval on the table-level sequence and
	// assigns it, so the new row lands above everything the key ever carried
	// — and a DML-only runtime role never has to touch the sequence itself.
	recreated := set("recreate after delete", "v3")
	if recreated <= climbed {
		t.Fatalf("recreated after delete: Set reported revision %d, want greater than the pre-delete %d", recreated, climbed)
	}

	back := recvEvent(t, events, "the upsert recreating ns/k")
	if back.Op != store.OpUpsert || back.Namespace != "ns" || back.Key != "k" {
		t.Fatalf("event = %+v, want an upsert of ns/k", back)
	}

	if back.Revision != recreated {
		t.Fatalf("recreate event revision = %d, want %d (what Set reported)", back.Revision, recreated)
	}
}

// The zero-scope feed is shared, so Start must open exactly one LISTEN
// connection no matter how many callers race into it. Two readers on one feed
// deliver every NOTIFY twice — the engine would then apply, and fence, each
// change against itself.
func TestIntegration_PostgresConcurrentStartOpensOneListener(t *testing.T) {
	base := startContainer(t)

	admin := adminDSN(t, base)

	defer func() { _ = admin.Close() }()

	dbName, tenantDSN, db := provisionTenantDB(t, admin, base, "concurrentstart")

	s, err := postgres.New(postgres.Config{DB: db, ListenDSN: tenantDSN})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()

	const callers = 8

	errs := make(chan error, callers)

	var wg sync.WaitGroup

	for range callers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			errs <- s.Start(ctx)
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Start: %v", err)
		}
	}

	waitForListenBackends(t, admin, dbName, 1, "concurrent Start calls share one LISTEN connection")

	events := make(chan store.Event, 16)

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

	if first := recvEvent(t, events, "joining resync"); first.Op != store.OpResync {
		t.Fatalf("first event = %+v, want %q", first, store.OpResync)
	}

	if _, err := s.Set(ctx, store.Scope{}, store.Entry{
		Namespace: "ns",
		Key:       "k",
		Value:     jsonBytes(t, "v1"),
	}); err != nil {
		t.Fatalf("set: %v", err)
	}

	evt := recvEvent(t, events, "upsert of ns/k")
	if evt.Op != store.OpUpsert || evt.Namespace != "ns" || evt.Key != "k" {
		t.Fatalf("event = %+v, want an upsert of ns/k", evt)
	}

	select {
	case dup := <-events:
		t.Fatalf("one write produced a second event %+v: more than one reader is attached to the zero-scope feed", dup)
	case <-time.After(3 * time.Second):
	}
}

// Against a real server: a ListenDSN that pins a schema is DIALED, not refused.
// lib-commons writes "options=-csearch_path=<schema>" for any tenant whose
// config declares a schema, whatever its isolation mode, so refusing on that
// signal alone would take the changefeed away from every install that merely
// names one — permanently, since a failed activation is retried from scratch
// on every later read.
func TestIntegration_PostgresStartAcceptsSchemaPinnedListenDSN(t *testing.T) {
	base := startContainer(t)

	admin := adminDSN(t, base)

	defer func() { _ = admin.Close() }()

	dbName, tenantDSN, db := provisionTenantDB(t, admin, base, "pinned_schema")

	if _, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS app`); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	s, err := postgres.New(postgres.Config{DB: db, ListenDSN: tenantDSN + "&options=-csearch_path%3Dapp,public"})
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start with a schema-pinned ListenDSN: %v", err)
	}

	if n := listenBackends(t, admin, dbName); n != 1 {
		t.Fatalf("a schema-pinned ListenDSN opened %d LISTEN connections on %s, want 1", n, dbName)
	}
}

// What IS refused is two scopes resolving to the same DATABASE — the shape
// schema-per-tenant actually produces. NOTIFY is database-wide and every feed
// listens on the same channel, so the second tenant's feed would receive the
// first tenant's notifications stamped with its own scope and the engine's
// revision fence would act on them.
func TestIntegration_PostgresTwoTenantsOnOneDatabase(t *testing.T) {
	base := startContainer(t)

	admin := adminDSN(t, base)

	defer func() { _ = admin.Close() }()

	dbName, tenantDSN, db := provisionTenantDB(t, admin, base, "shared_db")

	if _, err := db.Exec(`CREATE SCHEMA IF NOT EXISTS tenant_b`); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	conn := newFakeConnector()
	conn.set("t1", db, tenantDSN)
	// The classic schema-per-tenant DSN pair: one database, two search_paths.
	conn.set("t2", db, tenantDSN+"&options=-csearch_path%3Dtenant_b,public")

	s := tenantStore(t, conn)

	ctx := context.Background()

	unsub, err := s.Subscribe(ctx, store.Scope{Tenant: "t1"}, func(store.Event) {})
	if err != nil {
		t.Fatalf("subscribe t1: %v", err)
	}

	t.Cleanup(unsub)

	unsub2, err := s.Subscribe(ctx, store.Scope{Tenant: "t2"}, func(store.Event) {})
	if err == nil {
		unsub2()
		t.Fatal("a second tenant on t1's database was admitted; want ErrSharedDatabaseUnsupported")
	}

	if !errors.Is(err, postgres.ErrSharedDatabaseUnsupported) {
		t.Fatalf("subscribe t2 error = %v, want postgres.ErrSharedDatabaseUnsupported", err)
	}

	// The refused feed leaves nothing behind: t1 keeps its one backend. A
	// refused connection is closed by the creator and reaped by Postgres
	// asynchronously, so this waits for the count instead of sampling it once.
	// It runs before the respelling case below, which may skip: the refusal
	// above is proven in every environment and its assertions must not ride on
	// one that is not.
	waitForListenBackends(t, admin, dbName, 1, "after the shared-database refusal")

	// A third tenant on the same database, reached by a different SPELLING of
	// the same host. Nothing forces two operators to type one connection
	// string, and a key read off the DSN text calls "localhost" and
	// "127.0.0.1" two databases and lets this feed through. The server reports
	// one identity for both.
	//
	// Its own subtest because it is the one case here that depends on the
	// environment offering a second spelling that dials: a skip taken on the
	// parent would discard every assertion above it, which is the whole proof
	// of ErrSharedDatabaseUnsupported.
	t.Run("host respelled", func(t *testing.T) {
		altDSN, ok := respellHost(tenantDSN)
		if !ok {
			t.Skipf("container host in %q has no second spelling here", tenantDSN)
		}

		requireDialable(t, altDSN)

		conn.set("t3", db, altDSN)

		unsub3, err := s.Subscribe(ctx, store.Scope{Tenant: "t3"}, func(store.Event) {})
		if err == nil {
			unsub3()
			t.Fatal("a tenant spelling t1's host differently was admitted; want ErrSharedDatabaseUnsupported")
		}

		if !errors.Is(err, postgres.ErrSharedDatabaseUnsupported) {
			t.Fatalf("subscribe t3 error = %v, want postgres.ErrSharedDatabaseUnsupported", err)
		}
	})
}

// respellHost returns dsn with its host swapped for another spelling of the
// same address, and false when the environment offers none.
func respellHost(dsn string) (string, bool) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", false
	}

	var alt string

	switch u.Hostname() {
	case "localhost":
		alt = "127.0.0.1"
	case "127.0.0.1":
		alt = "localhost"
	default:
		return "", false
	}

	u.Host = net.JoinHostPort(alt, u.Port())

	return u.String(), true
}

// requireDialable skips the caller when the alternate spelling cannot reach the
// container at all — an environment fact (no IPv4 loopback publish, a resolver
// that sends localhost to ::1), not something the store decides. Pass the t of
// the SUBTEST that needs the spelling, never a parent's: the skip is taken on
// whatever t it is handed, and a parent's skip throws away every sibling
// assertion with it.
func requireDialable(t *testing.T, dsn string) {
	t.Helper()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Skipf("open %s: %v", dsn, err)
	}

	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("the alternate host spelling is not reachable here: %v", err)
	}
}
