//go:build integration

package mongodb_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-systemplane/v4/internal/mongodb"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"github.com/LerianStudio/lib-systemplane/v4/systemplanetest"
	"github.com/testcontainers/testcontainers-go"
	mongocontainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func startContainer(t *testing.T) (*mongo.Client, func()) {
	t.Helper()

	ctx := context.Background()

	container, err := mongocontainer.Run(ctx, "mongo:7", mongocontainer.WithReplicaSet("rs0"))
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		_ = testcontainers.TerminateContainer(container)

		t.Fatalf("connection string: %v", err)
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetDirect(true))
	if err != nil {
		_ = testcontainers.TerminateContainer(container)

		t.Fatalf("mongo connect: %v", err)
	}

	cleanup := func() {
		_ = client.Disconnect(context.Background())
		_ = testcontainers.TerminateContainer(container)
	}

	// The replica-set member transitions SECONDARY→PRIMARY shortly after the
	// container reports ready, and a write landing in that window fails with
	// NotWritablePrimary. The connection is direct, so the driver does no
	// primary selection of its own: wait here before handing the client out.
	if err := waitForWritablePrimary(client); err != nil {
		cleanup()

		t.Fatalf("wait for writable primary: %v", err)
	}

	return client, cleanup
}

func waitForWritablePrimary(client *mongo.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var lastErr error

	for {
		var hello struct {
			IsWritablePrimary bool `bson:"isWritablePrimary"`
		}

		lastErr = client.Database("admin").
			RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).
			Decode(&hello)
		if lastErr == nil && hello.IsWritablePrimary {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("node never became writable primary (last error: %v): %w", lastErr, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestIntegration_MongoDBSingleTenant(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	factory := func(t *testing.T) (store.Store, func()) {
		t.Helper()

		dbName := fmt.Sprintf("st_%d", time.Now().UnixNano())

		s, err := mongodb.New(mongodb.Config{
			Client:   client,
			Database: dbName,
		})
		if err != nil {
			t.Fatalf("mongodb.New: %v", err)
		}

		return s, func() {
			_ = s.Close()
			_ = client.Database(dbName).Drop(context.Background())
		}
	}

	systemplanetest.Run(t, factory, systemplanetest.RunOptions{
		EventWait: 5 * time.Second,
		// Phase 2 of lane-storage turns this off.
		SkipRevisionAndResync: true,
	})
}

func TestIntegration_MongoDBMultiTenantIsolation(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	ns := time.Now().UnixNano()
	dbA := client.Database(fmt.Sprintf("tenant_a_%d", ns))
	dbB := client.Database(fmt.Sprintf("tenant_b_%d", ns))

	t.Cleanup(func() {
		_ = dbA.Drop(context.Background())
		_ = dbB.Drop(context.Background())
	})

	s, err := mongodb.New(mongodb.Config{
		MultiTenantEnabled: true,
		Module:             "systemplane",
	})
	if err != nil {
		t.Fatalf("mongodb.New: %v", err)
	}

	defer s.Close()

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	ctxA := tmcore.ContextWithMB(context.Background(), dbA, "systemplane")
	ctxB := tmcore.ContextWithMB(context.Background(), dbB, "systemplane")

	mustSet(t, s, ctxA, "ns", "k", "value-A")
	mustSet(t, s, ctxB, "ns", "k", "value-B")

	if got := mustGet(t, s, ctxA, "ns", "k"); got != "value-A" {
		t.Errorf("tenant A read = %q, want value-A", got)
	}

	if got := mustGet(t, s, ctxB, "ns", "k"); got != "value-B" {
		t.Errorf("tenant B read = %q, want value-B", got)
	}

	// Tenant A's collection should not contain tenant B's value.
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

	// Subscribe is not supported in multi-tenant mode.
	if _, err := s.Subscribe(ctxA, store.Scope{}, func(_ store.Event) {}); err != store.ErrNotSupportedInMultiTenant {
		t.Errorf("subscribe should fail with ErrNotSupportedInMultiTenant, got %v", err)
	}
}

func TestIntegration_MongoDBMultiTenantMissingCtx(t *testing.T) {
	// This test only exercises the "missing tenant context" path: no real
	// Mongo client is required because resolveCollection short-circuits on
	// the missing tmcore.GetMBContext before touching any database. Starting
	// a container here just adds 5+ seconds and a Docker dependency for a
	// purely in-process assertion.
	s, err := mongodb.New(mongodb.Config{
		MultiTenantEnabled: true,
		Module:             "systemplane",
	})
	if err != nil {
		t.Fatalf("mongodb.New: %v", err)
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

func mustSet(t *testing.T, s store.Store, ctx context.Context, ns, key, value string) {
	t.Helper()

	raw, _ := json.Marshal(value)
	if _, err := s.Set(ctx, store.Scope{}, store.Entry{Namespace: ns, Key: key, Value: raw}); err != nil {
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

// fakeConnector resolves tenants from a static map, standing in for a
// tenant-manager Mongo Manager without a tenant-config gRPC client. It is
// guarded by a mutex so a test can teach it a tenant it previously did not
// know, and an unknown tenant fails the way the manager would. It counts
// resolutions so a test can prove that a re-subscribed tenant resolves again —
// which is how a credentials rotation is picked up.
type fakeConnector struct {
	mu    sync.Mutex
	dbs   map[string]*mongo.Database
	calls int
}

func newFakeConnector() *fakeConnector {
	return &fakeConnector{dbs: map[string]*mongo.Database{}}
}

func (c *fakeConnector) set(tenantID string, db *mongo.Database) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.dbs[tenantID] = db
}

func (c *fakeConnector) resolveCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.calls
}

func (c *fakeConnector) ResolveDatabase(_ context.Context, tenantID string) (*mongo.Database, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls++

	db, ok := c.dbs[tenantID]
	if !ok {
		return nil, fmt.Errorf("fakeConnector: unknown tenant %q", tenantID)
	}

	return db, nil
}

// TestIntegration_MongoScopedCRUDIsolation pins FC-2's scoped resolution on the
// MongoDB CRUD path: a named Scope.Tenant resolves its database through the
// connector and nothing else. Every call travels on a plain
// context.Background() carrying no tenant at all, so a passing test proves the
// handle came from the connector rather than from ctx, and each tenant sees
// only its own documents.
func TestIntegration_MongoScopedCRUDIsolation(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	conn := newFakeConnector()
	stamp := time.Now().UnixNano()

	for _, tenant := range []string{"t1", "t2"} {
		db := client.Database(fmt.Sprintf("scoped_%s_%d", tenant, stamp))

		t.Cleanup(func() { _ = db.Drop(context.Background()) })

		conn.set(tenant, db)
	}

	// No Client, no Database: every handle must come from the connector.
	s, err := mongodb.New(mongodb.Config{
		MultiTenantEnabled: true,
		Module:             "systemplane",
		Connector:          conn,
	})
	if err != nil {
		t.Fatalf("mongodb.New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()
	scope1 := store.Scope{Tenant: "t1"}
	scope2 := store.Scope{Tenant: "t2"}

	set := func(scope store.Scope, key, value string) {
		t.Helper()

		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}

		if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: key, Value: raw}); err != nil {
			t.Fatalf("set %s/%s: %v", scope.Tenant, key, err)
		}
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

	// A read before the first write must reach a materialized collection, not
	// an empty answer from a database that was never touched.
	if _, found := get(scope1); found {
		t.Fatal("t1 Get before any write found an entry")
	}

	set(scope1, "k", "value-t1")
	set(scope2, "k", "value-t2")

	if got, found := get(scope1); !found || got != "value-t1" {
		t.Errorf("t1 Get = %q (found=%v), want value-t1", got, found)
	}

	if got, found := get(scope2); !found || got != "value-t2" {
		t.Errorf("t2 Get = %q (found=%v), want value-t2", got, found)
	}

	// A t2-only write must stay invisible from t1's scope.
	set(scope2, "only-t2", "x")

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

// freshSingleTenantStore builds a single-tenant store over its own database on
// the shared container. CRUD needs no Start — Start only opens the change
// stream — so these revision tests leave no feed goroutine behind. The raw
// collection handle is returned alongside it so a test can inspect the stored
// document behind the store surface, which is the only way to observe a
// tombstone: Get and List filter them out by contract.
func freshSingleTenantStore(t *testing.T, client *mongo.Client, prefix string) (store.Store, *mongo.Collection) {
	t.Helper()

	dbName := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())

	s, err := mongodb.New(mongodb.Config{Client: client, Database: dbName})
	if err != nil {
		t.Fatalf("mongodb.New: %v", err)
	}

	t.Cleanup(func() {
		_ = s.Close()
		_ = client.Database(dbName).Drop(context.Background())
	})

	return s, client.Database(dbName).Collection("systemplane_entries")
}

// rawID mirrors the compound _id so a test can address a document directly.
type rawID struct {
	Namespace string `bson:"namespace"`
	Key       string `bson:"key"`
}

// rawDoc is the stored document as a test reads it, bypassing the store. Value
// is a pointer so an unset field (what a tombstone carries) is distinguishable
// from an empty string.
type rawDoc struct {
	Revision  int64     `bson:"revision"`
	Deleted   bool      `bson:"deleted"`
	UpdatedAt time.Time `bson:"updated_at"`
	UpdatedBy string    `bson:"updated_by"`
	Value     *string   `bson:"value"`
}

func readRaw(t *testing.T, coll *mongo.Collection, namespace, key string) rawDoc {
	t.Helper()

	var doc rawDoc
	if err := coll.FindOne(context.Background(), bson.D{
		{Key: "_id", Value: rawID{Namespace: namespace, Key: key}},
	}).Decode(&doc); err != nil {
		t.Fatalf("raw find %s/%s: %v", namespace, key, err)
	}

	return doc
}

// TestIntegration_MongoIdenticalWriteKeepsRevision pins FC-2/FC-9 on the
// MongoDB write path: the first write reports a revision above zero, a changed
// value strictly increases it, an identical rewrite leaves it alone, and both
// read paths report the number the write returned.
func TestIntegration_MongoIdenticalWriteKeepsRevision(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	s, _ := freshSingleTenantStore(t, client, "rev")
	ctx := context.Background()

	set := func(value string) int64 {
		t.Helper()

		rev, err := s.Set(ctx, store.Scope{}, store.Entry{
			Namespace: "ns",
			Key:       "k",
			Value:     []byte(value),
			UpdatedBy: "actor",
		})
		if err != nil {
			t.Fatalf("set %s: %v", value, err)
		}

		return rev
	}

	r1 := set(`{"a":1}`)
	if r1 <= 0 {
		t.Fatalf("first write revision = %d, want > 0", r1)
	}

	r2 := set(`{"a":2}`)
	if r2 <= r1 {
		t.Fatalf("changed-value revision = %d, want > %d", r2, r1)
	}

	r3 := set(`{"a":2}`)
	if r3 != r2 {
		t.Fatalf("identical rewrite revision = %d, want %d unchanged", r3, r2)
	}

	entry, found, err := s.Get(ctx, store.Scope{}, "ns", "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !found {
		t.Fatal("get: not found")
	}

	if entry.Revision != r2 {
		t.Errorf("Get revision = %d, want %d", entry.Revision, r2)
	}

	entries, err := s.List(ctx, store.Scope{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	var listed bool

	for _, e := range entries {
		if e.Namespace == "ns" && e.Key == "k" {
			listed = true

			if e.Revision != r2 {
				t.Errorf("List revision = %d, want %d", e.Revision, r2)
			}
		}
	}

	if !listed {
		t.Fatal("list: entry ns/k not found")
	}
}

// TestIntegration_MongoDollarPrefixedStringsStoredVerbatim is the MongoDB half
// of the cross-backend parity case whose Postgres twin is
// TestIntegration_PostgresDollarPrefixedStringsStoredVerbatim. The write is an
// aggregation-pipeline update, where a bare string beginning with "$" is an
// expression: unwrapped, an actor of "$value" would persist the document's own
// JSON payload and a namespace of "$ns" would resolve to missing and drop the
// field. $literal is the only permitted fix — neither backend rejects
// $-prefixed identifiers.
func TestIntegration_MongoDollarPrefixedStringsStoredVerbatim(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	s, _ := freshSingleTenantStore(t, client, "dollar")
	ctx := context.Background()

	const (
		ns      = "$ns"
		key     = "$key"
		actor   = "$value"
		payload = `{"payload":true}`
	)

	write := func() int64 {
		t.Helper()

		rev, err := s.Set(ctx, store.Scope{}, store.Entry{
			Namespace: ns,
			Key:       key,
			Value:     []byte(payload),
			UpdatedBy: actor,
		})
		if err != nil {
			t.Fatalf("set: %v", err)
		}

		return rev
	}

	r1 := write()

	entry, found, err := s.Get(ctx, store.Scope{}, ns, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !found {
		t.Fatal("get: not found")
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

	if string(entry.Value) != payload {
		t.Errorf("value = %q, want %q", string(entry.Value), payload)
	}

	// Rewriting the same value must still compare equal with every surrounding
	// field wrapped, so the revision must not move.
	if r2 := write(); r2 != r1 {
		t.Errorf("identical rewrite revision = %d, want %d unchanged", r2, r1)
	}
}

// setForDelete writes one entry through the store and returns its revision.
func setForDelete(t *testing.T, s store.Store, namespace, key, value string) int64 {
	t.Helper()

	rev, err := s.Set(context.Background(), store.Scope{}, store.Entry{
		Namespace: namespace,
		Key:       key,
		Value:     []byte(value),
		UpdatedBy: "writer",
	})
	if err != nil {
		t.Fatalf("set %s/%s: %v", namespace, key, err)
	}

	return rev
}

// TestIntegration_MongoDeleteLeavesTombstone pins FC-9/D11: Delete never
// removes the document. It rewrites it as a tombstone that the store surface
// treats as absent, so the revision the key reached survives the delete and a
// later recreate can be placed above it.
func TestIntegration_MongoDeleteLeavesTombstone(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	s, coll := freshSingleTenantStore(t, client, "tombstone")
	ctx := context.Background()

	r1 := setForDelete(t, s, "ns", "k", `{"a":1}`)

	if err := s.Delete(ctx, store.Scope{}, "ns", "k", "deleter"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if _, found, err := s.Get(ctx, store.Scope{}, "ns", "k"); err != nil {
		t.Fatalf("get after delete: %v", err)
	} else if found {
		t.Error("get after delete: tombstone is visible through the store surface")
	}

	entries, err := s.List(ctx, store.Scope{})
	if err != nil {
		t.Fatalf("list after delete: %v", err)
	}

	for _, e := range entries {
		if e.Namespace == "ns" && e.Key == "k" {
			t.Error("list after delete: tombstone is listed")
		}
	}

	doc := readRaw(t, coll, "ns", "k")

	if !doc.Deleted {
		t.Error("tombstone: deleted = false, want true")
	}

	if doc.Value != nil {
		t.Errorf("tombstone: value = %q, want the field unset", *doc.Value)
	}

	if doc.Revision <= r1 {
		t.Errorf("tombstone revision = %d, want > %d", doc.Revision, r1)
	}

	if doc.UpdatedBy != "deleter" {
		t.Errorf("tombstone updated_by = %q, want %q", doc.UpdatedBy, "deleter")
	}
}

// TestIntegration_MongoRecreateAfterDeleteExceedsTombstone is the reason the
// tombstone exists (D11): with the document gone, a recreate would fall back to
// the server-clock floor and could land at or below the pre-delete revision,
// and the engine's fence would then reject the recreated value indefinitely.
func TestIntegration_MongoRecreateAfterDeleteExceedsTombstone(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	s, coll := freshSingleTenantStore(t, client, "recreate")
	ctx := context.Background()

	r1 := setForDelete(t, s, "ns", "k", `{"a":1}`)

	if err := s.Delete(ctx, store.Scope{}, "ns", "k", "deleter"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	tombstone := readRaw(t, coll, "ns", "k").Revision
	if tombstone <= r1 {
		t.Fatalf("tombstone revision = %d, want > %d", tombstone, r1)
	}

	r2 := setForDelete(t, s, "ns", "k", `{"a":2}`)
	if r2 <= tombstone {
		t.Fatalf("recreated revision = %d, want strictly greater than the tombstone's %d", r2, tombstone)
	}

	entry, found, err := s.Get(ctx, store.Scope{}, "ns", "k")
	if err != nil {
		t.Fatalf("get after recreate: %v", err)
	}

	if !found {
		t.Fatal("get after recreate: not found")
	}

	if entry.Revision != r2 {
		t.Errorf("get revision after recreate = %d, want %d", entry.Revision, r2)
	}

	if string(entry.Value) != `{"a":2}` {
		t.Errorf("value after recreate = %q, want %q", string(entry.Value), `{"a":2}`)
	}
}

// TestIntegration_MongoRepeatDeleteWritesNothing pins the half of FC-9 that the
// filter — rather than a conditional pipeline — buys: a delete of an existing
// tombstone, or of a key that was never written, matches nothing and writes
// nothing at all. A write of any kind would move updated_at, and on a tombstone
// it would reach the change stream as a second OpDelete at revision 0, which
// nothing deduplicates.
func TestIntegration_MongoRepeatDeleteWritesNothing(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	s, coll := freshSingleTenantStore(t, client, "repeatdelete")
	ctx := context.Background()

	setForDelete(t, s, "ns", "k", `{"a":1}`)

	if err := s.Delete(ctx, store.Scope{}, "ns", "k", "deleter"); err != nil {
		t.Fatalf("first delete: %v", err)
	}

	before := readRaw(t, coll, "ns", "k")

	if err := s.Delete(ctx, store.Scope{}, "ns", "k", "second-deleter"); err != nil {
		t.Fatalf("second delete: %v", err)
	}

	after := readRaw(t, coll, "ns", "k")

	if after.Revision != before.Revision {
		t.Errorf("revision moved on repeat delete: %d → %d", before.Revision, after.Revision)
	}

	if !after.UpdatedAt.Equal(before.UpdatedAt) {
		t.Errorf("updated_at moved on repeat delete: %v → %v", before.UpdatedAt, after.UpdatedAt)
	}

	if after.UpdatedBy != before.UpdatedBy {
		t.Errorf("updated_by moved on repeat delete: %q → %q", before.UpdatedBy, after.UpdatedBy)
	}

	// A delete of a key that was never written creates nothing: no upsert.
	if err := s.Delete(ctx, store.Scope{}, "ns", "never-written", "deleter"); err != nil {
		t.Fatalf("delete of a missing key: %v", err)
	}

	count, err := coll.CountDocuments(ctx, bson.D{
		{Key: "_id", Value: rawID{Namespace: "ns", Key: "never-written"}},
	})
	if err != nil {
		t.Fatalf("count after deleting a missing key: %v", err)
	}

	if count != 0 {
		t.Errorf("delete of a missing key created %d document(s), want 0", count)
	}
}

// A clean Close must announce nothing. The reader's cursor dies because
// teardown killed it, so an OpDisconnect emitted there would tell the engine to
// mark stale a scope that is in fact gone — once per scope, on every shutdown.
//
// The upsert assertion in the middle is not scenery: it proves the stream was
// attached BEFORE that write landed. A change stream carries no resume token
// here, so a stream opened after the write never delivers it — and without that
// assertion this test would pass vacuously on a feed that never attached.
func TestIntegration_MongoCleanCloseEmitsNoDisconnect(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	s, _ := freshSingleTenantStore(t, client, "cleanclose")
	ctx := context.Background()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var (
		mu     sync.Mutex
		events []store.Event
	)

	unsub, err := s.Subscribe(ctx, store.Scope{}, func(evt store.Event) {
		mu.Lock()
		defer mu.Unlock()

		events = append(events, evt)
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	defer unsub()

	if _, err := s.Set(ctx, store.Scope{}, store.Entry{
		Namespace: "ns",
		Key:       "k",
		Value:     []byte(`{"a":1}`),
		UpdatedBy: "actor",
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)

	for !sawUpsert(&mu, &events) {
		if time.Now().After(deadline) {
			t.Fatal("the write that landed after Subscribe never reached the subscriber: the change stream attached too late")
		}

		time.Sleep(20 * time.Millisecond)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Whatever the reader still had in flight lands inside this window.
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	for _, evt := range events {
		if evt.Op == store.OpDisconnect {
			t.Fatalf("clean Close emitted OpDisconnect; events = %#v", events)
		}
	}
}

func sawUpsert(mu *sync.Mutex, events *[]store.Event) bool {
	mu.Lock()
	defer mu.Unlock()

	for _, evt := range *events {
		if evt.Op == store.OpUpsert && evt.Key == "k" {
			return true
		}
	}

	return false
}

// The engine subscribes AFTER Start has already connected the stream, so
// without a joining marker a subscriber on a quiet scope would hear nothing and
// never reconcile. The very first event it receives must be OpResync for its own
// scope, carrying no namespace, no key and no revision.
func TestIntegration_MongoSubscribeAfterStartGetsResyncFirst(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	s, _ := freshSingleTenantStore(t, client, "joinresync")
	ctx := context.Background()

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	first := make(chan store.Event, 4)

	unsub, err := s.Subscribe(ctx, store.Scope{}, func(evt store.Event) {
		select {
		case first <- evt:
		default:
		}
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	defer unsub()

	select {
	case evt := <-first:
		if evt.Op != store.OpResync {
			t.Fatalf("first event after Subscribe = %#v, want Op %q", evt, store.OpResync)
		}

		if evt.Scope != (store.Scope{}) || evt.Namespace != "" || evt.Key != "" || evt.Revision != 0 {
			t.Fatalf("joining resync = %#v, want the zero scope with no namespace, key or revision", evt)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a subscriber joining a connected feed never received its own OpResync")
	}
}

// tenantDB hands the connector a fresh database for tenant and drops it when
// the test ends. Nothing writes it up front: a tenant database that has never
// been touched is exactly the case a change stream must still be able to
// attach to.
func tenantDB(t *testing.T, client *mongo.Client, conn *fakeConnector, tenant, prefix string) *mongo.Database {
	t.Helper()

	db := client.Database(fmt.Sprintf("%s_%s_%d", prefix, tenant, time.Now().UnixNano()))

	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	conn.set(tenant, db)

	return db
}

// tenantStore builds a Store with no database of its own: every scope resolves
// through the connector, which is the shape the engine uses for tenant scopes.
func tenantStore(t *testing.T, conn mongodb.Connector) *mongodb.Store {
	t.Helper()

	s, err := mongodb.New(mongodb.Config{MultiTenantEnabled: true, Connector: conn})
	if err != nil {
		t.Fatalf("mongodb.New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	return s
}

// subscribeScope subscribes to scope and returns the delivered events. The
// channel is buffered because a joining subscriber's OpResync is delivered
// synchronously inside Subscribe.
func subscribeScope(t *testing.T, s *mongodb.Store, scope store.Scope) (<-chan store.Event, func()) {
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

// recvEvent waits for one changefeed event or fails the test.
func recvEvent(t *testing.T, events <-chan store.Event, what string) store.Event {
	t.Helper()

	select {
	case evt := <-events:
		return evt
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for %s", what)

		return store.Event{}
	}
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

// waitForFeedRefs blocks until tenant's reserved slot has taken want
// references — one per caller parked on it.
func waitForFeedRefs(t *testing.T, s *mongodb.Store, tenant string, want int) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)

	for {
		if _, refs := s.FeedsSnapshot(tenant); refs >= want {
			return
		}

		if time.Now().After(deadline) {
			_, refs := s.FeedsSnapshot(tenant)
			t.Fatalf("reserved slot for tenant %q holds %d references, want %d callers parked on it", tenant, refs, want)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

// TestIntegration_MongoTwoTenantFeedsAreIsolated pins the per-tenant change
// stream: each tenant's subscriber is told OpResync for ITS OWN scope, and a
// write in one tenant's database never reaches the other tenant's subscriber.
func TestIntegration_MongoTwoTenantFeedsAreIsolated(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	conn := newFakeConnector()

	for _, tenant := range []string{"t1", "t2"} {
		tenantDB(t, client, conn, tenant, "feed")
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

	// A plain background ctx carries no tenant at all: a passing test proves
	// every handle came from the connector rather than from ctx.
	ctx := context.Background()

	if _, err := s.Set(ctx, scope1, store.Entry{Namespace: "ns", Key: "only-t1", Value: []byte(`"v1"`)}); err != nil {
		t.Fatalf("set t1: %v", err)
	}

	if got := recvEvent(t, events1, "t1 upsert"); got.Op != store.OpUpsert || got.Namespace != "ns" || got.Key != "only-t1" {
		t.Fatalf("t1 event = %+v, want an upsert of ns/only-t1", got)
	}

	assertNoEvent(t, events2, 2*time.Second, "t2 must never see t1's write")

	if _, err := s.Set(ctx, scope2, store.Entry{Namespace: "ns", Key: "only-t2", Value: []byte(`"v2"`)}); err != nil {
		t.Fatalf("set t2: %v", err)
	}

	if got := recvEvent(t, events2, "t2 upsert"); got.Op != store.OpUpsert || got.Namespace != "ns" || got.Key != "only-t2" {
		t.Fatalf("t2 event = %+v, want an upsert of ns/only-t2", got)
	}

	assertNoEvent(t, events1, 2*time.Second, "t1 must never see t2's write")
}

// TestIntegration_MongoTenantFeedTornDownOnLastUnsubscribe pins the shared,
// reference-counted lifetime of a tenant feed: two subscribers ride ONE change
// stream, dropping the first keeps it alive for the second, the last one to
// leave closes it, and a later Subscribe resolves the tenant's database again —
// which is how a credentials rotation is picked up.
func TestIntegration_MongoTenantFeedTornDownOnLastUnsubscribe(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	conn := newFakeConnector()
	tenantDB(t, client, conn, "t1", "teardown")

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

	if total, refs := s.FeedsSnapshot(scope.Tenant); total != 1 || refs != 2 {
		t.Fatalf("feeds = %d slots / %d refs, want one shared feed held by both subscribers", total, refs)
	}

	if calls := conn.resolveCalls(); calls != 1 {
		t.Errorf("ResolveDatabase calls = %d, want 1: the second subscriber must join the existing feed", calls)
	}

	// Dropping one subscriber keeps the stream alive for the other.
	unsubA()

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`"v1"`)}); err != nil {
		t.Fatalf("set: %v", err)
	}

	if got := recvEvent(t, eventsB, "upsert after A left"); got.Op != store.OpUpsert || got.Key != "k" {
		t.Fatalf("B event = %+v, want an upsert of ns/k", got)
	}

	assertNoEvent(t, eventsA, time.Second, "A unsubscribed and must receive nothing")

	if total, refs := s.FeedsSnapshot(scope.Tenant); total != 1 || refs != 1 {
		t.Fatalf("feeds = %d slots / %d refs while one subscriber remains, want 1/1", total, refs)
	}

	// The last subscriber closes the tenant's stream.
	unsubB()

	if total, _ := s.FeedsSnapshot(scope.Tenant); total != 0 {
		t.Fatalf("feeds map holds %d entries after the last unsubscribe, want 0", total)
	}

	// Measured as a delta, not a total: MongoDB has ONE connector method, so
	// the Set above resolved the tenant too. What must be true is that the
	// rebuilt feed consults the connector once more of its own.
	before := conn.resolveCalls()

	// A later Subscribe rebuilds the feed from a freshly resolved database.
	eventsC, unsubC := subscribeScope(t, s, scope)
	defer unsubC()

	if first := recvEvent(t, eventsC, "resync after re-subscribe"); first.Op != store.OpResync || first.Scope != scope {
		t.Fatalf("re-subscribe first event = %+v, want {Scope:%+v Op:%q}", first, scope, store.OpResync)
	}

	if calls := conn.resolveCalls(); calls != before+1 {
		t.Errorf("ResolveDatabase calls = %d, want %d: a re-subscribed tenant must resolve its database again", calls, before+1)
	}
}

// subscribeBurst runs callers concurrent Subscribe calls on scope and returns
// what each one got back. It fails the test if any of them blocks, which is the
// half of the creation handshake a reserved slot could break.
func subscribeBurst(t *testing.T, s *mongodb.Store, scope store.Scope, callers int) []subscribeResult {
	t.Helper()

	results := make(chan subscribeResult, callers)

	var wg sync.WaitGroup

	for range callers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			unsub, err := s.Subscribe(context.Background(), scope, func(store.Event) {})
			results <- subscribeResult{unsub: unsub, err: err}
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

	out := make([]subscribeResult, 0, callers)
	for r := range results {
		out = append(out, r)
	}

	return out
}

type subscribeResult struct {
	unsub func()
	err   error
}

// TestIntegration_MongoConcurrentFirstSubscribeOpensOneFeed covers both halves
// of the creation handshake. A tenant the connector cannot resolve fails every
// concurrent caller — none of them blocks, and nothing is left running — and
// once the connector knows the tenant, the same burst of callers shares exactly
// ONE change stream, which is what "one live subscription per activated tenant"
// rests on.
func TestIntegration_MongoConcurrentFirstSubscribeOpensOneFeed(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	conn := newFakeConnector()
	s := tenantStore(t, conn)
	scope := store.Scope{Tenant: "t1"}

	const callers = 8

	for i, r := range subscribeBurst(t, s, scope, callers) {
		if r.err == nil {
			r.unsub()
			t.Fatalf("Subscribe %d succeeded for a tenant the connector cannot resolve", i)
		}

		if !strings.Contains(r.err.Error(), "unknown tenant") {
			t.Errorf("Subscribe %d error = %v, want the connector's resolution failure", i, r.err)
		}
	}

	if total, _ := s.FeedsSnapshot(scope.Tenant); total != 0 {
		t.Fatalf("a failed feed creation left %d feeds behind, want 0", total)
	}

	// The connector learns the tenant: every caller now succeeds on one feed.
	tenantDB(t, client, conn, "t1", "concurrent")

	unsubs := make([]func(), 0, callers)

	for i, r := range subscribeBurst(t, s, scope, callers) {
		if r.err != nil {
			t.Fatalf("Subscribe %d after the connector learned the tenant: %v", i, r.err)
		}

		unsubs = append(unsubs, r.unsub)
	}

	if total, refs := s.FeedsSnapshot(scope.Tenant); total != 1 || refs != callers {
		t.Fatalf("feeds = %d slots / %d refs, want one feed shared by all %d callers", total, refs, callers)
	}

	for _, unsub := range unsubs {
		unsub()
	}

	if total, _ := s.FeedsSnapshot(scope.Tenant); total != 0 {
		t.Fatalf("feeds map holds %d entries after the last unsubscribe, want 0", total)
	}
}

// blockingConnector parks the FIRST ResolveDatabase call inside the connector
// until the test releases it. That is the window Close has to survive: a
// creator still resolving, a reserved slot in the feeds map that carries no
// stop channel, and a second caller already waiting on it.
type blockingConnector struct {
	db *mongo.Database

	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingConnector) ResolveDatabase(ctx context.Context, _ string) (*mongo.Database, error) {
	c.once.Do(func() { close(c.entered) })

	select {
	case <-c.release:
		return c.db, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close must not be able to leave a live feed, a live goroutine or an open
// change stream behind when it lands while a tenant feed is still being
// created. Walking the feeds map is not enough on its own: the entry Close
// finds there is a reserved slot with nothing to stop yet, so the creator
// itself has to notice the shutdown after it opens the stream and throw it
// away.
func TestIntegration_MongoCloseDuringFeedCreationLeavesNothingRunning(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	db := client.Database(fmt.Sprintf("close_race_%d", time.Now().UnixNano()))
	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	conn := &blockingConnector{
		db:      db,
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
}

// A change stream needs a replica set, so a standalone MongoDB fails every
// Watch deterministically — no proxy, no mock, no production seam. The failure
// must reach the caller that asked for the feed and leave nothing behind: for a
// named tenant that caller is Subscribe, for the zero scope it is Start, which
// is where the single-tenant stream is opened.
func TestIntegration_MongoSubscribeReturnsErrorWhenWatchFails(t *testing.T) {
	client, cleanup := mongodb.StartStandaloneContainer(t)
	t.Cleanup(cleanup)

	t.Run("named tenant fails every concurrent caller", func(t *testing.T) {
		conn := newFakeConnector()
		tenantDB(t, client, conn, "t1", "watchfail")

		s := tenantStore(t, conn)
		scope := store.Scope{Tenant: "t1"}

		for i, r := range subscribeBurst(t, s, scope, 8) {
			if r.err == nil {
				r.unsub()
				t.Fatalf("Subscribe %d opened a change stream on a standalone MongoDB", i)
			}

			if !strings.Contains(r.err.Error(), "watch tenant t1") {
				t.Errorf("Subscribe %d error = %v, want the tenant's Watch failure", i, r.err)
			}
		}

		if total, _ := s.FeedsSnapshot(scope.Tenant); total != 0 {
			t.Fatalf("a failed Watch left %d feeds behind, want 0", total)
		}
	})

	t.Run("zero scope fails Start", func(t *testing.T) {
		dbName := fmt.Sprintf("watchfail_zero_%d", time.Now().UnixNano())
		t.Cleanup(func() { _ = client.Database(dbName).Drop(context.Background()) })

		s, err := mongodb.New(mongodb.Config{Client: client, Database: dbName})
		if err != nil {
			t.Fatalf("mongodb.New: %v", err)
		}

		t.Cleanup(func() { _ = s.Close() })

		err = s.Start(context.Background())
		if err == nil {
			t.Fatal("Start opened a change stream on a standalone MongoDB")
		}

		if !strings.Contains(err.Error(), "watch") {
			t.Fatalf("Start error = %v, want the Watch failure", err)
		}
	})
}
