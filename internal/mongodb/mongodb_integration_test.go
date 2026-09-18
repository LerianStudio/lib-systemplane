//go:build integration

package mongodb_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
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

	client, _, cleanup := startContainerAt(t)

	return client, cleanup
}

// startContainerAt is startContainer plus the container's own host:port. A test
// that severs the store's connection needs the real address to forward to,
// because the store reaches MongoDB through a proxy it can take down and the
// gap write must not.
func startContainerAt(t *testing.T) (*mongo.Client, string, func()) {
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

	parsed, err := url.Parse(uri)
	if err != nil {
		cleanup()

		t.Fatalf("parse connection string %q: %v", uri, err)
	}

	return client, parsed.Host, cleanup
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
		EventWait:             5 * time.Second,
		SkipRevisionAndResync: false,
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

// setEntry writes one entry through the store and returns its revision.
func setEntry(t *testing.T, s store.Store, namespace, key, value string) int64 {
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

	r1 := setEntry(t, s, "ns", "k", `{"a":1}`)

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

	r1 := setEntry(t, s, "ns", "k", `{"a":1}`)

	if err := s.Delete(ctx, store.Scope{}, "ns", "k", "deleter"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	tombstone := readRaw(t, coll, "ns", "k").Revision
	if tombstone <= r1 {
		t.Fatalf("tombstone revision = %d, want > %d", tombstone, r1)
	}

	r2 := setEntry(t, s, "ns", "k", `{"a":2}`)
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

	setEntry(t, s, "ns", "k", `{"a":1}`)

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
func subscribeScope(t *testing.T, s store.Store, scope store.Scope) (<-chan store.Event, func()) {
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

// collectUntil drains events until pred accepts one, returning every event seen
// including it. On timeout it fails the test printing the whole sequence, which
// is the only useful thing to look at when an outage narrates the wrong order.
func collectUntil(t *testing.T, events <-chan store.Event, what string, timeout time.Duration, pred func(store.Event) bool) []store.Event {
	t.Helper()

	var seen []store.Event

	deadline := time.After(timeout)

	for {
		select {
		case evt := <-events:
			seen = append(seen, evt)

			if pred(evt) {
				return seen
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s; sequence = %#v", what, seen)

			return seen
		}
	}
}

// drainKeyEvents collects every event for namespace/key until the feed has been
// quiet for quiet, or limit elapses. The quiet timer only starts once something
// has arrived, so a slow first delivery cannot end the drain early.
//
// FC-9 pins only the FINAL state a key converges to, never the intermediate
// sequence, so a test judging a collapse has to look at the whole settled run
// rather than at the next event.
func drainKeyEvents(events <-chan store.Event, namespace, key string, quiet, limit time.Duration) []store.Event {
	var seen []store.Event

	hard := time.After(limit)

	for {
		var settled <-chan time.Time
		if len(seen) > 0 {
			settled = time.After(quiet)
		}

		select {
		case evt := <-events:
			if evt.Namespace == namespace && evt.Key == key {
				seen = append(seen, evt)
			}
		case <-settled:
			return seen
		case <-hard:
			return seen
		}
	}
}

func countOps(events []store.Event, op string) int {
	n := 0

	for _, evt := range events {
		if evt.Op == op {
			n++
		}
	}

	return n
}

// startFeed builds a single-tenant store on its own database, starts it and
// subscribes, returning the events channel with the joining OpResync already
// consumed. Every changefeed test opens this way.
func startFeed(t *testing.T, client *mongo.Client, prefix string) (store.Store, <-chan store.Event) {
	t.Helper()

	s, _ := freshSingleTenantStore(t, client, prefix)

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	events, unsub := subscribeScope(t, s, store.Scope{})
	t.Cleanup(unsub)

	if evt := recvEvent(t, events, "the joining resync"); evt.Op != store.OpResync {
		t.Fatalf("first event after Subscribe = %#v, want %q", evt, store.OpResync)
	}

	return s, events
}

// TestIntegration_MongoEventCarriesRevision pins FC-2 on the changefeed: an
// upsert event carries the revision the write reported. Without it every event
// would arrive at revision 0 — "unknown" — and the engine would re-read and
// republish on every echo of its own Set instead of deduplicating it.
func TestIntegration_MongoEventCarriesRevision(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	s, events := startFeed(t, client, "eventrev")

	rev := setEntry(t, s, "ns", "k", `{"a":1}`)

	evt := recvEvent(t, events, "the upsert for ns/k")
	if evt.Op != store.OpUpsert || evt.Namespace != "ns" || evt.Key != "k" {
		t.Fatalf("event after Set = %#v, want an upsert for ns/k", evt)
	}

	if evt.Revision != rev {
		t.Fatalf("upsert event revision = %d, want %d — the revision Set reported", evt.Revision, rev)
	}

	if evt.Scope != (store.Scope{}) {
		t.Fatalf("upsert event scope = %#v, want the zero scope the subscription named", evt.Scope)
	}
}

// TestIntegration_MongoTombstoneEventIsADelete pins the other half of FC-9's
// classification: Delete writes a tombstone through an UPDATE, so the operation
// type would report an upsert and publish a deleted key as if it still had a
// value. The after-image decides instead, and the event is indistinguishable
// from the one a foreign writer's raw delete produces.
//
// The repeat Delete at the end is the assertion Task 2.2.2's filter buys and
// this is the first test with a subscriber to make it: an existing tombstone
// matches nothing, so nothing is written and nothing reaches the change stream.
// A write there would land as a second OpDelete at revision 0, which nothing
// deduplicates.
func TestIntegration_MongoTombstoneEventIsADelete(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	s, events := startFeed(t, client, "tombevent")
	ctx := context.Background()

	setEntry(t, s, "ns", "k", `{"a":1}`)

	if evt := recvEvent(t, events, "the upsert that proves the stream is attached"); evt.Op != store.OpUpsert {
		t.Fatalf("event after Set = %#v, want an upsert", evt)
	}

	if err := s.Delete(ctx, store.Scope{}, "ns", "k", "actor"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	evt := recvEvent(t, events, "the delete for ns/k")
	if evt.Op != store.OpDelete || evt.Namespace != "ns" || evt.Key != "k" || evt.Revision != 0 {
		t.Fatalf("tombstone event = %#v, want OpDelete for ns/k at revision 0", evt)
	}

	if err := s.Delete(ctx, store.Scope{}, "ns", "k", "actor"); err != nil {
		t.Fatalf("second Delete: %v", err)
	}

	assertNoEvent(t, events, 5*time.Second, "a repeat Delete of an existing tombstone")
}

// TestIntegration_MongoDeleteThenRecreateConvergesToLiveValue pins the
// observation model FC-9 states outright: classification reads the document as
// updateLookup returns it at PROCESSING time — the current majority-committed
// document, not a point-in-time image — so a delete and a recreate landing
// inside one lookup window collapse into a single upsert at the recreate's
// revision. The store contract on both backends is final-state convergence, the
// same model Postgres has where NOTIFY carries no value and the engine re-reads
// the row.
//
// The test therefore judges only the LAST event for the key. Asserting that the
// intermediate OpDelete was observed would encode a guarantee the contract does
// not make, and would flake exactly when the collapse happens.
func TestIntegration_MongoDeleteThenRecreateConvergesToLiveValue(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	s, events := startFeed(t, client, "converge")
	ctx := context.Background()

	setEntry(t, s, "ns", "k", `{"a":1}`)

	if evt := recvEvent(t, events, "the upsert that proves the stream is attached"); evt.Op != store.OpUpsert {
		t.Fatalf("event after Set = %#v, want an upsert", evt)
	}

	if err := s.Delete(ctx, store.Scope{}, "ns", "k", "actor"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	rev := setEntry(t, s, "ns", "k", `{"a":2}`)

	seen := drainKeyEvents(events, "ns", "k", 3*time.Second, 25*time.Second)
	if len(seen) == 0 {
		t.Fatal("the delete-then-recreate produced no event at all for ns/k")
	}

	last := seen[len(seen)-1]
	if last.Op != store.OpUpsert || last.Revision != rev {
		t.Fatalf("last event for ns/k = %#v, want an upsert at revision %d; sequence = %#v", last, rev, seen)
	}

	entry, found, err := s.Get(ctx, store.Scope{}, "ns", "k")
	if err != nil || !found {
		t.Fatalf("Get after recreate = (%#v, %v, %v), want the recreated entry", entry, found, err)
	}

	if entry.Revision != rev || string(entry.Value) != `{"a":2}` {
		t.Fatalf("Get after recreate = value %q at revision %d, want %q at %d", entry.Value, entry.Revision, `{"a":2}`, rev)
	}
}

// tcpProxy forwards a local port to the container's MongoDB port so a test can
// sever the store's connection and restore it on the same address.
//
// This is how an outage is produced deterministically. killCursors would also
// work — a CursorKilled carries no ResumableChangeStreamError label, so the
// driver does not resume it transparently — but obtaining the live cursor id
// means exposing the feed's *mongo.ChangeStream from production code purely for
// a test. Dropping the container is worse: a plain network error IS resumable,
// the driver resumes it internally, and a short outage would announce nothing
// at all. The same hazard applies here, which is why the store's client is
// built with a 2s server-selection bound and every outage below outlasts it.
type tcpProxy struct {
	target string
	addr   string

	mu      sync.Mutex
	ln      net.Listener
	conns   []net.Conn
	severed bool

	wg sync.WaitGroup
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for proxy: %v", err)
	}

	p := &tcpProxy{target: target, addr: ln.Addr().String()}
	p.serve(ln)

	t.Cleanup(p.sever)

	return p
}

func (p *tcpProxy) serve(ln net.Listener) {
	p.mu.Lock()
	p.ln = ln
	p.severed = false
	p.mu.Unlock()

	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			p.handle(conn)
		}
	}()
}

// handle wires one accepted connection to the target. The severed check and the
// append happen in ONE hold: a connection admitted after sever snapshotted the
// list would otherwise keep its io.Copy goroutines alive forever, and the
// package's goleak guard would fail the whole suite over it.
func (p *tcpProxy) handle(client net.Conn) {
	upstream, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()

		return
	}

	p.mu.Lock()

	if p.severed {
		p.mu.Unlock()

		_ = client.Close()
		_ = upstream.Close()

		return
	}

	p.conns = append(p.conns, client, upstream)
	p.mu.Unlock()

	p.pipe(client, upstream)
	p.pipe(upstream, client)
}

func (p *tcpProxy) pipe(dst, src net.Conn) {
	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		_, _ = io.Copy(dst, src)

		_ = dst.Close()
		_ = src.Close()
	}()
}

// sever takes the proxy down: no new connections, and every live one dropped.
// It waits for the forwarding goroutines so a severed proxy leaves nothing
// running, and is safe to call twice — t.Cleanup always calls it once more.
func (p *tcpProxy) sever() {
	p.mu.Lock()
	p.severed = true
	ln, conns := p.ln, p.conns
	p.ln, p.conns = nil, nil
	p.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}

	for _, conn := range conns {
		_ = conn.Close()
	}

	p.wg.Wait()
}

// restore brings the proxy back on the SAME address, which is what lets the
// store's own client reconnect without knowing anything happened.
func (p *tcpProxy) restore(t *testing.T) {
	t.Helper()

	ln, err := net.Listen("tcp", p.addr)
	if err != nil {
		t.Fatalf("restore proxy on %s: %v", p.addr, err)
	}

	p.serve(ln)
}

// proxiedStore builds a store whose every connection travels through proxy. The
// short server-selection bound is what makes the failure fast and the outage
// deterministic: without it a severed feed would sit in selection for 30s and
// the test would be timing out rather than observing anything.
// A positive pollInterval selects the polling fallback instead of a change
// stream; zero leaves the store on change streams.
func proxiedStore(t *testing.T, proxy *tcpProxy, prefix string, pollInterval time.Duration) (store.Store, string) {
	t.Helper()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + proxy.addr).
		SetDirect(true).
		SetServerSelectionTimeout(2 * time.Second))
	if err != nil {
		t.Fatalf("connect through proxy: %v", err)
	}

	dbName := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())

	s, err := mongodb.New(mongodb.Config{Client: client, Database: dbName, PollInterval: pollInterval})
	if err != nil {
		t.Fatalf("mongodb.New: %v", err)
	}

	t.Cleanup(func() {
		_ = s.Close()
		_ = client.Disconnect(context.Background())
	})

	return s, dbName
}

// outageHarness stands up the whole severable setup: a store that reaches
// MongoDB only through the proxy, already started and subscribed with its
// joining OpResync consumed, plus a second store wired DIRECTLY to the
// container for the write that has to land while the feed is blind.
func outageHarness(t *testing.T, prefix string) (proxied store.Store, direct store.Store, events <-chan store.Event, proxy *tcpProxy) {
	t.Helper()

	client, endpoint, cleanup := startContainerAt(t)
	t.Cleanup(cleanup)

	proxy = newTCPProxy(t, endpoint)

	proxied, dbName := proxiedStore(t, proxy, prefix, 0)

	direct, err := mongodb.New(mongodb.Config{Client: client, Database: dbName})
	if err != nil {
		t.Fatalf("mongodb.New direct: %v", err)
	}

	t.Cleanup(func() { _ = direct.Close() })

	if err := proxied.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	eventsCh, unsub := subscribeScope(t, proxied, store.Scope{})
	t.Cleanup(unsub)

	if evt := recvEvent(t, eventsCh, "the joining resync"); evt.Op != store.OpResync {
		t.Fatalf("first event after Subscribe = %#v, want %q", evt, store.OpResync)
	}

	setEntry(t, proxied, "ns", "k", `{"a":1}`)

	if evt := recvEvent(t, eventsCh, "the upsert that proves the stream is attached"); evt.Op != store.OpUpsert {
		t.Fatalf("event after Set = %#v, want an upsert", evt)
	}

	return proxied, direct, eventsCh, proxy
}

// TestIntegration_MongoResyncAfterCursorKill pins the outage narration FC-2
// requires. One lost connection produces exactly one OpDisconnect, the
// reconnect exactly one OpResync, and in that order. The change stream is
// reopened with no resume token, so a write that landed while the feed was
// blind is never replayed as an event: OpResync is the ONLY thing that tells
// the engine to go and find it, and the Get at the end is what proves it is
// there to be found.
func TestIntegration_MongoResyncAfterCursorKill(t *testing.T) {
	proxied, direct, events, proxy := outageHarness(t, "resync")

	proxy.sever()

	// Through the severed proxy this write would never land at all.
	gapRev := setEntry(t, direct, "ns", "k", `{"a":2}`)

	// The outage must outlast the driver's own resume attempt, which the 2s
	// server-selection bound caps. A shorter one is resumed transparently and
	// announces nothing.
	time.Sleep(5 * time.Second)

	proxy.restore(t)

	narration := collectUntil(t, events, "the OpResync after the reconnect", 60*time.Second,
		func(evt store.Event) bool { return evt.Op == store.OpResync })

	if narration[0].Op != store.OpDisconnect {
		t.Fatalf("outage began with %#v, want OpDisconnect first; sequence = %#v", narration[0], narration)
	}

	if got := countOps(narration, store.OpDisconnect); got != 1 {
		t.Fatalf("outage narrated %d disconnects, want exactly 1; sequence = %#v", got, narration)
	}

	if got := countOps(narration, store.OpResync); got != 1 {
		t.Fatalf("outage narrated %d resyncs, want exactly 1; sequence = %#v", got, narration)
	}

	entry, found, err := proxied.Get(context.Background(), store.Scope{}, "ns", "k")
	if err != nil || !found {
		t.Fatalf("Get after the resync = (%#v, %v, %v), want the gap write", entry, found, err)
	}

	if entry.Revision != gapRev || string(entry.Value) != `{"a":2}` {
		t.Fatalf("Get after the resync = value %q at revision %d, want %q at %d — the write that landed while the feed was blind", entry.Value, entry.Revision, `{"a":2}`, gapRev)
	}
}

// TestIntegration_MongoRepeatedReopenFailuresEmitOneDisconnect is the same
// outage held open long enough for several reopen attempts to fail: the backoff
// starts at 500ms and each attempt burns the 2s server-selection bound, so the
// window below covers at least two of them. However many fail, the engine must
// hear ONE disconnect and ONE resync — a disconnect per failed attempt would
// have it marking a scope stale it already believes is stale, and a resync per
// attempt would have it reloading a scope that never came back.
func TestIntegration_MongoRepeatedReopenFailuresEmitOneDisconnect(t *testing.T) {
	_, _, events, proxy := outageHarness(t, "reopenfail")

	proxy.sever()
	time.Sleep(12 * time.Second)
	proxy.restore(t)

	narration := collectUntil(t, events, "the OpResync after a long outage", 90*time.Second,
		func(evt store.Event) bool { return evt.Op == store.OpResync })

	if narration[0].Op != store.OpDisconnect {
		t.Fatalf("outage began with %#v, want OpDisconnect first; sequence = %#v", narration[0], narration)
	}

	if got := countOps(narration, store.OpDisconnect); got != 1 {
		t.Fatalf("a long outage narrated %d disconnects, want exactly 1; sequence = %#v", got, narration)
	}

	if got := countOps(narration, store.OpResync); got != 1 {
		t.Fatalf("a long outage narrated %d resyncs, want exactly 1; sequence = %#v", got, narration)
	}

	// Nothing but the pair: a failed reopen attempt announces nothing of its own.
	if len(narration) != 2 {
		t.Fatalf("outage narrated %d events, want exactly the disconnect/resync pair; sequence = %#v", len(narration), narration)
	}
}

// TestIntegration_MongoPollingDisconnectAndResyncAroundFailedRound holds the
// polling fallback to the SAME narration contract the change stream has
// (FC-2): a round trip that cannot reach MongoDB announces exactly one
// OpDisconnect for the whole failure streak — not one per tick — and the first
// round trip that succeeds afterwards announces exactly one OpResync. A
// disconnect per failed tick would have the engine re-marking a scope it
// already believes is stale; a resync per SUCCESSFUL tick would have it
// reloading the scope forever.
func TestIntegration_MongoPollingDisconnectAndResyncAroundFailedRound(t *testing.T) {
	client, endpoint, cleanup := startContainerAt(t)
	t.Cleanup(cleanup)

	proxy := newTCPProxy(t, endpoint)

	s, dbName := proxiedStore(t, proxy, "pollfail", 200*time.Millisecond)

	// Wired straight to the container, so the write below lands in the
	// collection the polling feed cannot reach while the proxy is severed.
	direct, err := mongodb.New(mongodb.Config{Client: client, Database: dbName})
	if err != nil {
		t.Fatalf("mongodb.New direct: %v", err)
	}

	t.Cleanup(func() { _ = direct.Close() })

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	events, unsub := subscribeScope(t, s, store.Scope{})
	t.Cleanup(unsub)

	if evt := recvEvent(t, events, "the joining resync"); evt.Op != store.OpResync {
		t.Fatalf("first event after Subscribe = %#v, want %q", evt, store.OpResync)
	}

	proxy.sever()

	// Several round trips must fail inside this window: each one burns the 2s
	// server-selection bound on top of the 200ms tick.
	time.Sleep(6 * time.Second)

	// The write the feed is blind to. The round trip that finds it is the one
	// that ends the failure streak, which is what makes the order below a real
	// assertion rather than a vacuous one.
	setEntry(t, direct, "ns", "k", `{"a":1}`)

	proxy.restore(t)

	narration := collectUntil(t, events, "the upsert the recovering round trip found", 60*time.Second,
		func(evt store.Event) bool { return evt.Op == store.OpUpsert })

	if narration[0].Op != store.OpDisconnect {
		t.Fatalf("outage began with %#v, want OpDisconnect first; sequence = %#v", narration[0], narration)
	}

	// FC-2: the marker that says the feed is back precedes every key event the
	// recovering round trip read. A subscriber told about the key first would
	// apply it while it still believes the scope is stale.
	if narration[1].Op != store.OpResync {
		t.Fatalf("recovery announced %#v before the OpResync, want the resync first; sequence = %#v", narration[1], narration)
	}

	if got := countOps(narration, store.OpDisconnect); got != 1 {
		t.Fatalf("the failure streak narrated %d disconnects, want exactly 1; sequence = %#v", got, narration)
	}

	if got := countOps(narration, store.OpResync); got != 1 {
		t.Fatalf("the recovery narrated %d resyncs, want exactly 1; sequence = %#v", got, narration)
	}

	if len(narration) != 3 {
		t.Fatalf("the outage narrated %d events, want exactly disconnect, resync, upsert; sequence = %#v", len(narration), narration)
	}

	// A recovered feed goes quiet: the round trips that follow announce
	// nothing, because nothing happened to the collection.
	assertNoEvent(t, events, time.Second, "after the polling recovery")
}

// Two Starts landing together must open ONE change stream between them. Before
// the interlock, both callers read "no reader yet", both opened a cursor and
// the second publish overwrote the first reader's done channel: every document
// event and every marker was then delivered twice by two readers, two resyncs
// were announced on open, and Close waited on only one of them.
func TestIntegration_MongoConcurrentStartOpensOneStream(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	dbName := fmt.Sprintf("concurrentstart_%d", time.Now().UnixNano())

	s, err := mongodb.New(mongodb.Config{Client: client, Database: dbName})
	if err != nil {
		t.Fatalf("mongodb.New: %v", err)
	}

	t.Cleanup(func() {
		_ = s.Close()
		_ = client.Database(dbName).Drop(context.Background())
	})

	ctx := context.Background()

	var (
		mu     sync.Mutex
		events []store.Event
	)

	// Subscribed BEFORE Start, so the feed has announced nothing yet: every
	// marker this subscriber sees comes from a reader, and one reader means one
	// resync.
	unsub, err := s.Subscribe(ctx, store.Scope{}, func(evt store.Event) {
		mu.Lock()
		defer mu.Unlock()

		events = append(events, evt)
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	defer unsub()

	startTwice(t, s.Start)

	if total, _ := s.FeedsSnapshot(""); total != 1 {
		t.Fatalf("feeds after two concurrent Starts = %d, want exactly 1", total)
	}

	if _, err := s.Set(ctx, store.Scope{}, store.Entry{
		Namespace: "ns",
		Key:       "k",
		Value:     []byte(`{"a":1}`),
		UpdatedBy: "actor",
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	assertOneReaderDelivered(t, &mu, &events)
}

// startTwice runs two Starts from one released barrier and fails on either
// error. Both must report success: the loser of the interlock returns the
// winner's outcome, not a refusal.
func startTwice(t *testing.T, start func(context.Context) error) {
	t.Helper()

	var (
		barrier sync.WaitGroup
		done    sync.WaitGroup
	)

	barrier.Add(1)
	done.Add(2)

	for i := range 2 {
		go func() {
			defer done.Done()

			barrier.Wait()

			if err := start(context.Background()); err != nil {
				t.Errorf("concurrent Start %d: %v", i, err)
			}
		}()
	}

	barrier.Done()
	done.Wait()
}

// assertOneReaderDelivered waits for the upsert of key "k" and then asserts that
// exactly one reader produced the feed's events: one resync for the open, one
// copy of the write. A second reader doubles both.
func assertOneReaderDelivered(t *testing.T, mu *sync.Mutex, events *[]store.Event) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)

	for !sawUpsert(mu, events) {
		if time.Now().After(deadline) {
			t.Fatal("the write never reached the subscriber")
		}

		time.Sleep(20 * time.Millisecond)
	}

	// A second reader delivers its own copy within this window.
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	resyncs, upserts := 0, 0

	for _, evt := range *events {
		switch {
		case evt.Op == store.OpResync:
			resyncs++
		case evt.Op == store.OpUpsert && evt.Key == "k":
			upserts++
		}
	}

	if resyncs != 1 {
		t.Errorf("OpResync count = %d, want 1: a second reader announced its own open; events = %#v", resyncs, *events)
	}

	if upserts != 1 {
		t.Errorf("upsert count for one Set = %d, want 1: two readers delivered the same event; events = %#v", upserts, *events)
	}
}
