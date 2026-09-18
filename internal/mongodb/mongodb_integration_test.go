//go:build integration

package mongodb_test

import (
	"context"
	"encoding/json"
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

	return client, cleanup
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
// know, and an unknown tenant fails the way the manager would.
type fakeConnector struct {
	mu  sync.Mutex
	dbs map[string]*mongo.Database
}

func newFakeConnector() *fakeConnector {
	return &fakeConnector{dbs: map[string]*mongo.Database{}}
}

func (c *fakeConnector) set(tenantID string, db *mongo.Database) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.dbs[tenantID] = db
}

func (c *fakeConnector) ResolveDatabase(_ context.Context, tenantID string) (*mongo.Database, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

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
