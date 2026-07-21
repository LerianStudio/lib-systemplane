//go:build integration

package mongodb_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v6/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-systemplane/v2/internal/mongodb"
	"github.com/LerianStudio/lib-systemplane/v2/internal/store"
	"github.com/LerianStudio/lib-systemplane/v2/systemplanetest"
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

	systemplanetest.Run(t, factory, systemplanetest.RunOptions{EventWait: 5 * time.Second})
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

	// Subscribe is not supported in multi-tenant mode.
	if _, err := s.Subscribe(ctxA, func(_ store.Event) {}); err != store.ErrNotSupportedInMultiTenant {
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

	_, _, err = s.Get(context.Background(), "ns", "k")
	if err != store.ErrTenantConnectionMissing {
		t.Errorf("expected ErrTenantConnectionMissing, got %v", err)
	}
}

func mustSet(t *testing.T, s store.Store, ctx context.Context, ns, key, value string) {
	t.Helper()

	raw, _ := json.Marshal(value)
	if err := s.Set(ctx, store.Entry{Namespace: ns, Key: key, Value: raw}); err != nil {
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
