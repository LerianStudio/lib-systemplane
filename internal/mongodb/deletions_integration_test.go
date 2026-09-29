//go:build integration

package mongodb_test

import (
	"context"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-systemplane/v4/internal/mongodb"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"github.com/testcontainers/testcontainers-go"
	mongocontainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// recordingTenantStore is tenantStore with RecordDeletions on.
func recordingTenantStore(t *testing.T, conn mongodb.Connector) *mongodb.Store {
	t.Helper()

	s, err := mongodb.New(mongodb.Config{MultiTenantEnabled: true, Connector: conn, RecordDeletions: true})
	if err != nil {
		t.Fatalf("mongodb.New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	return s
}

// setDeletionsValidator installs validator on the deletion history collection,
// which the recording store's bootstrap already created with its index.
func setDeletionsValidator(t *testing.T, db *mongo.Database, validator bson.D) {
	t.Helper()

	cmd := bson.D{
		{Key: "collMod", Value: "systemplane_deletions"},
		{Key: "validator", Value: validator},
		{Key: "validationLevel", Value: "strict"},
		{Key: "validationAction", Value: "error"},
	}

	if err := db.RunCommand(context.Background(), cmd).Err(); err != nil {
		t.Fatalf("collMod systemplane_deletions: %v", err)
	}
}

// TestIntegration_DeletionHistoryFailedRecordKeepsTheValue pins the atomic
// recording: the tombstone and its record are one transaction, so a record
// that cannot be written fails Delete with the value still live, exactly as
// the Postgres table missing does. The case the non-atomic write lost is here
// too: a Set lands between the failed Delete and its retry, and the retry
// records its own delete of the new value — no delete ever goes unrecorded,
// and none is credited to an actor that did not make it.
func TestIntegration_DeletionHistoryFailedRecordKeepsTheValue(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	conn := newFakeConnector()
	db := tenantDB(t, client, conn, "t1", "del_atomic")
	s := recordingTenantStore(t, conn)
	ctx := context.Background()
	scope := store.Scope{Tenant: "t1"}

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`"v"`), UpdatedBy: "writer"}); err != nil {
		t.Fatalf("set: %v", err)
	}

	// No deletion record can satisfy this: deleted_by is always a string.
	setDeletionsValidator(t, db, bson.D{{Key: "deleted_by", Value: bson.D{{Key: "$type", Value: "int"}}}})

	if err := s.Delete(ctx, scope, "ns", "k", "alice"); err == nil {
		t.Fatal("Delete whose record insert fails succeeded; want the failure reported")
	}

	if got, found, err := s.Get(ctx, scope, "ns", "k"); err != nil || !found || string(got.Value) != `"v"` {
		t.Fatalf("after the failed delete: Get = (%+v, %v, %v), want the value still live", got, found, err)
	}

	if raw := readRaw(t, db.Collection("systemplane_entries"), "ns", "k"); raw.Deleted {
		t.Fatalf("after the failed delete: document = %+v, want no tombstone", raw)
	}

	// A write lands before the retry.
	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`"v2"`), UpdatedBy: "writer"}); err != nil {
		t.Fatalf("set between: %v", err)
	}

	setDeletionsValidator(t, db, bson.D{})

	if err := s.Delete(ctx, scope, "ns", "k", "bob"); err != nil {
		t.Fatalf("retry: %v", err)
	}

	tomb := readRaw(t, db.Collection("systemplane_entries"), "ns", "k")

	got, err := s.ListDeletions(ctx, scope, "ns", "k", 10)
	if err != nil {
		t.Fatalf("list deletions: %v", err)
	}

	if len(got) != 1 || got[0].DeletedBy != "bob" || got[0].Revision != tomb.Revision {
		t.Fatalf("history = %+v, want one record by bob at the tombstone's revision %d", got, tomb.Revision)
	}
}

// TestIntegration_DeletionHistoryIgnoresAnUnrecordedTombstone pins that a
// repeat delete records nothing on MongoDB, as on Postgres, even when the
// tombstone it finds was written without the history on: the record is taken
// in the same transaction as the tombstone, never back-filled from one.
func TestIntegration_DeletionHistoryIgnoresAnUnrecordedTombstone(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	conn := newFakeConnector()
	tenantDB(t, client, conn, "t1", "del_old_tomb")
	ctx := context.Background()
	scope := store.Scope{Tenant: "t1"}

	plain := tenantStore(t, conn)

	if _, err := plain.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`)}); err != nil {
		t.Fatalf("set: %v", err)
	}

	if err := plain.Delete(ctx, scope, "ns", "k", "before-opt-in"); err != nil {
		t.Fatalf("delete without the history: %v", err)
	}

	s := recordingTenantStore(t, conn)

	if err := s.Delete(ctx, scope, "ns", "k", "mallory"); err != nil {
		t.Fatalf("repeat delete with the history: %v", err)
	}

	if got, err := s.ListDeletions(ctx, scope, "ns", "k", 10); err != nil || len(got) != 0 {
		t.Fatalf("history = (%+v, %v), want none: the repeat removed nothing", got, err)
	}
}

// TestIntegration_DeletionHistoryNeedsTransactions pins the standalone
// refusal: a server with no replica set cannot run the transaction the record
// rides in, so Delete fails, names the requirement and leaves the value live.
func TestIntegration_DeletionHistoryNeedsTransactions(t *testing.T) {
	ctx := context.Background()

	container, err := mongocontainer.Run(ctx, "mongo:7")
	if err != nil {
		t.Fatalf("start standalone container: %v", err)
	}

	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("mongo connect: %v", err)
	}

	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	conn := newFakeConnector()
	db := tenantDB(t, client, conn, "t1", "del_standalone")
	s := recordingTenantStore(t, conn)
	scope := store.Scope{Tenant: "t1"}

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`)}); err != nil {
		t.Fatalf("set: %v", err)
	}

	err = s.Delete(ctx, scope, "ns", "k", "alice")
	if err == nil || !strings.Contains(err.Error(), "replica set") {
		t.Fatalf("Delete on a standalone = %v, want a refusal naming the replica set requirement", err)
	}

	if raw := readRaw(t, db.Collection("systemplane_entries"), "ns", "k"); raw.Deleted {
		t.Fatalf("after the refused delete: document = %+v, want the value still live", raw)
	}
}

// TestIntegration_DeletionHistoryIsTenantScoped pins that a history lives in
// its tenant's own database: a delete in one tenant is invisible to another.
func TestIntegration_DeletionHistoryIsTenantScoped(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	conn := newFakeConnector()
	tenantDB(t, client, conn, "a", "del_ta")
	tenantDB(t, client, conn, "b", "del_tb")

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
	if err != nil || len(gotA) != 1 || gotA[0].DeletedBy != "alice" {
		t.Fatalf("history in a = (%+v, %v), want one record by alice", gotA, err)
	}

	gotB, err := s.ListDeletions(ctx, store.Scope{Tenant: "b"}, "ns", "k", 10)
	if err != nil || len(gotB) != 0 {
		t.Fatalf("history in b = (%+v, %v), want none", gotB, err)
	}
}
