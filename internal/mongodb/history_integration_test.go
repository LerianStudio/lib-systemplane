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

// recordingTenantStore is tenantStore with RecordChanges on.
func recordingTenantStore(t *testing.T, conn mongodb.Connector) *mongodb.Store {
	t.Helper()

	s, err := mongodb.New(mongodb.Config{MultiTenantEnabled: true, Connector: conn, RecordChanges: true})
	if err != nil {
		t.Fatalf("mongodb.New: %v", err)
	}

	t.Cleanup(func() { _ = s.Close() })

	return s
}

// setHistoryValidator installs validator on the change history collection,
// which the recording store's bootstrap already created with its index.
func setHistoryValidator(t *testing.T, db *mongo.Database, validator bson.D) {
	t.Helper()

	cmd := bson.D{
		{Key: "collMod", Value: "systemplane_history"},
		{Key: "validator", Value: validator},
		{Key: "validationLevel", Value: "strict"},
		{Key: "validationAction", Value: "error"},
	}

	if err := db.RunCommand(context.Background(), cmd).Err(); err != nil {
		t.Fatalf("collMod systemplane_history: %v", err)
	}
}

// TestIntegration_ChangeHistoryFailedRecordKeepsTheValue pins the atomic
// recording: every write and its record are one transaction, so a record that
// cannot be written fails Set and Delete with the stored value untouched,
// exactly as the Postgres table missing does. Once records can be written
// again, the retry records its own change and nothing the failed attempts did.
func TestIntegration_ChangeHistoryFailedRecordKeepsTheValue(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	conn := newFakeConnector()
	db := tenantDB(t, client, conn, "t1", "hist_atomic")
	s := recordingTenantStore(t, conn)
	ctx := context.Background()
	scope := store.Scope{Tenant: "t1"}

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`"v"`), UpdatedBy: "writer"}); err != nil {
		t.Fatalf("set: %v", err)
	}

	// No change record can satisfy this: changed_by is always a string.
	setHistoryValidator(t, db, bson.D{{Key: "changed_by", Value: bson.D{{Key: "$type", Value: "int"}}}})

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`"v2"`), UpdatedBy: "alice"}); err == nil {
		t.Fatal("Set whose record insert fails succeeded; want the failure reported")
	}

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "fresh", Value: []byte(`1`), UpdatedBy: "alice"}); err == nil {
		t.Fatal("creating Set whose record insert fails succeeded; want the failure reported")
	}

	if err := s.Delete(ctx, scope, "ns", "k", "alice"); err == nil {
		t.Fatal("Delete whose record insert fails succeeded; want the failure reported")
	}

	if got, found, err := s.Get(ctx, scope, "ns", "k"); err != nil || !found || string(got.Value) != `"v"` || got.UpdatedBy != "writer" {
		t.Fatalf("after the failed writes: Get = (%+v, %v, %v), want the value writer stored", got, found, err)
	}

	if raw := readRaw(t, db.Collection("systemplane_entries"), "ns", "k"); raw.Deleted {
		t.Fatalf("after the failed delete: document = %+v, want no tombstone", raw)
	}

	if _, found, err := s.Get(ctx, scope, "ns", "fresh"); err != nil || found {
		t.Fatalf("after the failed create: found = %v, err = %v; want no value", found, err)
	}

	setHistoryValidator(t, db, bson.D{})

	if err := s.Delete(ctx, scope, "ns", "k", "bob"); err != nil {
		t.Fatalf("retry: %v", err)
	}

	tomb := readRaw(t, db.Collection("systemplane_entries"), "ns", "k")

	got, err := s.ListHistory(ctx, scope, "ns", "k", 10)
	if err != nil {
		t.Fatalf("list history: %v", err)
	}

	if len(got) != 2 || got[0].Operation != store.ChangeDelete || got[0].ChangedBy != "bob" || got[0].Revision != tomb.Revision ||
		string(got[0].PreviousValue) != `"v"` || got[1].Operation != store.ChangeCreate || got[1].ChangedBy != "writer" {
		t.Fatalf("history = %+v, want bob's delete of \"v\" at the tombstone's revision %d after writer's create", got, tomb.Revision)
	}
}

// TestIntegration_ChangeHistoryIgnoresAnUnrecordedTombstone pins that a repeat
// delete records nothing on MongoDB, as on Postgres, even when the tombstone it
// finds was written without the history on: a record is taken in the same
// transaction as its write, never back-filled from one. A Set over that
// tombstone records a create from nothing.
func TestIntegration_ChangeHistoryIgnoresAnUnrecordedTombstone(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	conn := newFakeConnector()
	tenantDB(t, client, conn, "t1", "hist_old_tomb")
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

	if got, err := s.ListHistory(ctx, scope, "ns", "k", 10); err != nil || len(got) != 0 {
		t.Fatalf("history = (%+v, %v), want none: the repeat removed nothing", got, err)
	}

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`2`), UpdatedBy: "carol"}); err != nil {
		t.Fatalf("set over the tombstone: %v", err)
	}

	got, err := s.ListHistory(ctx, scope, "ns", "k", 10)
	if err != nil || len(got) != 1 || got[0].Operation != store.ChangeCreate || got[0].PreviousValue != nil || string(got[0].Value) != `2` {
		t.Fatalf("history = (%+v, %v), want one create of 2 from nothing", got, err)
	}
}

// TestIntegration_ChangeHistoryNeedsTransactions pins the standalone refusal:
// a server with no replica set cannot run the transaction a record rides in,
// so Set and Delete fail, name the requirement and leave the value as it was.
func TestIntegration_ChangeHistoryNeedsTransactions(t *testing.T) {
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
	db := tenantDB(t, client, conn, "t1", "hist_standalone")
	plain := tenantStore(t, conn)
	s := recordingTenantStore(t, conn)
	scope := store.Scope{Tenant: "t1"}

	if _, err := plain.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`)}); err != nil {
		t.Fatalf("set without the history: %v", err)
	}

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`2`)}); err == nil || !strings.Contains(err.Error(), "replica set") {
		t.Fatalf("Set on a standalone = %v, want a refusal naming the replica set requirement", err)
	}

	if got, found, err := s.Get(ctx, scope, "ns", "k"); err != nil || !found || string(got.Value) != `1` {
		t.Fatalf("after the refused set: Get = (%+v, %v, %v), want the value 1", got, found, err)
	}

	err = s.Delete(ctx, scope, "ns", "k", "alice")
	if err == nil || !strings.Contains(err.Error(), "replica set") {
		t.Fatalf("Delete on a standalone = %v, want a refusal naming the replica set requirement", err)
	}

	if raw := readRaw(t, db.Collection("systemplane_entries"), "ns", "k"); raw.Deleted {
		t.Fatalf("after the refused delete: document = %+v, want the value still live", raw)
	}
}

// TestIntegration_ChangeHistoryIsTenantScoped pins that a history lives in its
// tenant's own database: a write in one tenant is invisible to another.
func TestIntegration_ChangeHistoryIsTenantScoped(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	conn := newFakeConnector()
	tenantDB(t, client, conn, "a", "hist_ta")
	tenantDB(t, client, conn, "b", "hist_tb")

	s := recordingTenantStore(t, conn)
	ctx := context.Background()

	if _, err := s.Set(ctx, store.Scope{Tenant: "a"}, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`), UpdatedBy: "alice"}); err != nil {
		t.Fatalf("set in a: %v", err)
	}

	if err := s.Delete(ctx, store.Scope{Tenant: "a"}, "ns", "k", "alice"); err != nil {
		t.Fatalf("delete in a: %v", err)
	}

	gotA, err := s.ListHistory(ctx, store.Scope{Tenant: "a"}, "ns", "k", 10)
	if err != nil || len(gotA) != 2 || gotA[0].ChangedBy != "alice" {
		t.Fatalf("history in a = (%+v, %v), want alice's create and delete", gotA, err)
	}

	gotB, err := s.ListHistory(ctx, store.Scope{Tenant: "b"}, "ns", "k", 10)
	if err != nil || len(gotB) != 0 {
		t.Fatalf("history in b = (%+v, %v), want none", gotB, err)
	}
}
