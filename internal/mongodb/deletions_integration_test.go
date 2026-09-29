//go:build integration

package mongodb_test

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-systemplane/v4/internal/mongodb"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
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

// TestIntegration_DeletionHistoryRepairsAMissingRecordOnRetry pins the
// non-atomic half of the MongoDB recording: the tombstone lands, the record
// insert fails and Delete reports it. The retry finds no live value, reads the
// tombstone back and writes the record it was missing, with the ORIGINAL
// actor — so a failed record is recorded late rather than lost, and the
// retry's own actor is not credited with a delete it did not make.
func TestIntegration_DeletionHistoryRepairsAMissingRecordOnRetry(t *testing.T) {
	client, cleanup := startContainer(t)
	t.Cleanup(cleanup)

	conn := newFakeConnector()
	db := tenantDB(t, client, conn, "t1", "del_repair")
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

	tomb := readRaw(t, db.Collection("systemplane_entries"), "ns", "k")
	if !tomb.Deleted || tomb.UpdatedBy != "alice" {
		t.Fatalf("after the failed record: tombstone = %+v, want deleted by alice", tomb)
	}

	if got, err := s.ListDeletions(ctx, scope, "ns", "k", 10); err != nil || len(got) != 0 {
		t.Fatalf("history before the retry = (%+v, %v), want none", got, err)
	}

	setDeletionsValidator(t, db, bson.D{})

	if err := s.Delete(ctx, scope, "ns", "k", "bob"); err != nil {
		t.Fatalf("retry: %v", err)
	}

	got, err := s.ListDeletions(ctx, scope, "ns", "k", 10)
	if err != nil {
		t.Fatalf("list deletions: %v", err)
	}

	if len(got) != 1 || got[0].DeletedBy != "alice" || got[0].Revision != tomb.Revision {
		t.Fatalf("history after the retry = %+v, want one record by alice at the tombstone's revision %d", got, tomb.Revision)
	}

	// A further repeat finds the record already written and adds nothing.
	if err := s.Delete(ctx, scope, "ns", "k", "carol"); err != nil {
		t.Fatalf("repeat: %v", err)
	}

	if again, err := s.ListDeletions(ctx, scope, "ns", "k", 10); err != nil || len(again) != 1 {
		t.Fatalf("history after a repeat = (%+v, %v), want still one record", again, err)
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
