//go:build integration

package mongodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestIntegration_Mongo_Phase1_SetOverwritesLegacyObjectIDRow pins C1: a
// phase-1 Store (TenantSchemaEnabled=false) invoked against a collection
// that still carries the legacy ObjectId-keyed rows from a pre-tenant
// v5.0.x deploy MUST upsert in place rather than collide on the legacy
// unique index on (namespace, key). The symptom of a regression is an
// E11000 duplicate-key error thrown by the driver.
func TestIntegration_Mongo_Phase1_SetOverwritesLegacyObjectIDRow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)

	dbName := fmt.Sprintf("test_c1_%d", time.Now().UnixNano())
	collName := defaultCollection

	// Seed a legacy row directly via the driver so its _id is an ObjectId,
	// which is the shape every pre-tenant v5.0.x upsert produced.
	coll := client.Database(dbName).Collection(collName)
	legacyID := bson.NewObjectID()

	_, err := coll.InsertOne(context.Background(), bson.D{
		{Key: "_id", Value: legacyID},
		{Key: "namespace", Value: "legacy"},
		{Key: "key", Value: "fee.rate"},
		// tenant_id absent — phase-1 leaves legacy rows untouched.
		{Key: "value", Value: `"0.01"`},
		{Key: "updated_at", Value: time.Now().UTC()},
		{Key: "updated_by", Value: "seed"},
	})
	require.NoError(t, err, "seeding legacy row must succeed")

	// Build a phase-1 store. ensureLegacySchema creates the unique index on
	// (namespace, key) — this is the index the pre-tenant binaries write
	// against and the one a regression in upsert() would violate.
	s, err := New(Config{
		Client:              client,
		Database:            dbName,
		Collection:          collName,
		TenantSchemaEnabled: false,
	})
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close() })

	// A phase-1 Set with the same (namespace, key) MUST update the legacy
	// row in place and MUST NOT throw E11000.
	setErr := s.Set(context.Background(), store.Entry{
		Namespace: "legacy",
		Key:       "fee.rate",
		Value:     []byte(`"0.02"`),
		UpdatedBy: "phase1-writer",
	})
	require.NoError(t, setErr, "phase-1 Set against legacy ObjectId row must not collide on (namespace,key) unique index")

	// The Get surface is phase-1 aware only when tenant_id == "_global", so
	// we re-read via the driver to verify the value was actually updated in
	// place (not inserted as a second row).
	var got bson.M

	require.NoError(t,
		coll.FindOne(context.Background(), bson.D{
			{Key: "namespace", Value: "legacy"},
			{Key: "key", Value: "fee.rate"},
		}).Decode(&got),
	)

	assert.Equal(t, `"0.02"`, got["value"], "value must be overwritten")
	assert.Equal(t, "phase1-writer", got["updated_by"])

	// Exactly one row should exist for this (namespace, key).
	count, err := coll.CountDocuments(context.Background(), bson.D{
		{Key: "namespace", Value: "legacy"},
		{Key: "key", Value: "fee.rate"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), count, "phase-1 Set must not insert a duplicate row")
}

// TestIntegration_Mongo_Migration_FreshCollection pins the most common
// case: a brand-new deployment where the collection does not yet exist.
// ensureSchema must be a no-op on the unused collection and leave the
// compound unique index in place so subsequent writes land with the
// phase-2 shape.
func TestIntegration_Mongo_Migration_FreshCollection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)

	dbName := fmt.Sprintf("test_c4_fresh_%d", time.Now().UnixNano())

	s, err := New(Config{
		Client:              client,
		Database:            dbName,
		TenantSchemaEnabled: true,
	})
	require.NoError(t, err, "fresh collection must migrate cleanly")

	t.Cleanup(func() { _ = s.Close() })

	// Confirm the compound unique index is present.
	assertCompoundIndexExists(t, client, dbName, defaultCollection)
}

// TestIntegration_Mongo_Migration_BackfillsMissingTenantID seeds a legacy
// row that lacks the tenant_id field, then runs the migration and asserts
// the field is filled in with "_global". This is the step-3 half of
// ensureSchema.
func TestIntegration_Mongo_Migration_BackfillsMissingTenantID(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)

	dbName := fmt.Sprintf("test_c4_bf_%d", time.Now().UnixNano())
	coll := client.Database(dbName).Collection(defaultCollection)

	// Seed a legacy row missing tenant_id entirely (represents a document
	// inserted by a v5.0.x binary).
	_, err := coll.InsertOne(context.Background(), bson.D{
		{Key: "_id", Value: bson.NewObjectID()},
		{Key: "namespace", Value: "ns"},
		{Key: "key", Value: "k"},
		{Key: "value", Value: `"v"`},
		{Key: "updated_at", Value: time.Now().UTC()},
		{Key: "updated_by", Value: "seed"},
	})
	require.NoError(t, err)

	s, err := New(Config{
		Client:              client,
		Database:            dbName,
		TenantSchemaEnabled: true,
	})
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close() })

	// The row must now be present under the compound-id shape with
	// tenant_id="_global".
	entry, found, err := s.Get(context.Background(), "ns", "k")
	require.NoError(t, err)
	require.True(t, found, "backfilled row must be visible via phase-2 Get")
	assert.Equal(t, store.SentinelGlobal, entry.TenantID)
	assert.Equal(t, `"v"`, string(entry.Value))
}

func TestIntegration_Mongo_Migration_RewritesEmptyCompoundTenantID(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)

	dbName := fmt.Sprintf("test_c4_empty_compound_%d", time.Now().UnixNano())
	coll := client.Database(dbName).Collection(defaultCollection)

	_, err := coll.InsertOne(context.Background(), bson.D{
		{Key: "_id", Value: compoundID{Namespace: "ns", Key: "k", TenantID: ""}},
		{Key: "namespace", Value: "ns"},
		{Key: "key", Value: "k"},
		{Key: "tenant_id", Value: ""},
		{Key: "value", Value: `"v"`},
		{Key: "updated_at", Value: time.Now().UTC()},
		{Key: "updated_by", Value: "seed"},
	})
	require.NoError(t, err)

	s, err := New(Config{
		Client:              client,
		Database:            dbName,
		TenantSchemaEnabled: true,
	})
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close() })

	entry, found, err := s.Get(context.Background(), "ns", "k")
	require.NoError(t, err)
	require.True(t, found, "empty compound tenant row must be visible via phase-2 Get")
	assert.Equal(t, store.SentinelGlobal, entry.TenantID)
	assert.Equal(t, `"v"`, string(entry.Value))

	staleCount, err := coll.CountDocuments(context.Background(), bson.D{{Key: "_id.tenant_id", Value: ""}})
	require.NoError(t, err)
	assert.Equal(t, int64(0), staleCount, "migration must eliminate empty tenant_id compound _ids")
}

func TestIntegration_Mongo_Migration_CreatesPollingIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)
	dbName := fmt.Sprintf("test_c4_poll_idx_%d", time.Now().UnixNano())

	s, err := New(Config{Client: client, Database: dbName, TenantSchemaEnabled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	assertIndexExists(t, client, dbName, defaultCollection, pollingUpdatedAtIndex)
}

func TestIntegration_Mongo_Phase1_ListTenantOverridesExcludesLegacyGlobalRows(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)
	dbName := fmt.Sprintf("test_phase1_override_filter_%d", time.Now().UnixNano())
	coll := client.Database(dbName).Collection(defaultCollection)

	_, err := coll.InsertOne(context.Background(), bson.D{
		{Key: "_id", Value: bson.NewObjectID()},
		{Key: "namespace", Value: "legacy"},
		{Key: "key", Value: "fee.rate"},
		{Key: "value", Value: `"0.01"`},
		{Key: "updated_at", Value: time.Now().UTC()},
		{Key: "updated_by", Value: "seed"},
	})
	require.NoError(t, err)

	s, err := New(Config{Client: client, Database: dbName, TenantSchemaEnabled: false})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	overrides, err := s.ListTenantOverrides(context.Background(), "", "", "", 100)
	require.NoError(t, err)
	assert.Empty(t, overrides, "legacy global rows with missing tenant_id are not tenant overrides")
}

// TestIntegration_Mongo_Migration_ObjectIDRewrite seeds multiple legacy
// rows (more than one migration batch) to exercise the batched
// InsertMany/DeleteMany path (H4), then asserts every row survives with a
// compound _id and no duplicates remain.
func TestIntegration_Mongo_Migration_ObjectIDRewrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)

	dbName := fmt.Sprintf("test_c4_rewrite_%d", time.Now().UnixNano())
	coll := client.Database(dbName).Collection(defaultCollection)

	// Seed enough rows to span two batches (migrationBatchSize = 500).
	// Kept conservative at 1.5x to avoid long CI runs; the batching code
	// path fires on any input > migrationBatchSize.
	const seedCount = 750

	docs := make([]any, 0, seedCount)

	for i := range seedCount {
		docs = append(docs, bson.D{
			{Key: "_id", Value: bson.NewObjectID()},
			{Key: "namespace", Value: "ns"},
			{Key: "key", Value: fmt.Sprintf("k-%d", i)},
			{Key: "tenant_id", Value: store.SentinelGlobal},
			{Key: "value", Value: fmt.Sprintf(`"v-%d"`, i)},
			{Key: "updated_at", Value: time.Now().UTC()},
			{Key: "updated_by", Value: "seed"},
		})
	}

	_, err := coll.InsertMany(context.Background(), docs)
	require.NoError(t, err)

	// Run the migration.
	s, err := New(Config{
		Client:              client,
		Database:            dbName,
		TenantSchemaEnabled: true,
		SchemaInitTimeout:   2 * time.Minute, // generous for the 750-row walk
	})
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close() })

	// No ObjectId-keyed rows should remain.
	count, err := coll.CountDocuments(context.Background(), bson.D{
		{Key: "_id", Value: bson.D{{Key: "$type", Value: "objectId"}}},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "rewrite must eliminate every ObjectId _id")

	// Every seed row must be present under its compound-id shape. Exclude
	// the migration-lease sentinel and any other internal rows (none, but
	// an explicit filter guards against future drift).
	total, err := coll.CountDocuments(context.Background(), bson.D{
		{Key: "namespace", Value: "ns"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(seedCount), total, "row count must be preserved across migration")
}

// TestIntegration_Mongo_Migration_IdempotentRerun runs ensureSchema twice
// against the same collection. The second pass must be a no-op (no new
// writes, no E11000).
func TestIntegration_Mongo_Migration_IdempotentRerun(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)

	dbName := fmt.Sprintf("test_c4_rerun_%d", time.Now().UnixNano())
	coll := client.Database(dbName).Collection(defaultCollection)

	// Seed a legacy row so the first migration has non-trivial work.
	_, err := coll.InsertOne(context.Background(), bson.D{
		{Key: "_id", Value: bson.NewObjectID()},
		{Key: "namespace", Value: "ns"},
		{Key: "key", Value: "k"},
		{Key: "value", Value: `"v"`},
		{Key: "updated_at", Value: time.Now().UTC()},
	})
	require.NoError(t, err)

	// First pass.
	s1, err := New(Config{Client: client, Database: dbName, TenantSchemaEnabled: true})
	require.NoError(t, err)

	require.NoError(t, s1.Close())

	// Second pass against the same collection must not error and must not
	// duplicate the row.
	s2, err := New(Config{Client: client, Database: dbName, TenantSchemaEnabled: true})
	require.NoError(t, err, "re-running ensureSchema must be idempotent")

	t.Cleanup(func() { _ = s2.Close() })

	total, err := coll.CountDocuments(context.Background(), bson.D{
		{Key: "namespace", Value: "ns"},
		{Key: "key", Value: "k"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total, "idempotent rerun must not duplicate rows")
}

// TestIntegration_Mongo_Migration_AmbiguousAborts seeds a (namespace, key)
// pair with both a tenant_id-missing and a tenant_id="_global" row. The
// pre-flight verifyNoAmbiguousTenantDocs must fail the migration with a
// clear error instead of silently collapsing the rows at the backfill
// step.
func TestIntegration_Mongo_Migration_AmbiguousAborts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)

	dbName := fmt.Sprintf("test_c4_ambig_%d", time.Now().UnixNano())
	coll := client.Database(dbName).Collection(defaultCollection)

	_, err := coll.InsertMany(context.Background(), []any{
		// Missing tenant_id entirely.
		bson.D{
			{Key: "_id", Value: bson.NewObjectID()},
			{Key: "namespace", Value: "ambig"},
			{Key: "key", Value: "k"},
			{Key: "value", Value: `"a"`},
		},
		// Already has tenant_id="_global".
		bson.D{
			{Key: "_id", Value: bson.NewObjectID()},
			{Key: "namespace", Value: "ambig"},
			{Key: "key", Value: "k"},
			{Key: "tenant_id", Value: store.SentinelGlobal},
			{Key: "value", Value: `"b"`},
		},
	})
	require.NoError(t, err)

	_, err = New(Config{Client: client, Database: dbName, TenantSchemaEnabled: true})
	require.Error(t, err, "phase-2 migration must abort on ambiguous pre-migration state")
	assert.Contains(t, err.Error(), "ambiguous pre-migration state",
		"error message should be actionable for operators")
}

// TestIntegration_Mongo_Migration_CrashRecovery simulates H7: a partial
// migration (some rows already converted, some still on ObjectId _id) is
// interrupted. Re-running ensureSchema must pick up where it left off
// without duplicates and without errors.
//
// The partial state is fabricated by manually inserting both shapes and
// then running the migration once. Crashing mid-rewrite is impossible to
// trigger deterministically from inside the same process, but the
// rewrite path is designed so re-running it is a no-op against already-
// migrated rows.
func TestIntegration_Mongo_Migration_CrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)

	dbName := fmt.Sprintf("test_c4_crash_%d", time.Now().UnixNano())
	coll := client.Database(dbName).Collection(defaultCollection)

	// Already-migrated row (compound _id).
	_, err := coll.InsertOne(context.Background(), bson.D{
		{Key: "_id", Value: compoundID{Namespace: "ns", Key: "migrated", TenantID: store.SentinelGlobal}},
		{Key: "namespace", Value: "ns"},
		{Key: "key", Value: "migrated"},
		{Key: "tenant_id", Value: store.SentinelGlobal},
		{Key: "value", Value: `"m"`},
		{Key: "updated_at", Value: time.Now().UTC()},
	})
	require.NoError(t, err)

	// Not-yet-migrated row (ObjectId _id).
	_, err = coll.InsertOne(context.Background(), bson.D{
		{Key: "_id", Value: bson.NewObjectID()},
		{Key: "namespace", Value: "ns"},
		{Key: "key", Value: "pending"},
		{Key: "tenant_id", Value: store.SentinelGlobal},
		{Key: "value", Value: `"p"`},
		{Key: "updated_at", Value: time.Now().UTC()},
	})
	require.NoError(t, err)

	s, err := New(Config{Client: client, Database: dbName, TenantSchemaEnabled: true})
	require.NoError(t, err, "crash-recovery run must complete without error")

	t.Cleanup(func() { _ = s.Close() })

	// Both rows must be readable via the phase-2 Get surface.
	migrated, foundM, err := s.Get(context.Background(), "ns", "migrated")
	require.NoError(t, err)
	require.True(t, foundM)
	assert.Equal(t, `"m"`, string(migrated.Value))

	pending, foundP, err := s.Get(context.Background(), "ns", "pending")
	require.NoError(t, err)
	require.True(t, foundP)
	assert.Equal(t, `"p"`, string(pending.Value))

	// No ObjectId-_id rows should remain.
	count, err := coll.CountDocuments(context.Background(), bson.D{
		{Key: "_id", Value: bson.D{{Key: "$type", Value: "objectId"}}},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "rewrite must eliminate every ObjectId _id on retry")
}
