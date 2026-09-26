package mongodb

import (
	"context"
	"fmt"
	"time"

	obsconstants "github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// warnPollingIndexes is logged when the polling indexes cannot be created. The
// store keeps serving: the indexes only make polling cheaper, and polling MAY
// be slower without them. It is not necessarily a full scan: the creation also
// fails when an equivalent index already exists under another name, and that
// index still serves the poll queries.
const warnPollingIndexes = "could not create the polling indexes; polling may be slower without them"

// runSchema makes the collection ready for use. With a compound _id there is
// no separate unique index to create — the server enforces uniqueness on _id
// automatically.
//
// Behavior differs by mode:
//
//   - Any tenant database, whether carried by ctx in multi-tenant mode or
//     resolved through the tenant connector for a named scope: we MUST
//     eagerly materialize the collection via CreateCollection. MongoDB
//     creates collections lazily on first write, so a Get/List against a
//     fresh tenant DB before any Set would otherwise succeed (returning an
//     empty result) — masking permission problems and producing answers that
//     look correct. CreateCollection is treated as idempotent:
//     NamespaceExists (code 48 / "already exists") is success.
//     In polling mode it also creates the two indexes in pollingIndexes, which
//     is the same privilege class CreateCollection already assumes; a refusal
//     is logged, not returned, exactly as in the single-tenant branch.
//
//   - The single-tenant constructor collection: we deliberately DO NOT call
//     CreateCollection. The change stream that backs Subscribe attaches at
//     the current oplog position, and on freshly created replica-set members
//     there is a brief window after CreateCollection where the watcher can
//     miss the first insert. Listing indexes is also all the privilege a
//     consumer whose collection is provisioned externally may hold — it
//     confirms the connection can reach the collection, and the change stream
//     observes the very first write that auto-creates the namespace. In
//     POLLING mode it also creates the two indexes in pollingIndexes: there is
//     no change stream to race there, and with no index covering them both of
//     the poller's per-tick queries scan the whole collection on every tick
//     while the incremental one also sorts it in memory. A failed creation is
//     logged, not returned: a role that may not create an index keeps working,
//     possibly slower, and an equivalent index under another name (which makes
//     the creation fail) still serves the poll queries.
func (s *Store) runSchema(ctx context.Context, coll *mongo.Collection, tenant string, tenantScoped bool) error {
	if s.cfg.MultiTenantEnabled || tenantScoped {
		db := coll.Database()
		if err := db.CreateCollection(ctx, coll.Name()); err != nil && !isNamespaceExists(err) {
			return fmt.Errorf("systemplane/mongodb: create collection: %w", err)
		}
	} else {
		// Single-tenant: just touch the collection's index catalog. This both
		// confirms the connection has the required privileges and avoids the
		// change-stream attach race that affects single-tenant Subscribe.
		cur, err := coll.Indexes().List(ctx)
		if err != nil {
			return fmt.Errorf("systemplane/mongodb: list indexes: %w", err)
		}

		if err := cur.Close(ctx); err != nil {
			return fmt.Errorf("systemplane/mongodb: close index cursor: %w", err)
		}
	}

	if s.cfg.PollInterval <= 0 {
		return nil
	}

	if _, err := coll.Indexes().CreateMany(ctx, pollingIndexes()); err != nil {
		s.logWarn(ctx, warnPollingIndexes,
			log.Err(err),
			log.String(obsconstants.AttrKeyTenantID, tenant),
		)
	}

	return nil
}

// upsertPipeline builds the aggregation-pipeline update that backs Set.
//
// It is a function returning a pipeline rather than inline code so a unit test
// can assert the $literal wrapping without a live server; the expressions
// themselves are evaluated server-side and are covered by integration tests.
//
// Two stages, and the split is load-bearing. Stage 1 computes the revision
// against the PRE-UPDATE document, so "$value" there unambiguously means the
// value the document carried before this write; folding both stages into one
// $set would lean on same-stage input-document semantics. On an insert (and on
// a tombstone) "$value" is missing, so $ifNull yields BSON null, the $eq
// against the new value is false — a JSON payload is always a string, even a
// JSON null, which is the four-character string "null" — and the bump branch
// runs. $setOnInsert is illegal in a pipeline update, which is why the $ifNull
// defaults carry the insert case instead.
func upsertPipeline(e store.Entry) mongo.Pipeline {
	newValue := string(e.Value)

	return mongo.Pipeline{
		// Stage 1 — revision, computed against the PRE-UPDATE document.
		bson.D{{Key: opSet, Value: bson.D{{Key: fieldRevision, Value: bson.D{{Key: "$cond", Value: bson.A{
			bson.D{{Key: "$eq", Value: bson.A{
				bson.D{{Key: opIfNull, Value: bson.A{"$" + fieldValue, nil}}},
				bson.D{{Key: opLiteral, Value: newValue}},
			}}},
			bson.D{{Key: opIfNull, Value: bson.A{"$" + fieldRevision, int64(1)}}}, // unchanged value → keep
			bumpRevisionExpr(), // changed → bump above the old revision AND above the clock floor (D11)
		}}}}}}},
		// Stage 2 — the rest of the document. EVERY caller-supplied string is
		// wrapped in $literal: in a pipeline $set, a bare string beginning with
		// "$" is an expression, not a value. An UpdatedBy of "$value" would
		// otherwise persist the document's own JSON payload as the actor, and a
		// "$x" namespace would resolve to missing and DROP the field.
		bson.D{{Key: opSet, Value: bson.D{
			{Key: fieldNamespace, Value: bson.D{{Key: opLiteral, Value: e.Namespace}}},
			{Key: fieldKey, Value: bson.D{{Key: opLiteral, Value: e.Key}}},
			{Key: fieldValue, Value: bson.D{{Key: opLiteral, Value: newValue}}},
			// updated_at is left BARE on purpose: a BSON date is never parsed as
			// a field path, so it needs no $literal. This is a decision, not an
			// oversight — do not "fix" it.
			{Key: fieldUpdatedAt, Value: e.UpdatedAt},
			{Key: fieldUpdatedBy, Value: bson.D{{Key: opLiteral, Value: e.UpdatedBy}}},
		}}},
		// Stage 3 — a Set on a tombstone brings the key back to life, so the
		// marker goes. No stage reads "deleted", so this one is last by
		// choice rather than by necessity. The revision bumped in stage 1 for
		// free: a tombstone carries no "value", so the $eq there compared BSON
		// null against a JSON payload, which is always a string.
		bson.D{{Key: opUnset, Value: fieldDeleted}},
	}
}

// tombstonePipeline builds the aggregation-pipeline update that backs Delete.
// It is only ever run under notDeleted(), so there is no tombstone left to
// condition on and stage 1 always bumps.
func tombstonePipeline(actor string, now time.Time) mongo.Pipeline {
	return mongo.Pipeline{
		// Stage 1 — the revision the deleted key reached must keep climbing, so
		// a later recreate lands above every revision the key ever had.
		bson.D{{Key: opSet, Value: bson.D{{Key: fieldRevision, Value: bumpRevisionExpr()}}}},
		// Stage 2 — the marker and the provenance of the delete. actor is
		// caller-supplied, so it is wrapped; updated_at is a BSON date and is
		// left bare, exactly as in upsertPipeline.
		bson.D{{Key: opSet, Value: bson.D{
			{Key: fieldDeleted, Value: true},
			{Key: fieldUpdatedAt, Value: now},
			{Key: fieldUpdatedBy, Value: bson.D{{Key: opLiteral, Value: actor}}},
		}}},
		// Stage 3 — the value is gone; only the revision and the provenance
		// survive.
		bson.D{{Key: opUnset, Value: fieldValue}},
	}
}

// notDeleted is the tombstone guard shared by Get, List and Delete. $ne rather
// than $exists on purpose: a document written before v4 carries no "deleted"
// field at all and must stay visible, and $ne matches a missing field.
func notDeleted() bson.E {
	return bson.E{Key: fieldDeleted, Value: bson.D{{Key: "$ne", Value: true}}}
}

// pollingIndexes are the two indexes the polling fallback needs. Every reads
// path outside polling goes through _id, which the server indexes on its own —
// these exist for the two full-collection queries polling runs on EVERY tick,
// for every scope it serves:
//
//   - {updated_at, namespace, key} turns the incremental $gte query into a
//     bounded index scan AND satisfies its sort, so the round trip no longer
//     sorts the whole collection in memory.
//   - {deleted, namespace, key} covers snapshotKeys: the projection is
//     (namespace, key), so the live key set is read from the index alone and
//     the stored JSON value is never fetched or decoded.
//
// Both are created with the collection, so the driver's own names are used and
// re-creating them is a no-op.
func pollingIndexes() []mongo.IndexModel {
	return []mongo.IndexModel{
		{Keys: bson.D{
			{Key: fieldUpdatedAt, Value: 1},
			{Key: fieldNamespace, Value: 1},
			{Key: fieldKey, Value: 1},
		}},
		{Keys: bson.D{
			{Key: fieldDeleted, Value: 1},
			{Key: fieldNamespace, Value: 1},
			{Key: fieldKey, Value: 1},
		}},
	}
}

// bumpRevisionExpr is the revision bump: strictly above the revision the
// document already carried, and never below the server clock in milliseconds.
// The clock is only a floor for a key's first-ever write; "previous + 1" is
// what makes the revision strictly increasing across a delete and recreate
// whatever the clock does.
func bumpRevisionExpr() bson.D {
	return bson.D{{Key: "$max", Value: bson.A{
		bson.D{{Key: "$add", Value: bson.A{
			bson.D{{Key: opIfNull, Value: bson.A{"$" + fieldRevision, int64(0)}}},
			int64(1),
		}}},
		bson.D{{Key: "$toLong", Value: "$$NOW"}},
	}}}
}

// upsertReturningRevision writes an entry through upsertPipeline and returns
// the revision the document carries afterwards.
func upsertReturningRevision(ctx context.Context, coll *mongo.Collection, e store.Entry) (int64, error) {
	filter := bson.D{{Key: fieldID, Value: compoundID{Namespace: e.Namespace, Key: e.Key}}}

	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	var doc entryDoc

	// mongo.ErrNoDocuments is deliberately NOT special-cased: an upsert
	// returning the after-image always produces a document, so if it ever
	// appears it is a real error and must propagate rather than be swallowed
	// into revision 0.
	if err := coll.FindOneAndUpdate(ctx, filter, upsertPipeline(e), opts).Decode(&doc); err != nil {
		return 0, err //nolint:wrapcheck // caller wraps with method context
	}

	return doc.Revision, nil
}
