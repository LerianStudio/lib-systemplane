// CRUD helpers and schema bootstrap for the MongoDB backend.
package mongodb

import (
	"context"
	"fmt"

	"github.com/LerianStudio/lib-systemplane/v3/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// runSchema makes the collection ready for use. With a compound _id there is
// no separate unique index to create — the server enforces uniqueness on _id
// automatically.
//
// Behavior differs by mode:
//
//   - Multi-tenant: we MUST eagerly materialize the collection via
//     CreateCollection. MongoDB creates collections lazily on first write, so
//     a Get/List against a fresh tenant DB before any Set would otherwise
//     succeed (returning an empty result) — masking permission problems and
//     producing answers that look correct. CreateCollection is treated as
//     idempotent: NamespaceExists (code 48 / "already exists") is success.
//
//   - Single-tenant: we deliberately DO NOT call CreateCollection. The
//     change stream that backs Subscribe attaches at the current oplog
//     position, and on freshly created replica-set members there is a brief
//     window after CreateCollection where the watcher can miss the first
//     insert. Listing indexes is sufficient — it confirms the connection has
//     the required privileges, and the change stream observes the very first
//     write that auto-creates the namespace.
func (s *Store) runSchema(ctx context.Context, coll *mongo.Collection) error {
	if s.cfg.MultiTenantEnabled {
		db := coll.Database()
		if err := db.CreateCollection(ctx, coll.Name()); err != nil && !isNamespaceExists(err) {
			return fmt.Errorf("systemplane/mongodb: create collection: %w", err)
		}

		return nil
	}

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

	return nil
}

// upsert writes an entry using an upsert keyed on the compound _id.
func upsert(ctx context.Context, coll *mongo.Collection, e store.Entry) error {
	id := compoundID{Namespace: e.Namespace, Key: e.Key}

	filter := bson.D{{Key: fieldID, Value: id}}
	update := bson.D{
		{Key: opSet, Value: bson.D{
			{Key: fieldNamespace, Value: e.Namespace},
			{Key: fieldKey, Value: e.Key},
			{Key: fieldValue, Value: string(e.Value)},
			{Key: fieldUpdatedAt, Value: e.UpdatedAt},
			{Key: fieldUpdatedBy, Value: e.UpdatedBy},
		}},
	}

	opts := options.UpdateOne().SetUpsert(true)
	if _, err := coll.UpdateOne(ctx, filter, update, opts); err != nil {
		return err //nolint:wrapcheck // caller wraps with method context
	}

	return nil
}
