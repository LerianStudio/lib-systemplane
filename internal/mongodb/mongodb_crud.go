// CRUD helpers and schema bootstrap for the MongoDB backend.
package mongodb

import (
	"context"
	"fmt"

	"github.com/LerianStudio/lib-systemplane/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// runSchema makes the collection ready for use. With a compound _id there is
// no separate unique index to create — the server enforces uniqueness on _id
// automatically. We touch the collection so a brand-new namespace exists when
// the change stream first runs.
func (s *Store) runSchema(ctx context.Context, coll *mongo.Collection) error {
	// Touch the collection by listing indexes; this both ensures it exists in
	// the database catalog (MongoDB creates collections lazily) and confirms
	// the caller's connection has the required privileges.
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
