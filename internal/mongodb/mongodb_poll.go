package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-systemplane/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// subscribePoll uses a ticker to periodically query for entries updated since
// the initial watermark and emits events for each changed entry. Tenant deletes
// are soft-deleted with an updated_at tombstone, so polling mode observes them
// as ordinary updated documents and routes the same tuple to the Client refresh
// path.
//
// initialWatermark is the starting point: events with updated_at <= this value
// are NOT replayed. Callers typically pass the max(UpdatedAt) observed during
// hydration so rows written between hydration and the first tick are picked up
// exactly once.
func (s *Store) subscribePoll(
	ctx context.Context,
	handler func(store.Event),
	initialWatermark time.Time,
) error {
	cursor := pollCursor{updatedAt: initialWatermark}
	ticker := time.NewTicker(s.cfg.PollInterval)

	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			newCursor, err := s.pollChanges(ctx, cursor, handler)
			if err != nil {
				s.logWarn(ctx, "poll query failed", log.Err(err))

				continue
			}

			cursor = newCursor
		}
	}
}

type pollCursor struct {
	updatedAt time.Time
	namespace string
	key       string
	tenantID  string
}

func (s *Store) pollChanges(
	ctx context.Context,
	last pollCursor,
	handler func(store.Event),
) (pollCursor, error) {
	filter := pollCursorFilter(last)

	findOpts := options.Find().SetSort(bson.D{
		{Key: fieldUpdatedAt, Value: 1},
		{Key: fieldNamespace, Value: 1},
		{Key: fieldKey, Value: 1},
		{Key: fieldTenantID, Value: 1},
	})

	mongoCursor, err := s.coll.Find(ctx, filter, findOpts)
	if err != nil {
		return last, fmt.Errorf("poll find: %w", err)
	}
	defer mongoCursor.Close(ctx)

	newCursor := last

	for mongoCursor.Next(ctx) {
		var doc entryDoc
		if err := mongoCursor.Decode(&doc); err != nil {
			s.logWarn(ctx, "poll decode error, skipping document", log.Err(err))

			continue
		}

		tenantID := doc.TenantID
		if tenantID == "" {
			tenantID = store.SentinelGlobal
		}

		s.safeInvokeHandler(ctx, handler, store.Event{
			Namespace: doc.Namespace,
			Key:       doc.Key,
			TenantID:  tenantID,
		})

		newCursor = pollCursorFromDoc(doc, tenantID)
	}

	if err := mongoCursor.Err(); err != nil {
		return last, fmt.Errorf("poll cursor error: %w", err)
	}

	return newCursor, nil
}

func pollCursorFilter(cursor pollCursor) bson.D {
	return bson.D{{Key: opOr, Value: bson.A{
		bson.D{{Key: fieldUpdatedAt, Value: bson.D{{Key: opGt, Value: cursor.updatedAt}}}},
		bson.D{{Key: opAnd, Value: bson.A{
			bson.D{{Key: fieldUpdatedAt, Value: cursor.updatedAt}},
			lexicographicTupleFilter(cursor),
		}}},
	}}}
}

func lexicographicTupleFilter(cursor pollCursor) bson.D {
	return bson.D{{Key: opOr, Value: bson.A{
		bson.D{{Key: fieldNamespace, Value: bson.D{{Key: opGt, Value: cursor.namespace}}}},
		bson.D{{Key: opAnd, Value: bson.A{
			bson.D{{Key: fieldNamespace, Value: cursor.namespace}},
			bson.D{{Key: fieldKey, Value: bson.D{{Key: opGt, Value: cursor.key}}}},
		}}},
		bson.D{{Key: opAnd, Value: bson.A{
			bson.D{{Key: fieldNamespace, Value: cursor.namespace}},
			bson.D{{Key: fieldKey, Value: cursor.key}},
			bson.D{{Key: fieldTenantID, Value: bson.D{{Key: opGt, Value: cursor.tenantID}}}},
		}}},
	}}}
}

func pollCursorFromDoc(doc entryDoc, tenantID string) pollCursor {
	return pollCursor{
		updatedAt: doc.UpdatedAt,
		namespace: doc.Namespace,
		key:       doc.Key,
		tenantID:  tenantID,
	}
}
