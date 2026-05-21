// Change-stream and polling subscriptions for the single-tenant MongoDB
// backend.
//
// Multi-tenant deployments resolve a fresh database per call and have no
// shared process-wide changefeed; Subscribe in that mode returns
// store.ErrNotSupportedInMultiTenant.
package mongodb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/backoff"
	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/runtime"
	"github.com/LerianStudio/lib-systemplane/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const operationTypeDelete = "delete"

// changeEvent is the subset of a MongoDB change stream event we decode.
type changeEvent struct {
	OperationType string `bson:"operationType"`
	DocumentKey   struct {
		ID compoundID `bson:"_id"`
	} `bson:"documentKey"`
}

// Subscribe registers fn for the lifetime of ctx (or until unsubscribe is
// called). Multi-tenant mode returns store.ErrNotSupportedInMultiTenant.
func (s *Store) Subscribe(ctx context.Context, fn func(store.Event)) (func(), error) {
	if s == nil || s.isClosed() {
		return nil, store.ErrClosed
	}

	if s.cfg.MultiTenantEnabled {
		return nil, store.ErrNotSupportedInMultiTenant
	}

	if fn == nil {
		return func() {}, nil
	}

	s.subscriberMu.Lock()
	s.nextSubID++
	id := s.nextSubID
	s.subscribers[id] = fn
	s.subscriberMu.Unlock()

	// cancelCh stops the optional ctx-observer goroutine below. We gate every
	// teardown action (subscriber removal + cancelCh close) through a single
	// sync.Once so that concurrent invocations — for example a caller-driven
	// unsubscribe racing with ctx.Done() — never double-close the channel and
	// never delete the subscriber slot twice.
	cancelCh := make(chan struct{})

	var once sync.Once

	teardown := func() {
		once.Do(func() {
			s.subscriberMu.Lock()
			delete(s.subscribers, id)
			s.subscriberMu.Unlock()

			close(cancelCh)
		})
	}

	// Honor the caller's lifetime ctx: when it cancels, remove the handler
	// automatically so a forgotten unsubscribe does not leak the entry. We
	// only spawn the observer when ctx is non-nil — a nil ctx would panic on
	// ctx.Done(). Callers passing a nil ctx receive an unsubscribe func that
	// works exactly the same.
	if ctx != nil {
		go func() {
			select {
			case <-ctx.Done():
				teardown()
			case <-cancelCh:
			}
		}()
	}

	return teardown, nil
}

func (s *Store) startListener(_ context.Context) error {
	s.subscriberMu.Lock()
	if s.streamStop != nil {
		s.subscriberMu.Unlock()

		return nil
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	s.streamStop = stop
	s.streamDone = done
	s.subscriberMu.Unlock()

	go func() {
		defer close(done)
		defer runtime.RecoverAndLog(s.cfg.Logger, "systemplane.mongodb.listener")

		if s.cfg.PollInterval > 0 {
			s.pollForever(stop)

			return
		}

		s.streamForever(stop)
	}()

	return nil
}

func (s *Store) stopListener() {
	s.subscriberMu.Lock()
	stop := s.streamStop
	done := s.streamDone
	s.streamStop = nil
	s.streamDone = nil
	s.subscriberMu.Unlock()

	if stop == nil {
		return
	}

	close(stop)

	if done != nil {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}
}

func (s *Store) streamForever(stop <-chan struct{}) {
	attempt := 0

	for {
		select {
		case <-stop:
			return
		default:
		}

		if err := s.watchOnce(stop); err != nil {
			select {
			case <-stop:
				return
			default:
			}

			s.logWarn(context.Background(), "change stream disconnected, reconnecting",
				log.Err(err),
				log.Int("attempt", attempt),
			)

			delay := min(backoff.ExponentialWithJitter(reconnectBaseDelay, attempt), reconnectMaxDelay)
			attempt++

			select {
			case <-stop:
				return
			case <-time.After(delay):
			}

			continue
		}

		return
	}
}

func (s *Store) watchOnce(stop <-chan struct{}) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{
				{Key: "$in", Value: bson.A{"insert", "update", "replace", operationTypeDelete}},
			}},
		}}},
	}

	opts := options.ChangeStream()

	stream, err := s.coll.Watch(ctx, pipeline, opts)
	if err != nil {
		return fmt.Errorf("open change stream: %w", err)
	}
	defer stream.Close(ctx)

	s.logInfo(ctx, "change stream established",
		log.String("collection", s.cfg.Collection),
	)

	for stream.Next(ctx) {
		var event changeEvent
		if err := stream.Decode(&event); err != nil {
			s.logWarn(ctx, "change stream decode error, skipping event", log.Err(err))

			continue
		}

		evt, ok := eventFromChange(event)
		if !ok {
			s.droppedEvents.Add(1)

			s.logWarn(ctx, "change stream event dropped — missing identifiers",
				log.String("operationType", event.OperationType),
			)

			continue
		}

		s.dispatchEvent(evt)
	}

	if ctx.Err() != nil {
		return nil
	}

	if err := stream.Err(); err != nil {
		return fmt.Errorf("change stream error: %w", err)
	}

	return nil
}

// nsKey is the polling-mode set element used to detect deletes. We can't
// rely on _id (compound subdocument) as a map key directly, so we tuple it.
type nsKey struct {
	Namespace string
	Key       string
}

func (s *Store) pollForever(stop <-chan struct{}) {
	// Watermark anchored slightly in the past so the first tick observes any
	// row that already exists. Deduplication is keyed on (namespace, key)
	// pairs already emitted at the current watermark boundary.
	watermark := time.Now().UTC().Truncate(time.Millisecond)
	// known tracks the set of (namespace, key) tuples observed by the most
	// recent full poll. Anything present last time but absent now is a
	// delete that we synthesize an OpDelete event for.
	known := make(map[nsKey]struct{})
	// seenAtWatermark holds the ids whose updated_at equals the current
	// watermark, so a $gte query can include the boundary without emitting
	// duplicates for the same write twice in a row.
	seenAtWatermark := make(map[nsKey]struct{})
	// firstPoll suppresses delete synthesis on the very first iteration
	// (when `known` is empty by construction) and primes the watermark from
	// the maximum updated_at observed during the snapshot scan.
	firstPoll := true

	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			newWatermark, currentKnown, newSeen, err := s.pollOnce(watermark, seenAtWatermark, known, firstPoll)
			if err != nil {
				s.logWarn(context.Background(), "poll query failed", log.Err(err))

				continue
			}

			watermark = newWatermark
			seenAtWatermark = newSeen
			known = currentKnown
			firstPoll = false
		}
	}
}

// pollOnce performs one poll cycle:
//   - Emits OpUpsert for every doc with updated_at >= watermark whose
//     (namespace, key) is not already in seenAtWatermark (dedup at the
//     boundary).
//   - Performs a full collection scan to capture the current key set;
//     anything present in `prevKnown` but absent now becomes a synthesized
//     OpDelete event. Skipped on the first iteration (prevKnown empty).
//
// Returns the new watermark, the new full known set, and the new
// seenAtWatermark set (ids that touched the boundary millisecond).
func (s *Store) pollOnce(
	watermark time.Time,
	prevSeenAtWatermark map[nsKey]struct{},
	prevKnown map[nsKey]struct{},
	firstPoll bool,
) (time.Time, map[nsKey]struct{}, map[nsKey]struct{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Use >= so two writes that land in the same millisecond as the previous
	// boundary aren't silently skipped; dedup via prevSeenAtWatermark.
	filter := bson.D{{Key: fieldUpdatedAt, Value: bson.D{{Key: "$gte", Value: watermark}}}}

	findOpts := options.Find().SetSort(bson.D{
		{Key: fieldUpdatedAt, Value: 1},
		{Key: fieldNamespace, Value: 1},
		{Key: fieldKey, Value: 1},
	})

	cur, err := s.coll.Find(ctx, filter, findOpts)
	if err != nil {
		return watermark, prevKnown, prevSeenAtWatermark, fmt.Errorf("poll find: %w", err)
	}
	defer cur.Close(ctx)

	newWatermark := watermark
	newSeen := make(map[nsKey]struct{})

	for cur.Next(ctx) {
		var doc entryDoc
		if err := cur.Decode(&doc); err != nil {
			s.logWarn(ctx, "poll decode error, skipping document", log.Err(err))

			continue
		}

		nk := nsKey{Namespace: doc.Namespace, Key: doc.Key}

		// Dedup: if this id was already emitted at the current watermark
		// boundary, don't re-emit until something newer arrives for it.
		if doc.UpdatedAt.Equal(watermark) {
			if _, already := prevSeenAtWatermark[nk]; already {
				newSeen[nk] = struct{}{}

				continue
			}
		}

		s.dispatchEvent(store.Event{
			Namespace: doc.Namespace,
			Key:       doc.Key,
			Op:        store.OpUpsert,
		})

		switch {
		case doc.UpdatedAt.After(newWatermark):
			newWatermark = doc.UpdatedAt
			newSeen = map[nsKey]struct{}{nk: {}}
		case doc.UpdatedAt.Equal(newWatermark):
			newSeen[nk] = struct{}{}
		}
	}

	if err := cur.Err(); err != nil {
		return watermark, prevKnown, prevSeenAtWatermark, fmt.Errorf("poll cursor error: %w", err)
	}

	// Full-collection scan to detect deletes done by other processes. The
	// incremental updated_at scan above cannot see deletes (the row is gone
	// before its tombstone is observable); diffing key sets is the only way.
	currentKnown, err := s.snapshotKeys(ctx)
	if err != nil {
		// If the snapshot fails, keep prevKnown — we'd rather miss a delete
		// event than emit a spurious one based on a partial scan.
		s.logWarn(ctx, "poll snapshot failed, skipping delete diff", log.Err(err))

		return newWatermark, prevKnown, newSeen, nil
	}

	if !firstPoll {
		for nk := range prevKnown {
			if _, stillThere := currentKnown[nk]; !stillThere {
				s.dispatchEvent(store.Event{
					Namespace: nk.Namespace,
					Key:       nk.Key,
					Op:        store.OpDelete,
				})
			}
		}
	}

	return newWatermark, currentKnown, newSeen, nil
}

// snapshotKeys returns the set of (namespace, key) tuples currently present
// in the collection. Used by polling mode to diff against the prior poll and
// synthesize OpDelete events for rows that disappeared.
func (s *Store) snapshotKeys(ctx context.Context) (map[nsKey]struct{}, error) {
	projection := bson.D{
		{Key: fieldNamespace, Value: 1},
		{Key: fieldKey, Value: 1},
		{Key: fieldID, Value: 0},
	}
	findOpts := options.Find().SetProjection(projection)

	cur, err := s.coll.Find(ctx, bson.D{}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("snapshot find: %w", err)
	}
	defer cur.Close(ctx)

	out := make(map[nsKey]struct{})

	for cur.Next(ctx) {
		var doc struct {
			Namespace string `bson:"namespace"`
			Key       string `bson:"key"`
		}

		if err := cur.Decode(&doc); err != nil {
			s.logWarn(ctx, "snapshot decode error, skipping document", log.Err(err))

			continue
		}

		if doc.Namespace == "" || doc.Key == "" {
			continue
		}

		out[nsKey{Namespace: doc.Namespace, Key: doc.Key}] = struct{}{}
	}

	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("snapshot cursor: %w", err)
	}

	return out, nil
}

func eventFromChange(ce changeEvent) (store.Event, bool) {
	id := ce.DocumentKey.ID
	if id.Namespace == "" || id.Key == "" {
		return store.Event{}, false
	}

	op := store.OpUpsert
	if ce.OperationType == operationTypeDelete {
		op = store.OpDelete
	}

	return store.Event{Namespace: id.Namespace, Key: id.Key, Op: op}, true
}

func (s *Store) dispatchEvent(evt store.Event) {
	s.subscriberMu.Lock()
	subs := make([]func(store.Event), 0, len(s.subscribers))

	for _, fn := range s.subscribers {
		subs = append(subs, fn)
	}

	s.subscriberMu.Unlock()

	for _, fn := range subs {
		func() {
			defer runtime.RecoverAndLog(s.cfg.Logger, "systemplane.mongodb.handler")

			fn(evt)
		}()
	}
}
