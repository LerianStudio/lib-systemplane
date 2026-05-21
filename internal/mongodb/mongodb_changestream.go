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

	var once sync.Once

	return func() {
		once.Do(func() {
			s.subscriberMu.Lock()
			delete(s.subscribers, id)
			s.subscriberMu.Unlock()
		})
	}, nil
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

func (s *Store) pollForever(stop <-chan struct{}) {
	watermark := time.Now().UTC().Truncate(time.Millisecond)

	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			newWatermark, err := s.pollOnce(watermark)
			if err != nil {
				s.logWarn(context.Background(), "poll query failed", log.Err(err))

				continue
			}

			watermark = newWatermark
		}
	}
}

func (s *Store) pollOnce(watermark time.Time) (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := bson.D{{Key: fieldUpdatedAt, Value: bson.D{{Key: "$gt", Value: watermark}}}}

	findOpts := options.Find().SetSort(bson.D{
		{Key: fieldUpdatedAt, Value: 1},
		{Key: fieldNamespace, Value: 1},
		{Key: fieldKey, Value: 1},
	})

	cur, err := s.coll.Find(ctx, filter, findOpts)
	if err != nil {
		return watermark, fmt.Errorf("poll find: %w", err)
	}
	defer cur.Close(ctx)

	newWatermark := watermark

	for cur.Next(ctx) {
		var doc entryDoc
		if err := cur.Decode(&doc); err != nil {
			s.logWarn(ctx, "poll decode error, skipping document", log.Err(err))

			continue
		}

		s.dispatchEvent(store.Event{
			Namespace: doc.Namespace,
			Key:       doc.Key,
			Op:        store.OpUpsert,
		})

		if doc.UpdatedAt.After(newWatermark) {
			newWatermark = doc.UpdatedAt
		}
	}

	if err := cur.Err(); err != nil {
		return watermark, fmt.Errorf("poll cursor error: %w", err)
	}

	return newWatermark, nil
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
