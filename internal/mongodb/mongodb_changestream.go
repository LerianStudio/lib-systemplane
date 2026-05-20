// Change-stream and polling subscriptions for the MongoDB backend.
//
// The change-stream path is the default and requires a replica set; the
// polling path is the fallback for standalone deployments and is enabled by
// setting Config.PollInterval to a positive duration. Both paths emit
// store.Event values whose TenantID is populated from the row's tenant_id
// field so downstream subscribers can route global-vs-tenant dispatch.
//
// Delete events carry neither a fullDocument nor (under stock MongoDB) any
// pre-image. This backend sidesteps the problem by storing a compound _id
// {namespace, key, tenant_id} on every row, so the documentKey that the
// server always includes is sufficient on its own to reconstruct the event.
// See TRD §3.2 and research.md §MongoDB.

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/backoff"
	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/runtime"
	"github.com/LerianStudio/lib-systemplane/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// operationTypeDelete matches the string the MongoDB server places in
// changeEvent.OperationType for delete events.
const operationTypeDelete = "delete"

// resumeTokenBatchEvents bounds how many events can be processed before a
// persisted resume token is considered "too far behind" and forcibly saved.
const resumeTokenBatchEvents = 100

// resumeTokenBatchInterval bounds the wall-clock interval between resume
// token saves regardless of event volume — an idle change stream must still
// periodically persist its position so a later reconnect doesn't replay
// hours of oplog.
const resumeTokenBatchInterval = 5 * time.Second

var errResumeTokenSaveFailed = errors.New("resume token save failed")

// changeEventFullDoc captures the shape of an insert/update/replace event's
// fullDocument — the only branch where all three scoping fields are present
// in the payload.
type changeEventFullDoc struct {
	Namespace string `bson:"namespace"`
	Key       string `bson:"key"`
	TenantID  string `bson:"tenant_id"`
}

// changeEvent is the subset of a MongoDB change stream event we decode.
//
// DocumentKey decodes to the server-populated {_id: compoundID} document.
// On insert/update/replace events FullDocument is also populated courtesy
// of SetFullDocument(UpdateLookup); on delete events FullDocument is nil
// and DocumentKey is the only source of truth — hence the compound _id.
type changeEvent struct {
	OperationType string              `bson:"operationType"`
	FullDocument  *changeEventFullDoc `bson:"fullDocument,omitempty"`
	DocumentKey   struct {
		ID compoundID `bson:"_id"`
	} `bson:"documentKey"`
}

// Subscribe blocks until ctx is cancelled, watching for changes via
// change-streams or polling depending on cfg.PollInterval.
//
// Change-stream mode (PollInterval == 0) requires a MongoDB replica set.
// Polling mode (PollInterval > 0) works with standalone MongoDB deployments.
// The polling path uses an in-process watermark that starts at time.Now()
// on subscription — callers that need to capture updates already observed
// by the Client's hydration pass should plumb the hydration max(updated_at)
// through subscribePoll's initialWatermark argument (see M-S3-3 / caller
// Client.Start). The exported Subscribe always starts at "now" because
// that matches the change-stream semantic (subscriptions see only future
// events unless a resume token was supplied).
//
// Multiple concurrent Subscribe calls are independent — each gets its own
// stream or ticker. Store.Close cancels all in-flight Subscribe calls via
// the internal s.ctx merge-cancel below.
func (s *Store) Subscribe(ctx context.Context, handler func(store.Event)) error {
	return s.SubscribeReady(ctx, handler, nil)
}

// SubscribeReady is Subscribe with a setup readiness callback.
func (s *Store) SubscribeReady(ctx context.Context, handler func(store.Event), ready func(error)) error {
	if s == nil || s.isClosed() {
		return store.ErrClosed
	}

	if handler == nil {
		return errors.New("mongodb store subscribe: handler must not be nil")
	}

	// Merge the store-level ctx so Close() terminates the subscription even
	// when the caller's ctx is still live (M-S3-1).
	mergedCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	runtime.SafeGoWithContextAndComponent(mergedCtx, s.cfg.Logger, "systemplane", "mongodb.subscribe_cancel_bridge", runtime.KeepRunning, func(context.Context) {
		select {
		case <-s.ctx.Done():
			cancel()
		case <-mergedCtx.Done():
		}
	})

	if s.cfg.PollInterval > 0 {
		// MongoDB BSON datetimes have millisecond precision. Truncating the
		// initial watermark down prevents writes in the same millisecond as the
		// readiness signal from sorting before the cursor and being skipped.
		watermark := time.Now().UTC().Truncate(time.Millisecond)

		if ready != nil {
			ready(nil)
		}

		return s.subscribePoll(mergedCtx, handler, watermark)
	}

	return s.subscribeChangeStream(mergedCtx, handler, ready)
}

// subscribeChangeStream opens a MongoDB change stream on the collection and
// translates insert/update/replace/delete events into store.Event values.
// On stream error, it reconnects with exponential backoff. The resume token
// persisted by the prior watchOnce (if any) is handed to the new stream via
// SetResumeAfter, so events during the disconnect window are recovered.
func (s *Store) subscribeChangeStream(ctx context.Context, handler func(store.Event), ready func(error)) error {
	attempt := 0
	readyCalled := false

	for {
		if err := s.watchOnce(ctx, handler, func() {
			if !readyCalled && ready != nil {
				ready(nil)
			}

			readyCalled = true
		}); err != nil {
			// Context cancelled — clean exit regardless of the watch error.
			if ctx.Err() != nil {
				if !readyCalled && ready != nil {
					ready(ctx.Err())
				}

				return ctx.Err() //nolint:wrapcheck // propagate cancellation as-is
			}

			if !readyCalled && ready != nil {
				ready(err)

				return err
			}

			if errors.Is(err, errResumeTokenSaveFailed) && s.cfg.ResumeTokenFailClosed {
				return err
			}

			// Log and reconnect with backoff.
			s.logWarn(ctx, "change stream disconnected, reconnecting",
				log.Err(err),
				log.Int("attempt", attempt),
			)

			delay := min(backoff.ExponentialWithJitter(reconnectBaseDelay, attempt), reconnectMaxDelay)
			attempt++

			if waitErr := backoff.WaitContext(ctx, delay); waitErr != nil {
				return waitErr //nolint:wrapcheck // propagate cancellation as-is
			}

			continue
		}

		return nil
	}
}

// watchOnce opens a single change stream session and processes events until
// error or context cancellation.
//
// # Resume token behavior
//
// When Config.LoadResumeToken is non-nil, its return value (if it yields a
// non-nil bson.Raw with no error) is passed via
// options.ChangeStream().SetResumeAfter so the new stream picks up where
// the previous one left off — events that occurred during a disconnect are
// replayed from the oplog. A nil token or an error is logged and the stream
// opens from the current oplog position (the pre-H2 behavior).
//
// While the stream is running, the event loop batches ResumeToken captures
// and invokes Config.SaveResumeToken every resumeTokenBatchEvents events or
// every resumeTokenBatchInterval of wall-clock time, whichever comes first.
// SaveResumeToken failures are logged at WARN and, by default, the loop
// continues — the next successful save replaces the lost one. When
// ResumeTokenFailClosed is enabled, a save failure terminates the subscription
// instead of reconnecting without durable cursor progress.
//
// Operators who want strict at-least-once delivery across reconnects should
// wire both LoadResumeToken and SaveResumeToken to durable storage (Redis,
// a dedicated Postgres row, etc.). Without them, idle keys may hold stale
// values in a subscriber cache until the next write. See also
// MIGRATION_TENANT_SCOPED.md §8.1.
func (s *Store) watchOnce(ctx context.Context, handler func(store.Event), onReady func()) error {
	pipeline := mongo.Pipeline{
		{{Key: opMatch, Value: bson.D{
			{Key: "operationType", Value: bson.D{
				{Key: "$in", Value: bson.A{"insert", "update", "replace", operationTypeDelete}},
			}},
		}}},
	}

	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup)

	var err error

	opts, err = s.applyResumeToken(ctx, opts)
	if err != nil {
		return err
	}

	stream, err := s.coll.Watch(ctx, pipeline, opts)
	if err != nil {
		return fmt.Errorf("open change stream: %w", err)
	}
	defer stream.Close(ctx)

	if onReady != nil {
		onReady()
	}

	// Per-stream scratch space. pendingToken and the counters live across
	// iterations so the batched-save cadence is preserved; event is
	// re-declared per iteration because changeEvent carries a
	// *changeEventFullDoc pointer that BSON's Decode will NOT reset to nil
	// when the incoming document omits fullDocument (delete events). A
	// reused event from a prior insert would keep stale FullDocument data
	// on a subsequent delete, which extractEvent would then happily read.
	var (
		pendingToken  bson.Raw
		eventsInBatch int
		lastSaveAt    = time.Now()
	)

	flushToken := func() error {
		if len(pendingToken) == 0 {
			return nil
		}

		if err := s.saveResumeToken(ctx, pendingToken); err != nil {
			return err
		}

		pendingToken = nil
		eventsInBatch = 0
		lastSaveAt = time.Now()

		return nil
	}

	for stream.Next(ctx) {
		token, ok := s.processChangeStreamEvent(ctx, stream, handler)
		if !ok {
			continue
		}

		// Capture the resume token for this event and flush on batch
		// thresholds. ResumeToken is valid until the next Next() call so
		// we must consume it before the next iteration.
		pendingToken = token
		eventsInBatch++

		if eventsInBatch >= resumeTokenBatchEvents || time.Since(lastSaveAt) >= resumeTokenBatchInterval {
			if err := flushToken(); err != nil {
				return err
			}
		}
	}

	// Flush any pending token on clean exit — without this, the tail end
	// of a shutdown window would be replayed on next startup.
	if err := flushToken(); err != nil {
		return err
	}

	if ctx.Err() != nil {
		return nil
	}

	if err := stream.Err(); err != nil {
		return fmt.Errorf("change stream error: %w", err)
	}

	return nil
}

func (s *Store) saveResumeToken(ctx context.Context, token bson.Raw) error {
	if s.cfg.SaveResumeToken == nil || len(token) == 0 {
		return nil
	}

	// Copy the token into a stable allocation — the driver reuses the
	// underlying buffer on the next Next() call, so a save that escapes
	// into a goroutine would otherwise read a mutated slice.
	tokenCopy := make(bson.Raw, len(token))
	copy(tokenCopy, token)

	if saveErr := s.cfg.SaveResumeToken(ctx, tokenCopy); saveErr != nil {
		s.logWarn(ctx, "resume token save failed, will retry on next batch",
			log.Err(saveErr),
		)

		if s.cfg.ResumeTokenFailClosed {
			return fmt.Errorf("%w: %w", errResumeTokenSaveFailed, saveErr)
		}

		return nil
	}

	return nil
}

func (s *Store) processChangeStreamEvent(
	ctx context.Context,
	stream *mongo.ChangeStream,
	handler func(store.Event),
) (bson.Raw, bool) {
	var event changeEvent
	if err := stream.Decode(&event); err != nil {
		s.logWarn(ctx, "change stream decode error, skipping event", log.Err(err))

		return nil, false
	}

	evt, ok := extractEvent(event)
	if !ok {
		// Count the drop so operators can spot a runaway (malformed payload,
		// upstream bug) without grepping logs (L-S3-BL-1).
		s.droppedEvents.Add(1)

		s.logWarn(ctx, "change stream event dropped — missing identifiers",
			log.String("operationType", event.OperationType),
		)

		return nil, false
	}

	s.safeInvokeHandler(ctx, handler, evt)

	return stream.ResumeToken(), true
}

func (s *Store) applyResumeToken(
	ctx context.Context,
	opts *options.ChangeStreamOptionsBuilder,
) (*options.ChangeStreamOptionsBuilder, error) {
	if s.cfg.LoadResumeToken == nil {
		return opts, nil
	}

	token, err := s.cfg.LoadResumeToken(ctx)
	if err != nil {
		s.logWarn(ctx, "resume token load failed, starting from current oplog position",
			log.Err(err),
		)

		if s.cfg.ResumeTokenFailClosed {
			return opts, fmt.Errorf("load resume token: %w", err)
		}

		return opts, nil
	}

	if len(token) > 0 {
		opts = opts.SetResumeAfter(token)
	}

	return opts, nil
}

// extractEvent converts a decoded changeEvent into a store.Event.
// Returns false if the event cannot be mapped (missing fields, etc.).
//
// Two paths:
//   - Delete: documentKey._id carries the full tuple (namespace, key,
//     tenant_id). No fullDocument is ever available from the server in this
//     branch. The tuple is already decoded into event.DocumentKey.ID by the
//     driver's BSON codec.
//   - Insert/update/replace: fullDocument is populated thanks to
//     SetFullDocument(UpdateLookup). We prefer fullDocument because on
//     update events documentKey may refer to a previous tenant_id shape
//     from a pre-migration row; the server always re-reads the current
//     shape into fullDocument.
//
// The delete branch is the critical one for tenant attribution; getting it
// wrong would silently fire OnChange for tenant deletes. Keep both paths in
// this single function so reviewers can see the invariant.
func extractEvent(ce changeEvent) (store.Event, bool) {
	if ce.OperationType == operationTypeDelete {
		id := ce.DocumentKey.ID
		if id.Namespace == "" || id.Key == "" || id.TenantID == "" {
			return store.Event{}, false
		}

		return store.Event{
			Namespace: id.Namespace,
			Key:       id.Key,
			TenantID:  id.TenantID,
		}, true
	}

	if ce.FullDocument == nil {
		return store.Event{}, false
	}

	fd := *ce.FullDocument
	if fd.Namespace == "" || fd.Key == "" {
		return store.Event{}, false
	}

	// A tenant_id coming through as "" is only expected on legacy rows that
	// slipped past ensureSchema's backfill. Treat as the sentinel rather
	// than dropping the event.
	tenantID := fd.TenantID
	if tenantID == "" {
		tenantID = store.SentinelGlobal
	}

	return store.Event{
		Namespace: fd.Namespace,
		Key:       fd.Key,
		TenantID:  tenantID,
	}, true
}

// safeInvokeHandler calls handler inside a deferred panic recovery.
func (s *Store) safeInvokeHandler(ctx context.Context, handler func(store.Event), evt store.Event) {
	defer runtime.RecoverAndLogWithContext(ctx, s.cfg.Logger, "systemplane", "mongodb.handler")

	handler(evt)
}
