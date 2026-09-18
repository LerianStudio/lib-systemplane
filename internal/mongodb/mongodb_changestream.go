// Change-stream and polling changefeeds for the MongoDB backend.
//
// One feed per scope: the zero-scope feed is opened by Start and lives until
// Close. Multi-tenant deployments resolve a fresh database on every call, so
// the zero scope has no durable collection to watch there and Subscribe
// returns store.ErrNotSupportedInMultiTenant.
//
// Start opens the change stream SYNCHRONOUSLY, on the caller's goroutine,
// before it returns. That handshake is not a style choice: a change stream
// opened without a resume token attaches at the CURRENT oplog position, so a
// write that lands before the attach is never delivered — not late, never. The
// gap left by a LATER outage is covered instead by store.OpResync, which every
// (re)open announces so the engine reconciles the whole scope.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/LerianStudio/lib-commons/v7/commons/backoff"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const operationTypeDelete = "delete"

// Bounds on one change-stream open and on shutdown. EVERY open is bounded by
// the same pair — the first one as much as a reopen — so an unreachable
// MongoDB fails Start within watchTimeout instead of parking it for the life
// of the caller's ctx and pinning the reserved feed slot with it. They are
// vars, not consts, only so the tests can shrink them; nothing in production
// writes them.
var (
	watchTimeout = 10 * time.Second
	closeTimeout = 5 * time.Second
)

// errFeedStopped ends a reopen loop that lost its race with teardown. It never
// reaches a caller.
var errFeedStopped = errors.New("systemplane/mongodb: changefeed stopped")

// changeEvent is the subset of a MongoDB change stream event we decode.
type changeEvent struct {
	OperationType string `bson:"operationType"`
	DocumentKey   struct {
		ID compoundID `bson:"_id"`
	} `bson:"documentKey"`
	// FullDocument is the after-image, present for insert, update and replace
	// because the stream is opened with fullDocument: updateLookup. A raw
	// delete never carries one, which is why documentKey._id stays the
	// identity source and is never replaced by this.
	FullDocument *entryDoc `bson:"fullDocument"`
}

// feed is one changefeed for one scope. The zero-scope feed is created by
// Start and lives until Close.
type feed struct {
	scope store.Scope
	coll  *mongo.Collection

	// ready is closed exactly once, by the creator, when creation finishes:
	// with err nil the feed is live, with err non-nil creation failed and the
	// slot has already been retracted from the feeds map. err is written BEFORE
	// the close, so the close is the happens-before edge that publishes it.
	// Both are nil on the zero-scope feed, which Start owns and never waits on.
	// readyClosed guards the single close of ready. Both the creator and Close
	// can reach a reserved slot, so the flag lives under Store.feedsMu — the
	// lock both of them already take — not under f.mu.
	ready       chan struct{}
	err         error
	readyClosed bool

	// refs counts the callers holding this feed, guarded by Store.feedsMu (NOT
	// f.mu): the count decides the feed's lifetime and must be read and written
	// in the same lock hold that publishes or removes the map slot. Meaningful
	// only while the feed is in that map.
	refs int

	mu           sync.Mutex
	subs         map[uint64]*subscription
	nextID       uint64
	connected    bool // true between a successful open and the loss of that stream
	disconnected bool // true once OpDisconnect has been emitted for the CURRENT outage
	closing      bool // set by teardown under mu, BEFORE stop is closed

	// dispatching counts the deliveries the reader goroutine is currently
	// inside. A callback can reach teardown from there — unsubscribing itself
	// as the last subscriber of a tenant feed, or closing the store — and the
	// goroutine it would then wait on is the one running it, so the wait can
	// only ever end at the closeTimeout with the feed frozen meanwhile. While
	// this is non-zero a teardown signals and returns instead of waiting; the
	// reader still closes done on its way out.
	dispatching int

	stop chan struct{}
	done chan struct{}
}

// subscription serializes delivery to one callback. sub.mu is held for the
// whole of fn, which is what lets Subscribe emit a joining subscriber's marker
// without racing the reader goroutine.
type subscription struct {
	mu sync.Mutex
	fn func(store.Event)
}

// newFeed builds an unconnected feed. done stays nil until its reader
// goroutine launches, which is what marks the feed as running.
func newFeed(scope store.Scope, coll *mongo.Collection) *feed {
	return &feed{
		scope: scope,
		coll:  coll,
		subs:  make(map[uint64]*subscription),
		stop:  make(chan struct{}),
	}
}

// label names the feed's scope in an error or log message: empty for the zero,
// single-tenant scope, " tenant <id>" for a named one.
func (f *feed) label() string {
	if f.scope.Tenant == "" {
		return ""
	}

	return " tenant " + f.scope.Tenant
}

// beginDisconnect decides, atomically with any concurrent teardown, whether
// this stream loss must emit OpDisconnect to the returned subscribers.
// It is EDGE-TRIGGERED: ok is true only on the connected→disconnected
// transition. ok is false when f.closing is already set (clean shutdown, see
// below) or when f.disconnected is already set (a reopen attempt failed while
// the feed was already known to be down — one outage, one disconnect).
// Teardown sets f.closing under f.mu before closing f.stop, so a teardown
// that wins the race is always visible here.
func (f *feed) beginDisconnect() (subs []*subscription, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closing || f.disconnected {
		return nil, false
	}

	f.connected = false
	f.disconnected = true

	return f.snapshotLocked(), true
}

// beginResync marks the feed connected again, clears f.disconnected so the
// next real loss can emit once more, and returns the subscribers to receive
// OpResync. Called after every successful (re)open of the change stream.
//
// Like beginDisconnect it is atomic with teardown: ok is false once f.closing
// is set, so a (re)open that completes just as Close lands announces nothing.
// A resync emitted then would tell the engine to reconcile a scope whose feed
// is already gone.
func (f *feed) beginResync() (subs []*subscription, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closing {
		return nil, false
	}

	f.connected = true
	f.disconnected = false

	return f.snapshotLocked(), true
}

// zeroFeed returns the zero-scope feed, creating it when Subscribe runs before
// Start. Callers must NOT hold feedsMu.
func (s *Store) zeroFeed() (*feed, error) {
	s.feedsMu.Lock()
	defer s.feedsMu.Unlock()

	return s.zeroFeedLocked()
}

// zeroFeedLocked refuses to hand out — or resurrect — the zero-scope feed once
// Close has begun. The caller MUST hold Store.feedsMu, which is what makes the
// check atomic with the map walk in stopFeeds: a Start or a Subscribe that
// already passed the s.closed check would otherwise re-insert a slot into a
// shut-down store, and nothing would ever tear it down again.
func (s *Store) zeroFeedLocked() (*feed, error) {
	if s.closing {
		return nil, store.ErrClosed
	}

	if f, ok := s.feeds[""]; ok {
		return f, nil
	}

	f := newFeed(store.Scope{}, s.coll)
	s.feeds[""] = f

	return f, nil
}

// acquireFeed returns the zero-scope feed and takes one reference on it,
// released by releaseFeed. Feeds are SHARED: a scope has exactly one change
// stream no matter how many subscribers it has.
func (s *Store) acquireFeed() (*feed, error) {
	s.feedsMu.Lock()
	defer s.feedsMu.Unlock()

	f, err := s.zeroFeedLocked()
	if err != nil {
		return nil, err
	}

	f.refs++

	return f, nil
}

// releaseFeed drops one reference. When the last one goes, a NAMED feed leaves
// the map and its reader is stopped. The zero-scope feed is exempt: Start owns
// it and it must survive an empty subscriber map. Deciding under feedsMu is
// what stops a concurrent Subscribe from attaching to a feed that is being torn
// down.
func (s *Store) releaseFeed(f *feed) {
	s.feedsMu.Lock()

	f.refs--

	if f.scope.Tenant == "" || f.refs > 0 || s.feeds[f.scope.Tenant] != f {
		s.feedsMu.Unlock()

		return
	}

	delete(s.feeds, f.scope.Tenant)
	s.feedsMu.Unlock()

	s.stopFeed(f)
}

// Subscribe registers fn to be invoked for every change event in scope. The
// returned unsubscribe func removes fn from the dispatch list.
//
// The zero scope in multi-tenant mode returns
// store.ErrNotSupportedInMultiTenant: every method there resolves a per-call
// tenant database, so there is no shared process-wide changefeed to attach to.
// A named tenant scope is refused the same way for now.
//
// The subscription lives for the lifetime of ctx: when ctx is cancelled the
// callback is removed and the feed released, exactly as if unsubscribe had been
// called.
func (s *Store) Subscribe(ctx context.Context, scope store.Scope, fn func(store.Event)) (func(), error) {
	if s == nil || s.isClosed() {
		return nil, store.ErrClosed
	}

	if s.cfg.MultiTenantEnabled || scope.Tenant != "" {
		return nil, store.ErrNotSupportedInMultiTenant
	}

	// Checked before any feed work: a nil callback must never open a stream.
	if fn == nil {
		return func() {}, nil
	}

	f, err := s.acquireFeed()
	if err != nil {
		return nil, err
	}

	sub := &subscription{fn: fn}

	f.mu.Lock()
	f.nextID++
	id := f.nextID
	f.subs[id] = sub
	f.mu.Unlock()

	// cancelCh stops the ctx observer below. Every teardown action — removing
	// the callback, releasing the feed, stopping the observer — runs inside one
	// sync.Once, so a caller-driven unsubscribe racing ctx cancellation can
	// never double-release the feed or double-close the channel.
	cancelCh := make(chan struct{})

	var once sync.Once

	teardown := func() {
		once.Do(func() {
			f.mu.Lock()
			delete(f.subs, id)
			f.mu.Unlock()

			close(cancelCh)

			s.releaseFeed(f)
		})
	}

	// The observer exists only to watch a ctx that CAN end: a nil ctx would
	// panic on ctx.Done(), and context.Background() has no Done channel at all,
	// so both get no goroutine and the same unsubscribe func.
	//
	// s.closedCh is the third arm, and it is what bounds the observer's life by
	// the STORE's: a subscription whose ctx outlives the store (an engine root
	// ctx) would otherwise keep this goroutine — and the feed graph it closes
	// over — parked forever after Close. Teardown runs on that arm too, so the
	// subscriber slot goes with it.
	if ctx != nil && ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				teardown()
			case <-s.closedCh:
				teardown()
			case <-cancelCh:
			}
		}()
	}

	return teardown, nil
}

// startListener opens the zero-scope feed's change stream and launches its
// reader. The stream is established BEFORE this returns, so a caller that
// writes immediately after Start observes its own write, and an unreachable
// MongoDB surfaces as a Start error instead of looping in the background.
//
// It publishes through publishFeed for the same reason a tenant creator would:
// the stream is opened outside the feeds-map lock, so a Close that lands
// meanwhile must be able to throw it away instead of inheriting a live cursor
// and a reader goroutine no later Close will ever stop.
//
// Polling mode has no stream to open: its reader is launched the same way with
// a nil stream, and the poll loop attaches per tick.
func (s *Store) startListener(ctx context.Context) error {
	f, err := s.zeroFeed()
	if err != nil {
		return err
	}

	f.mu.Lock()
	running := f.done != nil
	f.mu.Unlock()

	if running {
		return nil
	}

	if s.cfg.PollInterval > 0 {
		return s.publishFeed(ctx, f, nil)
	}

	stream, err := s.openWatch(ctx, f)
	if err != nil {
		return err
	}

	return s.publishFeed(ctx, f, stream)
}

// openWatch opens one change stream on the feed's collection, bounded by
// watchTimeout. Inheriting the caller's ctx unbounded is what would let an
// unreachable MongoDB park Start for the life of that ctx, and with it the
// reserved feed slot.
//
// fullDocument: updateLookup is set here, once, so insert / update / replace
// all carry the after-image. No resume token is used: OpResync after every
// (re)open is what covers the gap instead.
//
// The ctx bounds the OPEN only — iteration runs on the reader's own ctx, which
// the driver takes per call to Next.
func (s *Store) openWatch(ctx context.Context, f *feed) (*mongo.ChangeStream, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{
				{Key: "$in", Value: bson.A{"insert", "update", "replace", operationTypeDelete}},
			}},
		}}},
	}

	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	watchCtx, cancel := context.WithTimeout(ctx, watchTimeout)
	defer cancel()

	stream, err := f.coll.Watch(watchCtx, pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("systemplane/mongodb: watch%s: %w", f.label(), err)
	}

	s.logInfo(ctx, "change stream established",
		log.String("collection", s.cfg.Collection),
		log.String("tenant", f.scope.Tenant),
	)

	return stream, nil
}

// publishFeed hands the opened feed to its waiters — or throws the stream away
// when Close ran while this creator was still opening it. The recheck is the
// only thing standing between a shut-down store and a live cursor, because the
// slot Close found in the map carried nothing it could stop.
//
// Publishing the feed and launching its reader happen in ONE feedsMu hold:
// Close decides what to tear down by walking that map, so a feed must never be
// visible there without its goroutine already running, or Close would wait the
// full closeTimeout on a done channel nothing will ever close.
func (s *Store) publishFeed(ctx context.Context, f *feed, stream *mongo.ChangeStream) error {
	s.feedsMu.Lock()

	if s.closing {
		cause := s.failLocked(f, store.ErrClosed)

		f.closeReadyLocked()
		s.feedsMu.Unlock()

		closeStream(ctx, stream)

		return cause
	}

	s.startFeedReader(f, stream)
	f.closeReadyLocked()
	s.feedsMu.Unlock()

	return nil
}

// closeReadyLocked wakes every waiter, exactly once. The caller MUST hold
// Store.feedsMu. The zero-scope feed has no ready channel — Start owns it and
// nobody waits on it.
func (f *feed) closeReadyLocked() {
	if f.ready == nil || f.readyClosed {
		return
	}

	f.readyClosed = true

	close(f.ready)
}

// failLocked records the cause a waiter will see and retracts the dead slot.
// The caller MUST hold Store.feedsMu, and MUST close ready afterwards in the
// same hold: err is written BEFORE the close, so the close is the
// happens-before edge that publishes it, and the slot is gone BEFORE the
// waiters wake, so the next Subscribe for that tenant builds a fresh
// placeholder instead of finding a corpse. The FIRST cause wins — once ready
// is closed err is never written again, which is what keeps a waiter's read
// race-free.
func (s *Store) failLocked(f *feed, err error) error {
	if f.err == nil {
		f.err = err
	}

	if s.feeds[f.scope.Tenant] == f {
		delete(s.feeds, f.scope.Tenant)
	}

	return f.err
}

// startFeedReader records the reader's done channel and launches it. done is
// what stopFeed waits on, so it must be recorded before the goroutine starts.
// A nil stream means polling mode.
func (s *Store) startFeedReader(f *feed, stream *mongo.ChangeStream) {
	done := make(chan struct{})

	f.mu.Lock()
	f.done = done
	f.mu.Unlock()

	go func() {
		defer close(done)
		defer runtime.RecoverAndLog(s.cfg.Logger, "systemplane.mongodb.listener")

		if stream == nil {
			s.pollForever(f)

			return
		}

		s.runFeed(f, stream)
	}()
}

// runFeed is THE change-stream loop, for every scope. stream is the
// already-open first cursor. Per stream it emits, in order:
// store.Event{Scope: f.scope, Op: store.OpResync}, then the decoded events from
// that cursor, then — on loss, before the first reopen attempt — exactly one
// store.Event{Scope: f.scope, Op: store.OpDisconnect}. Then it repeats through
// the existing backoff.
func (s *Store) runFeed(f *feed, stream *mongo.ChangeStream) {
	attempt := 0

	for {
		if subs, ok := f.beginResync(); ok {
			s.broadcast(f, subs, store.Event{Scope: f.scope, Op: store.OpResync})
		}

		s.consumeUntilFailure(f, stream)

		if subs, ok := f.beginDisconnect(); ok {
			s.broadcast(f, subs, store.Event{Scope: f.scope, Op: store.OpDisconnect})
		}

		closeStream(context.Background(), stream)

		select {
		case <-f.stop:
			return
		default:
		}

		var err error

		stream, err = s.reopenWatch(f, &attempt)
		if err != nil {
			return
		}
	}
}

// consumeUntilFailure drains one cursor until it dies or teardown closes
// f.stop. It never reports the difference: the f.closing check inside
// beginDisconnect is what keeps a clean shutdown from announcing an outage, and
// the ctx below is what keeps the shutdown from being logged as a failure.
func (s *Store) consumeUntilFailure(f *feed, stream *mongo.ChangeStream) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-f.stop:
			cancel()
		case <-ctx.Done():
		}
	}()

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

		f.dispatch(s.cfg.Logger, evt)
	}

	if err := stream.Err(); err != nil && ctx.Err() == nil {
		s.logDebug(ctx, "change stream read failed",
			log.Err(err),
			log.String("tenant", f.scope.Tenant),
		)
	}
}

// reopenWatch retries the open until it succeeds or teardown stops the feed.
// attempt is reset by a successful reopen, so a feed that flaps does not
// inherit the previous outage's backoff.
func (s *Store) reopenWatch(f *feed, attempt *int) (*mongo.ChangeStream, error) {
	if *attempt == 0 {
		s.logWarn(context.Background(), "change stream disconnected, reconnecting",
			log.Int("attempt", *attempt),
			log.String("tenant", f.scope.Tenant),
		)
	}

	for {
		select {
		case <-f.stop:
			return nil, errFeedStopped
		default:
		}

		delay := min(backoff.ExponentialWithJitter(reconnectBaseDelay, *attempt), reconnectMaxDelay)
		*attempt++

		select {
		case <-f.stop:
			return nil, errFeedStopped
		case <-time.After(delay):
		}

		stream, err := s.openWatch(context.Background(), f)
		if err != nil {
			s.logDebug(context.Background(), "change stream reopen failed",
				log.Err(err),
				log.String("tenant", f.scope.Tenant),
			)

			continue
		}

		*attempt = 0

		return stream, nil
	}
}

// closeStream closes a dead or abandoned cursor on a ctx of its own: the one
// that just died would abandon the server-side cursor instead of closing it.
func closeStream(ctx context.Context, stream *mongo.ChangeStream) {
	if stream == nil {
		return
	}

	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()

	_ = stream.Close(closeCtx)
}

// stopFeeds tears down every feed the store owns. Idempotent.
//
// The store-wide closing flag is raised in the SAME hold that walks the map, so
// no creator can publish a feed into a shut-down store afterwards. A slot whose
// creator is still opening its stream has no reader to stop and no cursor to
// close yet: it is failed with store.ErrClosed here — so every caller parked on
// it learns the store is gone instead of waiting for a feed that will never
// arrive — and its creator closes the stream it ends up with on its own.
func (s *Store) stopFeeds() {
	s.feedsMu.Lock()
	s.closing = true
	feeds := make([]*feed, 0, len(s.feeds))

	for _, f := range s.feeds {
		if f.ready != nil && !f.readyClosed {
			_ = s.failLocked(f, store.ErrClosed)

			f.closeReadyLocked()

			continue
		}

		feeds = append(feeds, f)
	}

	clear(s.feeds)
	s.feedsMu.Unlock()

	// Signal every feed FIRST, then wait on all of them against one shared
	// deadline. Stopping them one at a time costs tenants x closeTimeout, so
	// six unresponsive tenants alone overrun the engine's 30s Close budget and,
	// behind that, the pod's termination grace period — the process is killed
	// mid-shutdown instead of closing its cursors.
	waits := make([]<-chan struct{}, 0, len(feeds))

	for _, f := range feeds {
		if done := s.signalFeed(f, false); done != nil {
			waits = append(waits, done)
		}
	}

	if len(waits) == 0 {
		return
	}

	deadline := time.NewTimer(closeTimeout)
	defer deadline.Stop()

	for _, done := range waits {
		select {
		case <-done:
		case <-deadline.C:
			return
		}
	}
}

// signalFeed marks the feed closing under f.mu BEFORE closing f.stop, so the
// reader's beginDisconnect can never announce a disconnect for a shutdown. It
// returns the channel to wait on, or nil when there is nothing to wait for:
// the feed was already stopped, or it never had a reader.
//
// skipSelfWait is the self-teardown escape, and ONLY the last-unsubscribe path
// sets it. A callback can drop its scope's last subscription from inside the
// reader's own delivery, and waiting for the reader there is waiting on the
// goroutine doing the waiting — it can only end at closeTimeout, with the feed
// frozen meanwhile. f.dispatching cannot tell "the caller IS the reader" from
// "some other goroutine is mid-callback", so Close never skips on it: a Close
// that did would return with a live cursor and a live reader behind it whenever
// a subscriber happened to be running.
func (s *Store) signalFeed(f *feed, skipSelfWait bool) <-chan struct{} {
	f.mu.Lock()

	if f.closing {
		f.mu.Unlock()

		return nil
	}

	f.closing = true
	done := f.done
	selfTeardown := skipSelfWait && f.dispatching > 0

	f.mu.Unlock()

	close(f.stop)

	if selfTeardown {
		s.logDebug(context.Background(), "changefeed torn down from inside a callback; not waiting for its reader",
			log.String("tenant", f.scope.Tenant),
		)

		return nil
	}

	return done
}

// stopFeed signals one feed and waits up to closeTimeout for its reader to
// exit. Used by the last unsubscribe of a tenant feed; Close signals all of its
// feeds before waiting on any of them.
func (s *Store) stopFeed(f *feed) {
	done := s.signalFeed(f, true)
	if done == nil {
		return
	}

	select {
	case <-done:
	case <-time.After(closeTimeout):
	}
}

// nsKey is the polling-mode set element used to detect deletes. We can't
// rely on _id (compound subdocument) as a map key directly, so we tuple it.
type nsKey struct {
	Namespace string
	Key       string
}

func (s *Store) pollForever(f *feed) {
	// Watermark anchored slightly in the past so the first tick observes any
	// row that already exists. Deduplication is keyed on (namespace, key)
	// pairs already emitted at the current watermark boundary.
	watermark := time.Now().UTC().Truncate(time.Millisecond)
	// known tracks the set of (namespace, key) tuples observed by the most
	// recent full poll. Anything present last time but absent now is a
	// delete that we synthesize an OpDelete event for.
	known := make(map[nsKey]struct{})
	// seenAtWatermark holds the ids whose updated_at equals the current
	// watermark, paired with a content hash of the value we emitted for them.
	// Two consecutive polls observing the same (ns, key) at the boundary skip
	// re-emission only when the hash also matches — otherwise the second
	// write (same key, same ms, different value) would be silently swallowed
	// and never reach peer caches. See seenEntry / boundaryDedupHit.
	seenAtWatermark := make(map[nsKey]seenEntry)
	// firstPoll suppresses delete synthesis on the very first iteration
	// (when `known` is empty by construction) and primes the watermark from
	// the maximum updated_at observed during the snapshot scan.
	firstPoll := true

	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.stop:
			return
		case <-ticker.C:
			newWatermark, currentKnown, newSeen, err := s.pollOnce(f, watermark, seenAtWatermark, known, firstPoll)
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
//   - Emits OpUpsert for every doc with updated_at >= watermark unless it is
//     an idempotent rewrite at the watermark boundary (same (ns, key), same
//     boundary millisecond, AND same value-hash as what was emitted last
//     round). Same key at the same ms with a different value is a real new
//     write and IS emitted — otherwise peer caches would stay stale until a
//     later, strictly-newer write advances the watermark.
//   - Performs a full collection scan to capture the current key set;
//     anything present in `prevKnown` but absent now becomes a synthesized
//     OpDelete event. Skipped on the first iteration (prevKnown empty).
//
// Returns the new watermark, the new full known set, and the new
// seenAtWatermark set (ids that touched the boundary millisecond, with their
// value-hash content discriminator).
func (s *Store) pollOnce(
	f *feed,
	watermark time.Time,
	prevSeenAtWatermark map[nsKey]seenEntry,
	prevKnown map[nsKey]struct{},
	firstPoll bool,
) (time.Time, map[nsKey]struct{}, map[nsKey]seenEntry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Use >= so two writes that land in the same millisecond as the previous
	// boundary aren't silently skipped; dedup via prevSeenAtWatermark with
	// a value-hash discriminator (see seenEntry).
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
	newSeen := make(map[nsKey]seenEntry)

	for cur.Next(ctx) {
		var doc entryDoc
		if err := cur.Decode(&doc); err != nil {
			s.logWarn(ctx, "poll decode error, skipping document", log.Err(err))

			continue
		}

		nk := nsKey{Namespace: doc.Namespace, Key: doc.Key}
		valueHash := hashValue(doc.Value)
		atBoundary := doc.UpdatedAt.Equal(watermark)

		// Dedup: only skip when the same (ns, key) was emitted at this
		// boundary millisecond AND the value digest matches. Same key, same
		// ms, different value is a real write that MUST re-emit — otherwise
		// peer caches stay stale until a strictly newer write advances the
		// watermark.
		if boundaryDedupHit(prevSeenAtWatermark, nk, atBoundary, valueHash) {
			// Preserve the entry in newSeen so the next poll round still
			// dedupes against it (we re-observe the same row again via $gte
			// until the watermark advances).
			newSeen[nk] = seenEntry{valueHash: valueHash}

			continue
		}

		f.dispatch(s.cfg.Logger, store.Event{
			Namespace: doc.Namespace,
			Key:       doc.Key,
			Op:        store.OpUpsert,
		})

		switch {
		case doc.UpdatedAt.After(newWatermark):
			newWatermark = doc.UpdatedAt
			newSeen = map[nsKey]seenEntry{nk: {valueHash: valueHash}}
		case doc.UpdatedAt.Equal(newWatermark):
			newSeen[nk] = seenEntry{valueHash: valueHash}
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
				f.dispatch(s.cfg.Logger, store.Event{
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
