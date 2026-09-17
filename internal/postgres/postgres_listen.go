// LISTEN/NOTIFY changefeeds for the Postgres backend.
//
// One feed per scope: the zero-scope feed is opened by Start and lives until
// Close. Multi-tenant deployments resolve a fresh database on every call, so
// the zero scope has no durable DSN to LISTEN on there and Subscribe returns
// store.ErrNotSupportedInMultiTenant. A NAMED tenant scope is different: its
// DSN comes from the tenant connector, so the first Subscribe for that tenant
// opens a dedicated LISTEN connection, every later subscriber shares it, and
// the last one to leave closes it.
package postgres

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
	"github.com/jackc/pgx/v5"
)

const (
	backoffBase = 500 * time.Millisecond
	backoffCap  = 30 * time.Second
)

// Bounds on one changefeed connection attempt and on shutdown. EVERY connect
// is bounded by the same pair — the first one as much as a reconnect — so an
// unreachable tenant fails its Subscribe within connectTimeout instead of
// pinning the reserved feed slot for as long as the caller's ctx happens to
// live. They are vars, not consts, only so the unit tests can shrink them;
// nothing in production writes them.
var (
	connectTimeout = 10 * time.Second
	listenTimeout  = 5 * time.Second
	closeTimeout   = 5 * time.Second
)

// notifyPayload is the JSON shape emitted by the systemplane_notify_v4 trigger.
//
// Revision is optional: a v3 trigger omits it and the event carries revision 0
// ("unknown"), which the engine never deduplicates.
type notifyPayload struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Op        string `json:"op"`
	Revision  int64  `json:"revision"`
}

// feed is one changefeed for one scope. The zero-scope feed is created by
// Start and lives until Close; a named-tenant feed is created by the first
// Subscribe for that tenant and torn down when its last subscriber leaves.
type feed struct {
	scope store.Scope
	dsn   string

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
	connected    bool // true between a successful LISTEN and the loss of that connection
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
// whole of fn, which is what lets Subscribe emit the joining subscriber's
// OpResync without racing the reader goroutine.
type subscription struct {
	mu sync.Mutex
	fn func(store.Event)
}

// newFeed builds an unconnected feed. done stays nil until its reader
// goroutine launches, which is what marks the feed as running.
func newFeed(scope store.Scope, dsn string) *feed {
	return &feed{
		scope: scope,
		dsn:   dsn,
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

// snapshotLocked copies the subscriber set so it can be fanned out to after
// f.mu is released. The caller MUST already hold f.mu.
func (f *feed) snapshotLocked() []*subscription {
	subs := make([]*subscription, 0, len(f.subs))

	for _, sub := range f.subs {
		subs = append(subs, sub)
	}

	return subs
}

// beginDisconnect decides, atomically with any concurrent teardown, whether
// this connection loss must emit OpDisconnect to the returned subscribers.
// It is EDGE-TRIGGERED: ok is true only on the connected→disconnected
// transition. ok is false when f.closing is already set (clean shutdown, see
// below) or when f.disconnected is already set (a reconnect attempt failed
// while the feed was already known to be down — one outage, one disconnect).
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
// OpResync. Called after every successful (re)LISTEN.
//
// Like beginDisconnect it is atomic with teardown: ok is false once f.closing
// is set, so a (re)connect that completes just as Close lands announces
// nothing. A resync emitted then would tell the engine to reconcile a scope
// whose feed is already gone.
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

// deliverLocked runs fn under runtime.RecoverAndLog. The caller MUST already
// hold sub.mu; deliver is the variant that takes it. Both routes are
// panic-safe, and every caller unlocks through defer, so a panicking callback
// can never leave sub.mu held.
func (sub *subscription) deliverLocked(logger log.Logger, evt store.Event) {
	defer runtime.RecoverAndLog(logger, "systemplane.postgres.handler")

	sub.fn(evt)
}

func (sub *subscription) deliver(logger log.Logger, evt store.Event) {
	sub.mu.Lock()
	defer sub.mu.Unlock()

	sub.deliverLocked(logger, evt)
}

// broadcast fans a synthesized marker (OpResync / OpDisconnect) out to an
// already-taken snapshot of subscribers, outside f.mu.
func (s *Store) broadcast(subs []*subscription, evt store.Event) {
	for _, sub := range subs {
		sub.deliver(s.cfg.Logger, evt)
	}
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

	f := newFeed(store.Scope{}, s.cfg.ListenDSN)
	s.feeds[""] = f

	return f, nil
}

// acquireFeed returns the live feed for scope — creating it when this caller is
// the first to ask for that tenant — and takes one reference on it, released by
// releaseFeed. Feeds are SHARED: a tenant has exactly one LISTEN connection no
// matter how many subscribers it has.
//
// The feeds-map lock is never held across pgx.Connect, or one unreachable
// tenant would freeze every other tenant's Subscribe. The creator instead
// reserves the map slot with an unconnected placeholder, connects outside the
// lock, and then publishes or retracts it.
func (s *Store) acquireFeed(ctx context.Context, scope store.Scope) (*feed, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	s.feedsMu.Lock()

	if scope.Tenant == "" {
		f, err := s.zeroFeedLocked()
		if err != nil {
			s.feedsMu.Unlock()

			return nil, err
		}

		f.refs++
		s.feedsMu.Unlock()

		return f, nil
	}

	if f, ok := s.feeds[scope.Tenant]; ok {
		f.refs++
		s.feedsMu.Unlock()

		return s.awaitFeed(ctx, f)
	}

	f := newFeed(scope, "")
	f.ready = make(chan struct{})
	f.refs = 1
	s.feeds[scope.Tenant] = f
	s.feedsMu.Unlock()

	if err := s.createFeed(ctx, f); err != nil {
		return nil, err
	}

	return f, nil
}

// awaitFeed blocks until the creator publishes or retracts the feed.
//
// A creation failure reaches EVERY waiter carrying the creator's own cause: a
// waiter that blocked until its own ctx died instead would strand the caller.
// It is never retried here — the slot is already gone, so the next Subscribe
// for that tenant builds a fresh placeholder. On ctx cancellation the waiter
// leaves the creator alone; the creator finishes or retracts on its own.
func (s *Store) awaitFeed(ctx context.Context, f *feed) (*feed, error) {
	select {
	case <-f.ready:
		if f.err != nil {
			s.releaseFeed(f)

			return nil, f.err
		}

		return f, nil
	case <-ctx.Done():
		s.releaseFeed(f)

		return nil, ctx.Err()
	}
}

// createFeed resolves the tenant's DSN and opens its first LISTEN connection
// synchronously, so an unreachable tenant fails the Subscribe call instead of
// looping in the background, then publishes the feed to every waiter.
func (s *Store) createFeed(ctx context.Context, f *feed) error {
	tenant := f.scope.Tenant

	dsn, err := s.cfg.Connector.ResolveDSN(ctx, tenant)
	if err != nil {
		return s.retractFeed(f, fmt.Errorf("systemplane/postgres: resolve tenant %s DSN: %w", tenant, err))
	}

	// An empty DSN reported with a nil error is a connector bug; refuse it here
	// rather than hand pgx a string it cannot dial.
	if dsn == "" {
		return s.retractFeed(f, fmt.Errorf("systemplane/postgres: resolve tenant %s DSN: %w", tenant, store.ErrTenantConnectorMissing))
	}

	f.dsn = dsn

	conn, err := s.openListen(ctx, f)
	if err != nil {
		return s.retractFeed(f, err)
	}

	return s.publishFeed(ctx, f, conn)
}

// publishFeed hands the connected feed to its waiters — or throws it away when
// Close ran while this creator was still connecting. The recheck is the only
// thing standing between a shut-down store and a live LISTEN connection,
// because the slot Close found in the map carried nothing it could stop.
//
// Publishing the feed and launching its reader happen in ONE feedsMu hold:
// Close decides what to tear down by walking that map, so a feed must never be
// visible there without its goroutine already running, or Close would wait the
// full closeTimeout on a done channel nothing will ever close.
func (s *Store) publishFeed(ctx context.Context, f *feed, conn *pgx.Conn) error {
	s.feedsMu.Lock()

	if s.closing {
		cause := s.failLocked(f, store.ErrClosed)

		f.closeReadyLocked()
		s.feedsMu.Unlock()

		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
		defer cancel()

		_ = conn.Close(closeCtx)

		return cause
	}

	s.startFeedReader(f, conn)
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

// retractFeed publishes a creation failure to every waiter and removes the dead
// slot.
func (s *Store) retractFeed(f *feed, err error) error {
	s.feedsMu.Lock()
	defer s.feedsMu.Unlock()

	cause := s.failLocked(f, err)

	f.closeReadyLocked()

	return cause
}

// releaseFeed drops one reference. When the last one goes, a NAMED feed leaves
// the map and its reader is stopped — so a later Subscribe for that tenant
// resolves the DSN again and picks up a credentials rotation. The zero-scope
// feed is exempt: Start owns it and it must survive an empty subscriber map.
// Deciding under feedsMu is what stops a concurrent Subscribe from attaching to
// a feed that is being torn down.
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
// returned unsubscribe func removes fn from the dispatch list, and the last
// subscriber to leave a tenant scope closes that tenant's LISTEN connection.
//
// A subscriber that joins an already-connected feed receives its own
// store.OpResync before any key event: it missed everything published before
// it joined, and the engine subscribes after Start has already connected, so
// without it a quiet scope would never reconcile. Joining while the feed is
// down emits nothing — the scope is legitimately stale, and the next
// successful (re)connect broadcasts one.
//
// The zero scope in multi-tenant mode returns
// store.ErrNotSupportedInMultiTenant: every method there resolves a per-call
// tenant database, so there is no shared process-wide changefeed to attach to.
// A named tenant scope is served regardless of the mode — it resolves its own
// LISTEN DSN through the connector — and is refused with
// store.ErrTenantConnectorMissing when the Store was built without one.
//
// The subscription lives for the lifetime of ctx: when ctx is cancelled the
// callback is removed and the feed released, exactly as if unsubscribe had been
// called, so a cancelled scope never leaks a LISTEN connection.
func (s *Store) Subscribe(ctx context.Context, scope store.Scope, fn func(store.Event)) (func(), error) {
	if s == nil || s.isClosed() {
		return nil, store.ErrClosed
	}

	if scope.Tenant == "" {
		if s.cfg.MultiTenantEnabled {
			return nil, store.ErrNotSupportedInMultiTenant
		}
	} else if s.cfg.Connector == nil {
		return nil, store.ErrTenantConnectorMissing
	}

	// Checked before any feed work: a nil callback must never open a connection.
	if fn == nil {
		return func() {}, nil
	}

	f, err := s.acquireFeed(ctx, scope)
	if err != nil {
		return nil, err
	}

	sub := &subscription{fn: fn}

	// sub.mu is taken BEFORE the subscription becomes reachable and released
	// only when Subscribe returns. The reader goroutine can reach this
	// subscriber only after seeing it in f.subs, and any such delivery then
	// blocks here until the joining resync below has returned — so the joining
	// callback can never observe a key event before its own resync. The defer
	// is load-bearing on the panicking path: a manual Unlock skipped by an
	// unwinding callback would leave sub.mu held forever.
	sub.mu.Lock()
	defer sub.mu.Unlock()

	f.mu.Lock()
	f.nextID++
	id := f.nextID
	f.subs[id] = sub
	connected := f.connected
	f.mu.Unlock()

	// deliverLocked, never sub.fn directly: it puts the joining emission under
	// the same recovery guard as every reader-goroutine delivery, so a
	// panicking callback cannot escape through Subscribe to the caller. A
	// connection lost between the read above and this emission yields one
	// extra OpResync, which is harmless — a resync is idempotent.
	if connected {
		sub.deliverLocked(s.cfg.Logger, store.Event{Scope: f.scope, Op: store.OpResync})
	}

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

// openListen opens a dedicated pgx connection for the feed's DSN and installs
// LISTEN on it synchronously, so a bad DSN or a missing privilege surfaces to
// the caller instead of looping in the background.
//
// Both steps carry the same bounds a reconnect uses. Inheriting the caller's
// ctx unbounded is what would let a tenant whose host swallows packets — no
// refusal, no reset — park the Subscribe call for the life of that ctx, and
// with it the reserved feed slot every later Subscribe for that tenant waits
// on. The teardown interlock in publishFeed is only as tight as this bound.
func (s *Store) openListen(ctx context.Context, f *feed) (*pgx.Conn, error) {
	connectCtx, cancelConnect := context.WithTimeout(ctx, connectTimeout)
	defer cancelConnect()

	conn, err := pgx.Connect(connectCtx, f.dsn)
	if err != nil {
		return nil, fmt.Errorf("systemplane/postgres: listen connect%s: %w", f.label(), err)
	}

	listenCtx, cancelListen := context.WithTimeout(ctx, listenTimeout)
	defer cancelListen()

	if _, err := conn.Exec(listenCtx, "LISTEN "+quoteIdentifier(s.cfg.Channel)); err != nil {
		// The connection is closed on a ctx of its own: the one that just
		// expired would abandon the socket instead of closing it.
		closeCtx, cancelClose := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
		defer cancelClose()

		_ = conn.Close(closeCtx)

		return nil, fmt.Errorf("systemplane/postgres: listen%s: %w", f.label(), err)
	}

	s.logInfo(ctx, "LISTEN connection established",
		log.String("channel", s.cfg.Channel),
		log.String("tenant", f.scope.Tenant),
	)

	return conn, nil
}

// startFeedReader records the reader's done channel and launches it. done is
// what stopFeed waits on, so it must be recorded before the goroutine starts.
func (s *Store) startFeedReader(f *feed, conn *pgx.Conn) {
	done := make(chan struct{})

	f.mu.Lock()
	f.done = done
	f.mu.Unlock()

	go func() {
		defer close(done)
		defer runtime.RecoverAndLog(s.cfg.Logger, "systemplane.postgres.listener")

		s.runFeed(f, conn)
	}()
}

// startListener opens the zero-scope feed's dedicated pgx LISTEN connection
// and launches its reader. It synchronously verifies LISTEN was installed
// before returning, so callers can immediately observe events and a bad DSN
// surfaces as a Start error instead of looping in the background.
//
// It publishes through publishFeed for the same reason a tenant creator does:
// the connection is opened outside the feeds-map lock, so a Close that lands
// meanwhile must be able to throw it away instead of inheriting a live LISTEN
// connection and a reader goroutine no later Close will ever stop.
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

	conn, err := s.openListen(ctx, f)
	if err != nil {
		return err
	}

	return s.publishFeed(ctx, f, conn)
}

// stopFeeds tears down every feed the store owns. Idempotent.
//
// The store-wide closing flag is raised in the SAME hold that walks the map, so
// no creator can publish a feed into a shut-down store afterwards. A slot whose
// creator is still connecting has no reader to stop and no connection to close
// yet: it is failed with store.ErrClosed here — so every caller parked on it
// learns the store is gone instead of waiting for a feed that will never
// arrive — and its creator closes the connection it ends up with on its own.
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
	// mid-shutdown instead of closing its connections.
	waits := make([]<-chan struct{}, 0, len(feeds))

	for _, f := range feeds {
		if done := signalFeed(f); done != nil {
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
// the feed was already stopped, it never had a reader, or its reader is inside
// a callback right now and the caller may BE that goroutine.
func signalFeed(f *feed) <-chan struct{} {
	f.mu.Lock()

	if f.closing {
		f.mu.Unlock()

		return nil
	}

	f.closing = true
	done := f.done

	if f.dispatching > 0 {
		done = nil
	}

	f.mu.Unlock()

	close(f.stop)

	return done
}

// stopFeed signals one feed and waits up to closeTimeout for its reader to
// exit. Used by the last unsubscribe of a tenant feed; Close signals all of its
// feeds before waiting on any of them.
func (s *Store) stopFeed(f *feed) {
	done := signalFeed(f)
	if done == nil {
		return
	}

	select {
	case <-done:
	case <-time.After(closeTimeout):
	}
}

// runFeed is THE LISTEN loop, for every scope. conn is the already-connected,
// already-LISTENing first connection. Per connection it emits, in order:
// store.Event{Scope: f.scope, Op: store.OpResync}, then the decoded NOTIFY
// payloads from that connection, then — on connection loss, before the first
// reconnect attempt — exactly one
// store.Event{Scope: f.scope, Op: store.OpDisconnect}. Then it repeats
// through the existing backoff.
func (s *Store) runFeed(f *feed, conn *pgx.Conn) {
	attempt := 0

	for {
		if subs, ok := f.beginResync(); ok {
			s.broadcast(subs, store.Event{Scope: f.scope, Op: store.OpResync})
		}

		s.consumeUntilFailure(f, conn)

		if subs, ok := f.beginDisconnect(); ok {
			s.broadcast(subs, store.Event{Scope: f.scope, Op: store.OpDisconnect})
		}

		// Close the failed (or shutdown-time) connection before reconnect.
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), closeTimeout)

		_ = conn.Close(cleanCtx)

		cleanCancel()

		select {
		case <-f.stop:
			return
		default:
		}

		var err error

		conn, err = s.reconnect(f, &attempt)
		if err != nil {
			return
		}
	}
}

func (s *Store) consumeUntilFailure(f *feed, conn *pgx.Conn) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-f.stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.logDebug(ctx, "LISTEN wait failed",
					log.Err(err),
					log.String("tenant", f.scope.Tenant),
				)
			}

			return
		}

		evt, ok := parseNotifyPayload(notification.Payload)
		if !ok {
			s.logWarn(ctx, "failed to decode NOTIFY payload",
				log.String("payload", truncateString(notification.Payload, 200)),
			)

			continue
		}

		f.dispatch(s.cfg.Logger, evt)
	}
}

func (s *Store) reconnect(f *feed, attempt *int) (*pgx.Conn, error) {
	if *attempt == 0 {
		s.logWarn(context.Background(), "LISTEN connection lost, reconnecting",
			log.Int("attempt", *attempt),
			log.String("tenant", f.scope.Tenant),
		)
	}

	for {
		select {
		case <-f.stop:
			return nil, errors.New("stopped")
		default:
		}

		delay := min(backoff.ExponentialWithJitter(backoffBase, *attempt), backoffCap)
		*attempt++

		select {
		case <-f.stop:
			return nil, errors.New("stopped")
		case <-time.After(delay):
		}

		ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)

		conn, err := pgx.Connect(ctx, f.dsn)

		cancel()

		if err != nil {
			s.logDebug(context.Background(), "reconnect attempt failed",
				log.Err(err),
				log.String("tenant", f.scope.Tenant),
			)

			continue
		}

		listenCtx, listenCancel := context.WithTimeout(context.Background(), listenTimeout)

		_, err = conn.Exec(listenCtx, "LISTEN "+quoteIdentifier(s.cfg.Channel))

		listenCancel()

		if err != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), closeTimeout)

			_ = conn.Close(closeCtx)

			closeCancel()

			continue
		}

		*attempt = 0

		return conn, nil
	}
}
