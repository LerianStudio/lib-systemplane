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
	backoffBase  = 500 * time.Millisecond
	backoffCap   = 30 * time.Second
	closeTimeout = 5 * time.Second
)

// notifyPayload is the JSON shape emitted by the systemplane_notify_v3 trigger.
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
	ready chan struct{}
	err   error

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
func (f *feed) beginResync() (subs []*subscription) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.connected = true
	f.disconnected = false

	return f.snapshotLocked()
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
func (s *Store) zeroFeed() *feed {
	s.feedsMu.Lock()
	defer s.feedsMu.Unlock()

	return s.zeroFeedLocked()
}

func (s *Store) zeroFeedLocked() *feed {
	if f, ok := s.feeds[""]; ok {
		return f
	}

	f := newFeed(store.Scope{}, s.cfg.ListenDSN)
	s.feeds[""] = f

	return f
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
		f := s.zeroFeedLocked()
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

	s.startFeedReader(f, conn)

	close(f.ready)

	return nil
}

// retractFeed publishes a creation failure to every waiter and removes the dead
// slot. The order is load-bearing in both directions: err is written BEFORE the
// close, so the close is the happens-before edge that publishes it, and the
// slot is gone BEFORE the waiters wake, so the next Subscribe for that tenant
// builds a fresh placeholder instead of finding a corpse.
func (s *Store) retractFeed(f *feed, err error) error {
	f.err = err

	s.feedsMu.Lock()

	if s.feeds[f.scope.Tenant] == f {
		delete(s.feeds, f.scope.Tenant)
	}

	s.feedsMu.Unlock()

	close(f.ready)

	return err
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

	// A nil ctx would panic on ctx.Done(); such callers simply get no observer
	// and the same unsubscribe func.
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

// openListen opens a dedicated pgx connection for the feed's DSN and installs
// LISTEN on it synchronously, so a bad DSN or a missing privilege surfaces to
// the caller instead of looping in the background.
func (s *Store) openListen(ctx context.Context, f *feed) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, f.dsn)
	if err != nil {
		return nil, fmt.Errorf("systemplane/postgres: listen connect%s: %w", f.label(), err)
	}

	if _, err := conn.Exec(ctx, "LISTEN "+quoteIdentifier(s.cfg.Channel)); err != nil {
		_ = conn.Close(ctx)

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
func (s *Store) startListener(ctx context.Context) error {
	f := s.zeroFeed()

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

	s.startFeedReader(f, conn)

	return nil
}

// stopFeeds tears down every feed the store owns. Idempotent.
func (s *Store) stopFeeds() {
	s.feedsMu.Lock()
	feeds := make([]*feed, 0, len(s.feeds))

	for _, f := range s.feeds {
		feeds = append(feeds, f)
	}

	clear(s.feeds)
	s.feedsMu.Unlock()

	for _, f := range feeds {
		s.stopFeed(f)
	}
}

// stopFeed marks the feed closing under f.mu BEFORE closing f.stop, so the
// reader's beginDisconnect can never announce a disconnect for a shutdown,
// then waits up to closeTimeout for the reader to exit.
func (s *Store) stopFeed(f *feed) {
	f.mu.Lock()

	if f.closing {
		f.mu.Unlock()

		return
	}

	f.closing = true
	done := f.done
	f.mu.Unlock()

	close(f.stop)

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
		s.broadcast(f.beginResync(), store.Event{Scope: f.scope, Op: store.OpResync})

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
				s.logDebug(ctx, "LISTEN wait failed", log.Err(err))
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

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		conn, err := pgx.Connect(ctx, f.dsn)

		cancel()

		if err != nil {
			s.logDebug(context.Background(), "reconnect attempt failed", log.Err(err))

			continue
		}

		listenCtx, listenCancel := context.WithTimeout(context.Background(), 5*time.Second)

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
