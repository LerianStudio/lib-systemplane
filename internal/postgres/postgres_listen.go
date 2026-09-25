// LISTEN/NOTIFY changefeeds for the Postgres backend.
//
// One feed per scope: the zero-scope feed is opened by Start and lives until
// Close. Multi-tenant deployments resolve a fresh database on every call, so
// the zero scope has no durable DSN to LISTEN on there and Subscribe returns
// store.ErrNotSupportedInMultiTenant. A NAMED tenant scope is different: its
// DSN comes from the tenant connector, so the first Subscribe for that tenant
// opens a dedicated LISTEN connection, every later subscriber shares it, and
// the last one to leave closes it.
//
// NOTIFY is database-scoped and every feed listens on the same channel name,
// so the DSN resolved for a tenant MUST name a database no other tenant
// shares. Two tenants sharing one database (schema-per-tenant through a
// search_path in the DSN) would each receive the other's notifications
// stamped with their OWN scope, and the engine's revision fence would treat
// the other tenant's revision as authoritative. Values never cross — every
// read re-resolves through the tenant's own handle — but notifications and
// revisions would. The single-tenant ListenDSN is held to the same rule: two
// installations pinned to different schemas of one database cross-contaminate
// the same way.
//
// That rule is enforced where it is decidable: a feed whose DSN names a
// database another live feed already listens on is refused with
// ErrSharedDatabaseUnsupported, which is what schema-per-tenant produces
// inside one process. Two processes sharing one database cannot see each
// other, so one database per install remains the operator's responsibility
// beyond this one.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/LerianStudio/lib-commons/v7/commons/backoff"
	obsconstants "github.com/LerianStudio/lib-observability/v4/constants"
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

	// dbKey names the physical database this feed listens on, written once by
	// publishFeed and guarded by Store.feedsMu — the lock that also decides
	// which feeds are live. Empty until the feed is published, so a reserved
	// slot still connecting never collides with anything.
	dbKey string

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

// joiningOpLocked reports the marker a subscriber joining right now must be
// told, or "" when the feed has announced nothing yet and the reader's first
// announcement will reach this subscriber instead. The caller MUST hold f.mu,
// in the same hold that adds the subscriber.
//
// f.disconnected, not !f.connected: the latter is also true of a feed whose
// reader has not reached its first resync, and announcing a disconnect there
// would either double the imminent resync or precede it for no reason.
func (f *feed) joiningOpLocked() string {
	switch {
	case f.connected:
		return store.OpResync
	case f.disconnected:
		return store.OpDisconnect
	default:
		return ""
	}
}

// recoveryComponent is the component every panic recovered in this package is
// counted under on panic_recovered_total; the goroutine_name label says which
// site recovered it.
const recoveryComponent = "systemplane.postgres"

// deliverLocked runs fn under panic recovery (logged and counted; no span, the
// Subscribe ctx never carries one). The caller MUST hold sub.mu and unlock it
// through defer; deliver is the variant that takes it.
func (sub *subscription) deliverLocked(logger log.Logger, evt store.Event) {
	defer runtime.RecoverAndLogWithContext(context.Background(), logger, recoveryComponent, "handler")

	sub.fn(evt)
}

func (sub *subscription) deliver(logger log.Logger, evt store.Event) {
	sub.mu.Lock()
	defer sub.mu.Unlock()

	sub.deliverLocked(logger, evt)
}

// broadcast fans a synthesized marker (OpResync / OpDisconnect) out to an
// already-taken snapshot of subscribers, outside f.mu.
//
// The window is marked on the feed for the same reason feed.dispatch marks a
// key event: a marker is delivered on the reader goroutine too, and the engine
// reacts to markers by reconciling a scope — dropping the tenant's last
// subscription when that reconcile fails, or closing the store. Without the
// marker that teardown would wait the full closeTimeout on the goroutine
// running it, freezing the tenant's feed for those five seconds.
func (s *Store) broadcast(f *feed, subs []*subscription, evt store.Event) {
	f.beginDispatch()
	defer f.endDispatch()

	for _, sub := range subs {
		sub.deliver(s.cfg.Logger, evt)
	}
}

// beginDispatch and endDispatch bracket one delivery performed by the reader
// goroutine, so signalFeed can tell a teardown reached from inside a callback
// not to wait on that goroutine. Every reader-goroutine delivery — key events
// through feed.dispatch and markers through Store.broadcast — goes through
// this pair.
func (f *feed) beginDispatch() {
	f.mu.Lock()
	f.dispatching++
	f.mu.Unlock()
}

func (f *feed) endDispatch() {
	f.mu.Lock()
	f.dispatching--
	f.mu.Unlock()
}

// zeroFeedForStart hands Start the zero-scope feed and clears the refusal a
// PREVIOUS attempt recorded. Start retries the feed its subscribers are already
// holding, so that record belongs to the attempt that produced it, not to the
// feed forever. Callers must NOT hold feedsMu.
func (s *Store) zeroFeedForStart() (*feed, error) {
	s.feedsMu.Lock()
	defer s.feedsMu.Unlock()

	f, err := s.zeroFeedSlotLocked()
	if err != nil {
		return nil, err
	}

	f.err = nil

	return f, nil
}

// zeroFeedSlotLocked returns the zero-scope slot, creating it when Subscribe
// runs before Start, and refuses to hand it out — or resurrect it — once Close
// has begun. The caller MUST hold Store.feedsMu, which is what makes the check
// atomic with the map walk in stopFeeds: a Start or a Subscribe that already
// passed the s.closed check would otherwise re-insert a slot into a shut-down
// store, and nothing would ever tear it down again.
func (s *Store) zeroFeedSlotLocked() (*feed, error) {
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

// zeroFeedLocked is the SUBSCRIBE side of that slot: it reports the refusal a
// Start recorded instead of handing back a feed nothing is bringing up. The
// zero-scope slot is never retracted — its subscribers hold it and a retried
// Start must reconnect the one they hold — so the recorded cause is the only
// thing that can tell a later Subscribe the changefeed never came up.
func (s *Store) zeroFeedLocked() (*feed, error) {
	f, err := s.zeroFeedSlotLocked()
	if err != nil {
		return nil, err
	}

	if f.err != nil {
		return nil, f.err
	}

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

	// One shutdown fence for both branches, hoisted above them: a closing store
	// must not reserve a slot and dial a tenant, which is exactly what the
	// named branch did while only the zero scope was fenced.
	if s.closing {
		s.feedsMu.Unlock()

		return nil, store.ErrClosed
	}

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

	conn, dbKey, err := s.openListen(ctx, f)
	if err != nil {
		return s.retractFeed(f, err)
	}

	return s.publishFeed(ctx, f, conn, dbKey)
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
func (s *Store) publishFeed(ctx context.Context, f *feed, conn *pgx.Conn, dbKey string) error {
	s.feedsMu.Lock()

	var refusal error

	switch {
	case s.closing:
		refusal = store.ErrClosed
	default:
		refusal = s.refuseSharedDatabaseLocked(f, dbKey)
	}

	if refusal != nil {
		cause := s.failLocked(f, refusal)

		f.closeReadyLocked()
		s.feedsMu.Unlock()

		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
		defer cancel()

		_ = conn.Close(closeCtx)

		return cause
	}

	f.dbKey = dbKey

	s.startFeedReader(f, conn)
	f.closeReadyLocked()
	s.feedsMu.Unlock()

	return nil
}

// refuseSharedDatabaseLocked rejects a feed that is about to listen on a
// database a live feed already listens on. The caller MUST hold Store.feedsMu,
// which is what makes the decision race-free: dbKey is written in the same hold
// that starts the reader, so two creators racing to publish serialize and the
// second one always sees the first.
//
// A reserved slot that is still connecting carries no dbKey and is skipped —
// it is not listening yet, so it cannot receive anything.
func (s *Store) refuseSharedDatabaseLocked(f *feed, dbKey string) error {
	for _, other := range s.feeds {
		if other == f || other.dbKey == "" || other.dbKey != dbKey {
			continue
		}

		return fmt.Errorf(
			"systemplane/postgres: scope %q and scope %q both resolve to %s: %w",
			f.scope.Tenant, other.scope.Tenant, dbKey, ErrSharedDatabaseUnsupported,
		)
	}

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

	// The zero-scope slot is the exception: Start owns it, Subscribe can attach
	// to it BEFORE Start, and a retried Start must bring up the very feed those
	// subscribers hold. Retracting it would strand them on a feed nothing
	// reconnects and nothing tears down, while handing the next Subscribe a
	// fresh feed that has announced nothing and looks healthy. The recorded
	// cause stays on the slot instead, and zeroFeedLocked reports it.
	if f.scope.Tenant == "" {
		return f.err
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
// A joining subscriber is told whatever the feed last ANNOUNCED, before any key
// event. On a connected feed that is its own store.OpResync: it missed
// everything published before it joined, and the engine subscribes after Start
// has already connected, so without it a quiet scope would never reconcile. On
// a feed in an announced outage it is store.OpDisconnect, so the engine marks
// the scope stale immediately instead of reporting it fresh until the reconnect
// lands — up to the 30s backoff cap away.
//
// A feed that has announced nothing yet — created, not yet through its reader's
// first resync — emits nothing here: that resync is imminent and this
// subscriber is already in the map, so it receives that one. Announcing here
// too would double it.
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

	// The subscriber is added and the feed's announced state read in ONE hold,
	// which is what keeps the announcement exactly-once: whichever of this and
	// the reader's own beginResync/beginDisconnect runs second sees the other's
	// work, so the joiner is either announced to here or included in the
	// reader's broadcast, never both and never neither.
	f.mu.Lock()
	f.nextID++
	id := f.nextID
	f.subs[id] = sub
	joining := f.joiningOpLocked()
	f.mu.Unlock()

	// deliverLocked, never sub.fn directly: it puts the joining emission under
	// the same recovery guard as every reader-goroutine delivery, so a
	// panicking callback cannot escape through Subscribe to the caller. A
	// connection lost between the read above and this emission yields one
	// extra marker, which is harmless — both markers are idempotent.
	if joining != "" {
		sub.deliverLocked(s.cfg.Logger, store.Event{Scope: f.scope, Op: joining})
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
			defer runtime.RecoverAndLogWithContext(ctx, s.cfg.Logger, recoveryComponent, "subscriber")

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

// openListen opens a dedicated pgx connection for the feed's DSN, reads back
// which database it reached and installs LISTEN on it, all synchronously, so a
// bad DSN or a missing privilege surfaces to the caller instead of looping in
// the background. The returned key is what publishFeed compares against the
// live feeds to refuse two scopes sharing one database.
//
// Every step carries the same bounds a reconnect uses. Inheriting the caller's
// ctx unbounded is what would let a tenant whose host swallows packets — no
// refusal, no reset — park the Subscribe call for the life of that ctx, and
// with it the reserved feed slot every later Subscribe for that tenant waits
// on. The teardown interlock in publishFeed is only as tight as this bound.
func (s *Store) openListen(ctx context.Context, f *feed) (*pgx.Conn, string, error) {
	// Last look before the socket. The shutdown fence in acquireFeed is taken
	// before the DSN is resolved, and resolving it is a round trip through the
	// tenant manager: a Close landing in between would otherwise open a
	// connection for a store that is already tearing down, and publishFeed
	// would immediately throw it away. Refusing here keeps a closing store from
	// dialing at all.
	if s.isClosing() {
		return nil, "", store.ErrClosed
	}

	connectCtx, cancelConnect := context.WithTimeout(ctx, connectTimeout)
	defer cancelConnect()

	conn, err := pgx.Connect(connectCtx, f.dsn)
	if err != nil {
		return nil, "", fmt.Errorf("systemplane/postgres: listen connect%s: %w", f.label(), err)
	}

	// Any failure past this point closes the connection on a ctx of its own:
	// the one that just expired would abandon the socket instead of closing it.
	abort := func(err error) (*pgx.Conn, string, error) {
		closeCtx, cancelClose := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
		defer cancelClose()

		_ = conn.Close(closeCtx)

		return nil, "", err
	}

	// Which database this connection actually reached is a question only the
	// server can answer, and publishFeed cannot admit the feed without it, so
	// a failure here is a connect failure like any other. It runs BEFORE the
	// LISTEN so a refused feed never installs one, and so LISTEN stays the last
	// statement this connection ever ran — which is how a backend is told from
	// any other in pg_stat_activity.
	keyCtx, cancelKey := context.WithTimeout(ctx, listenTimeout)
	defer cancelKey()

	dbKey, err := serverDatabaseKey(keyCtx, conn, f.dsn)
	if err != nil {
		return abort(fmt.Errorf("systemplane/postgres: database identity%s: %w", f.label(), err))
	}

	listenCtx, cancelListen := context.WithTimeout(ctx, listenTimeout)
	defer cancelListen()

	if _, err := conn.Exec(listenCtx, "LISTEN "+channelName); err != nil {
		return abort(fmt.Errorf("systemplane/postgres: listen%s: %w", f.label(), err))
	}

	s.logInfo(ctx, "LISTEN connection established",
		log.String("channel", channelName),
		log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
		log.String("database", dbKey),
	)

	return conn, dbKey, nil
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
		defer runtime.RecoverAndLogWithContext(context.Background(), s.cfg.Logger, recoveryComponent, "listener")

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
	// One Start at a time. The zero-scope feed is SHARED, so the "already
	// running" check below and the reader launch inside publishFeed have to be
	// one decision: two concurrent Starts that both saw no reader would each
	// open a LISTEN backend on the same feed, and every NOTIFY would then be
	// delivered to every subscriber twice. The connect itself is bounded by
	// connectTimeout, so a second caller waits at most that long.
	s.startMu.Lock()
	defer s.startMu.Unlock()

	f, err := s.zeroFeedForStart()
	if err != nil {
		return err
	}

	f.mu.Lock()
	running := f.done != nil
	f.mu.Unlock()

	if running {
		return nil
	}

	// The zero scope is held to the same one-database rule as a tenant's: a
	// Store that also serves named tenants must not listen on a database one
	// of them listens on, or every NOTIFY would reach both feeds. openListen
	// reads that identity off the connection it just opened.
	conn, dbKey, err := s.openListen(ctx, f)
	if err != nil {
		// The refusal is recorded ON the slot, exactly as a publish-time refusal
		// is. Returning it bare leaves f.err nil, and the next zero-scope
		// Subscribe then attaches to a feed with no reader: it is never told a
		// resync or a disconnect, so the engine reads the scope as fresh forever.
		// failLocked keeps the zero slot — its subscribers hold it and a retried
		// Start brings up the one they hold — and zeroFeedForStart clears the
		// record for that retry.
		return s.retractFeed(f, err)
	}

	return s.publishFeed(ctx, f, conn, dbKey)
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
// sets it. A callback can drop its tenant's last subscription from inside the
// reader's own delivery, and waiting for the reader there is waiting on the
// goroutine doing the waiting — it can only end at closeTimeout, with the feed
// frozen meanwhile. f.dispatching cannot tell "the caller IS the reader" from
// "some other goroutine is mid-callback", so Close never skips on it: a Close
// that did would return with a live LISTEN connection and a live reader behind
// it whenever a subscriber happened to be running.
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
			log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
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

// runFeed is THE LISTEN loop, for every scope. conn is the already-connected,
// already-LISTENing first connection. Per connection it emits, in order:
// store.Event{Scope: f.scope, Op: store.OpResync}, then the decoded NOTIFY
// payloads from that connection, then — on connection loss, before the first
// reconnect attempt — exactly one
// store.Event{Scope: f.scope, Op: store.OpDisconnect}. Then it repeats
// through the existing backoff.
func (s *Store) runFeed(f *feed, conn *pgx.Conn) {
	var retry reconnectBackoff

	for {
		if subs, ok := f.beginResync(); ok {
			s.broadcast(f, subs, store.Event{Scope: f.scope, Op: store.OpResync})
		}

		connectedAt := time.Now()

		consumed := s.consumeUntilFailure(f, conn)

		// Only a connection that did some work clears the backoff. A backend
		// that accepts and drops at once would otherwise reset it every cycle
		// and the feed would reconnect at the first delay forever.
		retry.connectionEnded(consumed, time.Since(connectedAt))

		if subs, ok := f.beginDisconnect(); ok {
			s.broadcast(f, subs, store.Event{Scope: f.scope, Op: store.OpDisconnect})
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

		conn, err = s.reconnect(f, &retry)
		if err != nil {
			return
		}
	}
}

// consumeUntilFailure pumps notifications from conn until it dies or teardown
// closes f.stop. It reports whether the connection carried at least one
// notification, which is what tells runFeed the connection was worth keeping
// and its backoff can start over.
func (s *Store) consumeUntilFailure(f *feed, conn *pgx.Conn) (consumed bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		defer runtime.RecoverAndLogWithContext(ctx, s.cfg.Logger, recoveryComponent, "observer")

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
					log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
				)
			}

			return consumed
		}

		consumed = true

		s.handleNotification(ctx, f, notification.Payload)
	}
}

// handleNotification decodes one NOTIFY payload and fans it out to the feed's
// subscribers. A payload that does not decode is dropped with a warning naming
// the feed's tenant — without it an operator reading the logs of a process
// carrying dozens of tenant feeds cannot tell which database is emitting
// garbage — and the payload itself, truncated, so the warning cannot be turned
// into an unbounded log line by whatever wrote it.
func (s *Store) handleNotification(ctx context.Context, f *feed, payload string) {
	evt, ok := parseNotifyPayload(payload)
	if !ok {
		s.logWarn(ctx, "failed to decode NOTIFY payload",
			log.String("payload", truncateString(payload, 200)),
			log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
		)

		return
	}

	f.dispatch(s.cfg.Logger, evt)
}

// errFeedStopped ends the reconnect loop when the feed is torn down. It never
// reaches a caller of the package: runFeed is the only reader and it just
// returns.
var errFeedStopped = errors.New("systemplane/postgres: changefeed stopped")

// reconnectBackoff sequences the delays between one feed's reconnect attempts.
//
// The sequence restarts only after a connection that was USEFUL — one that
// carried at least one notification, or that outlived the backoff cap. A
// backend that accepts a connection and drops it immediately (a pgbouncer in
// transaction pooling, an idle_session_timeout shorter than the quiet period
// between notifications, a primary mid-failover) otherwise resets the delay on
// every cycle, so the feed reconnects at the first delay for as long as the
// condition lasts instead of escalating to the cap.
type reconnectBackoff struct {
	attempt int
}

// ceiling bounds the next delay. The delay is drawn uniformly from
// [0, ceiling), so feeds recovering from one outage spread out instead of
// dialing together.
func (b *reconnectBackoff) ceiling() time.Duration {
	return min(backoff.Exponential(backoffBase, b.attempt), backoffCap)
}

// next draws the delay before the next attempt and advances the sequence.
func (b *reconnectBackoff) next() time.Duration {
	delay := backoff.FullJitter(b.ceiling())
	b.attempt++

	return delay
}

// connectionEnded records the outcome of the connection that just died.
func (b *reconnectBackoff) connectionEnded(consumed bool, lifetime time.Duration) {
	if consumed || lifetime >= backoffCap {
		b.attempt = 0
	}
}

func (s *Store) reconnect(f *feed, retry *reconnectBackoff) (*pgx.Conn, error) {
	// One warning per connection loss: reconnect is entered once per loss and
	// retries internally. attempt is how many attempts this backoff sequence
	// has already spent, so a flapping backend reports a rising number instead
	// of a flat zero.
	s.logWarn(context.Background(), "LISTEN connection lost, reconnecting",
		log.Int("attempt", retry.attempt),
		log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
	)

	// The LOG streak, one bool per distinct cause, scoped to this entry into
	// reconnect — that is, to one connection loss. Kept apart from retry, whose
	// counter only a useful connection clears: after one unproductive cycle it
	// never returns to zero, and a loud line gated on it would go silent for
	// the life of the feed.
	var warnedAttemptFailed bool

	for {
		select {
		case <-f.stop:
			return nil, errFeedStopped
		default:
		}

		select {
		case <-f.stop:
			return nil, errFeedStopped
		case <-time.After(retry.next()):
		}

		// The fence openListen applies before its own socket. A store torn
		// down between the shutdown flag and this feed's stop signal must not
		// open one more connection to a database it will never read again.
		if s.isClosing() {
			return nil, store.ErrClosed
		}

		conn, err := s.dialAndListen(f)
		if err != nil {
			s.logStreakFailure(&warnedAttemptFailed, "reconnect attempt failed",
				log.Err(err),
				log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
			)

			continue
		}

		return conn, nil
	}
}

// dialAndListen is one reconnect attempt: connect, then install LISTEN on that
// connection, closing it when the LISTEN does not take. Both failures leave
// through the same error return, which is what makes the caller's single log
// line the ONLY exit from a failed attempt. A refused LISTEN used to be
// discarded silently, so a feed that reconnects fine but can never re-install
// it — pgbouncer in transaction pooling refuses LISTEN, so does a revoked
// grant — looped forever delivering nothing, with only the one warning emitted
// when the connection was first lost to go on. The wrapped cause names the
// stage, so that case reads differently from a dial nothing answered.
func (s *Store) dialAndListen(f *feed) (*pgx.Conn, error) {
	connectCtx, cancelConnect := context.WithTimeout(context.Background(), connectTimeout)
	defer cancelConnect()

	conn, err := pgx.Connect(connectCtx, f.dsn)
	if err != nil {
		return nil, fmt.Errorf("systemplane/postgres: listen connect%s: %w", f.label(), err)
	}

	listenCtx, cancelListen := context.WithTimeout(context.Background(), listenTimeout)
	defer cancelListen()

	if _, err := conn.Exec(listenCtx, "LISTEN "+channelName); err != nil {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), closeTimeout)
		defer cancelClose()

		_ = conn.Close(closeCtx)

		return nil, fmt.Errorf("systemplane/postgres: listen%s: %w", f.label(), err)
	}

	return conn, nil
}
