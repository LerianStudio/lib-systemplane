package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
	"github.com/LerianStudio/lib-systemplane/v4/internal/debounce"
	"github.com/LerianStudio/lib-systemplane/v4/internal/safelog"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// defaultCloseTimeout bounds how long Close waits for subscriber callbacks to
// return once their context has been canceled. It applies to any engine built
// without an explicit timeout.
const defaultCloseTimeout = 30 * time.Second

// Engine is the convergent runtime-configuration engine. One Engine tracks N
// scopes: the zero store.Scope is the single-tenant scope, every tenant is
// another key in the same map.
type Engine struct {
	store    store.Store
	registry Registry
	logger   log.Logger

	// debouncer collapses a burst of changefeed notifications for one key in
	// one scope into a single store re-read. It is keyed by scope as well as
	// by key so a busy tenant never swallows another tenant's notification.
	//
	// debounceAsync records whether the quiet window is non-zero, and
	// therefore whether the re-read runs on a timer goroutine of the
	// debouncer's own — engine work Close must wait for — or inline on the
	// changefeed goroutine, which it must not.
	debouncer     *debounce.Debouncer[scopeNSKey]
	debounceAsync bool

	scopesMu sync.RWMutex
	scopes   map[store.Scope]*scopeState

	// subscribers is keyed by NSKey alone, never by scope: OnChange covers
	// that key in every scope the engine tracks and Change.Tenant names the
	// one that fired. nextSubID makes each subscription removable by identity.
	subsMu      sync.RWMutex
	subscribers map[NSKey][]subscription
	nextSubID   atomic.Uint64

	// workersMu guards every scope's worker map and refusal flag, so the lock
	// that starts a delivery worker is the lock that sweeps one. The workers
	// themselves live on the scope state that owns them; dispatchWG tracks all
	// of them so shutdown can wait, and running names every worker currently
	// inside a subscriber callback, so a Close that times out reports the real
	// (scope, key) pairs it is stuck on instead of guessing.
	//
	// It is keyed by the WORKER, not by its (scope, key) triple: workers belong
	// to the scope state, so a dropped tenant's straggler and the re-activated
	// tenant's worker for the same key share that triple, and the straggler's
	// clear-on-exit would erase the live worker's mark — leaving a timed-out
	// Close blaming the backend for a subscriber that is holding it open.
	//
	// workersClosed is set under workersMu before Close waits on dispatchWG,
	// which is what makes every Add to that WaitGroup happen-before its Wait.
	workersMu     sync.Mutex
	workersClosed bool
	dispatchWG    sync.WaitGroup
	running       sync.Map // *dispatchWorker -> workerKey

	// startMu serializes scope bring-up so two concurrent Starts open one
	// subscription instead of two. It is held across Store.Subscribe and,
	// inside that, across the scope's own lock while the unsubscribe handle is
	// stored — that nesting is what settles the race with Close over the
	// handle. It is never acquired while a scope lock is already held, so the
	// order is one-way and cannot deadlock against it.
	startMu sync.Mutex

	// closed refuses new scopes, publications and subscriptions from the
	// moment Close begins. closeOnce makes Close idempotent and closeErr
	// carries its single outcome to every later caller.
	closed       atomic.Bool
	closeTimeout time.Duration
	closeOnce    sync.Once
	closeErr     error

	// reconcileTimeout bounds a reconcile's whole-scope Store.List. It is a
	// field rather than a constant so a test can lower it to a few
	// milliseconds and prove the bound is what ends a hung List.
	reconcileTimeout time.Duration

	// lifecycleCtx is the engine's process-wide context, canceled when the
	// engine shuts down. Feed rereads, reconciles and subscriber dispatch
	// derive from it so nothing outlives the engine.
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
}

// Config is what the Client hands the engine at construction. Every field is
// optional except Store and Registry, without which the engine can neither
// read a value nor know which keys exist.
type Config struct {
	Store    store.Store
	Registry Registry
	Logger   log.Logger
	// Debounce is the quiet window a key's changefeed notifications are
	// coalesced into one store re-read over. Zero submits synchronously,
	// which is what makes a test deterministic.
	Debounce time.Duration
	// CloseTimeout bounds how long Close waits for subscriber callbacks that
	// have been canceled. Zero means defaultCloseTimeout.
	CloseTimeout time.Duration
}

// swallowPanic discards a panic raised by the consumer's own observability
// code. There is nowhere left to report it — the logger is what panicked — and
// the alternative is unwinding an engine goroutine over a log line.
func swallowPanic() {
	_ = recover()
}

// New builds an engine from cfg, defaulting everything that has a sensible
// default: a nil logger becomes a no-op, a zero CloseTimeout becomes 30s, and
// a zero Debounce disables debouncing rather than dropping notifications.
//
// The logger is guarded ONCE, here, rather than at each of the sites that hand
// it to a recovery handler or a goroutine launcher: every one of them reads
// e.logger, and the debouncer is handed the same wrapped value, so the guard
// covers the whole engine and nothing new has to remember it. safelog.Guard is
// idempotent, so a Client that guarded the same logger already pays for one
// wrapper, not two.
//
// It opens no connection and starts no goroutine — Start does that — so a
// Client that is constructed and never started leaves nothing behind.
func New(cfg Config) *Engine {
	logger := safelog.Guard(cfg.Logger)

	closeTimeout := cfg.CloseTimeout
	if closeTimeout <= 0 {
		closeTimeout = defaultCloseTimeout
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Engine{
		store:            cfg.Store,
		registry:         cfg.Registry,
		logger:           logger,
		debouncer:        debounce.New(cfg.Debounce, debounce.WithLogger[scopeNSKey](logger)),
		debounceAsync:    cfg.Debounce > 0,
		scopes:           make(map[store.Scope]*scopeState),
		subscribers:      make(map[NSKey][]subscription),
		closeTimeout:     closeTimeout,
		reconcileTimeout: defaultReconcileTimeout,
		lifecycleCtx:     ctx,
		lifecycleCancel:  cancel,
	}
}

// Start brings up the single-tenant scope and returns once its first reconcile
// has completed, so a caller reading afterwards is looking at a cache the
// store has confirmed and a subscriber registered beforehand cannot have
// missed the announcement of a key (FC-11).
//
// Start runs NO reconcile of its own. It creates the scope, opens the
// changefeed, and waits: the store guarantees an OpResync after every
// (re)connect, and that resync drives the one initial reconcile. Reconciling
// here as well would publish a registered key's default twice for a key with
// no row — revision 0 is never deduplicated, so the second publication is
// accepted and delivered, which is the double delivery FC-11 forbids.
//
// Four failures, four different outcomes:
//
//   - Subscribe fails: the scope is dropped entirely and the error returned. A
//     tracked scope whose feed never opened would look fresh forever. A
//     Subscribe that SUCCEEDS after Close began is rolled back the same way,
//     releasing the changefeed it just opened.
//   - ctx expires before the first reconcile completes: the ctx error is
//     returned and the scope is left tracked and stale. A backend that never
//     emits OpResync is broken, and failing loudly beats serving registered
//     defaults forever while pretending they are current.
//   - Close runs while Start is waiting: store.ErrClosed is returned at once.
//     Close drops the event that would have driven the first reconcile and
//     stops the goroutine that would have run it, so nothing will ever close
//     the channel Start waits on — a caller that passed context.Background(),
//     which is what a service does, would wait for the life of the process.
//   - the first reconcile ran and failed: its error is returned wrapped and
//     the scope is left stale, for the next OpResync to retry.
//
// An engine missing a store or a registry cannot converge a single key, so
// Start refuses it — store.ErrNilBackend or ErrNilRegistry — rather than
// opening a changefeed whose events it could not answer. A nil Engine reports
// the same nil-backend sentinel; the read paths stay nil-safe instead.
//
// Start is idempotent on success: a second call finds the changefeed open and
// the first reconcile finished, and returns that same recorded outcome without
// subscribing or listing again.
//
// A FAILED first reconcile is the exception, and deliberately so. Its outcome
// is recorded once and no later reconcile rewrites it, so without a retry a
// consumer whose database blinked during boot would get that same error from
// every Start for the life of the process — a configuration library that needs
// a new Client after one hiccup. So a Start that finds its scope's recorded
// outcome is an error tears the scope down and brings it up again, and the
// store's OpResync after the fresh Subscribe drives a fresh first reconcile.
// Subscriptions survive it: they belong to the engine and are keyed by key,
// never by scope, so an OnChange registered before the failed Start hears the
// retry's announcement (FC-11). A ctx expiry needs none of this — the scope is
// still subscribed and still waiting for its first resync, so the next Start
// waits on the same channel.
func (e *Engine) Start(ctx context.Context) error {
	if e == nil {
		return store.ErrNilBackend
	}

	if e.closed.Load() {
		return store.ErrClosed
	}

	if e.store == nil {
		return store.ErrNilBackend
	}

	if e.registry == nil {
		return ErrNilRegistry
	}

	scope := store.Scope{}

	e.retryFailedScope(scope)

	sc, err := e.bringUpScope(scope)
	if err != nil {
		return err
	}

	select {
	case <-sc.firstReconcileDone:
	case <-e.dispatchContext().Done():
		return store.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}

	// The close is the release and this receive is the acquire, so the write
	// is guaranteed visible; the lock is taken anyway, which keeps the race
	// detector honest and costs nothing on a once-per-scope path.
	sc.mu.RLock()
	reconcileErr := sc.firstReconcileErr
	sc.mu.RUnlock()

	if reconcileErr != nil {
		return fmt.Errorf("systemplane: first reconcile of %s failed: %w", scopeLabel(scope), reconcileErr)
	}

	return nil
}

// retryFailedScope drops scope when its first reconcile is on record as having
// FAILED, so the Start that follows brings it up again from nothing.
//
// The check and the drop are one critical section under startMu, so two
// concurrent retries cannot both drop. The lock is released before bringUpScope
// takes it again, so what keeps a retry from dropping a scope another Start has
// just rebuilt is not this lock but Client.Start, which holds its own startMu
// across the pair. A scope whose first reconcile has not finished is left
// alone: it is still subscribed and its resync is still coming, and tearing it
// down would throw away the changefeed the caller is waiting on.
func (e *Engine) retryFailedScope(scope store.Scope) {
	e.startMu.Lock()
	defer e.startMu.Unlock()

	sc := e.trackedScope(scope)
	if sc == nil {
		return
	}

	select {
	case <-sc.firstReconcileDone:
	default:
		return
	}

	sc.mu.RLock()
	failed := sc.firstReconcileErr != nil
	sc.mu.RUnlock()

	if failed {
		e.dropScope(scope)
	}
}

// bringUpScope creates scope's state and opens its changefeed, exactly once.
//
// The subscription is opened on the engine's lifecycle context rather than the
// caller's: the feed must outlive Start and die with Close. A failed Subscribe
// drops the scope rather than leaving a half-built one behind.
func (e *Engine) bringUpScope(scope store.Scope) (*scopeState, error) {
	e.startMu.Lock()
	defer e.startMu.Unlock()

	sc := e.scopeFor(scope)
	if sc == nil {
		return nil, store.ErrClosed
	}

	sc.mu.RLock()
	subscribed := sc.unsubscribe != nil
	sc.mu.RUnlock()

	if subscribed {
		return sc, nil
	}

	unsubscribe, err := e.store.Subscribe(e.dispatchContext(), scope, e.onEvent)
	if err != nil {
		e.dropScope(scope)

		return nil, fmt.Errorf("systemplane: changefeed for %s failed to open: %w", scopeLabel(scope), err)
	}

	// Close reads each tracked scope's unsubscribe exactly once, and a Subscribe
	// still in flight at that moment has none to be read. Storing the handle
	// afterwards would leave it held by nobody: the callback stays registered in
	// the store's subscriber list for the life of the store, keeping the whole
	// Engine reachable and — once a scope is a tenant — one live connection per
	// tenant whose bring-up lost this race. So the loser releases its own
	// subscription and reports the scope it cannot keep.
	//
	// The check sits under the SAME lock Close reads the handle under, and that
	// is what decides the race rather than leaving a window between them. Close
	// stores closed before it acquires sc.mu, so a bring-up that takes the lock
	// after Close released it is guaranteed to observe closed and release its
	// own subscription; a bring-up that takes the lock first stores the handle,
	// and Close then reads it and releases it. Checking outside the lock left
	// the third interleaving open: the check reads false, Close runs its whole
	// unsubscribe loop and finds nil, and the handle is stored into a scope
	// nobody will ever read it from again.
	sc.mu.Lock()

	if e.closed.Load() {
		sc.mu.Unlock()
		unsubscribe()
		e.dropScope(scope)

		return nil, store.ErrClosed
	}

	sc.unsubscribe = unsubscribe
	sc.mu.Unlock()

	return sc, nil
}

// dropScope stops tracking scope. Everything it held — cached entries, the
// stale flag, the first-reconcile channel — goes with it, which is the point:
// a scope nothing feeds must not be readable as though it were current.
//
// Its changefeed goes with it: the subscription is released here, not at
// Close. A tenant that is suspended, deleted or rotated is dropped while the
// process keeps running, so a subscription left open is one live connection
// per dropped tenant for the life of the process, still delivering events into
// an engine that no longer tracks the scope.
//
// Its goroutines go with it too: the reconcile goroutine and every delivery
// worker of that scope are stopped here, for the same reason — otherwise one
// parked goroutine per key per dropped tenant, alive until shutdown. A
// publication racing the drop is discarded rather than delivered: one that
// passed publish's refusal before this ran finds the scope's workers already
// refused and starts none, so its value reaches neither a subscriber nor a
// goroutine that outlives the tenant.
//
// It is idempotent, but only in effect: a second concurrent drop of the same
// scope finds the state already out of the map and returns from the early exit
// below BEFORE the first has unsubscribed and swept. That return is therefore
// not a "teardown finished" point for its caller — the changefeed may still be
// open and the delivery workers still running when it lands. Nothing in the
// engine reads it as one; a caller that needs teardown to have completed would
// have to be given something to wait on. A tenant suspended and then deleted
// arrives as two lifecycle events, and the second must not release a
// subscription the backend has already forgotten, so the unsubscribe is taken
// out of the scope as it is called. It runs OUTSIDE the scope lock: a backend's unsubscribe waits for
// its changefeed goroutine, which may be inside onEvent, which takes that
// lock.
func (e *Engine) dropScope(scope store.Scope) {
	e.scopesMu.Lock()
	sc := e.scopes[scope]
	delete(e.scopes, scope)
	e.scopesMu.Unlock()

	// Already dropped, or never tracked: the drop that removed it swept its
	// workers and refused every later one under the worker lock, and a scope
	// that was never tracked never had a publication to start one.
	if sc == nil {
		return
	}

	sc.mu.Lock()
	unsubscribe := sc.unsubscribe
	sc.unsubscribe = nil
	sc.mu.Unlock()

	if unsubscribe != nil {
		unsubscribe()
	}

	sc.stopReconcileWorker()

	e.stopScopeWorkers(sc)
}

// stopScopeWorkers ends every delivery worker of sc, forgets them, and refuses
// every later one — all under the single lock that starts a worker, which is
// what makes the refusal and the start agree.
//
// Refusing is the half a sweep alone cannot do. publish declines a publication
// for a dropped scope by selecting on the scope's reconcileStop channel, which
// dropScope closes one step before this sweep runs; a publisher that passed
// that select in between reaches workerFor after the sweep and leaves a parked
// goroutine, and a WaitGroup entry, for a scope only Close ever ends.
//
// It sweeps sc's own map, which is what makes the sweep identity-scoped rather
// than value-scoped: a tenant re-activated while this drop was still inside
// unsubscribe is a different state with a map of its own, and its workers —
// and whatever is waiting in their mailboxes — survive a sweep that is ending
// the state before it.
func (e *Engine) stopScopeWorkers(sc *scopeState) {
	e.workersMu.Lock()
	defer e.workersMu.Unlock()

	sc.workersDropped = true

	for _, w := range sc.workers {
		w.stop()
	}

	clear(sc.workers)
}

// beginWork registers one engine-owned goroutine in the WaitGroup Close
// drains, reporting false once the door is shut.
//
// The flag and the Add are under one lock Close takes before it waits, which
// is what makes every Add happen-before that Wait. Go answers an Add that
// races a Wait by killing the process, so this is the only way work may join
// the drain.
func (e *Engine) beginWork() bool {
	e.workersMu.Lock()
	defer e.workersMu.Unlock()

	if e.workersClosed {
		return false
	}

	e.dispatchWG.Add(1)

	return true
}

// scopeLabel renders a scope for an error message.
func scopeLabel(scope store.Scope) string {
	if scope.Tenant == "" {
		return "the single-tenant scope"
	}

	return "tenant " + scope.Tenant
}

// Publish takes the row the Client has just persisted and puts it through the
// engine's ingress, so the caller's own next read sees its write before the
// changefeed echoes it (D4).
//
// It takes a store.Entry — marshaled bytes, the revision the store returned,
// the provenance — and deliberately NOT an already-decoded Go value: the feed
// decodes JSON, so a caller's []string{"a"} and the echo's []any{"a"} would
// not compare equal and every echo would fire a redundant callback. One
// ingress, one canonical shape, and the echo of this write is then deduplicated
// by revision.
//
// A local publication is TRUSTED and is not graded again here: Client.Set
// marshals the value, decodes it back to the canonical shape these same bytes
// produce, and runs the registered validator on it under the caller's own
// context a moment before the store write — the same function, the same value,
// the same shape, the same ctx. Grading it a second time bought nothing and
// cost correctness: a validator that answered differently on the second call
// dropped the publication while the row stayed written and Set still returned
// nil, so a caller was told its write landed while the next read served the
// previous value. Changefeed and reconcile ingress stays fully graded — those
// rows were written by somebody else, and nothing in this process has ever
// looked at them.
//
// What Publish can still refuse it REPORTS, and Client.Set passes that error
// on: a closed or nil engine, a scope it does not track, a key nothing
// registered, bytes that do not decode, and the two the ingress meets only
// AFTER the guards above have passed — the engine closed under this write, and
// the scope torn down under it. Every one of them means the row is persisted
// and the next read will not serve it, which is exactly what a caller of Set
// needs to be told.
//
// A nil error means exactly one thing: the next Lookup of this key in this
// scope serves this write or something newer. It does not mean subscribers
// have seen it — an accepted publication is queued on the key's delivery
// worker, whose callbacks run after this returns — and it does not mean the
// value was cached, because the fence refusing a write the cache already
// holds at a higher revision is a nil error too.
//
// A write whose revision the store could not report (0) still takes effect,
// because revision 0 always wins the fence — at the cost of the echo
// publishing a second time. A value the caller just wrote must be readable.
//
// Publish takes no write into a scope the engine is not tracking: before
// Start, or after the scope was dropped. Caching it would rebuild that scope
// around one value with no changefeed behind it and no reconcile goroutine to
// confirm it — readable forever as though it were current. A nil Engine
// reports the write refused instead of panicking, and a closed one drops it
// rather than resurrecting a scope during shutdown.
//
// The write is fenced against a reconcile in flight exactly as a changefeed
// publication is: the outcome is recorded, under the same lock, in the same
// step. Without that, a reconcile whose List predates the write finds the key
// absent from its photograph and publishes the registered default at revision
// 0 — which always wins the fence — over the value the caller just wrote and
// already read back.
//
// The outcome is recorded whether or not the ingress accepted it, for the same
// reason the changefeed's re-read records both: a row the registered validator
// rejects still means the engine learned nothing usable about that key, and a
// concurrent reconcile that sees neither fence treats the key as absent and
// publishes the registered default over the cached value.
//
// ctx is the WRITER's context — the one the Client received from the caller of
// Set — and it is used for everything this path emits: the rejection lines an
// operator reads to explain why a write did not take effect, and the panic
// record of a validator that dies on the value. This is the one ingress that
// runs on the consumer's own goroutine, inside the consumer's own span; the
// engine's background context belongs to the goroutines the engine owns (the
// workers, the reconcile, the debounced re-read), and using it here detached
// every one of those records from the request that caused it.
func (e *Engine) Publish(ctx context.Context, scope store.Scope, se store.Entry) error {
	if e == nil || e.closed.Load() {
		return ErrClosed
	}

	sc, err := e.writeScope(ctx, scope, NSKey{Namespace: se.Namespace, Key: se.Key})
	if err != nil {
		return err
	}

	return e.ingest(ctx, sc, se, feedFence{}, true)
}

// writeScope resolves the scope a LOCAL WRITE addresses — Publish and
// PublishDelete, the two entry points a consumer's own goroutine reaches — and
// reports ErrScopeNotTracked for a scope the engine holds no state for.
//
// It is deliberately not scopeForEvent, which answers the same question for
// the changefeed and reports the drop as changefeed work under the engine's
// background context. A write refused here was made by a caller that is still
// waiting, inside that caller's span: the line is logged under ctx so it
// belongs to the request that caused it, and says a write was dropped, so an
// operator reading it does not go looking for a feed that was never involved.
//
// DEBUG, and unguarded, because this line runs once per refused write rather
// than once per event: the guard scopeForEvent's own line carries would cost
// more than the fields it skips.
func (e *Engine) writeScope(ctx context.Context, scope store.Scope, nk NSKey) (*scopeState, error) {
	sc := e.trackedScope(scope)
	if sc != nil {
		return sc, nil
	}

	e.logDebug(ctx, "write for an untracked scope, dropping",
		log.String(constants.AttrKeyTenantID, scope.Tenant),
		log.String("namespace", nk.Namespace),
		log.String("keyname", nk.Key),
	)

	return nil, fmt.Errorf("%w: %s", ErrScopeNotTracked, scopeLabel(scope))
}

// Lookup returns the published state of nk in scope.
//
// ok is false for a scope the engine does not track and for a key that scope
// has not published yet; the caller then falls back to the registered
// default. Value is a deep copy the caller owns. A nil Engine reports a miss
// instead of panicking.
//
// Stale reports that nothing is currently confirming the scope, which is two
// facts read as one: the changefeed is disconnected or has not been reconciled
// since it connected, OR at least one key could not be re-read after its last
// change (see scopeState.unconfirmed). Both are read under the same lock as
// the entry, so one Lookup is an atomic read of value and freshness.
//
// A miss inside a tracked scope still carries that scope's Stale flag, and
// only the scope the engine does not track at all reports the zero Entry.
// Discarding staleness on the miss path was a silent lie: after a first
// reconcile that published nothing — a transient List failure at Start — every
// registered key is a miss, so every read is answered by the caller's
// registered default, and dropping Stale reported each of those defaults as a
// value the store had confirmed (FC-5).
func (e *Engine) Lookup(scope store.Scope, nk NSKey) (Entry, bool) {
	if e == nil {
		return Entry{}, false
	}

	sc := e.trackedScope(scope)
	if sc == nil {
		return Entry{}, false
	}

	sc.mu.RLock()
	cached, ok := sc.entries[nk]
	stale := sc.stale || len(sc.unconfirmed) > 0
	sc.mu.RUnlock()

	if !ok {
		return Entry{Stale: stale}, false
	}

	// Clone runs OUTSIDE the lock. A read lock excludes every writer, so
	// holding it across a reflective deep copy stalls every publisher waiting
	// for the write lock — measured at 90us of write-lock wait against 12ns of
	// map read. It is safe to drop because a cached value is immutable once
	// stored: publish replaces the whole map entry rather than mutating one,
	// so nothing can alter the object graph this copy is walking.
	return Entry{
		Value:     Clone(cached.Value),
		Revision:  cached.Revision,
		UpdatedAt: cached.UpdatedAt,
		UpdatedBy: cached.UpdatedBy,
		Stale:     stale,
	}, true
}

// Close shuts the engine down and waits, bounded, for in-flight subscriber
// callbacks to finish.
//
// The order is load-bearing. The engine is marked closed first, so no new
// scope, publication or subscription is accepted while shutdown runs. Then the
// lifecycle context is canceled, which both terminates every scope's
// Store.Subscribe and cancels the context every in-flight callback holds.
// Every scope's unsubscribe runs next, then the debouncer is closed, which
// discards pending re-reads that nobody is waiting for. Only then does Close
// wait for the dispatch workers.
//
// A callback that honors its context ends and Close returns nil with no
// goroutine left. One that ignores it makes Close return ErrCloseTimeout
// naming the (scope, key) it is stuck in, and that goroutine is the
// subscriber's leak, made visible rather than hidden. A timeout still leaves
// the engine fully closed: no new work is accepted and the store is
// releasable, which is what lets the Client close it afterwards.
//
// Close does NOT close the store. The Client opened it and owns its lifecycle;
// an engine that closed a store it did not open would break NewForTesting.
//
// Close is idempotent — every call returns the first call's outcome — and
// nil-receiver safe.
func (e *Engine) Close() error {
	if e == nil {
		return nil
	}

	e.closeOnce.Do(func() {
		e.closed.Store(true)

		if e.lifecycleCancel != nil {
			e.lifecycleCancel()
		}

		for _, sc := range e.trackedScopes() {
			sc.mu.Lock()
			unsubscribe := sc.unsubscribe
			sc.mu.Unlock()

			if unsubscribe != nil {
				unsubscribe()
			}
		}

		e.debouncer.Close()

		e.closeWorkers()

		e.closeErr = e.waitForWorkers()
	})

	return e.closeErr
}

// closeWorkers shuts the door on new dispatch workers before Close waits for
// the existing ones. The flag and workerFor's dispatchWG.Add are under the same
// lock, so no straggler publication can increment the WaitGroup while it is
// being waited on — a race Go answers by killing the process, not by returning
// an error.
func (e *Engine) closeWorkers() {
	e.workersMu.Lock()
	defer e.workersMu.Unlock()

	e.workersClosed = true
}

// trackedScopes snapshots the scopes under the engine lock, so unsubscribing
// (which may re-enter the store) never runs while the map is held.
func (e *Engine) trackedScopes() []*scopeState {
	e.scopesMu.RLock()
	defer e.scopesMu.RUnlock()

	scopes := make([]*scopeState, 0, len(e.scopes))
	for _, sc := range e.scopes {
		scopes = append(scopes, sc)
	}

	return scopes
}

// waitForWorkers drains the dispatch WaitGroup in a goroutine racing a timer,
// which is what makes the wait bounded. The drain goroutine only ever closes a
// channel, so it cannot outlive the workers even when the timer wins.
//
// It is launched the way every other engine goroutine is — with the context
// and the component — so a panic here is metered and recorded on the span
// rather than only logged. Its context is already canceled by the time it
// runs, which is why the function ignores it.
func (e *Engine) waitForWorkers() error {
	drained := make(chan struct{})

	runtime.SafeGoWithContextAndComponent(e.dispatchContext(), e.logger,
		"systemplane.engine", "close", runtime.KeepRunning, func(context.Context) {
			e.dispatchWG.Wait()
			close(drained)
		})

	timeout := e.closeTimeout
	if timeout <= 0 {
		timeout = defaultCloseTimeout
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-drained:
		return nil
	case <-timer.C:
		return e.stuckError(timeout)
	}
}

// stuckError names every worker still inside a subscriber callback. The set is
// read from the workers themselves rather than derived from the cache, so the
// message reports what is actually stuck.
//
// An empty set is a different diagnosis, not a missing one: no callback is
// running, so what held shutdown is engine work inside the store — a reconcile
// whose List has not answered, or a debounced re-read — or a worker caught
// between its wake-up and the callback. Blaming a subscriber there sends
// whoever reads the message hunting through consumer code for a fault that is
// in the backend or the network.
func (e *Engine) stuckError(timeout time.Duration) error {
	stuck := make([]string, 0, 1)

	e.running.Range(func(_, value any) bool {
		wk, ok := value.(workerKey)
		if !ok {
			return true
		}

		tenant := wk.Scope.Tenant
		if tenant == "" {
			tenant = "single-tenant"
		}

		stuck = append(stuck, fmt.Sprintf("%s/%s/%s", tenant, wk.Namespace, wk.Key))

		return true
	})

	if len(stuck) == 0 {
		return fmt.Errorf("%w after %s: engine still inside a store call (reconcile or re-read)",
			ErrCloseTimeout, timeout)
	}

	sort.Strings(stuck)

	return fmt.Errorf("%w after %s: subscriber still running for %s",
		ErrCloseTimeout, timeout, strings.Join(stuck, ", "))
}
