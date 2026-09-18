package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
	"github.com/LerianStudio/lib-systemplane/v4/internal/debounce"
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

	// workers holds one delivery goroutine per (scope, key), started on the
	// first notification for that pair and tracked by dispatchWG so shutdown
	// can wait for them. running holds the workerKey of every worker currently
	// inside a subscriber callback, so a Close that times out names the real
	// (scope, key) pairs it is stuck on instead of guessing.
	//
	// workersClosed is set under workersMu before Close waits on dispatchWG,
	// which is what makes every Add to that WaitGroup happen-before its Wait.
	workersMu     sync.Mutex
	workers       map[workerKey]*dispatchWorker
	workersClosed bool
	dispatchWG    sync.WaitGroup
	running       sync.Map // workerKey -> struct{}

	// startMu serializes scope bring-up so two concurrent Starts open one
	// subscription instead of two. It is held across Store.Subscribe and never
	// together with a scope's own lock.
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

// New builds an engine from cfg, defaulting everything that has a sensible
// default: a nil logger becomes a no-op, a zero CloseTimeout becomes 30s, and
// a zero Debounce disables debouncing rather than dropping notifications.
//
// It opens no connection and starts no goroutine — Start does that — so a
// Client that is constructed and never started leaves nothing behind.
func New(cfg Config) *Engine {
	logger := cfg.Logger
	if logger == nil {
		logger = log.NewNop()
	}

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
		workers:          make(map[workerKey]*dispatchWorker),
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
// Three failures, three different outcomes:
//
//   - Subscribe fails: the scope is dropped entirely and the error returned. A
//     tracked scope whose feed never opened would look fresh forever.
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
// Start is idempotent: a second call finds the changefeed open and the first
// reconcile finished, and returns that same recorded outcome without
// subscribing or listing again.
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

	sc.mu.Lock()
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
// parked goroutine per key per dropped tenant, alive until shutdown.
//
// It is idempotent. A tenant suspended and then deleted arrives as two
// lifecycle events, and the second must not release a subscription the backend
// has already forgotten, so the unsubscribe is taken out of the scope as it is
// called. It runs OUTSIDE the scope lock: a backend's unsubscribe waits for
// its changefeed goroutine, which may be inside onEvent, which takes that
// lock.
func (e *Engine) dropScope(scope store.Scope) {
	e.scopesMu.Lock()
	sc := e.scopes[scope]
	delete(e.scopes, scope)
	e.scopesMu.Unlock()

	if sc == nil {
		e.stopScopeWorkers(scope)

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

	e.stopScopeWorkers(scope)
}

// stopScopeWorkers ends every delivery worker of scope and forgets them, under
// the same lock that starts one: a worker created between the stop and the
// delete would otherwise survive with nothing to feed it.
func (e *Engine) stopScopeWorkers(scope store.Scope) {
	e.workersMu.Lock()
	defer e.workersMu.Unlock()

	for wk, w := range e.workers {
		if wk.Scope != scope {
			continue
		}

		w.stop()
		delete(e.workers, wk)
	}
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
// A write whose revision the store could not report (0) still takes effect,
// because revision 0 always wins the fence — at the cost of the echo
// publishing a second time. A value the caller just wrote must be readable.
//
// Publish takes no write into a scope the engine is not tracking: before
// Start, or after the scope was dropped. Caching it would rebuild that scope
// around one value with no changefeed behind it and no reconcile goroutine to
// confirm it — readable forever as though it were current. A nil Engine
// ignores the write instead of panicking, and a closed one drops it rather
// than resurrecting a scope during shutdown.
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
func (e *Engine) Publish(ctx context.Context, scope store.Scope, se store.Entry) {
	if e == nil || e.closed.Load() {
		return
	}

	sc := e.trackedScope(scope)
	if sc == nil {
		e.logDebug(ctx, "write for an untracked scope, dropping",
			log.String("tenant", scope.Tenant),
			log.String("namespace", se.Namespace),
			log.String("keyname", se.Key),
		)

		return
	}

	e.ingest(ctx, sc, se)
}

// Lookup returns the published state of nk in scope.
//
// ok is false for a scope the engine does not track and for a key that scope
// has not published yet; the caller then falls back to the registered
// default. Value is a deep copy the caller owns, and Stale reports whether the
// scope's changefeed is disconnected or has not been reconciled yet. A nil
// Engine reports a miss instead of panicking.
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
	stale := sc.stale
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

	e.running.Range(func(key, _ any) bool {
		wk, ok := key.(workerKey)
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
