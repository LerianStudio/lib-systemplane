package engine

import (
	"sync"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// entry is the whole published state of one key: the value plus the
// provenance of the row backing it. Revision 0 means no row — the registered
// default is in force.
type entry struct {
	Value any
	// Raw is the row's JSON exactly as the store handed it over, kept beside
	// the decoded value so the fence can recognise the echo of a write by
	// comparing bytes instead of walking the decoded document. It is nil for a
	// publication that has no row behind it — a delete, or a reconcile-absent
	// publishing the registered default.
	//
	// The engine RETAINS the store's own slice here for the life of the cache
	// entry and compares it against every later publication of the key, so a
	// Store must return a slice it will never mutate afterwards. database/sql
	// decodes into a fresh *[]byte per row today; a pooled buffer or a
	// sql.RawBytes handed back instead would leave the fence comparing bytes
	// that changed underneath it, and a real configuration change whose new
	// text happened to land in the same buffer would be swallowed as an echo.
	//
	// Retaining it costs one copy of every key's row JSON, so the engine's
	// cache holds the registered keys times the tracked scopes in raw bytes on
	// top of the decoded values. Negligible single-tenant, where there is one
	// scope; in wave 3 it is multiplied by the number of tenants the process
	// has activated.
	Raw      []byte
	Revision int64
	// Deletes counts the deletes this key has taken in this scope. A delete
	// publishes the registered default at Revision 0, which FC-4 and FC-5
	// require at the public surface, so the revision alone cannot tell a
	// changefeed re-read that the row it is holding has since been removed:
	// every revision beats 0.
	//
	// A re-read reads this counter BEFORE its store call and hands it back
	// afterwards, and a publication whose count no longer matches is refused.
	// That is causal rather than numeric — it asks "did a delete land while I
	// was reading?", not "is this revision high enough" — so it refuses a row
	// read under a snapshot that predates the DELETE (READ COMMITTED gives a
	// reader exactly that) while still accepting a recreate at any revision,
	// including one below the deleted row's. D11 makes a recreate through the
	// library come back strictly above every earlier revision, but it names a
	// residual where it does not, and the engine does not need to care.
	//
	// A reconcile that finds a key absent does NOT bump it: absence from a
	// photograph is a conclusion about a snapshot, and a re-read in flight may
	// hold the fresher fact.
	Deletes   uint64
	UpdatedAt time.Time
	UpdatedBy string
}

// deleteFence is what a changefeed re-read carries across its store call: the
// number of deletes the key had taken when the read began. The zero value is
// unarmed, and every ingress that is not a re-read passes it — a write's own
// value is never superseded by a delete it did not see.
type deleteFence struct {
	deletes uint64
	armed   bool
}

// deleteFenceFor arms a fence for nk as it stands now. A key with nothing
// cached yet reads zero, which is what makes the FIRST delete of a key — the
// one that has no cached revision to fence against either — still refuse a
// re-read that started before it.
func (sc *scopeState) deleteFenceFor(nk NSKey) deleteFence {
	cached, _ := sc.cached(nk)

	return deleteFence{deletes: cached.Deletes, armed: true}
}

// supersededByDelete reports whether a delete landed on nk since fence was
// armed, which makes whatever the re-read is holding older than the cache.
//
// The caller holds sc.reconcileMu, which is what makes the answer and the
// publication it gates one step: PublishDelete holds the same lock across its own
// publish-and-record pair, so a delete can no longer land between this check
// and the publication it permitted.
func (sc *scopeState) supersededByDelete(nk NSKey, fence deleteFence) bool {
	if !fence.armed {
		return false
	}

	cached, _ := sc.cached(nk)

	return cached.Deletes != fence.deletes
}

// scopeState is one tracked scope.
type scopeState struct {
	scope store.Scope

	mu      sync.RWMutex
	entries map[NSKey]entry
	stale   bool
	// disconnectGen is bumped on every OpDisconnect. A reconcile records it
	// when it starts and clears stale only if it is unchanged at completion,
	// so a reconcile that spans a new disconnect cannot clear the flag that
	// disconnect just set. Guarded by mu, alongside stale.
	disconnectGen uint64

	// firstReconcileDone is closed exactly once, when this scope's first
	// reconcile finishes — successfully or not. Start waits on it instead of
	// running a reconcile of its own. firstReconcileErr carries that
	// reconcile's outcome to Start; it is written under mu BEFORE the channel
	// is closed, so a reader that observed the close is guaranteed to see it.
	// Both belong to the FIRST reconcile alone: no later reconcile writes
	// either one.
	firstReconcileDone chan struct{}
	firstReconcileOnce sync.Once
	firstReconcileErr  error // guarded by mu

	// resyncMu guards the single-slot reconcile mailbox below. One goroutine
	// per scope drains that slot, which is what serializes reconciles — two
	// OpResync events can never apply two snapshots at once — and what makes a
	// burst of reconnects coalesce into ONE pending reconcile instead of one
	// goroutine each. armReconcile still runs on the changefeed goroutine
	// and never waits on a List that is in flight.
	//
	// reconcileStop is closed when the scope is dropped, so the goroutine ends
	// with the scope instead of lingering until Close. workers, workerStarted
	// and workersDropped are guarded by Engine.workersMu, alongside the
	// WaitGroup the goroutines are registered in.
	//
	// workers holds this scope's delivery goroutines, one per key that has
	// actually published a change to a subscribed key. It lives on the state
	// rather than in one engine-wide map keyed by scope, for the same reason
	// workersDropped does: a scope dropped and brought back up is a NEW state,
	// so the drop's sweep reaches only the workers of the state it is ending,
	// and a re-activation racing that sweep keeps its own. The sweep also
	// touches only this scope's keys instead of walking every tenant's under
	// the lock each publication takes.
	//
	// workersDropped is set when this scope's delivery workers are swept, and
	// refuses every later one. A publisher still holding the old state is
	// refused forever while the new one starts workers freely.
	resyncMu       sync.Mutex
	resyncPending  *reconcileArming
	resyncSignal   chan struct{}
	reconcileStop  chan struct{}
	stopOnce       sync.Once
	workers        map[NSKey]*dispatchWorker
	workerStarted  bool
	workersDropped bool

	// reconcileMu guards reconcileGen and windows, and is held across every
	// check-and-publish pair on both sides of the fence: a reconcile deciding
	// one key, and the changefeed publishing one. That is what makes the two
	// atomic against each other — without it a feed delete lands between a
	// reconcile reading the fence and applying its snapshot row, and the
	// photograph resurrects the deleted key.
	//
	// windows holds one reconcileWindow per reconcile in flight, keyed by the
	// generation that opened it. Each reconcile reads only its own fences and
	// the feed writes into all of them, so two overlapping reconciles never
	// inherit each other's: a window that did would skip keys ITS OWN List
	// answered freshly.
	//
	// reconcileGen names the newest window. Every armReconcile bumps it, so
	// a reconcile whose generation no longer matches knows a newer OpResync
	// took the scope and abandons its snapshot instead of publishing a
	// photograph of a connection that has already dropped.
	reconcileMu  sync.Mutex
	reconcileGen uint64
	windows      map[uint64]*reconcileWindow

	unsubscribe func()
}

// newScopeState begins tracking scope.
//
// The scope starts stale: until its first reconcile completes, nothing has
// confirmed the cache against the store. Its entries map starts EMPTY and is
// deliberately NOT pre-seeded with registered defaults — the first reconcile
// must publish every registered key as a FIRST publication, which is what
// makes the fence accept it and notify subscribers registered before Start.
// A Lookup miss is the signal the Client uses to fall back to the registered
// default, so reads before the first reconcile keep working.
func newScopeState(scope store.Scope) *scopeState {
	return &scopeState{
		scope:              scope,
		entries:            make(map[NSKey]entry),
		workers:            make(map[NSKey]*dispatchWorker),
		stale:              true,
		firstReconcileDone: make(chan struct{}),
		resyncSignal:       make(chan struct{}, 1),
		reconcileStop:      make(chan struct{}),
	}
}

// armReconcile opens a reconcile window and puts it in the scope's single-slot
// mailbox as ONE indivisible step, waking the reconcile goroutine. It runs on
// the changefeed goroutine, before the List. The send is non-blocking: a full
// buffer already means "there is work", and blocking here would push a
// reconcile's List latency onto the changefeed goroutine.
//
// The arming names the generations the reconcile must still see to be allowed
// to apply its snapshot and to clear stale.
//
// It returns the arming it displaced, if any. A displaced reconcile will never
// take a photograph, so the caller releases its window: fences nobody closes
// go on collecting every feed event for the life of the scope.
//
// Opening the window and queueing it are ONE function body, with no separately
// callable step between them, because two OpResync events that opened under
// one lock and queued under another could reach the mailbox in the opposite
// order to the one they opened in. The mailbox then held the OLDER arming —
// which the reconcile goroutine drops as superseded — while the newer window
// had already been released as the one it displaced. Nothing reconciled, and
// the scope stayed stale until another resync happened to arrive. With one
// body there is no seam to split: mailbox order is arming order.
//
// Every call gets its OWN empty fences, even while another reconcile is still
// applying a snapshot under a window of its own. Sharing them was the defect:
// the feed publishes revision 3 during the first window, the store moves to
// revision 9 during the second outage, and a second reconcile that inherited
// the first window's touched set skips the very row its own List went and
// fetched — leaving the cache six revisions behind and reporting it as fresh.
// Both windows stay open and the feed fills both, so neither photograph can
// overwrite a publication the feed made while it was being taken.
func (sc *scopeState) armReconcile() (displaced *reconcileArming) {
	sc.resyncMu.Lock()
	defer sc.resyncMu.Unlock()

	// One acquisition covers marking the scope stale AND bumping the
	// generation, so clearStale — which holds the same lock across its own
	// check-and-write — can never land between the two and clear a flag this
	// arming has just set.
	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	sc.mu.Lock()
	sc.stale = true
	disconnect := sc.disconnectGen
	sc.mu.Unlock()

	sc.reconcileGen++

	if sc.windows == nil {
		sc.windows = make(map[uint64]*reconcileWindow, 1)
	}

	window := &reconcileWindow{
		touched:  make(map[NSKey]struct{}),
		unusable: make(map[NSKey]struct{}),
	}
	sc.windows[sc.reconcileGen] = window

	arm := reconcileArming{reconcile: sc.reconcileGen, disconnect: disconnect, window: window}
	displaced, sc.resyncPending = sc.resyncPending, &arm

	select {
	case sc.resyncSignal <- struct{}{}:
	default:
	}

	return displaced
}

// takeReconcile empties the mailbox, reporting whether anything was in it. A
// wake with an empty slot is normal: two submits can coalesce into one
// buffered signal.
func (sc *scopeState) takeReconcile() (reconcileArming, bool) {
	sc.resyncMu.Lock()
	defer sc.resyncMu.Unlock()

	if sc.resyncPending == nil {
		return reconcileArming{}, false
	}

	arm := *sc.resyncPending
	sc.resyncPending = nil

	return arm, true
}

// stopReconcileWorker ends this scope's reconcile goroutine. It is what a
// dropped scope uses: the scope is gone, so a goroutine still waiting for its
// next OpResync has nothing left to reconcile.
func (sc *scopeState) stopReconcileWorker() {
	sc.stopOnce.Do(func() { close(sc.reconcileStop) })
}
