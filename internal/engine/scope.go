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
	Value     any
	Revision  int64
	UpdatedAt time.Time
	UpdatedBy string
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
	// goroutine each. beginReconcile still runs on the changefeed goroutine
	// and never waits on a List that is in flight.
	//
	// reconcileStop is closed when the scope is dropped, so the goroutine ends
	// with the scope instead of lingering until Close. workerStarted is
	// guarded by Engine.workersMu, alongside the WaitGroup the goroutine is
	// registered in.
	resyncMu      sync.Mutex
	resyncPending *reconcileArming
	resyncSignal  chan struct{}
	reconcileStop chan struct{}
	stopOnce      sync.Once
	workerStarted bool

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
	// reconcileGen names the newest window. Every beginReconcile bumps it, so
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
		stale:              true,
		firstReconcileDone: make(chan struct{}),
		resyncSignal:       make(chan struct{}, 1),
		reconcileStop:      make(chan struct{}),
	}
}

// submitReconcile puts arm in the scope's single-slot mailbox and wakes its
// reconcile goroutine. The send is non-blocking: a full buffer already means
// "there is work", and blocking here would push a reconcile's List latency
// onto the changefeed goroutine.
//
// It returns the arming it displaced, if any. A displaced reconcile will never
// take a photograph, so the caller releases its window: fences nobody closes
// go on collecting every feed event for the life of the scope.
func (sc *scopeState) submitReconcile(arm reconcileArming) (displaced *reconcileArming) {
	sc.resyncMu.Lock()
	displaced, sc.resyncPending = sc.resyncPending, &arm
	sc.resyncMu.Unlock()

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

// firstReconcilePending reports whether this scope's first reconcile has not
// completed yet. It is a non-blocking read of the very channel Start waits on,
// so the two can never disagree about which reconcile is the first one.
func (sc *scopeState) firstReconcilePending() bool {
	select {
	case <-sc.firstReconcileDone:
		return false
	default:
		return true
	}
}

// stopReconcileWorker ends this scope's reconcile goroutine. It is what a
// dropped scope uses: the scope is gone, so a goroutine still waiting for its
// next OpResync has nothing left to reconcile.
func (sc *scopeState) stopReconcileWorker() {
	sc.stopOnce.Do(func() { close(sc.reconcileStop) })
}
