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

	// runMu serializes the List-and-apply body of a reconcile, so two
	// OpResync events can never apply two snapshots at once. beginReconcile
	// deliberately does NOT take it: arming runs on the changefeed goroutine
	// and must never wait on a List that is still in flight.
	runMu sync.Mutex

	// reconcileMu guards reconciling, reconcileGen, touched and unusable. The
	// feed callback records every key it publishes while a reconcile is in
	// flight so the reconcile skips those keys when applying its List
	// snapshot, and every key whose reread failed or was rejected so the
	// reconcile does not treat it as absent.
	//
	// reconcileGen names the open window. Every beginReconcile bumps it, so a
	// reconcile whose generation no longer matches knows a newer OpResync took
	// the scope and abandons its snapshot instead of publishing a photograph
	// of a connection that has already dropped.
	reconcileMu  sync.Mutex
	reconciling  bool
	reconcileGen uint64
	touched      map[NSKey]struct{}
	unusable     map[NSKey]struct{}

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
	}
}
