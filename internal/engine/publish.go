package engine

import (
	"reflect"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// publication is one candidate value arriving at the engine's single ingress.
type publication struct {
	Scope store.Scope
	NSKey
	Revision  int64 // 0 = no row: the registered default is in force
	Value     any   // decoded and validated; the engine owns this copy
	UpdatedAt time.Time
	UpdatedBy string
}

// publish applies pub to its scope's cache under the revision fence and
// reports whether subscribers must be notified.
//
//   - accepted (notify=true): pub.Revision > cached.Revision, the key is not
//     cached yet, or pub.Revision == 0 (a delete or a reconcile-absent; never
//     deduplicated, and it resets the cached revision to 0).
//   - accepted (notify=true): pub.Revision == cached.Revision && != 0 but the
//     values differ — D3's foreign-writer rule. A writer that changes value
//     without bumping revision is observed, not deduplicated away.
//   - refreshed (notify=false): pub.Revision == cached.Revision && != 0 and
//     the values are equal — UpdatedAt and UpdatedBy are overwritten so
//     GetEntry provenance never lags the row, no callback fires.
//   - rejected (notify=false): pub.Revision < cached.Revision && pub.Revision != 0.
//
// publish is the only writer of scopeState.entries in the library. The whole
// body runs under one acquisition of the scope's write lock, so a compare can
// never interleave with another publication's store; splitting it into a
// check-then-refresh pair reopens the race it exists to close.
//
// It does not clone. The caller owns producing a value the engine may keep —
// the ingress already decoded fresh JSON, and cloning again per publication
// would cost a reflective walk on the hot path for nothing.
func (e *Engine) publish(pub publication) (notify bool) {
	sc := e.scopeFor(pub.Scope)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	cached, ok := sc.entries[pub.NSKey]

	switch {
	case !ok, pub.Revision == 0, pub.Revision > cached.Revision:
		// A first publication, a no-row publication (a delete or a
		// reconcile-absent, which always wins and resets the counter), or a
		// newer row. All three fall through to the store below.
	case pub.Revision < cached.Revision:
		return false
	case reflect.DeepEqual(pub.Value, cached.Value):
		// Equal non-zero revision carrying the same value: the row was
		// re-read, not rewritten. Equality is on the decoded value, never on
		// raw bytes, so a writer reformatting JSON or reordering object keys
		// does not fire a callback. Refresh provenance and stop.
		cached.UpdatedAt = pub.UpdatedAt
		cached.UpdatedBy = pub.UpdatedBy
		sc.entries[pub.NSKey] = cached

		return false
	default:
		// Equal non-zero revision carrying a different value: D3's foreign
		// writer, which changed value without bumping revision. Observed.
	}

	sc.entries[pub.NSKey] = entry{
		Value:     pub.Value,
		Revision:  pub.Revision,
		UpdatedAt: pub.UpdatedAt,
		UpdatedBy: pub.UpdatedBy,
	}

	// Still under the scope's write lock, on purpose: queueing a notification
	// is what keeps deliveries in revision order. If the queueing happened
	// after the unlock, a publication that won the fence could be overtaken on
	// the way to the worker's mailbox by one that lost it, and the subscriber
	// would see the older revision last. No callback runs here — the worker
	// goroutine does that — so the lock is held for a mutex and a
	// non-blocking channel send.
	e.dispatch(pub)

	return true
}

// scopeFor returns the tracked state for scope, creating it — stale — when the
// engine is not tracking it yet. A Set that lands before the scope's first
// reconcile is what makes the lazy creation necessary.
func (e *Engine) scopeFor(scope store.Scope) *scopeState {
	e.scopesMu.RLock()
	sc := e.scopes[scope]
	e.scopesMu.RUnlock()

	if sc != nil {
		return sc
	}

	e.scopesMu.Lock()
	defer e.scopesMu.Unlock()

	if sc = e.scopes[scope]; sc != nil {
		return sc
	}

	if e.scopes == nil {
		e.scopes = make(map[store.Scope]*scopeState)
	}

	sc = newScopeState(scope)
	e.scopes[scope] = sc

	return sc
}
