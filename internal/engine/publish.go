package engine

import (
	"bytes"
	"reflect"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// publication is one candidate value arriving at the engine's single ingress.
type publication struct {
	Scope store.Scope
	NSKey
	Revision int64 // 0 = no row: the registered default is in force
	Value    any   // decoded and validated; the engine owns this copy
	// Raw is the row's JSON as the store returned it, or nil when no row
	// backs the publication. publish compares it against the cached bytes
	// before it compares decoded values; see the fence below.
	Raw       []byte
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
// sc is the caller's own scope state and is never nil: every path here — the
// ingress, the no-row ingress, a reconcile row — resolved it already, and
// logged the drop when the scope was gone. Re-resolving it through the tracked
// map would cost an RLock and a lookup per publication to answer a question
// the caller has answered, and would answer it DIFFERENTLY when the scope was
// dropped and brought back up in between: the value would land in the new
// state while the reconcile fences the caller recorded live on the old one.
//
// It does not clone. The caller owns producing a value the engine may keep —
// the ingress already decoded fresh JSON, and cloning again per publication
// would cost a reflective walk on the hot path for nothing.
func (e *Engine) publish(sc *scopeState, pub publication) (notify bool) {
	// A closed engine takes no publication: its workers are gone or going, so
	// caching a value nobody can be told about only resurrects a scope during
	// shutdown.
	if e.closed.Load() {
		return false
	}

	// The caller's state, refused once its scope has been dropped. publish no
	// longer re-resolves the scope, so this is what keeps a publication that
	// was already past its caller's resolve — a slow validator, a contended
	// reconcile mutex — from caching into a torn-down scope and starting a
	// delivery worker nothing will ever stop before Close.
	select {
	case <-sc.reconcileStop:
		return false
	default:
	}

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
	case len(pub.Raw) > 0 && bytes.Equal(pub.Raw, cached.Raw):
		// The common case by far: the echo of a write this process just made.
		// Set publishes with the revision the store returned (D4) and the
		// changefeed then re-reads the same row, so an equal revision carrying
		// identical bytes is what every write produces. Bytes are compared
		// FIRST because they answer it in one memcmp, while reflect.DeepEqual
		// walks the whole decoded document under this scope's write lock —
		// with every Lookup of the scope waiting behind it, and with equal
		// values as its worst case, since nothing short-circuits. Refresh
		// provenance and stop.
		cached.UpdatedAt = pub.UpdatedAt
		cached.UpdatedBy = pub.UpdatedBy
		sc.entries[pub.NSKey] = cached

		return false
	case reflect.DeepEqual(pub.Value, cached.Value):
		// Equal non-zero revision, different bytes, same meaning: a writer
		// reformatted the JSON or reordered object keys. Equality is on the
		// decoded value, never on raw bytes alone, so that no-op does not fire
		// a callback. This walk only runs when the bytes already differ — a
		// reformatted row, or D3's foreign writer — both rare. Refresh
		// provenance and stop.
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
		Raw:       pub.Raw,
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
	e.dispatch(sc, pub)

	return true
}

// trackedScope returns scope's state, or nil when the engine is not tracking
// it. It never creates one.
//
// Every path except bring-up resolves a scope through this: a changefeed
// event, a reconcile, a debounced re-read and a write all address a scope
// somebody else brought up, and a scope that has since been dropped — a
// suspended, deleted or rotated tenant — must stay dropped. Creating one there
// gave the tenant a cache with no changefeed behind it, no reconcile goroutine
// to confirm it, and a delivery worker registered in the WaitGroup Close
// drains: a resurrection that reads as current forever.
func (e *Engine) trackedScope(scope store.Scope) *scopeState {
	e.scopesMu.RLock()
	defer e.scopesMu.RUnlock()

	return e.scopes[scope]
}

// scopeFor returns the tracked state for scope, creating it — stale — when the
// engine is not tracking it yet. It is the bring-up path's resolver and
// nothing else's: a scope exists because Start or a tenant activation opened a
// changefeed for it, never because an event arrived addressed to it.
//
// It returns nil once Close has begun, and every caller treats that as "drop
// this work". A scope created during shutdown has no changefeed, no reconcile
// goroutine and no worker, so it could only ever be read as a fresh-looking
// cache nobody is confirming — and creating one after Close has snapshotted
// the scopes it must unsubscribe leaves state behind that nothing tears down.
func (e *Engine) scopeFor(scope store.Scope) *scopeState {
	e.scopesMu.RLock()
	sc := e.scopes[scope]
	e.scopesMu.RUnlock()

	if sc != nil {
		return sc
	}

	if e.closed.Load() {
		return nil
	}

	e.scopesMu.Lock()
	defer e.scopesMu.Unlock()

	if sc = e.scopes[scope]; sc != nil {
		return sc
	}

	if e.closed.Load() {
		return nil
	}

	if e.scopes == nil {
		e.scopes = make(map[store.Scope]*scopeState)
	}

	sc = newScopeState(scope)
	e.scopes[scope] = sc

	return sc
}
