package engine

import (
	"reflect"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// reconcileArming is what beginReconcile hands the reconcile that follows it.
//
// reconcile names the window this reconcile opened: a newer OpResync bumps it,
// which is how a reconcile learns its snapshot has been superseded and must be
// abandoned rather than applied. disconnect is the connection generation the
// reconcile must still see at the end to be allowed to clear stale.
type reconcileArming struct {
	reconcile  uint64
	disconnect uint64
}

// onResync answers store.OpResync, which the store emits after every
// successful (re)connect of a scope's changefeed. It is the convergence
// guarantee: a value written while the feed was down becomes visible without
// a second write.
//
// It runs on the changefeed goroutine and therefore does exactly two things.
// It arms the scope synchronously — stale, reconciling, the touched and
// unusable sets, both generations recorded — and then hands the snapshot to
// its own goroutine. The split is the point: the store guarantees OpResync
// precedes every per-key event of that connection, so arming before returning
// means no event from the new connection can slip past the fence, while the
// List never blocks the feed.
func (e *Engine) onResync(scope store.Scope) {
	sc := e.scopeFor(scope)
	arm := sc.beginReconcile()

	runtime.SafeGo(e.logger, "systemplane.engine.reconcile", runtime.KeepRunning, func() {
		e.reconcileScope(sc, arm)
	})
}

// reconcileScope reloads scope from the store and republishes what changed,
// one reconcile at a time.
//
// Two OpResync events must never apply two snapshots at once. runMu is what
// serializes them; beginReconcile deliberately does not take it, so arming
// stays instant on the changefeed goroutine and only the List and its
// application queue here.
//
// A reconcile that finds itself superseded publishes nothing and completes
// nothing. The newer window owns the scope, takes its own photograph, and is
// what hands Start the first reconcile's outcome — announcing a result here
// would let Start return on a snapshot that was never applied.
func (e *Engine) reconcileScope(sc *scopeState, arm reconcileArming) {
	sc.runMu.Lock()
	defer sc.runMu.Unlock()

	if sc.superseded(arm) {
		return
	}

	superseded, err := e.applyScope(sc, arm)
	if superseded {
		return
	}

	// The first reconcile's completion path runs on every exit that owned the
	// window, success or failure: a Start waiting on it would otherwise block
	// until its context expired on a transient List error.
	sc.finishFirstReconcile(err)
}

// applyScope takes the photograph and applies it.
//
// The List result is exactly that — a photograph: by the time its rows are
// applied the reconnected feed may already have delivered newer facts, so
// every row is fenced twice. The touched set covers "the feed said something
// about this key at all"; the revision fence inside the ingress covers "this
// row is older than what is cached". They are independent and both required —
// a key deleted and recreated during the window comes back at a LOWER revision
// than the snapshot holds, and only the touched set can keep it.
//
// A third fence sits on top of both: the window generation. A newer OpResync
// arriving while the List is in flight means the feed dropped and came back,
// so these rows are older than the snapshot about to be taken. Applying them
// anyway would let step 2 publish a registered default at revision 0 — which
// always wins the fence — over a value the newer reconcile has converged on,
// a silent config reset announced to subscribers.
//
// A failing List publishes nothing and leaves the scope stale: serving a
// possibly-old value is strictly better than serving defaults, and Stale is
// how the caller learns the difference.
func (e *Engine) applyScope(sc *scopeState, arm reconcileArming) (superseded bool, err error) {
	ctx := e.dispatchContext()

	entries, err := e.store.List(ctx, sc.scope)
	if err != nil {
		e.logWarn(ctx, "scope reconcile failed to list, keeping cached values",
			log.String("tenant", sc.scope.Tenant),
			log.Err(err),
		)

		sc.endReconcile(arm, false)

		return false, err
	}

	if sc.superseded(arm) {
		return true, nil
	}

	seen := make(map[NSKey]struct{}, len(entries))

	for _, se := range entries {
		nk := NSKey{Namespace: se.Namespace, Key: se.Key}
		seen[nk] = struct{}{}

		// The unusable set is deliberately NOT consulted here: a key whose
		// feed re-read failed still takes a perfectly good snapshot row.
		if touched, _ := sc.feedRecorded(nk); touched {
			continue
		}

		e.ingest(ctx, sc.scope, se)
	}

	for _, nk := range e.registry.Keys() {
		if _, ok := seen[nk]; ok {
			continue
		}

		// Re-checked per key, not once before the loop: this is the only
		// publication that cannot be undone by a revision fence, so it must
		// not outlive the window that authorised it.
		if sc.superseded(arm) {
			return true, nil
		}

		if e.keepsCachedValue(sc, nk) {
			continue
		}

		e.ingestDefault(ctx, sc.scope, nk)
	}

	sc.endReconcile(arm, true)

	return false, nil
}

// keepsCachedValue reports whether a registered key absent from the snapshot
// must be left alone instead of falling back to its registered default.
//
// Three reasons, in order:
//
//   - the feed published the key during this reconcile's window: the feed
//     holds the fresher fact, which is what makes delete-then-recreate survive
//     a concurrent reconcile;
//   - the feed tried and learned nothing usable — a re-read that errored, or a
//     row the validator rejected — while the cache holds a value. Absence from
//     the snapshot is then not evidence the row is gone, and publishing the
//     default would turn one transient read failure into a silent config
//     reset. With nothing cached there is nothing to protect, so the default
//     is published and the first reconcile still announces every key.
//   - the cache already holds exactly the registered default with no row
//     behind it. Revision 0 never loses the fence, so republishing it would
//     deliver a second Change for a key that never changed.
func (e *Engine) keepsCachedValue(sc *scopeState, nk NSKey) bool {
	touched, unusable := sc.feedRecorded(nk)
	if touched {
		return true
	}

	cached, isCached := sc.cached(nk)
	if !isCached {
		return false
	}

	if unusable {
		return true
	}

	def, registered := e.registry.Lookup(nk.Namespace, nk.Key)

	return registered && cached.Revision == 0 && reflect.DeepEqual(cached.Value, def.Default)
}

// beginReconcile opens the reconcile window and reports the generations the
// reconcile must still see to be allowed to apply its snapshot and to clear
// stale. It runs on the changefeed goroutine, before the List.
//
// The touched and unusable sets are re-used, not replaced, when a window is
// already open: a reconcile may be applying a snapshot under it right now, and
// discarding what the feed recorded for that reconcile would let its
// photograph overwrite a value the feed has just published. A superset of the
// fence only ever skips more keys, which is the safe direction. A closed
// window starts from fresh sets, so a key the feed touched during one reconcile
// never fences the next one.
func (sc *scopeState) beginReconcile() reconcileArming {
	sc.mu.Lock()
	sc.stale = true
	disconnect := sc.disconnectGen
	sc.mu.Unlock()

	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	sc.reconcileGen++

	if !sc.reconciling {
		sc.touched = make(map[NSKey]struct{})
		sc.unusable = make(map[NSKey]struct{})
	}

	sc.reconciling = true

	return reconcileArming{reconcile: sc.reconcileGen, disconnect: disconnect}
}

// superseded reports whether a newer OpResync has taken the window this
// reconcile armed. Such a reconcile abandons its snapshot: it is holding a
// photograph of a connection that has already dropped.
func (sc *scopeState) superseded(arm reconcileArming) bool {
	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	return sc.reconcileGen != arm.reconcile
}

// endReconcile closes the window this reconcile opened, and is fenced by both
// generations.
//
// A window generation that moved means a newer OpResync owns the window now;
// closing it here would disarm the fence that reconcile is relying on and
// leave every later feed event of its window unrecorded.
//
// A disconnect generation that moved means the feed dropped again while this
// reconcile ran: the data just applied may already be behind, so the scope
// stays stale and the OpResync for the new connection runs its own reconcile.
//
// applied is false for a reconcile that published nothing (a failed List), so
// stale survives even when both generations are unchanged.
func (sc *scopeState) endReconcile(arm reconcileArming, applied bool) {
	sc.reconcileMu.Lock()

	if sc.reconcileGen != arm.reconcile {
		sc.reconcileMu.Unlock()

		return
	}

	sc.reconciling = false
	sc.touched = nil
	sc.unusable = nil
	sc.reconcileMu.Unlock()

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.disconnectGen == arm.disconnect && applied {
		sc.stale = false
	}
}

// feedRecorded reports what the feed learned about nk since this reconcile
// armed: touched means it published a value, unusable means it tried and
// learned nothing it could publish. It is read per key at the moment that key
// is decided, not once up front, so an event landing while the snapshot is
// being applied still fences the key it names.
func (sc *scopeState) feedRecorded(nk NSKey) (touched, unusable bool) {
	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	_, touched = sc.touched[nk]
	_, unusable = sc.unusable[nk]

	return touched, unusable
}

// cached returns nk's published entry without copying its value: the reconcile
// only inspects the revision and compares against the registered default, and
// never hands what it reads to a caller.
func (sc *scopeState) cached(nk NSKey) (entry, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	cached, ok := sc.entries[nk]

	return cached, ok
}

// finishFirstReconcile hands this scope's FIRST reconcile outcome to whoever
// waits on it — Start, for the single-tenant scope. The error is written under
// the scope mutex BEFORE the channel closes, so a waiter that observed the
// close is guaranteed to see it, and both live inside the Once so no later
// reconcile can overwrite a result Start already depends on.
func (sc *scopeState) finishFirstReconcile(err error) {
	sc.firstReconcileOnce.Do(func() {
		sc.mu.Lock()
		sc.firstReconcileErr = err
		sc.mu.Unlock()

		close(sc.firstReconcileDone)
	})
}
