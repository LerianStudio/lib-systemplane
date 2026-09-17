package engine

import (
	"reflect"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// onResync answers store.OpResync, which the store emits after every
// successful (re)connect of a scope's changefeed. It is the convergence
// guarantee: a value written while the feed was down becomes visible without
// a second write.
//
// It runs on the changefeed goroutine and therefore does exactly two things.
// It arms the scope synchronously — stale, reconciling, fresh touched and
// unusable sets, the disconnect generation recorded — and then hands the
// snapshot to its own goroutine. The split is the point: the store guarantees
// OpResync precedes every per-key event of that connection, so arming before
// returning means no event from the new connection can slip past the fence,
// while the List never blocks the feed.
func (e *Engine) onResync(scope store.Scope) {
	sc := e.scopeFor(scope)

	go e.reconcileScope(sc, sc.beginReconcile())
}

// reconcileScope reloads scope from the store and republishes what changed.
//
// The List result is a photograph: by the time its rows are applied the
// reconnected feed may already have delivered newer facts, so every row is
// fenced twice. The touched set covers "the feed said something about this key
// at all"; the revision fence inside the ingress covers "this row is older
// than what is cached". They are independent and both required — a key deleted
// and recreated during the window comes back at a LOWER revision than the
// snapshot holds, and only the touched set can keep it.
//
// A failing List publishes nothing and leaves the scope stale: serving a
// possibly-old value is strictly better than serving defaults, and Stale is
// how the caller learns the difference.
func (e *Engine) reconcileScope(sc *scopeState, gen uint64) {
	ctx := e.dispatchContext()

	var err error

	// The first reconcile's completion path runs on EVERY exit, success or
	// failure, which is why it is a defer: a Start waiting on it would
	// otherwise block until its context expired on a transient List error.
	defer func() { sc.finishFirstReconcile(err) }()

	entries, err := e.store.List(ctx, sc.scope)
	if err != nil {
		e.logWarn(ctx, "scope reconcile failed to list, keeping cached values",
			log.String("tenant", sc.scope.Tenant),
			log.Err(err),
		)

		sc.endReconcile(gen, false)

		return
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

		if e.keepsCachedValue(sc, nk) {
			continue
		}

		e.ingestDefault(ctx, sc.scope, nk)
	}

	sc.endReconcile(gen, true)
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

// beginReconcile opens the reconcile window and reports the disconnect
// generation the reconcile must still see at the end to be allowed to clear
// stale. It runs on the changefeed goroutine, before the List.
func (sc *scopeState) beginReconcile() (gen uint64) {
	sc.mu.Lock()
	sc.stale = true
	gen = sc.disconnectGen
	sc.mu.Unlock()

	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	sc.reconciling = true
	sc.touched = make(map[NSKey]struct{})
	sc.unusable = make(map[NSKey]struct{})

	return gen
}

// endReconcile closes the window, and is fenced by the disconnect generation
// recorded when it opened.
//
// A generation that moved means the feed dropped again while this reconcile
// ran: the data just applied may already be behind, so the scope stays stale
// and the OpResync for the new connection runs its own reconcile. The window
// itself is left alone in that case too — a later OpResync may already have
// armed it, and clearing another reconcile's touched set would let a stale
// snapshot overwrite a value the feed had just published.
//
// applied is false for a reconcile that published nothing (a failed List), so
// stale survives even when the generation is unchanged.
func (sc *scopeState) endReconcile(gen uint64, applied bool) {
	sc.mu.Lock()
	current := sc.disconnectGen

	if current == gen && applied {
		sc.stale = false
	}
	sc.mu.Unlock()

	if current != gen {
		return
	}

	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	sc.reconciling = false
	sc.touched = nil
	sc.unusable = nil
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
