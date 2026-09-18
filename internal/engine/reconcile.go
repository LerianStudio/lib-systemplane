package engine

import (
	"context"
	"errors"
	"reflect"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// reconcileWindow is one reconcile's own pair of fences, and nobody else's.
//
// touched names every key the feed published since this reconcile armed: the
// feed holds the fresher fact, so the snapshot must not overwrite it. unusable
// names every key the feed tried and learned nothing about — a re-read that
// errored, a row the validator rejected — so absence from the snapshot is not
// evidence the row is gone.
//
// The two sets are per window rather than per scope because windows overlap: a
// second OpResync arrives while the first reconcile is still applying its
// photograph. A window that inherited the first one's touched set would skip
// exactly the keys its own, fresher List just answered, leaving the cache at
// whatever revision the earlier feed happened to publish while reporting the
// scope as no longer stale.
type reconcileWindow struct {
	touched  map[NSKey]struct{}
	unusable map[NSKey]struct{}
}

// reconcileArming is what beginReconcile hands the reconcile that follows it.
//
// reconcile names the window this reconcile opened: a newer OpResync bumps it,
// which is how a reconcile learns its snapshot has been superseded and must be
// abandoned rather than applied. disconnect is the connection generation the
// reconcile must still see at the end to be allowed to clear stale. window is
// this reconcile's own fences, read only by it.
type reconcileArming struct {
	reconcile  uint64
	disconnect uint64
	window     *reconcileWindow
}

// onResync answers store.OpResync, which the store emits after every
// successful (re)connect of a scope's changefeed. It is the convergence
// guarantee: a value written while the feed was down becomes visible without
// a second write.
//
// It runs on the changefeed goroutine and therefore does exactly two things.
// It arms the scope synchronously — stale, reconciling, the touched and
// unusable sets, both generations recorded — and then hands the snapshot to
// the scope's ONE reconcile goroutine. The split is the point: the store
// guarantees OpResync precedes every per-key event of that connection, so
// arming before returning means no event from the new connection can slip
// past the fence, while the List never blocks the feed.
//
// The handoff is a single-slot mailbox, not a goroutine per event. A flapping
// connection emits a burst of OpResync, and a goroutine each meant N
// untracked goroutines queued behind one List, none of which Close waited for.
// The burst now coalesces to one pending reconcile, and the one goroutine
// draining the slot is registered in the WaitGroup Close drains.
func (e *Engine) onResync(scope store.Scope) {
	sc := e.scopeForEvent(scope, NSKey{})
	if sc == nil {
		return
	}

	if displaced := sc.armReconcile(); displaced != nil {
		sc.closeWindow(*displaced)
	}

	if !e.ensureReconcileWorker(sc) {
		// Shutdown won the race: nothing will drain the mailbox, so this
		// arming's fences are released rather than left open on a scope the
		// engine is tearing down.
		if pending, ok := sc.takeReconcile(); ok {
			sc.closeWindow(pending)
		}
	}
}

// ensureReconcileWorker starts sc's one reconcile goroutine, at most once, and
// reports whether the scope has one.
//
// The started flag and the WaitGroup Add are under the same lock Close takes
// before it waits, so every Add provably happens-before that Wait. It returns
// false once Close has shut the door: a goroutine started then would either be
// waited on by a Wait already in progress — which Go answers by killing the
// process — or never be waited on at all.
func (e *Engine) ensureReconcileWorker(sc *scopeState) bool {
	e.workersMu.Lock()

	if e.workersClosed {
		e.workersMu.Unlock()

		return false
	}

	if sc.workerStarted {
		e.workersMu.Unlock()

		return true
	}

	sc.workerStarted = true

	e.dispatchWG.Add(1)
	e.workersMu.Unlock()

	runtime.SafeGoWithContextAndComponent(e.dispatchContext(), e.logger,
		"systemplane.engine", "reconcile", runtime.KeepRunning,
		func(ctx context.Context) {
			defer e.dispatchWG.Done()

			e.runReconcileWorker(ctx, sc)
		})

	return true
}

// runReconcileWorker drains sc's reconcile mailbox until the engine shuts down
// or the scope is dropped. It is the only goroutine that reconciles this
// scope, which is what serializes two OpResync events without a mutex a
// reconcile could queue on for the whole of a hung List.
func (e *Engine) runReconcileWorker(ctx context.Context, sc *scopeState) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-sc.reconcileStop:
			return
		case <-sc.resyncSignal:
			if arm, ok := sc.takeReconcile(); ok {
				e.runOneReconcile(ctx, sc, arm)
			}
		}
	}
}

// errFirstReconcilePanicked is what a caller waiting on the first reconcile is
// told when that reconcile panicked. The panic itself is reported through
// lib-observability, which recovers it so the scope keeps its one reconcile
// goroutine; this sentinel is how the wait ends instead of blocking until the
// caller's own context expires.
//
// It deliberately wraps nothing from the store: a panic is an internal engine
// failure, not a value the backend rejected.
var errFirstReconcilePanicked = errors.New("systemplane: the first reconcile panicked")

// runOneReconcile keeps a panicking reconcile from taking the scope's only
// reconcile goroutine with it. The validator the ingress runs is consumer
// code: recovering per reconcile rather than per goroutine is what makes the
// next OpResync still reconcile after one bad row.
//
// A recovered panic must still complete the first reconcile. reconcileScope is
// what hands Start that outcome, and a panic skips it: Start then waits on a
// channel nothing will ever close, for as long as its own context allows.
//
// The two defers are ordered deliberately. This one is registered FIRST so it
// runs LAST, after the recovery below has absorbed the panic and reported it,
// and it fires only when reconcileScope did not return — a superseded
// reconcile returns normally and completes nothing, which is correct.
func (e *Engine) runOneReconcile(ctx context.Context, sc *scopeState, arm reconcileArming) {
	returned := false

	defer func() {
		if !returned {
			sc.finishFirstReconcile(errFirstReconcilePanicked)
		}
	}()

	defer runtime.RecoverAndLogWithContext(ctx, e.logger, "systemplane.engine", "reconcile")

	e.reconcileScope(sc, arm)

	returned = true
}

// reconcileScope reloads scope from the store and republishes what changed.
//
// Two OpResync events must never apply two snapshots at once. The scope's one
// reconcile goroutine is what serializes them: this function is only ever
// called from it, one mailbox item at a time.
//
// A reconcile that finds itself superseded publishes nothing and completes
// nothing. The newer window owns the scope, takes its own photograph, and is
// what hands Start the first reconcile's outcome — announcing a result here
// would let Start return on a snapshot that was never applied.
func (e *Engine) reconcileScope(sc *scopeState, arm reconcileArming) {
	// Every exit releases this reconcile's OWN window, superseded or not: the
	// fences exist only for the span between this reconcile's List and its
	// application, and a window nobody closes goes on collecting every feed
	// event for the life of the scope.
	defer sc.closeWindow(arm)

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

	entries, err := e.listSnapshot(ctx, sc.scope)
	if err != nil {
		const msg = "scope reconcile failed to list, keeping cached values"

		fields := []log.Field{log.String("tenant", sc.scope.Tenant), log.Err(err)}

		// Every ordinary Close with a reconcile in flight cancels its List, so
		// reporting that at WARN makes a clean shutdown look like an incident
		// and trains operators to ignore the channel a real failure uses. The
		// reconcile timeout that ends a hung List is DeadlineExceeded, not
		// Canceled, and stays at WARN like every other failure.
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			e.logDebug(ctx, msg, fields...)
		} else {
			e.logWarn(ctx, msg, fields...)
		}

		// Nothing is cleared here: the deferred closeWindow releases this
		// reconcile's own fences and stale stays true because clearStale is
		// never reached. A failing reconcile must not disturb the window a
		// newer OpResync has already opened.
		return false, err
	}

	if sc.superseded(arm) {
		return true, nil
	}

	seen := make(map[NSKey]struct{}, len(entries))

	for _, se := range entries {
		seen[NSKey{Namespace: se.Namespace, Key: se.Key}] = struct{}{}

		if e.applySnapshotRow(ctx, sc, arm, se) {
			return true, nil
		}
	}

	for _, nk := range e.registeredKeys() {
		if _, ok := seen[nk]; ok {
			continue
		}

		if e.applyAbsentKey(ctx, sc, arm, nk) {
			return true, nil
		}
	}

	sc.clearStale(arm)

	return false, nil
}

// listSnapshot takes the photograph under a bound.
//
// The lifecycle context alone is not a bound: it is canceled only by Close, so
// a backend that accepted the call and never answered would hold the scope's
// one reconcile goroutine — and therefore every later OpResync for that scope
// — for as long as the process ran. The timeout turns that into an ordinary
// reconcile failure: the cache is kept, the scope stays Stale, and the next
// OpResync retries.
func (e *Engine) listSnapshot(ctx context.Context, scope store.Scope) ([]store.Entry, error) {
	timeout := e.reconcileTimeout
	if timeout <= 0 {
		timeout = defaultReconcileTimeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return e.store.List(ctx, scope)
}

// applySnapshotRow decides one row of the photograph, holding the scope's
// fences across the decision AND the publication.
//
// One lock over the pair is what makes a reconcile atomic against the
// changefeed: the feed takes the same lock around its own publish-and-record
// pair, so a delete can no longer land between "the fence says nothing" and
// "the snapshot row is published" and be overwritten by a photograph that
// predates it.
//
// Reporting superseded abandons the rest of the snapshot: a newer OpResync
// owns the scope and is taking a fresher one. The check is per row, not once
// before the loop, so a window that moves mid-application stops immediately
// instead of finishing a photograph of a connection that has already dropped.
//
// A row the ingress refuses is the one case where "the snapshot carried this
// key" is not the same as "the engine learned its value"; see the FC-11 note
// below the ingest call.
func (e *Engine) applySnapshotRow(ctx context.Context, sc *scopeState, arm reconcileArming, se store.Entry) (superseded bool) {
	nk := NSKey{Namespace: se.Namespace, Key: se.Key}

	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	if sc.reconcileGen != arm.reconcile {
		return true
	}

	// The unusable set is deliberately NOT consulted here: a key whose feed
	// re-read failed still takes a perfectly good snapshot row.
	if _, touched := arm.window.touched[nk]; touched {
		return false
	}

	if e.ingest(ctx, sc.scope, se) {
		return false
	}

	// The ingress refused the row — undecodable, or refused by the registered
	// validator — and has already said so at WARN. FC-11 as amended: on the
	// FIRST reconcile, with nothing cached, the key is announced with its
	// registered default at Revision 0 exactly like an absent row. That is the
	// same resolution keepsCachedValue gives an absent-and-unusable key with an
	// empty cache, and it is what makes a read serve the default rather than
	// report a miss for a key the consumer registered.
	//
	// Later reconciles never repeat it: the default is cached by then, so a row
	// that stays rejected leaves the value in force in force (D-G4).
	if _, isCached := sc.cached(nk); !isCached && sc.firstReconcilePending() {
		e.ingestDefault(ctx, sc.scope, nk)
	}

	return false
}

// applyAbsentKey decides one registered key the photograph did not carry,
// under the same lock and for the same reason as applySnapshotRow.
//
// This publication is the one that cannot be undone by a revision fence —
// revision 0 always wins — so it must not outlive the window that authorised
// it, and it must not be able to land on top of a value the feed published
// microseconds earlier.
func (e *Engine) applyAbsentKey(ctx context.Context, sc *scopeState, arm reconcileArming, nk NSKey) (superseded bool) {
	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	if sc.reconcileGen != arm.reconcile {
		return true
	}

	if e.keepsCachedValue(sc, arm, nk) {
		return false
	}

	e.ingestDefault(ctx, sc.scope, nk)

	return false
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
//
// The caller holds sc.reconcileMu: the answer and the publication it gates are
// one atomic step against the feed.
func (e *Engine) keepsCachedValue(sc *scopeState, arm reconcileArming, nk NSKey) bool {
	if _, touched := arm.window.touched[nk]; touched {
		return true
	}

	cached, isCached := sc.cached(nk)
	if !isCached {
		return false
	}

	if _, unusable := arm.window.unusable[nk]; unusable {
		return true
	}

	def, registered := e.lookup(nk.Namespace, nk.Key)

	return registered && cached.Revision == 0 && reflect.DeepEqual(cached.Value, def.Default)
}

// beginReconcile opens a reconcile window and reports the generations the
// reconcile must still see to be allowed to apply its snapshot and to clear
// stale. It runs on the changefeed goroutine, before the List.
//
// Every call gets its OWN empty fences, even while another reconcile is still
// applying a snapshot under a window of its own. Sharing them was the defect:
// the feed publishes revision 3 during the first window, the store moves to
// revision 9 during the second outage, and a second reconcile that inherited
// the first window's touched set skips the very row its own List went and
// fetched — leaving the cache six revisions behind and reporting it as fresh.
// Both windows stay open and the feed fills both, so neither photograph can
// overwrite a publication the feed made while it was being taken.
func (sc *scopeState) beginReconcile() reconcileArming {
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

	window := &reconcileWindow{
		touched:  make(map[NSKey]struct{}),
		unusable: make(map[NSKey]struct{}),
	}

	if sc.windows == nil {
		sc.windows = make(map[uint64]*reconcileWindow, 1)
	}

	sc.windows[sc.reconcileGen] = window

	return reconcileArming{reconcile: sc.reconcileGen, disconnect: disconnect, window: window}
}

// superseded reports whether a newer OpResync has taken the window this
// reconcile armed. Such a reconcile abandons its snapshot: it is holding a
// photograph of a connection that has already dropped.
func (sc *scopeState) superseded(arm reconcileArming) bool {
	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	return sc.reconcileGen != arm.reconcile
}

// closeWindow releases the fences this reconcile armed, and only those. A
// window still open belongs to a newer OpResync that is relying on the feed to
// keep filling it, which is why a failing or superseded reconcile can no
// longer disarm anyone but itself.
func (sc *scopeState) closeWindow(arm reconcileArming) {
	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	delete(sc.windows, arm.reconcile)
}

// clearStale reports the scope confirmed against the store, fenced by both
// generations.
//
// A window generation that moved means a newer OpResync owns the scope: this
// reconcile is holding a photograph of a connection that has already dropped.
// A disconnect generation that moved means the feed dropped again while this
// reconcile ran, so the data it just applied may already be behind. Either
// way the scope stays stale and the next OpResync reconciles it.
//
// It is called only by a reconcile that applied a snapshot: a failed List
// never reaches it, so stale survives even when both generations are
// unchanged.
//
// The check and the write are ONE acquisition of reconcileMu, the lock
// beginReconcile holds across arming. Split in two, an OpResync arriving
// between them marked the scope stale and armed its window, and this reconcile
// then cleared the flag that resync had just set — a scope reporting itself
// confirmed against a connection that had already dropped.
func (sc *scopeState) clearStale(arm reconcileArming) {
	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	if sc.reconcileGen != arm.reconcile {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.disconnectGen == arm.disconnect {
		sc.stale = false
	}
}

// record writes nk's outcome into EVERY open reconcile window. Windows overlap
// whenever the feed drops and comes back while a reconcile is still applying
// its photograph, and each of them needs the fact for its own snapshot: the
// older one to stop its rows overwriting this publication, the newer one for
// the same reason a moment later.
//
// A usable value goes in touched, so a reconcile skips that key — the feed
// holds the fresher fact. Anything the feed could not turn into a value goes
// in unusable, so a reconcile keeps the cached value instead of treating the
// key as absent and publishing the registered default over it.
//
// The caller holds sc.reconcileMu, and for a publication it holds it across
// the publication too: that pairing is what makes the feed atomic against a
// reconcile.
func (sc *scopeState) record(nk NSKey, usable bool) {
	for _, window := range sc.windows {
		if usable {
			window.touched[nk] = struct{}{}

			continue
		}

		window.unusable[nk] = struct{}{}
	}
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
