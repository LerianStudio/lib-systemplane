package engine

import (
	"context"
	"errors"
	"time"

	"github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// feedTimeout bounds the store re-read a changefeed event schedules. It is
// derived from the engine's lifecycle context, so a Close cancels an in-flight
// re-read instead of holding shutdown for up to the full window.
const feedTimeout = 5 * time.Second

// defaultReconcileTimeout bounds a reconcile's whole-scope Store.List for any
// engine that does not lower it. It is longer than feedTimeout because a
// snapshot of every key is a bigger read than one row, and shorter than the
// DEFAULT close timeout — a consumer that configures a close timeout below it
// can still see a hung List hold shutdown until this bound expires.
const defaultReconcileTimeout = 15 * time.Second

// scopeNSKey is the debouncer's key: one quiet window per key per scope, so a
// burst of notifications for one tenant's key never collapses another tenant's
// notification for the same key.
type scopeNSKey struct {
	Tenant    string
	Namespace string
	Key       string
}

// onEvent is the callback the engine hands to Store.Subscribe. It runs on the
// backend's changefeed goroutine, so it must never call a subscriber and never
// block on one: everything it publishes goes through publish, which hands the
// delivery to that key's worker.
//
// Four behaviors, one per operation:
//
//   - OpDisconnect marks the scope stale and does nothing else. There is no
//     connection to read through, so there is nothing to reconcile and nothing
//     to publish; reads keep serving the last published value and Stale is how
//     a caller learns nobody is confirming it.
//   - OpResync carries no namespace or key. It says the whole scope must be
//     reloaded, so it is neither debounced per key nor re-read as an upsert:
//     it arms the scope's reconcile window here, synchronously, and the reload
//     itself runs on its own goroutine.
//   - OpDelete publishes the registered default at revision 0 with no store
//     read at all: a delete is self-describing.
//   - anything else is treated as an upsert: the store is re-read once the
//     key's quiet window closes, and the row goes through the ingress.
func (e *Engine) onEvent(evt store.Event) {
	// An event that arrives while Close is running is dropped whole: there is
	// nobody left to deliver it to, and answering it would create a scope or
	// start a reconcile the engine is in the middle of tearing down.
	if e.closed.Load() {
		return
	}

	switch evt.Op {
	case store.OpDisconnect:
		e.markStale(evt.Scope)

		return
	case store.OpResync:
		e.onResync(evt.Scope)

		return
	}

	nk := NSKey{Namespace: evt.Namespace, Key: evt.Key}

	// A key this process never registered is dropped here, at the feed, rather
	// than after a re-read the ingress throws away. `systemplane_entries` is
	// one table per database and every consumer sharing it notifies on its own
	// keys, so answering a foreign upsert cost a debounce timer, a goroutine
	// Close waits for and a pooled connection, per foreign write.
	//
	// The trade that bought those back is the drop line itself: it used to sit
	// behind the debouncer's quiet window, so a chatty foreign writer reported
	// one line per window and now reports one per write — which is why the
	// fields below are built only when DEBUG is actually enabled.
	//
	// What makes that drop safe is the SETUP ORDER the facade documents, not a
	// lock: registration and Start are sequential setup calls — Register, then
	// Start, and a Register after Start is refused with ErrRegisterAfterStart
	// — so within supported usage every key this process will ever register is
	// in the registry before bringUpScope opens the feed. Nothing here
	// synchronizes against a registry write: the engine takes no lock the
	// registry writer takes, and the feed reads through Registry.Lookup like
	// any other caller.
	//
	// A Register that does land after this feed opened is therefore
	// unsupported rather than handled, and its residual is bounded: the
	// notifications for that key that arrived before it landed were dropped
	// here, so the key holds its registered default until the next reconcile
	// reads its row — the whole-scope repair every OpResync drives.
	//
	// Outcomes are unchanged — prepare rejects the same rows on the upsert
	// path, ingestDefault the same keys on the delete path, and a nil registry
	// still reports nothing registered, so it rejects everything exactly as
	// before.
	if _, registered := e.lookup(nk.Namespace, nk.Key); !registered {
		if e.debugEnabled() {
			e.logDebug(e.dispatchContext(), "changefeed event for unregistered key, skipping",
				log.String(constants.AttrKeyTenantID, evt.Scope.Tenant),
				log.String("namespace", nk.Namespace),
				log.String("keyname", nk.Key),
			)
		}

		return
	}

	if evt.Op == store.OpDelete {
		e.applyDelete(evt.Scope, nk)

		return
	}

	// A nil debouncer refreshes inline rather than dropping the event:
	// swallowing a changefeed notification would leave the cache silently
	// behind the store until the next reconcile.
	if e.debouncer == nil {
		e.refreshKey(evt.Scope, nk)

		return
	}

	// With a real quiet window the debouncer fires the re-read on a timer
	// goroutine of its own, which Close must wait for: an untracked one is
	// still inside Store.Get after Close has returned and the Client is about
	// to close the store under it. With no window Submit runs inline on this
	// changefeed goroutine, whose lifetime the store owns — registering THAT
	// in the WaitGroup would make Close wait on the goroutine it is
	// unsubscribing.
	//
	// What that mode costs is head-of-line blocking, and it is the documented
	// trade of WithDebounce(0) rather than an oversight: the inline re-read
	// occupies the scope's single changefeed goroutine for a Store.Get bounded
	// by feedTimeout PLUS the consumer's validator, which is bounded by
	// nothing, and every other key's notification waits behind it.
	//
	// Exactly one closure is built, in the branch that wants it. Building the
	// inline one up front and overwriting it here cost one discarded heap
	// allocation on every upsert event the feed delivers.
	var refresh func()

	if e.debounceAsync {
		refresh = func() { e.trackedRefresh(evt.Scope, nk) }
	} else {
		refresh = func() { e.refreshKey(evt.Scope, nk) }
	}

	e.debouncer.Submit(scopeNSKey{Tenant: evt.Scope.Tenant, Namespace: nk.Namespace, Key: nk.Key}, refresh)
}

// trackedRefresh runs a debounced re-read as engine work Close waits for.
//
// A timer that has already fired cannot be canceled, so the only way Close can
// promise "no store call after I return" is to know about the goroutine. The
// door beginWork checks is the same one dispatch workers pass, so a re-read
// that loses the race to Close is dropped whole rather than reaching a store
// the Client is about to close.
//
// These re-reads run concurrently, one goroutine per key whose quiet window
// closed, and nothing bounds how many are in flight at once. What the
// debouncer bounds is PENDING TIMERS — at most one per (scope, key), the feed
// having dropped unregistered keys before the debouncer ever sees them. It
// does not bound the work those timers start: Debouncer.fire deletes the timer
// entry BEFORE it invokes fn, so a notification arriving while the re-read is
// still inside Store.Get arms a fresh timer and the two overlap. Under
// sustained notifications on one key against a store slower than the window,
// in-flight re-reads for that single key grow to roughly feedTimeout divided
// by the window — about 50 at the 5s and 100ms defaults.
//
// There is no semaphore. A consumer sizing its *sql.DB pool should read
// registered keys times tracked scopes as the floor, and add that per-key
// overlap for as many keys as a degraded store can be hot on at once.
func (e *Engine) trackedRefresh(scope store.Scope, nk NSKey) {
	if !e.beginWork() {
		return
	}

	defer e.dispatchWG.Done()

	e.refreshKey(scope, nk)
}

// recoverRefresh reports a panic raised under a changefeed re-read, naming the
// key it happened on, and records that key as unusable.
//
// It is deferred by refreshKey rather than by the one caller that wraps it, so
// all three re-read paths carry the identity: the tracked one, the inline one
// a consumer on WithDebounce(0) takes, and the one an engine with no debouncer
// takes. The debouncer's own guard would catch a panic on two of them, and
// that is what this replaces at the top of the stack rather than duplicates:
// runtime.RecoverAndLog logs source="debounce" and nothing else, and in
// production mode the value and the stack are redacted out of that line, so an
// operator learns something under the debouncer blew up and never which
// tenant, namespace or key. The debouncer's guard stays the outer net —
// including for a consumer logger that panics on the line below.
//
// Sitting inside refreshKey is also what orders it against Close. On the
// tracked path trackedRefresh releases the WaitGroup with a defer of its own,
// and an unwind runs this frame's defers first, so the report and the panic
// metric are complete before Close can return; deferred alongside that
// release, the recovery ran after it and Close returned mid-line.
//
// The key is recorded as unusable for the same reason a re-read that errored
// is: a panic teaches the engine nothing about the key, and a reconcile in
// flight that sees an empty fence treats the key absent from its snapshot as
// deleted and publishes the registered default at revision 0 — turning one
// exploding store call into a silent config reset. The deferred unlocks of
// every frame between here and the panic have already run by the time this
// one does, so the scope's reconcile mutex is free to take.
//
// That fence is written FIRST, before any consumer code can run. Everything
// below it is the consumer's — its logger, and whatever lib-observability's
// handler calls into — and nothing bounds how long it holds this goroutine. A
// reconcile that reaches the key while one slow log line is in flight would
// read the empty fence and publish the default over a live value, which is
// precisely the reset the fence exists to prevent.
//
// HandlePanicValue rather than a re-panic into that net because only it
// records panic_recovered_total and the span event: RecoverAndLog takes no
// context and records neither. The recovered value stays out of the identity
// line — it is whatever the panicking code was holding, and redacting it
// belongs with the handler.
func (e *Engine) recoverRefresh(scope store.Scope, nk NSKey) {
	recovered := recover()
	if recovered == nil {
		return
	}

	ctx := e.dispatchContext()

	e.recordFeedOutcome(scope, nk, false)

	e.logError(ctx, "systemplane.engine: changefeed re-read panicked",
		log.String(constants.AttrKeyTenantID, scope.Tenant),
		log.String("namespace", nk.Namespace),
		log.String("keyname", nk.Key),
	)

	runtime.HandlePanicValue(ctx, e.logger, recovered, "systemplane.engine", "refresh")
}

// scopeForEvent resolves the scope a changefeed event, a reconcile or a
// debounced re-read addresses, and reports nil for a scope the engine has
// stopped tracking.
//
// Such work is dropped, not answered. The event was produced by a feed that is
// being torn down — a dropped tenant's subscription is released while its
// changefeed goroutine may already be inside the callback — and answering it
// would rebuild the scope with no feed, no reconcile goroutine and a cache
// nothing confirms. DEBUG, not WARN: it is the ordinary end of a subscription,
// not a fault.
//
// Namespace and Key are empty for the events that carry none (OpResync,
// OpDisconnect); the tenant is what identifies the drop.
func (e *Engine) scopeForEvent(scope store.Scope, nk NSKey) *scopeState {
	sc := e.trackedScope(scope)
	if sc != nil {
		return sc
	}

	if e.debugEnabled() {
		e.logDebug(e.dispatchContext(), "changefeed work for an untracked scope, dropping",
			log.String(constants.AttrKeyTenantID, scope.Tenant),
			log.String("namespace", nk.Namespace),
			log.String("keyname", nk.Key),
		)
	}

	return nil
}

// markStale records that the scope's changefeed is down. The generation is
// bumped on every disconnect, which is what lets a reconcile that spans one
// detect it and refuse to clear the flag; the flag itself is already true on a
// repeat, so a second disconnect changes nothing else.
func (e *Engine) markStale(scope store.Scope) {
	sc := e.scopeForEvent(scope, NSKey{})
	if sc == nil {
		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.stale = true
	sc.disconnectGen++
}

// applyDelete publishes the registered default at revision 0 for a deleted
// row. Revision 0 always wins the fence, so the delete is never deduplicated
// away, and the key is recorded as touched so a reconcile running concurrently
// does not resurrect the deleted row from its snapshot.
//
// The publication and the fence it writes are ONE atomic step. A reconcile
// takes the same lock across its own check-and-apply pair, so it can no longer
// read an empty fence, wait, and then republish a snapshot row that predates
// this delete — which is exactly how a deleted key came back to life.
func (e *Engine) applyDelete(scope store.Scope, nk NSKey) {
	sc := e.scopeForEvent(scope, nk)
	if sc == nil {
		return
	}

	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	if e.ingestDefault(e.dispatchContext(), sc, nk) {
		sc.record(nk, true)
	}
}

// refreshKey re-reads one key and puts the row through the ingress. It is what
// the debouncer invokes when a key's quiet window closes.
//
// Three outcomes that are not a publication, each deliberately different:
//
//   - a store error teaches the engine nothing, so the key is recorded as
//     unusable and the cached value stands. The fence is written before the
//     line, for the reason recoverRefresh states: the logger is the consumer's
//     and a reconcile must never reach the key while it is still unfenced. An
//     error that is the lifecycle context being canceled is a shutdown, not an
//     incident, and logs at DEBUG.
//   - not found keeps the current value and publishes nothing: the write may
//     simply not be visible to this reader yet, and a real removal arrives as
//     OpDelete, so this is expected rather than wrong and logs at DEBUG. It is
//     recorded in neither set, so a concurrent reconcile's snapshot decides
//     the key.
//   - a row the ingress rejects (undecodable or refused by the validator) is
//     recorded as unusable, so a concurrent reconcile keeps the cached value
//     instead of concluding the key is absent.
//
// A panic under any of it is the fourth, and recoverRefresh decides it: the
// deferred recovery lives here, on the one function every re-read path runs
// through, so an exploding store call names its key whether the re-read was
// tracked, inline or debouncer-less. A validator that panics never reaches it
// — runValidator turns that into an ordinary rejection.
func (e *Engine) refreshKey(scope store.Scope, nk NSKey) {
	defer e.recoverRefresh(scope, nk)

	// Checked before the store call, not only after it: a re-read for a
	// dropped tenant would otherwise open a connection to a database that
	// tenant no longer has, to publish into a scope nothing tracks.
	if e.scopeForEvent(scope, nk) == nil {
		return
	}

	ctx, cancel := context.WithTimeout(e.dispatchContext(), feedTimeout)
	defer cancel()

	se, found, err := e.store.Get(ctx, scope, nk.Namespace, nk.Key)
	if err != nil {
		e.recordFeedOutcome(scope, nk, false)

		if e.canceledByShutdown(err) {
			e.logDebug(ctx, "changefeed re-read canceled during shutdown",
				log.String("namespace", nk.Namespace),
				log.String("keyname", nk.Key),
			)
		} else {
			e.logWarn(ctx, "changefeed re-read failed, keeping current value",
				log.String("namespace", nk.Namespace),
				log.String("keyname", nk.Key),
				log.Err(err),
			)
		}

		return
	}

	if !found {
		e.logDebug(ctx, "changefeed re-read found no row, keeping current value",
			log.String("namespace", nk.Namespace),
			log.String("keyname", nk.Key),
		)

		return
	}

	// The publication and the fence it writes are one atomic step, for the
	// same reason as in applyDelete: a reconcile deciding this key must see
	// either both or neither, never an empty fence followed by this
	// publication. The store read above deliberately stays outside the lock —
	// holding it across a network round trip would stall every reconcile of
	// the scope, and so does the validator ingest runs before taking it.
	// The scope is resolved again because it can be dropped during that round
	// trip, and this publication must not bring it back.
	sc := e.scopeForEvent(scope, nk)
	if sc == nil {
		return
	}

	e.ingest(ctx, sc, se)
}

// recordFeedOutcome tells every reconcile in flight what the feed learned
// about nk, and is a no-op otherwise: the fences only exist for the window
// between a reconcile's List and its application.
//
// It is for outcomes with no publication of their own — a re-read that
// errored. Anything that DOES publish holds reconcileMu across the pair and
// calls record directly, so the two are indivisible to a reconcile.
func (e *Engine) recordFeedOutcome(scope store.Scope, nk NSKey, usable bool) {
	sc := e.scopeForEvent(scope, nk)
	if sc == nil {
		return
	}

	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	sc.record(nk, usable)
}

// canceledByShutdown reports whether err is nothing more than this engine
// ending: a context.Canceled raised because Close canceled the lifecycle
// context every store call derives from.
//
// Both halves are required, and it is the one predicate the re-read and the
// reconcile both ask. A clean Close cancels whatever store calls are in
// flight, so reporting those at WARN makes every shutdown look like an
// incident. But a store may surface a wrapped context.Canceled for a reason
// that is NOT this shutdown — a pool checkout aborted, a driver cancelling
// internally — and that is a real read failure: the cache goes on serving a
// value nothing confirmed, so it belongs at WARN like any other. The lifecycle
// context is what tells the two apart.
//
// The bound that ends a hung store call is DeadlineExceeded, not Canceled, so
// it is unaffected and stays at WARN.
func (e *Engine) canceledByShutdown(err error) bool {
	return errors.Is(err, context.Canceled) && e.dispatchContext().Err() != nil
}

// debugEnabled reports whether a DEBUG line would survive the logger's level,
// so a caller can skip building its fields.
//
// The guard belongs at the call site, not inside logDebug: the []log.Field is
// built by the variadic before the call, and boxed into the ...any Logger.Log
// takes, so by the time logDebug could check anything the cost is already
// paid. It is worth the noise only on the lines the changefeed emits per
// event — everywhere else the line runs once per failure, not per write.
func (e *Engine) debugEnabled() bool {
	return e.logger != nil && e.logger.Enabled(log.LevelDebug)
}

// logDebug reports something that is expected rather than wrong — a re-read
// canceled by shutdown. A nil logger is a no-op, like logWarn.
func (e *Engine) logDebug(ctx context.Context, msg string, fields ...log.Field) {
	if e.logger == nil {
		return
	}

	e.logger.Log(ctx, log.LevelDebug, msg, fields)
}

// logError reports something an operator has to act on — a panic raised under
// engine work. A nil logger is a no-op, like logWarn.
func (e *Engine) logError(ctx context.Context, msg string, fields ...log.Field) {
	if e.logger == nil {
		return
	}

	e.logger.Log(ctx, log.LevelError, msg, fields)
}
