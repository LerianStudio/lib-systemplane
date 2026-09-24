package engine

import (
	"context"
	"errors"
	"fmt"
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
//   - OpDelete is fenced here, synchronously, and then re-read like an upsert:
//     the store is the only thing that can say what the key holds NOW, and a
//     delete that published blind reverted whatever the caller wrote after it.
//     What the re-read finds decides the outcome — a row (the key was
//     recreated) goes through the ingress at its own revision, an empty read
//     puts the registered default in force at revision 0.
//   - anything else is treated as an upsert: the store is re-read once the
//     key's quiet window closes, and the row goes through the ingress. An
//     empty read is then a non-answer rather than a removal, and the key keeps
//     the value it holds.
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
	// fields below are built only when DEBUG is actually enabled. That guard
	// is real for a lib-observability logger and inert for the one-method
	// Logger the public boundary accepts, which reports every level enabled
	// until lib-observability's shim delegates Enabled; debugEnabled states
	// the whole shape and carries the follow-up.
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

	// A delete is counted the MOMENT it arrives, before the re-read below
	// waits for the key's quiet window, and that is the half of a delete that
	// cannot be deferred. Every revision beats the 0 a delete publishes, so a
	// re-read already inside Store.Get when the DELETE commits comes back —
	// under READ COMMITTED — holding the removed row and wins the publish
	// fence with it. Counting the delete here refuses that reader from this
	// instant; recording the key as touched does the same for a reconcile
	// whose List was taken before the delete.
	deleted := evt.Op == store.OpDelete
	if deleted {
		e.recordFeedDelete(evt.Scope, nk)
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
	// A delete takes the same per-key quiet window AND the same store read as
	// an upsert; the op it carries only decides what an EMPTY read means. That
	// is what fixed the inversion a self-describing delete used to cause: the
	// echo of a Delete and the echo of the Set that followed it arrive on the
	// feed in that order, and re-reading the store answers the delete with the
	// row the write left behind instead of reverting the key to its registered
	// default. Coalescing alone could not — it orders only the echoes that
	// land inside one window, and a busy feed delivers most pairs further
	// apart than that.
	e.submitRefresh(evt.Scope, nk, deleted, false)
}

// submitRefresh queues nk's re-read behind the key's quiet window. retried says
// this is the second attempt at the same event, which is what bounds the one
// retry a re-read that could not answer asks for.
//
// Exactly one closure is built, in the branch that wants it: the two differ
// only in whether the re-read registers itself as work Close waits for, and
// building both would cost one discarded heap allocation on every event the
// feed delivers.
func (e *Engine) submitRefresh(scope store.Scope, nk NSKey, deleted, retried bool) {
	var work func()

	if e.debounceAsync {
		work = func() { e.trackedRefresh(scope, nk, deleted, retried) }
	} else {
		work = func() { e.refreshKey(scope, nk, deleted, retried) }
	}

	e.debouncer.Submit(scopeNSKey{Tenant: scope.Tenant, Namespace: nk.Namespace, Key: nk.Key}, work)
}

// retryRefresh answers a re-read that could not say what the key holds: its
// store call failed, or it panicked.
//
// Returning was the whole outcome, and every repair is then fenced out. A
// delete has already recorded the key as touched at event arrival, so a
// reconcile in flight skips it; the key's own fences stop nothing else from
// asking; and a reconcile is armed only by an OpResync, so on a connection
// that never drops nothing ever asks the store again. A row an operator
// deleted therefore stayed in force at its old revision, reported as not
// stale, for the life of the process.
//
// So the re-read is submitted once more, through the same debouncer and
// carrying the op it was serving: a transient failure — a pool exhausted for a
// moment, one statement timeout — converges on the retry, and the retry
// coalesces with a fresh notification for the key exactly as any other
// submission does.
//
// A second failure is not transient, so the scope is marked stale and the next
// OpResync reconciles the whole of it. The disconnect generation is
// deliberately NOT bumped: a reconcile already in flight holds a snapshot of
// every key and is entitled to clear the flag when it lands.
//
// Both are addressed to the state the re-read was armed on, by IDENTITY, for
// the reason recordFeedOutcome states: a tenant dropped and brought back up
// during the store call is a new state under the same scope value, and neither
// a retry read under an entitlement this process no longer holds nor a stale
// flag raised by the dead state's failure belongs to it.
func (e *Engine) retryRefresh(sc *scopeState, nk NSKey, deleted, retried bool) {
	if sc == nil || e.trackedScope(sc.scope) != sc {
		return
	}

	if !retried {
		e.submitRefresh(sc.scope, nk, deleted, true)

		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.stale = true
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
// still inside Store.Get arms a fresh timer and the two overlap.
//
// How many overlap is how long one re-read lives divided by the window, and a
// re-read is not only its store call: refreshKey hands the row to the ingress,
// which runs the CONSUMER's registered validator, bounded by nothing. So under
// sustained notifications on one key the in-flight count for that single key
// approaches (feedTimeout PLUS the validator's runtime) divided by the window
// — about 50 at the 5s and 100ms defaults is the FLOOR, what the store call
// alone produces, and a validator that blocks raises it without limit.
//
// There is no semaphore. The two halves size different things, so a consumer
// reads them apart: connections are held for the store call only, so a
// *sql.DB pool takes registered keys times tracked scopes as its floor plus
// the feedTimeout share of that overlap for as many keys as a degraded store
// can be hot on at once, while the validator's share adds goroutines and the
// values they hold rather than connections — a slow validator therefore shows
// up as goroutine growth under a flapping feed, not as pool exhaustion.
func (e *Engine) trackedRefresh(scope store.Scope, nk NSKey, deleted, retried bool) {
	if !e.beginWork() {
		return
	}

	defer e.dispatchWG.Done()

	e.refreshKey(scope, nk, deleted, retried)
}

// recoverRefresh reports a panic raised under a changefeed re-read, naming the
// key it happened on, and records that key as unusable.
//
// It is deferred by refreshKey rather than by the one caller that wraps it, so
// both re-read paths carry the identity: the tracked one, and the inline one a
// consumer on WithDebounce(0) takes. The debouncer's own guard would catch a
// panic on either, and that is what this replaces at the top of the stack
// rather than duplicates:
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
// That record goes into the state the re-read was armed on, which is why the
// caller hands this a POINTER to its own resolution rather than the scope
// value: the recovery is deferred before the scope is resolved, and resolving
// one of its own afterwards would find whatever is live now. A tenant dropped
// and brought back up during the store call is a new state, and fencing ITS
// window with what the dead state's read failed to learn makes the
// re-activation's own reconcile keep a cached value instead of repairing the
// key. Nothing is recorded while the pointer is still nil — a panic raised
// before the scope was resolved fences nobody — and recordFeedOutcome drops
// the write when the state is no longer the tracked one.
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
func (e *Engine) recoverRefresh(scope store.Scope, nk NSKey, deleted, retried bool, state **scopeState) {
	recovered := recover()
	if recovered == nil {
		return
	}

	ctx := e.dispatchContext()

	var sc *scopeState

	if state != nil {
		sc = *state

		e.recordFeedOutcome(sc, nk, false)
	}

	// Deferred rather than called last, which is where it used to sit. It
	// still RUNS last — on a zero quiet window the retry runs inline, right
	// here — but registering it before the reporting is what keeps the repair
	// this function promises independent of the reporting surviving. Both
	// lines below end up inside the consumer's logger, and the guard this
	// engine puts under that logger covers the one it holds, not whatever
	// lib-observability's handler reaches on its way to a sink.
	defer e.retryRefresh(sc, nk, deleted, retried)

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

	e.logUntrackedDrop(scope, nk)

	return nil
}

// logUntrackedDrop reports work the engine is dropping because the scope it
// addresses is no longer the one being tracked — gone, or replaced by the
// state a re-activation created.
//
// DEBUG, because it is the ordinary end of a subscription rather than a fault.
// The level guard is inside rather than at the two call sites: this line runs
// per event, not per failure, and the fields are built by the variadic before
// the call, so asking first is what actually skips the work.
func (e *Engine) logUntrackedDrop(scope store.Scope, nk NSKey) {
	if !e.debugEnabled() {
		return
	}

	e.logDebug(e.dispatchContext(), "changefeed work for an untracked scope, dropping",
		log.String(constants.AttrKeyTenantID, scope.Tenant),
		log.String("namespace", nk.Namespace),
		log.String("keyname", nk.Key),
	)
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

// recordFeedDelete fences nk against everything already in flight, at the
// instant the changefeed reports the row removed. It publishes nothing: what
// goes in force is decided by the re-read the event schedules.
//
// Two fences, because two kinds of reader can be holding the pre-delete row.
// A changefeed re-read already inside Store.Get is refused by the delete
// counter, which it captured before its call and compares afterwards. A
// reconcile whose List was taken before the delete is refused by the touched
// record, since every revision in its photograph beats the revision 0 a delete
// leads to. Both are the half that cannot wait for the key's quiet window:
// until the re-read runs, these two are the only things that could publish the
// removed row back over the removal.
//
// Recording the key as touched is not a guess — the feed really does hold the
// fresher fact about it from here on, and a reconcile that skips it keeps the
// cached value until the re-read lands.
//
// The two locks are taken in the engine's standing order, reconcileMu then mu,
// which is the order publish takes them under the ingress.
func (e *Engine) recordFeedDelete(scope store.Scope, nk NSKey) {
	sc := e.scopeForEvent(scope, nk)
	if sc == nil {
		return
	}

	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	sc.record(nk, true)
	sc.bumpDeletes(nk)
}

// PublishDelete publishes the registered default at revision 0 for a deleted
// row. Revision 0 always wins the fence, so the delete is never deduplicated
// away, and the key is recorded as touched so a reconcile running concurrently
// does not resurrect the deleted row from its snapshot.
//
// The publication and the fence it writes are ONE atomic step. A reconcile
// takes the same lock across its own check-and-apply pair, so it can no longer
// read an empty fence, wait, and then republish a snapshot row that predates
// this delete — which is exactly how a deleted key came back to life.
//
// The Client's own Delete is its one caller. Leaving Delete to the feed alone
// would make read-your-writes on a delete wait for a NOTIFY round trip, so the
// caller's next read could still be answered by the value it just removed
// (D4). It bumps the key's delete counter through publish, so a changefeed
// re-read that was already in flight when the caller deleted is refused
// exactly as it is for a delete the feed reports.
//
// The feed does NOT come through here. A notification says a row is gone, not
// what the key holds now, and answering it without asking the store reverted
// whatever the caller wrote next; the feed counts its delete at arrival and
// re-reads instead (onEvent, refreshKey).
//
// Every refusal it can make is REPORTED, exactly as Publish reports a write's:
// a nil or closed engine, a scope the engine does not track, a key nothing
// registered, and a publication the engine or the scope went away under. All
// of them mean the row is deleted in the store and the caller's next read in
// this process still serves the value it removed — which is precisely what
// Client.Delete must pass on rather than swallow. A nil error means the
// registered default is in force for the key.
func (e *Engine) PublishDelete(scope store.Scope, nk NSKey) error {
	// Exported, so this runs on the consumer's goroutine: the same guard
	// Publish takes, for the same reason. The feed's own callers can never
	// reach a nil or closed engine; a Client built by a path that never
	// reached New can.
	if e == nil || e.closed.Load() {
		return ErrClosed
	}

	sc := e.scopeForEvent(scope, nk)
	if sc == nil {
		return fmt.Errorf("%w: %s", ErrScopeNotTracked, scopeLabel(scope))
	}

	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	notify, err := e.ingestDefault(e.dispatchContext(), sc, nk, true)
	if err != nil {
		return err
	}

	if notify {
		sc.record(nk, true)
	}

	return nil
}

// refreshKey re-reads one key and puts the row through the ingress. It is what
// the debouncer invokes when a key's quiet window closes.
//
// Three outcomes that are not the ordinary row, each deliberately different:
//
//   - a store error teaches the engine nothing, so the key is recorded as
//     unusable and the cached value stands. The fence is written before the
//     line, for the reason recoverRefresh states: the logger is the consumer's
//     and a reconcile must never reach the key while it is still unfenced. An
//     error that is the lifecycle context being canceled is a shutdown, not an
//     incident, and logs at DEBUG. Any other error is retried once and then
//     leaves the scope stale — retryRefresh says why the event cannot simply
//     be dropped.
//   - not found means different things for the two ops, which is the whole of
//     what deleted decides. After an upsert the write may simply not be
//     visible to this reader yet, so the current value stands and nothing is
//     published: expected rather than wrong, logged at DEBUG, and recorded in
//     neither set so a concurrent reconcile's snapshot decides the key. After
//     a DELETE the row really is gone, and publishAbsentDelete puts the
//     registered default in force at revision 0.
//   - a row the ingress rejects (undecodable or refused by the validator) is
//     recorded as unusable, so a concurrent reconcile keeps the cached value
//     instead of concluding the key is absent.
//
// A panic under any of it is the fourth, and recoverRefresh decides it: the
// deferred recovery lives here, on the one function every re-read path runs
// through, so an exploding store call names its key whether the re-read was
// tracked or inline. A validator that panics never reaches it — runValidator
// turns that into an ordinary rejection.
//
// All three of this function's writes into a scope — the publication, the
// store error's fence, and the recovery's — are addressed by IDENTITY to the
// state resolved before the store call, never by scope value. A tenant dropped
// and brought back up during that round trip is a live state under the same
// scope value, and both a publication and a fence written into it are wrong in
// the same way: the row was read under an entitlement this process no longer
// holds, and the new state's own reconcile is the only thing entitled to
// decide the key. That is why sc is declared before the recovery is deferred
// and handed to it by pointer.
func (e *Engine) refreshKey(scope store.Scope, nk NSKey, deleted, retried bool) {
	var sc *scopeState

	defer e.recoverRefresh(scope, nk, deleted, retried, &sc)

	// Checked before the store call, not only after it: a re-read for a
	// dropped tenant would otherwise open a connection to a database that
	// tenant no longer has, to publish into a scope nothing tracks.
	sc = e.scopeForEvent(scope, nk)
	if sc == nil {
		return
	}

	// Armed BEFORE the store call, because the whole question is what happened
	// during it. A delete that lands while this read is in flight makes the row
	// it comes back with older than the cache, however high its revision — and
	// for a read that comes back empty, so does any publication at all, since
	// the registered default it would publish wins the revision fence
	// unconditionally.
	fence := sc.fenceFor(nk)

	ctx, cancel := context.WithTimeout(e.dispatchContext(), feedTimeout)
	defer cancel()

	se, found, err := e.store.Get(ctx, scope, nk.Namespace, nk.Key)
	if err != nil {
		e.recordFeedOutcome(sc, nk, false)

		if e.canceledByShutdown(err) {
			e.logDebug(ctx, "changefeed re-read canceled during shutdown",
				log.String(constants.AttrKeyTenantID, scope.Tenant),
				log.String("namespace", nk.Namespace),
				log.String("keyname", nk.Key),
			)

			// No retry: there is nothing left to converge on, and the engine
			// is about to stop answering this scope at all.
			return
		}

		e.logWarn(ctx, "changefeed re-read failed, keeping current value",
			log.String(constants.AttrKeyTenantID, scope.Tenant),
			log.String("namespace", nk.Namespace),
			log.String("keyname", nk.Key),
			log.Err(err),
		)

		e.retryRefresh(sc, nk, deleted, retried)

		return
	}

	// The scope is resolved again because it can be dropped during the store
	// round trip, and nothing this read learned may bring it back. It is
	// compared by IDENTITY, exactly as applyScope compares before applying a
	// snapshot: a tenant dropped and brought back up meanwhile is a NEW state
	// under the same scope value, so a by-value check finds a live scope and
	// publishes a row read under an entitlement this process no longer holds.
	// The delete fence cannot stand in for that — it counts deletes, and a
	// fresh state's counter starts at zero, so for the ordinary key that has
	// never been deleted it compares zero against zero and lets the row
	// through. The new state reconciles the key from the store itself.
	//
	// It gates BOTH conclusions, not only the row: the registered default a
	// delete's empty read publishes is as much a decision about the key as a
	// value is, and the dropped state is entitled to neither.
	if live := e.scopeForEvent(scope, nk); live != sc {
		// scopeForEvent already reported the scope that is simply gone. This
		// line is the other half: a state that IS tracked, under the same
		// scope value, whose row this re-read is not entitled to hand over.
		// Both are drops, and a drop nothing says a word about is one an
		// operator chasing a key that never updated cannot see.
		if live != nil {
			e.logUntrackedDrop(scope, nk)
		}

		return
	}

	if !found {
		if deleted {
			e.publishAbsentDelete(ctx, sc, scope, nk, fence)

			return
		}

		e.logDebug(ctx, "changefeed re-read found no row, keeping current value",
			log.String(constants.AttrKeyTenantID, scope.Tenant),
			log.String("namespace", nk.Namespace),
			log.String("keyname", nk.Key),
		)

		return
	}

	// The publication and the fence it writes are one atomic step, for the
	// same reason as in PublishDelete: a reconcile deciding this key must see
	// either both or neither, never an empty fence followed by this
	// publication. The store read above deliberately stays outside the lock —
	// holding it across a network round trip would stall every reconcile of
	// the scope, and so does the validator ingest runs before taking it.
	// A changefeed row is graded here and nowhere else, so the outcome is the
	// ingress's own business: no caller is waiting to be told.
	_ = e.ingest(ctx, sc, se, fence, false)
}

// publishAbsentDelete puts the registered default in force at revision 0 for a
// key the changefeed reported deleted and whose row the re-read then found
// gone. It is the one outcome that concludes something from an EMPTY read, and
// the op is what entitles it to: an upsert's empty read is a non-answer, a
// delete's is the removal itself.
//
// What it publishes wins the revision fence unconditionally, so it is graded
// on the wider question instead: anything at all having landed on the key
// since the read began means the cache holds a fact this read did not see. The
// case that matters is the caller's own Set, published for read-your-writes
// (D4) the moment the store acknowledged it, while this reader was still
// inside a Store.Get that could not yet see the row — publishing the default
// on top of it would revert a write the caller has already been told landed.
// A refusal publishes nothing and records nothing: whatever won recorded
// itself.
//
// The delete counter is NOT bumped again here. The feed counted this delete at
// event arrival, which is what refused every re-read already in flight, and a
// second bump would refuse a re-read armed after it for a delete it had
// already seen.
//
// The scope is resolved by the caller and compared by identity there, and the
// publication and the fence it writes are one step under reconcileMu, for the
// reasons refreshKey and PublishDelete state.
func (e *Engine) publishAbsentDelete(ctx context.Context, sc *scopeState, scope store.Scope, nk NSKey, fence feedFence) {
	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	if sc.supersededByPublication(nk, fence) {
		e.logDebug(ctx, "changefeed delete superseded by a later publication, keeping current value",
			log.String(constants.AttrKeyTenantID, scope.Tenant),
			log.String("namespace", nk.Namespace),
			log.String("keyname", nk.Key),
		)

		return
	}

	// The feed's own ingress: nothing is waiting to be told, and the only
	// errors ingestDefault reports here are a scope going away and a key the
	// caller already confirmed registered.
	if notify, _ := e.ingestDefault(ctx, sc, nk, false); notify {
		sc.record(nk, true)
	}
}

// recordFeedOutcome tells every reconcile in flight ON sc what the feed
// learned about nk, and is a no-op otherwise: the fences only exist for the
// window between a reconcile's List and its application.
//
// It is for outcomes with no publication of their own — a re-read that errored
// or panicked. Anything that DOES publish holds reconcileMu across the pair and
// calls record directly, so the two are indivisible to a reconcile.
//
// The caller passes the state its re-read was armed on, and the write is
// dropped unless that state is still the one being tracked. Resolving the
// scope by VALUE here was the hole: a tenant dropped and brought back up
// inside Store.Get is a new state under the same scope value, and recording
// "unusable" into ITS window makes the re-activation's own reconcile keep a
// cached value for a key the snapshot no longer carries — a tenant left on a
// configuration the store does not have, reporting itself fresh, until some
// later reconnect. The publishing path refuses the same state by the same
// identity; this closes the two paths that publish nothing.
func (e *Engine) recordFeedOutcome(sc *scopeState, nk NSKey, usable bool) {
	if sc == nil || e.trackedScope(sc.scope) != sc {
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
// paid. It is worth the noise only on the lines that run per row or per
// event — everywhere else the line runs once per failure, not per write.
//
// How far it actually bites today depends on what the consumer passed to
// WithLogger, and the honest answer is narrower than the guard looks:
//
//   - a lib-observability logger (its stdlib one, its zap adapter, its no-op)
//     carries its own level check, so the guard reports the deployment's real
//     level and the skipped lines are genuinely skipped;
//   - so does a consumer type that happens to implement the whole
//     lib-observability log.Logger — log.Adapt returns such a value untouched;
//   - a consumer implementing only the one method the public Logger interface
//     declares, which is the shape that boundary exists to allow, is wrapped
//     by log.Adapt in a shim whose Enabled answers true for every VALID level.
//     For that logger this reports DEBUG enabled whatever the consumer's own
//     level is, the fields are built, and the line is thrown away inside the
//     consumer's Log.
//
// Follow-up: the fix belongs in lib-observability, whose shim can consult the
// wrapped logger when it implements Enabled(int) bool — queued there as its
// own change, not worked around here.
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
