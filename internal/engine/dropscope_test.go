//go:build unit

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// dropTenant is the scope every test in this file drops. A tenant rather than
// the single-tenant scope because that is who gets dropped in production: a
// tenant suspended, deleted or rotated while the process keeps running.
var dropTenant = store.Scope{Tenant: "acme"}

// TestDropScopeUnsubscribesTheFeed is the leak this unit closes. Dropping a
// scope stopped its goroutines and forgot its cache but left the backend's
// changefeed open: one live subscription per suspended, deleted or rotated
// tenant, held for the life of the process, still delivering events into an
// engine that no longer tracks the scope.
func TestDropScopeUnsubscribesTheFeed(t *testing.T) {
	fs := newFakeStore()
	e := storeEngine(t, nil, fs, 0, 2*time.Second)

	bringUp(t, e, dropTenant)

	if got := fs.liveSubscriptions(); got != 1 {
		t.Fatalf("%d live changefeeds after bring-up, want 1", got)
	}

	e.dropScope(dropTenant)

	if got := fs.liveSubscriptions(); got != 0 {
		t.Errorf("%d changefeeds still open after the scope was dropped, want 0: "+
			"a dropped tenant keeps a live subscription for the life of the process", got)
	}

	if got := fs.unsubscribeCount(); got != 1 {
		t.Errorf("unsubscribe called %d times for one dropped scope, want 1", got)
	}

	// Dropping twice is ordinary: a tenant suspended and then deleted arrives
	// as two lifecycle events. The second must be a no-op, not a second
	// unsubscribe of a subscription the backend has already released.
	e.dropScope(dropTenant)

	if got := fs.unsubscribeCount(); got != 1 {
		t.Errorf("unsubscribe called %d times after a second drop, want 1", got)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	if got := fs.unsubscribeCount(); got != 1 {
		t.Errorf("unsubscribe called %d times after Close, want 1: "+
			"Close unsubscribed a scope that was already dropped", got)
	}
}

// TestLateFeedEventAfterDropDoesNotRecreateScope drives the changefeed the way
// a backend does — through the callback it was handed — after the scope is
// gone. Unsubscribe does not preempt a goroutine already inside that callback,
// so these events are the ordinary case.
//
// Answering them re-created the scope: stale, unreconciled, with no feed and
// no reconcile goroutine, and readable as though it were current. That is the
// resurrection, and it is worse than the leak it followed.
//
// All four operations the engine branches on are played, because the guard
// sits in four separate call sites — onResync, refreshKey, markStale and
// applyDelete — and each can regress alone. A late delete is the worst of
// them: it re-creates the scope AND publishes the registered default into it,
// so the dropped tenant reads as current with a value nothing confirms.
func TestLateFeedEventAfterDropDoesNotRecreateScope(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	bringUp(t, e, dropTenant)

	feed := fs.feedFor(dropTenant)
	if feed == nil {
		t.Fatal("the fake store registered no changefeed callback for the tenant scope")
	}

	fs.seed(dropTenant, jsonRow(nk, 7, `"seven"`, "ops"))

	e.dropScope(dropTenant)

	lists, gets := fs.listCount(), fs.getCount()

	late := []struct {
		op  string
		evt store.Event
	}{
		{"resync", resyncEvent(dropTenant)},
		{"upsert", upsertEvent(dropTenant, nk, 7)},
		{"disconnect", disconnectEvent(dropTenant)},
		{"delete", deleteEvent(dropTenant, nk)},
	}

	// Fatal rather than Error: a re-created scope makes every assertion below
	// report the consequence instead of the cause.
	for _, ev := range late {
		feed(ev.evt)

		if tracked(e, dropTenant) {
			t.Fatalf("a late %s event re-created the dropped scope: "+
				"it has no feed and no reconcile goroutine, and reads would report it as current", ev.op)
		}
	}

	if got := fs.listCount(); got != lists {
		t.Errorf("Store.List called %d times after the drop, want %d: a reconcile ran for a dropped scope", got, lists)
	}

	if got := fs.getCount(); got != gets {
		t.Errorf("Store.Get called %d times after the drop, want %d: a re-read ran for a dropped scope", got, gets)
	}

	// After the delete above, specifically: a delete that reaches a re-created
	// scope publishes the registered default, which is the one late event that
	// makes a dropped tenant readable rather than merely tracked.
	if _, ok := e.Lookup(dropTenant, nk); ok {
		t.Error("the dropped scope is readable again after a late changefeed event")
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

// TestPublishAfterDropIsRefused covers the other ingress into a scope. A Set
// for a tenant the engine stopped tracking must not rebuild the scope around
// the written value: nothing would ever confirm it, and the next read would be
// served from a cache with no changefeed behind it.
func TestPublishAfterDropIsRefused(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	bringUp(t, e, dropTenant)
	e.dropScope(dropTenant)

	e.Publish(context.Background(), dropTenant, jsonRow(nk, 5, `"written"`, "ops"))

	if tracked(e, dropTenant) {
		t.Error("Publish re-created a dropped scope")
	}

	if _, ok := e.Lookup(dropTenant, nk); ok {
		t.Error("a write into a dropped scope became readable")
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

// TestPublishIntoAlreadyDroppedStateIsRefused covers the window publish no
// longer closes by re-resolving the scope. A caller resolves the scope, then
// spends time outside every lock — decoding the row, running the consumer's
// validator, waiting on the reconcile mutex — and the tenant is suspended in
// the meantime. Its publication must not cache into the torn-down state, and
// above all must not start a delivery worker for a scope nothing will stop
// again before Close.
func TestPublishIntoAlreadyDroppedStateIsRefused(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	bringUp(t, e, dropTenant)

	sc := e.trackedScope(dropTenant)
	if sc == nil {
		t.Fatal("the tenant scope was not brought up")
	}

	e.OnChange(nk, func(context.Context, Change) {})
	e.dropScope(dropTenant)

	if notify := e.publish(sc, publication{Scope: dropTenant, NSKey: nk, Revision: 9, Value: "late"}); notify {
		t.Error("publish into a dropped scope state: notify is true, want false")
	}

	if _, ok := e.Lookup(dropTenant, nk); ok {
		t.Error("a publication into a dropped scope state became readable")
	}

	if workers := scopeWorkerCount(e, sc); workers != 0 {
		t.Errorf("publish started %d delivery worker(s) for a dropped scope, want 0", workers)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

// TestPublishRacingDropScopeStartsNoWorker closes the window publish's own
// refusal cannot. That refusal selects on the scope's reconcileStop channel;
// dropScope closes that channel and sweeps the worker map as two steps, so a
// publisher that passed the select microseconds before the drop began reaches
// the dispatch step after the sweep has already run. It then started a parked
// delivery goroutine — and a WaitGroup entry — for a scope the engine no
// longer tracks, which nothing ends before Close.
//
// The race is driven rather than raced for: the publication is handed to
// dispatch after the drop has completed, which is exactly the state that
// publisher is in.
func TestPublishRacingDropScopeStartsNoWorker(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	bringUp(t, e, dropTenant)

	sc := e.trackedScope(dropTenant)
	if sc == nil {
		t.Fatal("the tenant scope was not brought up")
	}

	// A subscriber, or dispatch would decline the publication for having
	// nowhere to deliver it and the test would prove nothing.
	e.OnChange(nk, func(context.Context, Change) {})
	e.dropScope(dropTenant)

	e.dispatch(sc, publication{Scope: dropTenant, NSKey: nk, Revision: 9, Value: "late"})

	if workers := scopeWorkerCount(e, sc); workers != 0 {
		t.Errorf("a publication racing the drop started %d delivery worker(s), want 0: "+
			"a dropped tenant keeps a parked goroutine per key until the process shuts down", workers)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

// TestReconcileForDroppedScopeDoesNotList mirrors onto the reconcile the guard
// the changefeed re-read already has. The reconcile worker's select gives the
// stop signal no priority over a resync already sitting in the mailbox, so a
// scope dropped with work queued gets one more reconcile — a whole-scope
// Store.List against a tenant database that tenant no longer has.
//
// The worker's exact state is reproduced rather than raced for: the mailbox
// item is taken, the scope is dropped, and the reconcile then runs.
func TestReconcileForDroppedScopeDoesNotList(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	bringUp(t, e, dropTenant)

	sc := e.trackedScope(dropTenant)
	if sc == nil {
		t.Fatal("the tenant scope was not brought up")
	}

	sc.armReconcile()

	pending, ok := sc.takeReconcile()
	if !ok {
		t.Fatal("arming a reconcile left the mailbox empty")
	}

	e.dropScope(dropTenant)

	lists := fs.listCount()

	e.runOneReconcile(e.dispatchContext(), sc, pending)

	if got := fs.listCount(); got != lists {
		t.Errorf("Store.List called %d times for a dropped scope, want %d", got, lists)
	}

	if tracked(e, dropTenant) {
		t.Error("a queued reconcile re-created the dropped scope")
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

// TestDropSweepDoesNotStopAReactivatedScopesWorkers is the other half of the
// drop-versus-publish race. dropScope removes the scope from the map, releases
// its changefeed — a backend unsubscribe waits for its own feed goroutine, so
// that step is not instant — and only then sweeps the scope's delivery
// workers. A tenant re-activated inside that gap is a NEW state, and the
// straggler sweep of the old one must not touch the new one's workers.
//
// A sweep that selected workers by scope VALUE did exactly that: it stopped
// the re-activated tenant's goroutine and discarded the Change waiting in its
// mailbox, contradicting the refusal flag, which lives on the state precisely
// so that a new state starts workers freely.
//
// The gap is reproduced rather than raced for: the old state's sweep runs
// after the re-activation has published, which is where a drop stuck inside
// unsubscribe lands.
func TestDropSweepDoesNotStopAReactivatedScopesWorkers(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	bringUp(t, e, dropTenant)

	old := e.trackedScope(dropTenant)
	if old == nil {
		t.Fatal("the tenant scope was not brought up")
	}

	e.dropScope(dropTenant)
	bringUp(t, e, dropTenant)

	sc := e.trackedScope(dropTenant)
	if sc == nil {
		t.Fatal("the re-activated tenant scope was not brought up")
	}

	if sc == old {
		t.Fatal("re-activation reused the dropped scope state")
	}

	unsub := e.OnChange(nk, func(context.Context, Change) {})
	defer unsub()

	if notify := e.publish(sc, publication{Scope: dropTenant, NSKey: nk, Revision: 9, Value: "back"}); !notify {
		t.Fatal("the re-activated scope refused a first publication")
	}

	w := scopeWorker(e, sc, nk)
	if w == nil {
		t.Fatal("a publication into the re-activated scope started no delivery worker")
	}

	// The drop that was still inside unsubscribe finally reaches its sweep.
	e.stopScopeWorkers(old)

	if got := scopeWorkerCount(e, sc); got != 1 {
		t.Errorf("the re-activated scope owns %d delivery worker(s) after the old state's sweep, want 1", got)
	}

	select {
	case <-w.done:
		t.Error("the old state's sweep stopped the re-activated scope's delivery worker: " +
			"its pending Change is discarded and the tenant goes silent until Close")
	default:
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

// TestScopeBroughtUpAfterADropDeliversAgain walks the one escape from the
// sticky refusal a sweep leaves behind. A scope whose workers were swept
// refuses every later one forever, and the only way back is a new state — so
// if bring-up ever reused the dropped state, the scope would be tracked, fed
// and reconciled while no subscriber ever heard from it again.
//
// The path is reachable today: Start opens the changefeed, a Subscribe failure
// drops the scope it had just created, and the consumer retries Start.
func TestScopeBroughtUpAfterADropDeliversAgain(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	fs.onSubscribe(func(store.Scope) error { return errSubscribe })

	if _, err := e.bringUpScope(dropTenant); err == nil {
		t.Fatal("bringUpScope succeeded with a failing Subscribe, want an error")
	}

	if tracked(e, dropTenant) {
		t.Fatal("a failed Subscribe left the scope tracked")
	}

	fs.onSubscribe(nil)
	bringUp(t, e, dropTenant)

	sc := e.trackedScope(dropTenant)
	if sc == nil {
		t.Fatal("the retried bring-up did not track the scope")
	}

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	if notify := e.publish(sc, publication{Scope: dropTenant, NSKey: nk, Revision: 4, Value: "retried"}); !notify {
		t.Fatal("the scope brought up after a drop refused a first publication")
	}

	waitFor(t, hangGuard, "a delivery from the scope brought up after a drop",
		func() bool { return rec.len() == 1 })

	if got := rec.changes()[0].Value; got != "retried" {
		t.Errorf("delivered value %v, want %q", got, "retried")
	}

	if got := scopeWorkerCount(e, sc); got != 1 {
		t.Errorf("the scope brought up after a drop owns %d delivery worker(s), want 1", got)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

// scopeWorker returns the delivery worker sc owns for nk, or nil when it owns
// none, under the engine lock that guards them.
func scopeWorker(e *Engine, sc *scopeState, nk NSKey) *dispatchWorker {
	e.workersMu.Lock()
	defer e.workersMu.Unlock()

	return sc.workers[nk]
}

// scopeWorkerCount returns how many delivery workers sc owns. A scope owns its
// own workers, so this is the count a drop of ANOTHER state must leave alone
// and the count a dropped state must be left with.
func scopeWorkerCount(e *Engine, sc *scopeState) int {
	e.workersMu.Lock()
	defer e.workersMu.Unlock()

	return len(sc.workers)
}
