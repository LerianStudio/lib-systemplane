//go:build unit

package engine

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// errRereadFailed is what the store hands a changefeed re-read that fails
// after its tenant was dropped.
var errRereadFailed = errors.New("the store connection dropped")

// reactivatedValue is what a tenant brought back up inside Store.Get has in
// its cache: the row as its OWN bring-up read it, which has moved on from the
// one the refused re-read is carrying.
const reactivatedValue = "eight"

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
// PublishDelete — and each can regress alone. A late delete is the worst of
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

	if notify, err := e.publish(sc, publication{Scope: dropTenant, NSKey: nk, Revision: 9, Value: "late"}); notify {
		t.Error("publish into a dropped scope state: notify is true, want false")
	} else if !errors.Is(err, ErrScopeNotTracked) {
		t.Errorf("publish into a dropped scope state: got %v, want errors.Is ErrScopeNotTracked", err)
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

	if notify, _ := e.publish(sc, publication{Scope: dropTenant, NSKey: nk, Revision: 9, Value: "back"}); !notify {
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

	if notify, _ := e.publish(sc, publication{Scope: dropTenant, NSKey: nk, Revision: 4, Value: "retried"}); !notify {
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

// TestReconcileForReactivatedScopeDoesNotListForTheDeadState is the other half
// of the queued-reconcile guard. A scope dropped with work in its mailbox gets
// one more reconcile; if the tenant was re-activated in between, resolving
// that reconcile's scope by VALUE finds the new state and lets the dead one
// run a whole-scope Store.List — plus the consumer's validator on every row —
// on behalf of state nothing tracks, publishing into a cache no reader can
// reach. Identity is what tells the two states apart.
func TestReconcileForReactivatedScopeDoesNotListForTheDeadState(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	bringUp(t, e, dropTenant)

	old := e.trackedScope(dropTenant)
	if old == nil {
		t.Fatal("the tenant scope was not brought up")
	}

	old.armReconcile()

	pending, ok := old.takeReconcile()
	if !ok {
		t.Fatal("arming a reconcile left the mailbox empty")
	}

	e.dropScope(dropTenant)
	bringUp(t, e, dropTenant)

	if sc := e.trackedScope(dropTenant); sc == old {
		t.Fatal("re-activation reused the dropped scope state")
	}

	lists := fs.listCount()

	e.runOneReconcile(e.dispatchContext(), old, pending)

	if got := fs.listCount(); got != lists {
		t.Errorf("Store.List called %d times for a dropped scope state, want %d: the queued reconcile "+
			"of the dead state reloaded the whole scope because it resolved its scope by value and "+
			"found the re-activated tenant", got, lists)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

// TestStragglerWorkerDoesNotEraseTheLiveMarker keys the stuck-worker markers by
// worker identity rather than by scope value. Delivery workers belong to the
// scope STATE, so a dropped tenant's worker still inside a subscriber callback
// and the re-activated tenant's worker for the same key are two goroutines
// sharing one (tenant, namespace, key) triple. Marked by that triple, the
// straggler's clear-on-exit erases the live worker's marker — and a Close that
// times out then blames the backend, telling whoever reads the message the
// engine is stuck inside a store call while a subscriber is the one holding
// shutdown open.
func TestStragglerWorkerDoesNotEraseTheLiveMarker(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 100*time.Millisecond)

	straggler, live := newDeliveryGate(), newDeliveryGate()
	gates := make(chan *deliveryGate, 2)
	gates <- straggler
	gates <- live

	unsub := e.OnChange(nk, func(context.Context, Change) {
		g := <-gates
		close(g.entered)
		<-g.release // deliberately ignores ctx: the subscriber's own leak
	})
	defer unsub()

	bringUp(t, e, dropTenant)

	old := e.trackedScope(dropTenant)
	if old == nil {
		t.Fatal("the tenant scope was not brought up")
	}

	if notify, _ := e.publish(old, publication{Scope: dropTenant, NSKey: nk, Revision: 1, Value: "before"}); !notify {
		t.Fatal("the scope refused a first publication")
	}

	mustReceive(t, straggler.entered, "the dropped scope's worker to enter its callback")

	e.dropScope(dropTenant)
	bringUp(t, e, dropTenant)

	sc := e.trackedScope(dropTenant)
	if sc == nil || sc == old {
		t.Fatal("the tenant was not re-activated into a new scope state")
	}

	if notify, _ := e.publish(sc, publication{Scope: dropTenant, NSKey: nk, Revision: 2, Value: "after"}); !notify {
		t.Fatal("the re-activated scope refused a first publication")
	}

	mustReceive(t, live.entered, "the re-activated scope's worker to enter its callback")

	if got := runningCount(e); got != 2 {
		t.Fatalf("%d worker(s) marked as inside a callback, want 2: the straggler of the dropped state "+
			"shares the re-activated state's marker and erases it on the way out", got)
	}

	close(straggler.release)
	waitFor(t, hangGuard, "the straggler to leave its callback", func() bool { return runningCount(e) == 1 })

	err := e.Close()
	if !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("Close() = %v, want an error wrapping ErrCloseTimeout", err)
	}

	for _, want := range []string{dropTenant.Tenant, nk.Namespace, nk.Key} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Close() error %q does not name %q, so the operator is sent hunting for a "+
				"backend fault while a subscriber holds shutdown open", err, want)
		}
	}

	// Release the stuck callback and wait for it: a test that leaks on purpose
	// fails the whole package under goleak.
	close(live.release)
	e.dispatchWG.Wait()
}

// deliveryGate holds one delivery open. entered closes when the subscriber is
// inside the callback, release lets it return.
type deliveryGate struct {
	entered chan struct{}
	release chan struct{}
}

func newDeliveryGate() *deliveryGate {
	return &deliveryGate{entered: make(chan struct{}), release: make(chan struct{})}
}

// runningCount reports how many delivery workers are marked as inside a
// subscriber callback — the set a timed-out Close names.
func runningCount(e *Engine) int {
	n := 0

	e.running.Range(func(_, _ any) bool {
		n++

		return true
	})

	return n
}

// TestRereadRefusesAScopeDroppedDuringTheStoreCall closes the last window a
// changefeed re-read leaves open. refreshKey resolves the scope, then spends a
// whole network round trip outside every lock — deliberately, so a slow store
// cannot stall the scope's reconciles — and a tenant suspended in the meantime
// is gone by the time the row comes back.
//
// Nothing of that row may reach the tenant: not the cache, which would be
// tracked and readable with no changefeed behind it and no reconcile goroutine
// to confirm it, and not the CONSUMER's registered validator, which would be
// handed a configuration row for a tenant this process is no longer entitled
// to read. The pre-call check cannot cover either, because at that point the
// scope is still very much alive.
//
// Two guards answer it, and the assertions below separate them. publish's own
// drop check refuses the cache write, which is why the scope stays untracked
// and the read reports a miss — that guard has its own test in
// TestPublishIntoAlreadyDroppedStateIsRefused. Re-resolving the scope after
// the round trip is what stops everything BEFORE that: hoist the pre-call
// resolution and reuse it, and the value is still refused, but the validator
// has run and the dead state's reconcile fences have been written. Only the
// validator assertion goes red for that mutation.
//
// The third case is why that re-resolution compares IDENTITY and not the scope
// VALUE. A tenant dropped and brought back up during the round trip is a LIVE
// state under the same scope value, so a by-value lookup hands the re-read a
// perfectly tracked scope to publish into — and publish's own drop check,
// which only refuses a state that was swept, waves it through. The row was
// read under an entitlement this process no longer had when it came back; the
// new state must reconcile the key from the store itself. Worse than a stale
// value: the publication also records the key as answered by the feed, so the
// re-activation's own reconcile skips the one snapshot row that would have
// decided it.
//
// The drop is driven from inside Store.Get rather than raced for: that is
// exactly where the re-read is when a suspension lands, and the fake invokes
// the hook outside its own lock, the way a real driver holds nothing of the
// engine's.
func TestRereadRefusesAScopeDroppedDuringTheStoreCall(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}

	for _, tc := range []struct {
		name string
		// getErr fails the store call once the drop has happened, which is the
		// one outcome that teaches the engine nothing about the key and must
		// therefore be recorded somewhere — the question being whose window.
		getErr error
		// panics says the re-read unwinds instead of returning a row, which
		// takes the recovery — and its own scope resolution — down the same
		// untracked path.
		panics bool
		// reactivates says the tenant is brought back up from inside
		// Store.Get, so the scope is tracked again by the time the row comes
		// back — a NEW state, under the same scope value, that this row still
		// has no business reaching.
		reactivates bool
	}{
		{
			name: "the row comes back after the tenant is gone",
		},
		{
			// The recovery resolves the scope of its own to fence the key as
			// unusable, so a panic raised after the drop is the one path that
			// reaches that resolution with nothing to resolve.
			name:   "the store explodes after the tenant is gone",
			panics: true,
		},
		{
			// The re-activation opens a reconcile window of its own, which is
			// how production brings a tenant back: subscribe, then reconcile.
			// That window is what the refused row must not be recorded into.
			name:        "the tenant is dropped and brought back up during the read",
			reactivates: true,
		},
		{
			// The publishing path refuses the re-activated state by identity,
			// but a re-read that FAILS publishes nothing and still has an
			// outcome to record — and resolving the scope for that record by
			// value hands it to the live state's window. The key sits there as
			// unusable, and the re-activation's own reconcile then keeps a
			// value the store no longer has.
			name:        "the tenant is dropped and brought back up, then the store errors",
			getErr:      errRereadFailed,
			reactivates: true,
		},
		{
			// Same write, reached from the recovery instead of the error path:
			// it resolves the scope after the stack has unwound, when the
			// tenant it is fencing may already be a different state.
			name:        "the tenant is dropped and brought back up, then the store panics",
			panics:      true,
			reactivates: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFakeStore()
			rec := &recordingLogger{Logger: log.NewNop()}

			// A zero quiet window runs the whole re-read inline on this
			// goroutine, so the validator — if it runs at all — runs here too
			// and a plain bool is all the flag needs.
			validated := false
			defs := map[NSKey]KeyDef{nk: {
				Default: "fallback",
				Validate: func(context.Context, any) error {
					validated = true

					return nil
				},
			}}

			e := loggingEngineWith(t, defs, fs, rec)

			bringUp(t, e, dropTenant)
			fs.seed(dropTenant, jsonRow(nk, 7, `"seven"`, "ops"))

			var delivered recorder

			unsub := e.OnChange(nk, delivered.record)
			defer unsub()

			// arm names the reconcile window the RE-ACTIVATED state opens.
			// The refused re-read must write nothing into it, and the very
			// same reconcile then decides the key, so the fence and its
			// consequence are one assertion apart.
			var arm reconcileArming

			fs.onGet(func(scope store.Scope, _ NSKey) error {
				e.dropScope(scope)

				if tc.reactivates {
					bringUp(t, e, dropTenant)

					live := e.scopeFor(dropTenant)

					// What the re-activation's own reconcile leaves behind:
					// the tenant's row as it stands now, read under the
					// entitlement this process still holds. The re-read is
					// carrying an older one, and nothing it learned may unseat
					// this or fence it.
					live.mu.Lock()
					live.entries[nk] = entry{Value: reactivatedValue, Revision: 8}
					live.mu.Unlock()

					arm = armWindow(live)
				}

				if tc.panics {
					panic("the store driver exploded")
				}

				return tc.getErr
			})

			// A zero quiet window runs the re-read inline on this goroutine,
			// so the publication has already been decided — or refused — by
			// the time onEvent returns.
			e.onEvent(upsertEvent(dropTenant, nk, 7))

			if tracked(e, dropTenant) != tc.reactivates {
				if tc.reactivates {
					t.Error("the tenant brought back up inside Store.Get was not tracked when the " +
						"re-read returned, so this case no longer exercises the identity check")
				} else {
					t.Error("the re-read re-created the tenant scope after it was dropped mid-Get: " +
						"it has no changefeed and no reconcile goroutine, and reads would report it as current")
				}
			}

			cached, readable := e.Lookup(dropTenant, nk)

			switch {
			case !tc.reactivates:
				if readable {
					t.Error("a row read for a dropped tenant became readable")
				}
			case cached.Value != reactivatedValue:
				t.Errorf("the re-activated tenant reads %v (present=%t), want %q: the row the re-read "+
					"held under the DROPPED state reached a tenant it was no longer entitled to",
					cached.Value, readable, reactivatedValue)
			}

			if got := delivered.len(); got != 0 {
				t.Errorf("%d Change(s) delivered for a tenant dropped mid-Get, want 0", got)
			}

			if validated {
				t.Error("the consumer's registered validator was handed a row belonging to a tenant " +
					"dropped mid-Get: the re-read must re-resolve the scope after its round trip and " +
					"stop there, not run consumer code and let publish refuse the result")
			}

			if tc.reactivates {
				touched, unusable := recordedSets(e, dropTenant)
				if len(touched) != 0 || len(unusable) != 0 {
					t.Errorf("the re-activated scope recorded %v as answered by the feed and %v as "+
						"unusable: the re-read ran against the DROPPED state, so the new state's own "+
						"reconcile is the only thing that may decide the key, and it skips every key "+
						"the feed touched and keeps every key it could not read", touched, unusable)
				}

				// End to end, which is what the fence costs when it lands in
				// the wrong window: the row is gone from the store, so the
				// window armed above photographs a scope without the key, and
				// a key the feed could not read is KEPT rather than reset —
				// leaving the tenant on a value the store no longer holds,
				// reporting itself fresh.
				fs.remove(dropTenant, nk)

				e.reconcileScope(e.scopeFor(dropTenant), arm)

				got, ok := e.Lookup(dropTenant, nk)
				if !ok || got.Revision != 0 || got.Value != "fallback" {
					t.Errorf("after a reconcile whose snapshot lacks the key the re-activated tenant "+
						"reads %v at revision %d (present=%t), want the registered default at "+
						"revision 0", got.Value, got.Revision, ok)
				}
			}

			if !tc.panics {
				return
			}

			// The identity line is the whole point of the engine's own
			// recovery, and it must survive a scope that no longer exists:
			// resolving the scope to fence the key comes first, and a
			// recovery that dereferenced the miss would take the process with
			// it instead of naming the key.
			requireLogged(t, rec, log.LevelError, rereadPanicMsg, dropTenant, nk)
			requirePanicAccounted(t, rec, "refresh")
		})
	}
}

// nilUnsubscribeStore is a backend whose Subscribe reports success and hands
// back no unsubscribe handle. Nothing in store.Store forbids it, and the
// public NewForTesting passes a consumer's return straight through its
// adapter, so it is a shape a caller outside this module can build.
//
// onSubscribed runs while the engine is inside Subscribe, which is what lets a
// test land a Close in the one window bring-up rolls back in.
type nilUnsubscribeStore struct {
	*fakeStore

	calls        *atomic.Int64
	onSubscribed func()
}

func (s nilUnsubscribeStore) Subscribe(context.Context, store.Scope, func(store.Event)) (func(), error) {
	s.calls.Add(1)

	if s.onSubscribed != nil {
		s.onSubscribed()
	}

	return nil, nil
}

// TestNilUnsubscribeHandleNeverPanics pins the normalisation bring-up does on
// the handle Store.Subscribe returns. Three sites call it — bring-up's
// Close-raced rollback, dropScope and Close — and a nil handle is a nil
// function call at every one of them. Two of the three guard it; the rollback
// does not, and the idempotence check at the top of bring-up reads a nil
// handle as "not subscribed", so the scope would re-subscribe forever.
//
// Normalising once, where the handle arrives, is what makes all three agree.
func TestNilUnsubscribeHandleNeverPanics(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	defs := map[NSKey]KeyDef{nk: {Default: "fallback"}}

	t.Run("on the Close-raced rollback", func(t *testing.T) {
		var calls atomic.Int64

		fs := newFakeStore()
		e := New(Config{Store: nilUnsubscribeStore{fakeStore: fs, calls: &calls}, Registry: fakeRegistry{defs: defs}})

		// The engine is closed while it is inside Subscribe, so bring-up takes
		// the scope lock after Close released it, observes closed, and releases
		// the subscription it can no longer keep.
		e.store = nilUnsubscribeStore{fakeStore: fs, calls: &calls, onSubscribed: func() { _ = e.Close() }}

		if _, err := e.bringUpScope(dropTenant); !errors.Is(err, store.ErrClosed) {
			t.Fatalf("bringUpScope on a closed engine = %v, want one wrapping store.ErrClosed", err)
		}

		if tracked(e, dropTenant) {
			t.Error("the rolled-back bring-up left the scope tracked")
		}
	})

	t.Run("on Close and on a drop", func(t *testing.T) {
		var calls atomic.Int64

		fs := newFakeStore()
		e := New(Config{Store: nilUnsubscribeStore{fakeStore: fs, calls: &calls}, Registry: fakeRegistry{defs: defs}})

		bringUp(t, e, dropTenant)

		// Bring-up ran once and stored a handle, so the second call is the
		// idempotence check: it must see a subscribed scope rather than open a
		// second changefeed for it.
		bringUp(t, e, dropTenant)

		if got := calls.Load(); got != 1 {
			t.Errorf("Subscribe was called %d times for one scope, want 1: a nil handle reads as "+
				"an unsubscribed scope", got)
		}

		e.dropScope(dropTenant)

		if err := e.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
}
