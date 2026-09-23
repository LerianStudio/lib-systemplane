//go:build unit

package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// closeEngine returns an Engine ready to be closed by the test itself: no
// t.Cleanup cancels the lifecycle context here, because Close is the thing
// under test and a cleanup that canceled it would hide a Close that did not.
//
// It is built through New, exactly as storeEngine below and as the Client
// will, so timeout arrives the way a consumer's WithCloseTimeout does — as a
// Config field New has to honour. Hand-building an Engine with closeTimeout
// already set would test the field rather than the option, and an engine that
// ignored the configured value and waited the 30s default would still return
// ErrCloseTimeout eventually, leaving every assertion here green.
func closeEngine(t *testing.T, timeout time.Duration) *Engine {
	t.Helper()

	e := New(Config{
		Store:        newFakeStore(),
		Registry:     fakeRegistry{},
		CloseTimeout: timeout,
	})

	track(t, e, store.Scope{})

	return e
}

// hangGuard is the bound every wait in these tests that is ONLY a hang guard
// uses. It is deliberately far longer than anything being waited for: the wait
// ends on the event, not on the clock, so the only thing a tighter bound buys
// is a red test on a loaded runner. Bounds that are themselves under test —
// the close timeout a stuck callback must trip — stay short and explicit.
const hangGuard = 30 * time.Second

// mustReceive waits for ch to fire, failing the test rather than hanging the
// package when a delivery never happens.
func mustReceive(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(hangGuard):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestCloseWaitsForCtxHonoringCallbacks(t *testing.T) {
	e := closeEngine(t, 5*time.Second)
	nk := NSKey{Namespace: "ns", Key: "honors-ctx"}

	entered := make(chan struct{})
	returned := make(chan struct{})

	e.OnChange(nk, func(ctx context.Context, _ Change) {
		close(entered)
		<-ctx.Done()
		// Winding down takes a moment. Without it the assertion below would
		// pass whether or not Close waited, because the callback would finish
		// in the same instant cancellation reached it.
		time.Sleep(50 * time.Millisecond)
		close(returned)
	})

	e.publishInto(pub(nk, 1, "v1"))
	mustReceive(t, entered, "the subscriber to start running")

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil for a callback that honors ctx", err)
	}

	// The callback must be finished BEFORE Close returned, not merely on its
	// way out: that is the whole difference between waiting and cancelling.
	select {
	case <-returned:
	default:
		t.Fatal("Close returned while a ctx-honoring callback was still running")
	}
}

func TestCloseReportsTimeoutNamingStuckKey(t *testing.T) {
	e := closeEngine(t, 100*time.Millisecond)
	nk := NSKey{Namespace: "ns", Key: "ignores-ctx"}

	entered := make(chan struct{})
	returned := make(chan struct{})
	release := make(chan struct{})

	e.OnChange(nk, func(context.Context, Change) {
		close(entered)
		<-release // deliberately ignores ctx: the subscriber's own leak
		close(returned)
	})

	e.publishInto(pub(nk, 1, "v1"))
	mustReceive(t, entered, "the subscriber to start running")

	start := time.Now()

	err := e.Close()
	if !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("Close() = %v, want an error wrapping ErrCloseTimeout", err)
	}

	// The bound itself, not just the error. An engine that dropped the
	// configured 100ms and waited the 30s default still returns ErrCloseTimeout
	// naming the same key, so only the clock can tell that WithCloseTimeout was
	// honoured — and a consumer that sets 1s and silently waits 30s is long
	// past a Kubernetes termination grace period by the time it gives up.
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Close() took %s for a 100ms close timeout, want it far below 5s", elapsed)
	}

	for _, want := range []string{"single-tenant", nk.Namespace, nk.Key} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Close() error %q does not name %q", err, want)
		}
	}

	// A timeout still leaves the engine fully closed.
	if e.publishInto(pub(nk, 2, "v2")) {
		t.Error("publish after a timed-out Close was accepted, want dropped")
	}

	// The half TestCloseIsIdempotent cannot reach: it replays a nil outcome,
	// where the outcome that matters is a timeout. A second Close must replay
	// that timeout rather than report the clean shutdown that never happened —
	// a consumer retrying Close on its way out would otherwise be told the
	// stuck subscriber let go.
	second := e.Close()
	if !errors.Is(second, ErrCloseTimeout) {
		t.Fatalf("second Close() = %v, want the first Close's ErrCloseTimeout replayed", second)
	}

	for _, want := range []string{"single-tenant", nk.Namespace, nk.Key} {
		if !strings.Contains(second.Error(), want) {
			t.Errorf("second Close() error %q does not name %q", second, want)
		}
	}

	// Release the stuck callback and wait for it: a test that leaks on purpose
	// fails the whole package under goleak.
	close(release)
	mustReceive(t, returned, "the released subscriber to finish")
	e.dispatchWG.Wait()
}

func TestCloseIsIdempotent(t *testing.T) {
	e := closeEngine(t, 5*time.Second)

	var unsubscribes atomic.Int64

	sc := newScopeState(store.Scope{})
	sc.unsubscribe = func() { unsubscribes.Add(1) }
	e.scopes[store.Scope{}] = sc

	if err := e.Close(); err != nil {
		t.Fatalf("first Close() = %v, want nil", err)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("second Close() = %v, want nil", err)
	}

	if got := unsubscribes.Load(); got != 1 {
		t.Errorf("scope unsubscribed %d times across two Close calls, want 1", got)
	}
}

func TestCloseOnNilEngineReturnsNil(t *testing.T) {
	var e *Engine

	if err := e.Close(); err != nil {
		t.Fatalf("(*Engine)(nil).Close() = %v, want nil", err)
	}
}

func TestPublishAfterCloseIsDropped(t *testing.T) {
	e := closeEngine(t, 5*time.Second)
	nk := NSKey{Namespace: "ns", Key: "after-close"}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	if e.publishInto(pub(nk, 1, "v1")) {
		t.Error("publish after Close was accepted, want dropped")
	}

	if _, ok := e.Lookup(store.Scope{}, nk); ok {
		t.Error("publish after Close created a scope entry, want none")
	}
}

func TestPublishRacingCloseStartsNoWorker(t *testing.T) {
	// publish's own closed check is a check-then-act: a publication that
	// passes it can reach the dispatch WaitGroup microseconds later, while
	// Close is already inside Wait. Go answers that with an unrecovered
	// "WaitGroup misuse: Add called concurrently with Wait" — a process kill
	// during shutdown, which this test reproduces by racing the two.
	e := closeEngine(t, 5*time.Second)

	sc := e.trackedScope(store.Scope{})
	if sc == nil {
		t.Fatal("the single-tenant scope was not tracked")
	}

	for i := range 64 {
		nk := NSKey{Namespace: "ns", Key: fmt.Sprintf("key-%d", i)}
		e.OnChange(nk, func(context.Context, Change) {})
	}

	var wg sync.WaitGroup

	start := make(chan struct{})

	for i := range 64 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start
			e.publishInto(pub(NSKey{Namespace: "ns", Key: fmt.Sprintf("key-%d", i)}, 1, "v1"))
		}()
	}

	wg.Add(1)

	go func() {
		defer wg.Done()

		<-start

		if err := e.Close(); err != nil {
			t.Errorf("Close() = %v, want nil", err)
		}
	}()

	close(start)
	wg.Wait()

	// Nothing may start a worker once Close has shut the door, whichever side
	// of the race a straggler landed on.
	if w := e.workerFor(sc, NSKey{Namespace: "ns", Key: "after"}); w != nil {
		t.Error("workerFor started a worker after Close")
	}
}

// TestRefreshRacingCloseNeverReachesTheStore pins the drop that keeps a
// debounced re-read out of a store the Client is about to close. A timer that
// has already fired cannot be canceled, so the fired-but-not-yet-running
// re-read is the one straggler Close cannot revoke — it can only refuse to let
// it join the drain, and a re-read that cannot join must not run at all.
//
// This is TestPublishRacingCloseStartsNoWorker's sibling for the other door
// beginWork guards: without the guard, the re-read reaches Store.Get after
// Close returned, and its unpaired dispatchWG.Done kills the process.
func TestRefreshRacingCloseNeverReachesTheStore(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, time.Millisecond, 2*time.Second)

	fs.seed(scope, jsonRow(nk, 1, `"live"`, "ops"))

	// The same door Close shuts, taken here alone so the re-read below lands
	// squarely in the window between "Close shut the door" and "Close is
	// inside Wait".
	e.closeWorkers()

	e.trackedRefresh(scope, nk)

	if got := fs.getCount(); got != 0 {
		t.Errorf("Store.Get called %d times by a re-read that lost the race to Close, want 0", got)
	}

	// Dropped whole, not merely dropped late: a re-read that took the
	// WaitGroup and skipped the read would make Close wait for work nobody is
	// doing, and one that skipped the read and released the WaitGroup anyway
	// would panic on the unpaired Done.
	drained := make(chan struct{})

	go func() {
		e.dispatchWG.Wait()
		close(drained)
	}()

	mustReceive(t, drained, "the dispatch WaitGroup to be already drained")
}

// storeEngine builds the engine the way the Client will — through New — and
// leaves Close to the test, because Close is the thing under test here and a
// cleanup that canceled the lifecycle context would hide a Close that never
// waited. The cleanup only covers a test that fails before reaching its own
// Close; Close is idempotent, so a second one is free.
func storeEngine(t *testing.T, defs map[NSKey]KeyDef, fs *fakeStore, window, timeout time.Duration) *Engine {
	t.Helper()

	e := New(Config{
		Store:        fs,
		Registry:     fakeRegistry{defs: defs},
		Debounce:     window,
		CloseTimeout: timeout,
	})

	track(t, e, store.Scope{})

	t.Cleanup(func() { _ = e.Close() })

	return e
}

// heldGet blocks the NEXT Get until release is called and leaves every later
// Get unhooked, so a test can catch the engine inside a debounced re-read
// instead of guessing at the window with a sleep.
func heldGet(fs *fakeStore) (release func()) {
	gate := make(chan struct{})

	fs.onGet(func(store.Scope, NSKey) error {
		fs.onGet(nil)
		<-gate

		return nil
	})

	var once sync.Once

	return func() { once.Do(func() { close(gate) }) }
}

// closeInBackground calls Close on its own goroutine and hands back the
// channel its outcome arrives on, so a test can assert that Close is still
// waiting rather than merely that it eventually returned.
func closeInBackground(e *Engine) <-chan error {
	done := make(chan error, 1)

	go func() { done <- e.Close() }()

	return done
}

// openWindows counts the reconcile windows armed on a scope: one per reconcile
// that is taking a snapshot or waiting to.
func openWindows(e *Engine, scope store.Scope) int {
	sc := e.scopeFor(scope)

	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	return len(sc.windows)
}

// pendingReconciles counts what the scope's single-slot reconcile mailbox
// holds, which is 0 or 1 by construction — that is the coalescing.
func pendingReconciles(e *Engine, scope store.Scope) int {
	sc := e.scopeFor(scope)

	sc.resyncMu.Lock()
	defer sc.resyncMu.Unlock()

	if sc.resyncPending == nil {
		return 0
	}

	return 1
}

func mustStillBeWaiting(t *testing.T, done <-chan error, what string) {
	t.Helper()

	select {
	case err := <-done:
		t.Fatalf("Close() returned %v while %s", err, what)
	case <-time.After(100 * time.Millisecond):
	}
}

func mustCloseCleanly(t *testing.T, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
	case <-time.After(hangGuard):
		t.Fatal("Close() never returned")
	}
}

func TestCloseWaitsForAReconcileInsideList(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	release := heldList(fs)

	e.onEvent(resyncEvent(scope))
	waitFor(t, time.Second, "the reconcile to reach its List", func() bool { return fs.listCount() == 1 })

	done := closeInBackground(e)
	mustStillBeWaiting(t, done, "a reconcile was still inside Store.List")

	release()
	mustCloseCleanly(t, done)

	// Nothing may touch the store once Close has returned.
	gets, lists := fs.getCount(), fs.listCount()

	e.onEvent(upsertEvent(scope, nk, 9))
	e.onEvent(resyncEvent(scope))

	// A pause rather than quiesce: a closed engine accepts no publication, so
	// there is no sentinel it could deliver and no signal to wait on. onEvent
	// returns synchronously on a closed engine, so this only covers an
	// implementation that would schedule the work instead of dropping it.
	time.Sleep(100 * time.Millisecond)

	if got := fs.getCount(); got != gets {
		t.Errorf("Store.Get called %d times after Close returned, want %d", got, gets)
	}

	if got := fs.listCount(); got != lists {
		t.Errorf("Store.List called %d times after Close returned, want %d", got, lists)
	}
}

func TestCloseWaitsForADebouncedReReadInFlight(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, time.Millisecond, hangGuard)

	fs.seed(scope, jsonRow(nk, 1, `"live"`, "ops"))

	release := heldGet(fs)

	e.onEvent(upsertEvent(scope, nk, 1))
	waitFor(t, hangGuard, "the debounced re-read to reach its Get", func() bool { return fs.getCount() == 1 })

	done := closeInBackground(e)
	mustStillBeWaiting(t, done, "a debounced re-read was still inside Store.Get")

	release()
	mustCloseCleanly(t, done)
}

func TestClosePendingReReadNeverReachesTheStore(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 200*time.Millisecond, 2*time.Second)

	fs.seed(scope, jsonRow(nk, 1, `"live"`, "ops"))
	e.onEvent(upsertEvent(scope, nk, 1))

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	// Well past the quiet window: a pending re-read Close discarded must never
	// reach the store afterwards. The debounce window IS what this test is
	// about, so waiting it out is the assertion, not a guess at one.
	time.Sleep(400 * time.Millisecond)

	if got := fs.getCount(); got != 0 {
		t.Errorf("Store.Get called %d times after Close, want 0", got)
	}
}

func TestEventsAfterCloseAreDroppedWhole(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	// A scope the engine never brought up, so "no scope was created" is an
	// assertion about the events rather than about the test's own setup.
	scope := store.Scope{Tenant: "late"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	fs.seed(scope, jsonRow(nk, 1, `"live"`, "ops"))

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	e.onEvent(upsertEvent(scope, nk, 1))
	e.onEvent(deleteEvent(scope, nk))
	e.onEvent(resyncEvent(scope))
	e.onEvent(disconnectEvent(scope))

	// A pause rather than quiesce, for the same reason as above: a closed
	// engine has no signal to wait on.
	time.Sleep(100 * time.Millisecond)

	if tracked(e, scope) {
		t.Error("an event after Close created a scope, want none tracked")
	}

	if got := fs.getCount(); got != 0 {
		t.Errorf("Store.Get called %d times after Close, want 0", got)
	}

	if got := fs.listCount(); got != 0 {
		t.Errorf("Store.List called %d times after Close, want 0", got)
	}

	if got := rec.len(); got != 0 {
		t.Errorf("%d deliveries after Close, want 0", got)
	}
}

func TestScopeForRefusesToCreateAfterClose(t *testing.T) {
	e := closeEngine(t, time.Second)

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	if sc := e.scopeFor(store.Scope{Tenant: "late"}); sc != nil {
		t.Error("scopeFor created a scope after Close, want none")
	}
}

func TestResyncBurstCoalescesAndCloseWaitsForIt(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	release := heldList(fs)

	e.onEvent(resyncEvent(scope))
	waitFor(t, time.Second, "the reconcile to reach its List", func() bool { return fs.listCount() == 1 })

	// Five more reconnects while the first snapshot is still being taken.
	for range 5 {
		e.onEvent(resyncEvent(scope))
	}

	// The burst coalesced: the one reconcile taking its snapshot, and ONE
	// pending behind it. A reconcile per event would leave six armed windows
	// and six goroutines queued on the held List.
	if got := openWindows(e, scope); got != 2 {
		t.Errorf("%d reconcile windows open for a burst of six resyncs, want 2 "+
			"(one taking its snapshot, one pending)", got)
	}

	if got := pendingReconciles(e, scope); got != 1 {
		t.Errorf("%d pending reconciles for a burst of six resyncs, want 1", got)
	}

	done := closeInBackground(e)
	mustStillBeWaiting(t, done, "a reconcile was still inside Store.List")

	release()
	mustCloseCleanly(t, done)

	// Close waited for the reconcile it found in flight and for nothing else:
	// the pending one either ran once or was abandoned by the cancellation, so
	// a burst of six can never have listed more than twice.
	if got := fs.listCount(); got > 2 {
		t.Errorf("Store.List called %d times for a burst of six resyncs, want at most 2", got)
	}
}

// TestResyncRacingCloseArmsNoReconcile pins the fences an OpResync takes back
// out when shutdown wins the race. Arming happens on the changefeed goroutine
// before anything can drain the mailbox, so a resync that arrives once the
// door is shut leaves a window open and a reconcile queued for a goroutine
// that will never start — and an open window silences the feed's own fence
// bookkeeping for a scope nothing is reconciling.
func TestResyncRacingCloseArmsNoReconcile(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	e.closeWorkers()

	e.onResync(scope)

	if got := openWindows(e, scope); got != 0 {
		t.Errorf("%d reconcile windows open after a resync that lost the race to Close, want 0", got)
	}

	if got := pendingReconciles(e, scope); got != 0 {
		t.Errorf("%d reconciles queued after a resync that lost the race to Close, want 0", got)
	}

	if got := fs.listCount(); got != 0 {
		t.Errorf("Store.List called %d times by a resync that lost the race to Close, want 0", got)
	}
}

func TestDropScopeStopsThatScopesWorkers(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	tenant := store.Scope{Tenant: "acme"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, hangGuard)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	// A reconcile goroutine and a delivery worker, both belonging to the
	// tenant scope and nothing else.
	bringUp(t, e, tenant)
	e.onEvent(resyncEvent(tenant))
	waitFor(t, hangGuard, "the tenant's first reconcile to announce its keys",
		func() bool { return rec.len() == 1 })

	e.dropScope(tenant)

	drained := make(chan struct{})

	go func() {
		e.dispatchWG.Wait()
		close(drained)
	}()

	mustReceive(t, drained, "the dropped scope's goroutines to exit")

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

func TestDropScopeLeavesOtherScopesWorkersRunning(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	tenant := store.Scope{Tenant: "acme"}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 2*time.Second)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	bringUp(t, e, tenant)
	bringUp(t, e, store.Scope{})

	e.publishInto(publication{Scope: tenant, NSKey: nk, Revision: 1, Value: "tenant"})
	e.publishInto(publication{NSKey: nk, Revision: 1, Value: "single"})
	waitFor(t, time.Second, "both scopes to deliver", func() bool { return rec.len() == 2 })

	e.dropScope(tenant)

	// The surviving scope's worker must still deliver.
	e.publishInto(publication{NSKey: nk, Revision: 2, Value: "single-again"})
	waitFor(t, time.Second, "the surviving scope to keep delivering", func() bool { return rec.len() == 3 })

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

// TestCloseDoesNotCloseTheStore pins the ownership line between the Client and
// the engine. The Client opens the backend, hands it to the engine, and closes
// it after the engine has stopped using it. An engine that closed the store
// itself would break every caller that outlives one engine — NewForTesting
// most visibly — by tearing down a connection it never opened.
func TestCloseDoesNotCloseTheStore(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()

	fs.resyncOnSubscribe()

	e := startEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs)

	if err := e.Start(startCtx(t, 2*time.Second)); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := fs.closeCount(); got != 0 {
		t.Errorf("Engine.Close closed the store %d times, want 0: the Client owns the store's lifecycle", got)
	}
}

// TestCloseTimeoutInsideAStoreCallSaysSo pins the diagnosis a timed-out Close
// hands the operator. Nothing is inside a subscriber here — the engine is stuck
// in Store.List — so blaming a callback sends whoever reads the message hunting
// through consumer code for a bug that is in the backend or the network.
func TestCloseTimeoutInsideAStoreCallSaysSo(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 100*time.Millisecond)

	release := heldList(fs)

	e.onEvent(resyncEvent(scope))
	waitFor(t, time.Second, "the reconcile to reach its List", func() bool { return fs.listCount() == 1 })

	err := e.Close()
	if !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("Close() = %v, want an error wrapping ErrCloseTimeout", err)
	}

	if strings.Contains(err.Error(), "subscriber") {
		t.Errorf("Close() error %q blames a subscriber, but the engine is stuck inside Store.List", err)
	}

	if !strings.Contains(err.Error(), "store call") {
		t.Errorf("Close() error %q does not say the engine is stuck inside a store call", err)
	}

	// Release the held List and wait for the reconcile goroutine: a test that
	// leaks on purpose fails the whole package under goleak.
	release()
	e.dispatchWG.Wait()
}

// TestSubscribeLosingTheRaceToCloseUnsubscribes closes a lifecycle leak that
// only a losing race can reach. Close reads each tracked scope's unsubscribe
// exactly once; a Start whose Store.Subscribe is still in flight at that
// moment has none to be read, and the handle it stores microseconds later is
// then held by nobody. The changefeed stays registered in the store's
// subscriber list for the life of the store, keeping the whole Engine
// reachable and — once a scope means a tenant — one live connection per
// tenant whose Start lost the race.
//
// The race is driven rather than raced for: Subscribe is held open until Close
// has marked the engine closed, which is exactly the window the leak needs.
func TestSubscribeLosingTheRaceToCloseUnsubscribes(t *testing.T) {
	fs := newFakeStore()
	e := New(Config{Store: fs, Registry: fakeRegistry{}, CloseTimeout: 2 * time.Second})

	t.Cleanup(func() { _ = e.Close() })

	reached := make(chan struct{})
	release := make(chan struct{})

	fs.onSubscribe(func(store.Scope) error {
		fs.onSubscribe(nil)
		close(reached)
		<-release

		return nil
	})

	started := make(chan error, 1)

	go func() { started <- e.Start(context.Background()) }()

	mustReceive(t, reached, "Start to reach Store.Subscribe")

	closed := closeInBackground(e)

	waitFor(t, hangGuard, "Close to mark the engine closed", func() bool { return e.closed.Load() })

	close(release)

	mustCloseCleanly(t, closed)

	select {
	case err := <-started:
		if !errors.Is(err, store.ErrClosed) {
			t.Errorf("Start() = %v, want store.ErrClosed: a Start that lost the race to Close must not "+
				"report a scope it cannot keep", err)
		}
	case <-time.After(hangGuard):
		t.Fatal("Start never returned")
	}

	if got := fs.unsubscribeCount(); got != 1 {
		t.Errorf("unsubscribe called %d times, want 1: the changefeed Start opened outlives the engine", got)
	}

	if got := fs.liveSubscriptions(); got != 0 {
		t.Errorf("%d changefeeds still registered after Close, want 0: the engine stays reachable "+
			"from the store's subscriber list forever", got)
	}

	if tracked(e, store.Scope{}) {
		t.Error("the engine still tracks a scope whose bring-up lost the race to Close")
	}
}
