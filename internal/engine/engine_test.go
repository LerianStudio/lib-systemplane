//go:build unit

package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

var errSubscribe = errors.New("subscribe failed")

// startEngine builds the engine the way the Client will — through New, with
// the defaults New applies — and closes it when the test ends, so goleak sees
// no survivor.
func startEngine(t *testing.T, defs map[NSKey]KeyDef, fs *fakeStore) *Engine {
	t.Helper()

	e := New(Config{Store: fs, Registry: fakeRegistry{defs: defs}})

	track(t, e, store.Scope{})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	return e
}

// startCtx bounds every Start in this file, so a Start that hangs fails its
// own test instead of the whole package run.
func startCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)

	return ctx
}

// bringUp opens a scope's changefeed the way Start and a tenant activation do.
// Bring-up is the only path that creates a scope — a changefeed event, a
// reconcile and a write all refuse to — so a test that drives any of those has
// to bring its scope up first, exactly as production does.
func bringUp(t *testing.T, e *Engine, scope store.Scope) {
	t.Helper()

	if _, err := e.bringUpScope(scope); err != nil {
		t.Fatalf("bringUpScope(%+v) = %v, want nil", scope, err)
	}
}

// track makes the engine treat scope as one it brought up, without opening a
// changefeed for it. Tests that drive publish, dispatch or the feed directly
// need the scope to exist — in production it does, from the moment Start or a
// tenant activation created it — and most of them are not about the feed.
func track(t *testing.T, e *Engine, scope store.Scope) {
	t.Helper()

	if e.scopeFor(scope) == nil {
		t.Fatalf("could not track scope %+v: the engine is closed", scope)
	}
}

// tracked reports whether the engine still holds scope in its map. It is the
// difference between "Start rolled the scope back" and "Start left a
// half-built scope behind that will look fresh forever".
func tracked(e *Engine, scope store.Scope) bool {
	e.scopesMu.RLock()
	defer e.scopesMu.RUnlock()

	_, ok := e.scopes[scope]

	return ok
}

func scopeStale(t *testing.T, e *Engine, scope store.Scope) bool {
	t.Helper()

	e.scopesMu.RLock()
	sc := e.scopes[scope]
	e.scopesMu.RUnlock()

	if sc == nil {
		t.Fatalf("the engine does not track scope %+v", scope)
	}

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return sc.stale
}

func firstReconcileOutcome(t *testing.T, e *Engine, scope store.Scope) error {
	t.Helper()

	sc := e.scopeFor(scope)

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return sc.firstReconcileErr
}

func TestNewAppliesDefaults(t *testing.T) {
	e := New(Config{})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	if e.logger == nil {
		t.Error("a nil logger was not defaulted; every log call would be a nil check away from a panic")
	}

	if e.closeTimeout != defaultCloseTimeout {
		t.Errorf("close timeout: got %s, want the %s default", e.closeTimeout, defaultCloseTimeout)
	}

	if e.debouncer == nil {
		t.Error("no debouncer was built; a zero window must mean synchronous submit, not no debouncer")
	}

	if e.lifecycleCtx == nil || e.lifecycleCtx.Err() != nil {
		t.Error("the lifecycle context is missing or already canceled")
	}
}

func TestPublishMakesSetVisibleBeforeFeedEcho(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := startEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	// The Client has persisted the row and hands the engine what it wrote.
	// No Start has run: the scope is created lazily by the write itself.
	e.Publish(context.Background(), scope, jsonRow(nk, 7, `"written"`, "ops"))

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss right after Publish")
	}

	if got.Value != "written" || got.Revision != 7 {
		t.Errorf("after Publish: got (%v, rev %d), want (\"written\", rev 7)", got.Value, got.Revision)
	}

	waitFor(t, time.Second, "the write's delivery", func() bool { return rec.len() == 1 })

	// The changefeed echo of that same write: same revision, same bytes.
	fs.seed(scope, jsonRow(nk, 7, `"written"`, "ops"))
	e.onEvent(upsertEvent(scope, nk, 7))

	waitFor(t, time.Second, "the echo's re-read", func() bool { return fs.getCount() == 1 })
	quiesce(t, e)

	if revs := rec.revisions(); len(revs) != 1 || revs[0] != 7 {
		t.Errorf("deliveries: got %v, want exactly [7] — the echo fired a redundant callback", revs)
	}
}

func TestStartSubscribesBeforeReconciling(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	fs.resyncOnSubscribe()

	subsWhenListed := -1

	fs.onList(func(store.Scope) error {
		subsWhenListed = fs.liveSubscriptions()

		return nil
	})

	e := startEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs)

	if err := e.Start(startCtx(t, 5*time.Second)); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if subsWhenListed != 1 {
		t.Errorf("live subscriptions when the reconcile listed: got %d, want 1 — the engine read the store before opening the feed",
			subsWhenListed)
	}
}

func TestStartRunsExactlyOneInitialReconcile(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "absent"}
	fs := newFakeStore()
	fs.resyncOnSubscribe()

	e := startEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	if err := e.Start(startCtx(t, 5*time.Second)); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitFor(t, time.Second, "the initial announcement", func() bool { return rec.len() == 1 })
	quiesce(t, e)

	if got := fs.listCount(); got != 1 {
		t.Errorf("List calls across Start: got %d, want 1 — Start reconciled on top of the resync-driven reconcile", got)
	}

	if revs := rec.revisions(); len(revs) != 1 || revs[0] != 0 {
		t.Errorf("deliveries for a key with no row: got %v, want exactly [0] — revision 0 is never deduplicated, so a second reconcile delivers twice",
			revs)
	}
}

func TestStartRollsBackWhenSubscribeFails(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	fs.onSubscribe(func(store.Scope) error { return errSubscribe })

	e := startEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs)

	err := e.Start(startCtx(t, 5*time.Second))
	if !errors.Is(err, errSubscribe) {
		t.Fatalf("Start error: got %v, want one wrapping %v", err, errSubscribe)
	}

	if tracked(e, scope) {
		t.Error("the engine still tracks a scope whose feed never opened")
	}

	if _, ok := e.Lookup(scope, nk); ok {
		t.Error("Lookup reports a hit after a failed Start")
	}
}

func TestStartReturnsCtxErrorWhenNoResyncArrives(t *testing.T) {
	scope := store.Scope{}
	// A backend that never emits OpResync is broken per FC-2. Failing loudly
	// at Start beats serving registered defaults forever.
	fs := newFakeStore()

	e := startEngine(t, nil, fs)

	err := e.Start(startCtx(t, 100*time.Millisecond))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start error: got %v, want context.DeadlineExceeded", err)
	}

	if !tracked(e, scope) {
		t.Fatal("the scope was dropped; only a failed Subscribe rolls it back")
	}

	if !scopeStale(t, e, scope) {
		t.Error("the scope is not stale after a Start that never saw a reconcile")
	}
}

func TestStartReturnsWrappedFirstReconcileError(t *testing.T) {
	scope := store.Scope{}
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	fs.resyncOnSubscribe()
	fs.onList(func(store.Scope) error { return errList })

	e := startEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs)

	err := e.Start(startCtx(t, 2*time.Second))
	if !errors.Is(err, errList) {
		t.Fatalf("Start error: got %v, want one wrapping %v", err, errList)
	}

	if errors.Is(err, context.DeadlineExceeded) {
		t.Error("Start blocked until its context expired instead of returning the first reconcile's error")
	}

	if !tracked(e, scope) {
		t.Fatal("a failed first reconcile dropped the scope; the next resync must be able to retry it")
	}

	// A first reconcile that published nothing leaves every registered key a
	// miss, and the Client answers a miss with the registered default. The
	// miss has to carry Stale, or that default is reported to the caller as a
	// value the store confirmed (FC-5).
	got, ok := e.Lookup(scope, nk)
	if ok {
		t.Fatalf("Lookup: got the cached entry %+v, want a miss after a reconcile that published nothing", got)
	}

	if !got.Stale {
		t.Error("the miss reports Stale false: the registered default behind it would read as confirmed")
	}
}

func TestLaterReconcileDoesNotOverwriteFirstReconcileOutcome(t *testing.T) {
	scope := store.Scope{}
	fs := newFakeStore()
	fs.resyncOnSubscribe()

	e := startEngine(t, nil, fs)
	ctx := startCtx(t, 5*time.Second)

	if err := e.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// A reconnect whose reload fails must not rewrite the outcome Start has
	// already returned to its caller.
	fs.onList(func(store.Scope) error { return errList })
	e.onEvent(resyncEvent(scope))
	waitReconcileIdle(t, e, scope)

	if err := firstReconcileOutcome(t, e, scope); err != nil {
		t.Errorf("recorded first-reconcile outcome: got %v, want nil", err)
	}

	if err := e.Start(ctx); err != nil {
		t.Errorf("second Start: got %v, want nil", err)
	}
}

func TestStartIsIdempotent(t *testing.T) {
	fs := newFakeStore()
	fs.resyncOnSubscribe()

	e := startEngine(t, nil, fs)
	ctx := startCtx(t, 5*time.Second)

	if err := e.Start(ctx); err != nil {
		t.Fatalf("first Start: %v", err)
	}

	if err := e.Start(ctx); err != nil {
		t.Fatalf("second Start: %v", err)
	}

	if got := fs.subscribeCount(); got != 1 {
		t.Errorf("Subscribe calls: got %d, want 1", got)
	}

	if got := fs.listCount(); got != 1 {
		t.Errorf("List calls: got %d, want 1", got)
	}
}

func TestStartAfterCloseReturnsClosed(t *testing.T) {
	fs := newFakeStore()
	e := New(Config{Store: fs, Registry: fakeRegistry{}})

	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := e.Start(startCtx(t, time.Second)); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Start after Close: got %v, want %v", err, store.ErrClosed)
	}

	if got := fs.subscribeCount(); got != 0 {
		t.Errorf("Subscribe calls on a closed engine: got %d, want 0", got)
	}
}

func TestWriteDuringStartSurvives(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	fs.resyncOnSubscribe()

	entered := make(chan struct{})
	gate := make(chan struct{})

	fs.onList(func(store.Scope) error {
		fs.onList(nil)
		close(entered)
		<-gate

		return nil
	})

	e := startEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs)

	started := make(chan error, 1)
	go func() { started <- e.Start(startCtx(t, 5*time.Second)) }()

	<-entered

	// While the initial snapshot is held open the feed publishes revision 5,
	// and the row the snapshot will carry is an older revision 3.
	fs.seed(scope, jsonRow(nk, 5, `"feed"`, "feed"))
	e.onEvent(upsertEvent(scope, nk, 5))
	fs.seed(scope, jsonRow(nk, 3, `"snapshot"`, "list"))

	close(gate)

	if err := <-started; err != nil {
		t.Fatalf("Start: %v", err)
	}

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after Start")
	}

	if got.Value != "feed" || got.Revision != 5 {
		t.Errorf("after Start: got (%v, rev %d), want (\"feed\", rev 5) — the snapshot overwrote a write made during Start",
			got.Value, got.Revision)
	}
}

// TestStartReturnsClosedWhenCloseRacesIt pins the release path. Start waits for
// the first reconcile, which a Close cancels without ever completing: the event
// that would have driven it is dropped, the reconcile worker refuses to start,
// and the channel Start waits on is never closed. A caller that started the
// engine with a context of its own choosing — context.Background() is what a
// service does — was wedged for the life of the process.
func TestStartReturnsClosedWhenCloseRacesIt(t *testing.T) {
	fs := newFakeStore() // no resyncOnSubscribe: the first reconcile never completes
	e := startEngine(t, map[NSKey]KeyDef{}, fs)

	subscribed := make(chan struct{})

	fs.onSubscribe(func(store.Scope) error {
		fs.onSubscribe(nil)
		close(subscribed)

		return nil
	})

	started := make(chan error, 1)

	go func() { started <- e.Start(context.Background()) }()

	select {
	case <-subscribed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Start to open the changefeed")
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	select {
	case err := <-started:
		if !errors.Is(err, store.ErrClosed) {
			t.Fatalf("Start() = %v, want an error wrapping %v", err, store.ErrClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("Start() never returned after Close(); a caller waiting on a first reconcile nobody will run is wedged for the life of the process")
	}
}

// TestStartFailsWhenFirstReconcilePanics pins the other way out of that wait. A
// panic inside the first reconcile — the registered validator is consumer code —
// is recovered so the scope keeps its reconcile goroutine, but the recovery used
// to swallow the completion too, leaving Start blocked until its own context
// expired.
func TestStartFailsWhenFirstReconcilePanics(t *testing.T) {
	fs := newFakeStore()
	fs.resyncOnSubscribe()

	fs.onList(func(store.Scope) error {
		fs.onList(nil)

		panic("reconcile blew up")
	})

	e := startEngine(t, map[NSKey]KeyDef{}, fs)

	err := e.Start(startCtx(t, 2*time.Second))
	if !errors.Is(err, errFirstReconcilePanicked) {
		t.Fatalf("Start() = %v, want an error wrapping %v", err, errFirstReconcilePanicked)
	}

	if errors.Is(err, context.DeadlineExceeded) {
		t.Error("Start blocked on a panicked first reconcile until its own context expired")
	}

	if errors.Is(err, store.ErrValidation) {
		t.Error("a panicked reconcile reports as a store validation failure; it is an internal engine failure")
	}
}
