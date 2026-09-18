//go:build unit

package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/debounce"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// feedEngine returns an Engine wired to fs whose dispatch workers and pending
// debounce timers are stopped when the test ends, so goleak sees no survivor.
// window is the debouncer's quiet window; zero makes Submit synchronous, which
// is what every test that is not about coalescing wants.
func feedEngine(t *testing.T, defs map[NSKey]KeyDef, fs *fakeStore, window time.Duration) *Engine {
	t.Helper()

	return registryEngine(t, fakeRegistry{defs: defs}, fs, window)
}

// registryEngine is feedEngine with the registry supplied by the caller, so a
// test can hand the engine a registry that fires a hook at the exact moment a
// reconcile decides a key — which is how a feed event is landed inside the gap
// a fence is supposed to close, without guessing at it with a sleep.
func registryEngine(t *testing.T, reg Registry, fs *fakeStore, window time.Duration) *Engine {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		store:           fs,
		registry:        reg,
		scopes:          map[store.Scope]*scopeState{},
		debouncer:       debounce.New[scopeNSKey](window),
		lifecycleCtx:    ctx,
		lifecycleCancel: cancel,
	}

	track(t, e, store.Scope{})

	t.Cleanup(func() {
		e.debouncer.Close()
		cancel()
		// The same door Close uses, for the same reason: a straggler re-read
		// or reconcile that reached the WaitGroup while this Wait ran would
		// kill the test binary rather than fail a test.
		e.closeWorkers()
		e.dispatchWG.Wait()
	})

	return e
}

func upsertEvent(scope store.Scope, nk NSKey, revision int64) store.Event {
	return store.Event{
		Scope:     scope,
		Namespace: nk.Namespace,
		Key:       nk.Key,
		Op:        store.OpUpsert,
		Revision:  revision,
	}
}

func deleteEvent(scope store.Scope, nk NSKey) store.Event {
	return store.Event{Scope: scope, Namespace: nk.Namespace, Key: nk.Key, Op: store.OpDelete}
}

func jsonRow(nk NSKey, revision int64, value, actor string) store.Entry {
	return store.Entry{
		Namespace: nk.Namespace,
		Key:       nk.Key,
		Value:     []byte(value),
		Revision:  revision,
		UpdatedAt: time.Unix(1700000000, 0).UTC(),
		UpdatedBy: actor,
	}
}

// armReconcile opens a reconcile window on the scope the way an OpResync does,
// so a test can assert what the feed records during that window without owning
// the reconcile itself. It returns the arming, which names the window.
func armReconcile(e *Engine, scope store.Scope) reconcileArming {
	return e.scopeFor(scope).beginReconcile()
}

// recordedSets unions the fences of every window currently open on the scope.
// A union is what a test wants: a key recorded in any open window is a key the
// feed has spoken about, and the per-window separation is asserted by the
// overlapping-reconcile tests through the values that end up cached.
func recordedSets(e *Engine, scope store.Scope) (touched, unusable []NSKey) {
	sc := e.scopeFor(scope)

	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	for _, window := range sc.windows {
		for nk := range window.touched {
			touched = append(touched, nk)
		}

		for nk := range window.unusable {
			unusable = append(unusable, nk)
		}
	}

	return touched, unusable
}

// clearStale stands in for a completed first reconcile, which Task 1.2.3 owns:
// a scope is born stale, so a test about OpDisconnect has to start from a
// scope that is NOT stale or it asserts nothing.
func clearStale(e *Engine, scope store.Scope) {
	sc := e.scopeFor(scope)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.stale = false
}

func TestDeleteEventPublishesDefaultAtRevisionZero(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	fs.seed(store.Scope{}, jsonRow(nk, 3, `"live"`, "ops"))
	e.onEvent(upsertEvent(store.Scope{}, nk, 3))
	waitFor(t, time.Second, "the upsert delivery", func() bool { return rec.len() == 1 })

	readsBeforeDelete := fs.getCount()

	e.onEvent(deleteEvent(store.Scope{}, nk))
	waitFor(t, time.Second, "the delete delivery", func() bool { return rec.len() == 2 })

	got, ok := e.Lookup(store.Scope{}, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after a delete, want the registered default")
	}

	if got.Value != "fallback" || got.Revision != 0 {
		t.Errorf("cached after delete: got (%v, rev %d), want (\"fallback\", rev 0)", got.Value, got.Revision)
	}

	if !got.UpdatedAt.IsZero() || got.UpdatedBy != "" {
		t.Errorf("provenance after delete: got (%s, %q), want (zero time, \"\")", got.UpdatedAt, got.UpdatedBy)
	}

	if revs := rec.revisions(); len(revs) != 2 || revs[1] != 0 {
		t.Errorf("delivered revisions: got %v, want the second to be 0", revs)
	}

	if after := fs.getCount(); after != readsBeforeDelete {
		t.Errorf("delete triggered %d store read(s), want 0: a delete is self-describing", after-readsBeforeDelete)
	}
}

func TestUpsertEventReReadsAndIngests(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	fs.seed(store.Scope{}, jsonRow(nk, 4, `{"max":10}`, "ops"))
	e.onEvent(upsertEvent(store.Scope{}, nk, 4))

	waitFor(t, time.Second, "the upsert delivery", func() bool { return rec.len() == 1 })

	got, ok := e.Lookup(store.Scope{}, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after an upsert")
	}

	value, ok := got.Value.(map[string]any)
	if !ok {
		t.Fatalf("cached value: got %T, want map[string]any", got.Value)
	}

	if value["max"] != float64(10) || got.Revision != 4 || got.UpdatedBy != "ops" {
		t.Errorf("cached after upsert: got (%v, rev %d, %q), want (max 10, rev 4, %q)",
			got.Value, got.Revision, got.UpdatedBy, "ops")
	}

	if reads := fs.getCount(); reads != 1 {
		t.Errorf("store reads: got %d, want 1", reads)
	}
}

func TestUpsertReReadNotFoundKeepsCurrentValue(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	fs.seed(store.Scope{}, jsonRow(nk, 2, `"current"`, "ops"))
	e.onEvent(upsertEvent(store.Scope{}, nk, 2))
	waitFor(t, time.Second, "the first delivery", func() bool { return rec.len() == 1 })

	// The row is not visible to this reader: an upsert notification whose
	// re-read finds nothing is a non-answer, never a deletion.
	fs.remove(store.Scope{}, nk)
	e.onEvent(upsertEvent(store.Scope{}, nk, 3))

	quiesce(t, e)

	got, ok := e.Lookup(store.Scope{}, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after a not-found re-read, want the previous value")
	}

	if got.Value != "current" || got.Revision != 2 {
		t.Errorf("cached after not-found re-read: got (%v, rev %d), want (\"current\", rev 2)", got.Value, got.Revision)
	}

	if n := rec.len(); n != 1 {
		t.Errorf("deliveries: got %d, want 1: a not-found re-read publishes nothing", n)
	}
}

func TestFeedBurstForOneKeyCausesOneStoreRead(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 50*time.Millisecond)

	fs.seed(store.Scope{}, jsonRow(nk, 5, `"newest"`, "ops"))

	for rev := int64(1); rev <= 5; rev++ {
		e.onEvent(upsertEvent(store.Scope{}, nk, rev))
	}

	waitFor(t, 2*time.Second, "the coalesced re-read", func() bool {
		got, ok := e.Lookup(store.Scope{}, nk)

		return ok && got.Revision == 5
	})

	quiesce(t, e)

	if reads := fs.getCount(); reads != 1 {
		t.Errorf("store reads for a burst of 5 events on one key: got %d, want 1", reads)
	}
}

func TestFeedRecordsTouchedOnlyWhileReconciling(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	other := NSKey{Namespace: "billing", Key: "other"}
	missing := NSKey{Namespace: "billing", Key: "missing"}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{
		nk:      {Default: "fallback"},
		other:   {Default: "fallback"},
		missing: {Default: "fallback"},
	}, fs, 0)

	fs.seed(store.Scope{}, jsonRow(nk, 1, `"one"`, "ops"))
	e.onEvent(upsertEvent(store.Scope{}, nk, 1))

	if touched, unusable := recordedSets(e, store.Scope{}); len(touched) != 0 || len(unusable) != 0 {
		t.Errorf("outside a reconcile: touched=%v unusable=%v, want both empty", touched, unusable)
	}

	_ = armReconcile(e, store.Scope{})

	fs.seed(store.Scope{}, jsonRow(nk, 2, `"two"`, "ops"))
	e.onEvent(upsertEvent(store.Scope{}, nk, 2))

	// A re-read that errors taught the engine nothing: unusable, never touched.
	fs.seed(store.Scope{}, jsonRow(other, 1, `"other"`, "ops"))
	fs.onGet(func(_ store.Scope, got NSKey) error {
		if got == other {
			return errors.New("backend down")
		}

		return nil
	})

	e.onEvent(upsertEvent(store.Scope{}, other, 1))
	fs.onGet(nil)

	// A re-read that reports not found goes in neither set: the snapshot is
	// still the better answer for that key.
	e.onEvent(upsertEvent(store.Scope{}, missing, 1))

	touched, unusable := recordedSets(e, store.Scope{})
	if len(touched) != 1 || touched[0] != nk {
		t.Errorf("touched: got %v, want [%v]", touched, nk)
	}

	if len(unusable) != 1 || unusable[0] != other {
		t.Errorf("unusable: got %v, want [%v]", unusable, other)
	}
}

func TestDisconnectMarksScopeStaleWithoutPublishing(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	fs.seed(store.Scope{}, jsonRow(nk, 3, `"live"`, "ops"))
	e.onEvent(upsertEvent(store.Scope{}, nk, 3))
	waitFor(t, time.Second, "the upsert delivery", func() bool { return rec.len() == 1 })
	clearStale(e, store.Scope{})

	readsBefore := fs.getCount()

	e.onEvent(store.Event{Scope: store.Scope{}, Op: store.OpDisconnect})

	got, ok := e.Lookup(store.Scope{}, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after a disconnect, want the last published value")
	}

	if !got.Stale {
		t.Error("Stale is false after OpDisconnect, want true")
	}

	if got.Value != "live" || got.Revision != 3 {
		t.Errorf("cached after disconnect: got (%v, rev %d), want (\"live\", rev 3)", got.Value, got.Revision)
	}

	quiesce(t, e)

	if n := rec.len(); n != 1 {
		t.Errorf("deliveries: got %d, want 1: a disconnect publishes nothing", n)
	}

	if after := fs.getCount(); after != readsBefore {
		t.Errorf("disconnect triggered %d store read(s), want 0", after-readsBefore)
	}
}

func TestRepeatedDisconnectIsIdempotent(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	fs.seed(store.Scope{}, jsonRow(nk, 3, `"live"`, "ops"))
	e.onEvent(upsertEvent(store.Scope{}, nk, 3))
	waitFor(t, time.Second, "the upsert delivery", func() bool { return rec.len() == 1 })
	clearStale(e, store.Scope{})

	disconnect := store.Event{Scope: store.Scope{}, Op: store.OpDisconnect}
	e.onEvent(disconnect)
	e.onEvent(disconnect)

	got, ok := e.Lookup(store.Scope{}, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after two disconnects")
	}

	if !got.Stale || got.Value != "live" || got.Revision != 3 {
		t.Errorf("after two disconnects: got (stale %t, %v, rev %d), want (true, \"live\", rev 3)",
			got.Stale, got.Value, got.Revision)
	}

	quiesce(t, e)

	if n := rec.len(); n != 1 {
		t.Errorf("deliveries: got %d, want 1: a repeated disconnect publishes nothing", n)
	}

	// Nothing observable changed, but the generation MUST move on every
	// disconnect: a reconcile that started before the second one and finishes
	// after it compares generations to decide it may not clear Stale.
	sc := e.scopeFor(store.Scope{})

	sc.mu.RLock()
	gen := sc.disconnectGen
	sc.mu.RUnlock()

	if gen != 2 {
		t.Errorf("disconnect generation after two disconnects: got %d, want 2", gen)
	}
}

// blockingSubscriber registers a subscriber of nk that parks inside its first
// delivery until the test ends, and returns a channel closed once it is parked.
// It is how a test holds one key's delivery worker hostage while asserting
// that nothing upstream of it waits.
func blockingSubscriber(t *testing.T, e *Engine, nk NSKey) (parked <-chan struct{}) {
	t.Helper()

	blocked := make(chan struct{})
	release := make(chan struct{})

	var once sync.Once

	unsub := e.OnChange(nk, func(ctx context.Context, _ Change) {
		once.Do(func() { close(blocked) })

		select {
		case <-release:
		case <-ctx.Done():
		}
	})

	// Released before the engine cleanup runs, so a failed assertion never
	// leaves a parked callback for goleak to report as the real problem.
	t.Cleanup(func() {
		close(release)
		unsub()
	})

	return blocked
}

// emitAsync plays evt into the engine from a goroutine of its own and returns
// a channel closed when onEvent returns. Every event in these tests is played
// this way, including the first: a callback that invoked subscribers inline
// would park on the FIRST delivery, and a test that played that one
// synchronously would hang instead of failing.
func emitAsync(e *Engine, evt store.Event) <-chan struct{} {
	returned := make(chan struct{})

	go func() {
		defer close(returned)

		e.onEvent(evt)
	}()

	return returned
}

// mustReturnQuickly fails unless done is closed inside the window. It is the
// assertion that the changefeed goroutine is not inside a subscriber.
func mustReturnQuickly(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("%s did not return within 500ms: the changefeed goroutine is waiting on a subscriber", what)
	}
}

// TestUpsertEventNeverBlocksOnASubscriber pins the rule the whole dispatch
// queue exists for: the callback the backend hands its changefeed goroutine to
// must never wait on consumer code. One slow subscriber would otherwise stall
// the pump for every other key — and, once tenants share a connection, for
// every other tenant.
func TestUpsertEventNeverBlocksOnASubscriber(t *testing.T) {
	keyA := NSKey{Namespace: "billing", Key: "a"}
	keyB := NSKey{Namespace: "billing", Key: "b"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{keyA: {Default: "da"}, keyB: {Default: "db"}}, fs, 0)

	fs.seed(scope, jsonRow(keyA, 1, `"a1"`, "ops"))

	parked := blockingSubscriber(t, e, keyA)

	var recB recorder

	unsubB := e.OnChange(keyB, recB.record)
	defer unsubB()

	first := emitAsync(e, upsertEvent(scope, keyA, 1))
	<-parked
	mustReturnQuickly(t, first, "the changefeed callback that started the blocked delivery")

	fs.seed(scope, jsonRow(keyA, 2, `"a2"`, "ops"))
	mustReturnQuickly(t, emitAsync(e, upsertEvent(scope, keyA, 2)),
		"the changefeed callback for a key whose subscriber is blocked")

	fs.seed(scope, jsonRow(keyB, 1, `"b1"`, "ops"))
	mustReturnQuickly(t, emitAsync(e, upsertEvent(scope, keyB, 1)), "the changefeed callback for another key")

	waitFor(t, 500*time.Millisecond, "key b's delivery while key a's subscriber is blocked", func() bool {
		return recB.len() == 1
	})
}

// TestDeleteEventNeverBlocksOnASubscriber is the same rule for the delete
// path, which reaches publish without passing through the debouncer: a delete
// is self-describing, so it is applied inline on the changefeed goroutine and
// would be the one operation able to park that goroutine in a subscriber.
func TestDeleteEventNeverBlocksOnASubscriber(t *testing.T) {
	keyA := NSKey{Namespace: "billing", Key: "a"}
	keyB := NSKey{Namespace: "billing", Key: "b"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{keyA: {Default: "da"}, keyB: {Default: "db"}}, fs, 0)

	fs.seed(scope, jsonRow(keyA, 1, `"a1"`, "ops"))

	parked := blockingSubscriber(t, e, keyA)

	var recB recorder

	unsubB := e.OnChange(keyB, recB.record)
	defer unsubB()

	first := emitAsync(e, upsertEvent(scope, keyA, 1))
	<-parked
	mustReturnQuickly(t, first, "the changefeed callback that started the blocked delivery")

	mustReturnQuickly(t, emitAsync(e, deleteEvent(scope, keyA)),
		"the changefeed delete callback for a key whose subscriber is blocked")

	mustReturnQuickly(t, emitAsync(e, deleteEvent(scope, keyB)), "the changefeed delete callback for another key")

	waitFor(t, 500*time.Millisecond, "key b's default while key a's subscriber is blocked", func() bool {
		return recB.len() == 1
	})

	if got := recB.changes()[0]; got.Revision != 0 || got.Value != "db" {
		t.Errorf("delete delivery for key b: got (%v, rev %d), want (\"db\", rev 0)", got.Value, got.Revision)
	}
}
