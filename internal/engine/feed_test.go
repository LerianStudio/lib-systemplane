//go:build unit

package engine

import (
	"context"
	"errors"
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

	time.Sleep(50 * time.Millisecond)

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

	time.Sleep(100 * time.Millisecond)

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

	time.Sleep(50 * time.Millisecond)

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

	time.Sleep(50 * time.Millisecond)

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
