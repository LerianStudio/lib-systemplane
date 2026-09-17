//go:build unit

package engine

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

var errList = errors.New("list failed")

func resyncEvent(scope store.Scope) store.Event {
	return store.Event{Scope: scope, Op: store.OpResync}
}

func disconnectEvent(scope store.Scope) store.Event {
	return store.Event{Scope: scope, Op: store.OpDisconnect}
}

// heldList blocks the NEXT List call until the returned release is called, and
// leaves every later List unhooked. It is how a test opens the window the two
// fences exist for: the reconcile is armed and its snapshot is not taken yet,
// so anything the test does in between is concurrent with the photograph.
func heldList(fs *fakeStore) (release func()) {
	gate := make(chan struct{})

	fs.onList(func(store.Scope) error {
		fs.onList(nil)
		<-gate

		return nil
	})

	var once sync.Once

	return func() { once.Do(func() { close(gate) }) }
}

// waitFirstReconcile blocks until the scope's first reconcile has completed
// and reports its outcome, which is exactly what Start will do.
func waitFirstReconcile(t *testing.T, e *Engine, scope store.Scope) error {
	t.Helper()

	sc := e.scopeFor(scope)

	select {
	case <-sc.firstReconcileDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the first reconcile to complete")
	}

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return sc.firstReconcileErr
}

// waitReconcileIdle waits until no reconcile holds the scope's window. Reading
// the flag is synchronization, not an assertion: every claim a test makes
// about the reconcile's effect goes through Lookup.
func waitReconcileIdle(t *testing.T, e *Engine, scope store.Scope) {
	t.Helper()

	sc := e.scopeFor(scope)

	waitFor(t, 2*time.Second, "the reconcile to finish", func() bool {
		sc.reconcileMu.Lock()
		defer sc.reconcileMu.Unlock()

		return !sc.reconciling
	})
}

// settled brings a scope up the way Start will: the first OpResync, its
// reconcile completed, the scope no longer stale.
func settled(t *testing.T, e *Engine, scope store.Scope) {
	t.Helper()

	e.onEvent(resyncEvent(scope))

	if err := waitFirstReconcile(t, e, scope); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
}

// deliveries copies what the recorder has seen so far.
func deliveries(rec *recorder) []Change {
	rec.mu.Lock()
	defer rec.mu.Unlock()

	out := make([]Change, len(rec.got))
	copy(out, rec.got)

	return out
}

func TestReconcileAppliesValueWrittenDuringFeedGap(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 1, `"live"`, "ops"))
	settled(t, e, scope)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	// The feed drops and a writer the engine cannot hear bumps the row.
	e.onEvent(disconnectEvent(scope))
	fs.seed(scope, jsonRow(nk, 2, `"gapped"`, "ops"))

	// The connection comes back. Nothing writes again: the reconcile is what
	// makes revision 2 visible.
	e.onEvent(resyncEvent(scope))
	waitReconcileIdle(t, e, scope)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after the reconcile")
	}

	if got.Value != "gapped" || got.Revision != 2 || got.Stale {
		t.Errorf("after the reconcile: got (%v, rev %d, stale %t), want (\"gapped\", rev 2, stale false)",
			got.Value, got.Revision, got.Stale)
	}

	waitFor(t, time.Second, "the reconcile's delivery", func() bool { return rec.len() == 1 })

	if revs := rec.revisions(); len(revs) != 1 || revs[0] != 2 {
		t.Errorf("deliveries: got %v, want exactly [2]", revs)
	}
}

func TestReconcileSkipsKeyTouchedByFeed(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 1, `"initial"`, "ops"))
	settled(t, e, scope)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	e.onEvent(disconnectEvent(scope))

	release := heldList(fs)
	e.onEvent(resyncEvent(scope))

	// While the snapshot is held the feed publishes revision 5, and the row
	// the snapshot will carry is an older revision 3.
	fs.seed(scope, jsonRow(nk, 5, `"feed"`, "feed"))
	e.onEvent(upsertEvent(scope, nk, 5))
	fs.seed(scope, jsonRow(nk, 3, `"snapshot"`, "list"))

	release()
	waitReconcileIdle(t, e, scope)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after the reconcile")
	}

	if got.Value != "feed" || got.Revision != 5 {
		t.Errorf("after the reconcile: got (%v, rev %d), want (\"feed\", rev 5)", got.Value, got.Revision)
	}

	for _, ch := range deliveries(&rec) {
		if ch.Revision == 3 {
			t.Error("a subscriber saw the stale snapshot revision 3")
		}
	}
}

func TestReconcileKeepsRecreatedValueOverListSnapshot(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 5, `"snapshot"`, "ops"))
	settled(t, e, scope)

	e.onEvent(disconnectEvent(scope))

	release := heldList(fs)
	e.onEvent(resyncEvent(scope))

	// A delete and a recreate reach the feed while the snapshot is held. The
	// recreated row carries revision 1, lower than the snapshot's revision 5,
	// so only the touched fence can keep it.
	e.onEvent(deleteEvent(scope, nk))
	fs.seed(scope, jsonRow(nk, 1, `"recreated"`, "ops"))
	e.onEvent(upsertEvent(scope, nk, 1))
	fs.seed(scope, jsonRow(nk, 5, `"snapshot"`, "ops"))

	release()
	waitReconcileIdle(t, e, scope)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after the reconcile")
	}

	if got.Value != "recreated" || got.Revision != 1 {
		t.Errorf("after the reconcile: got (%v, rev %d), want (\"recreated\", rev 1)", got.Value, got.Revision)
	}
}

func TestReconcileAbsentKeyFallsBackToDefault(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 4, `"live"`, "ops"))
	settled(t, e, scope)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	// The row is gone and nothing told the feed, which is what a reconcile is
	// for.
	fs.remove(scope, nk)
	e.onEvent(disconnectEvent(scope))
	e.onEvent(resyncEvent(scope))
	waitReconcileIdle(t, e, scope)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after the reconcile")
	}

	if got.Value != "fallback" || got.Revision != 0 {
		t.Errorf("after the reconcile: got (%v, rev %d), want (\"fallback\", rev 0)", got.Value, got.Revision)
	}

	waitFor(t, time.Second, "the default delivery", func() bool { return rec.len() == 1 })
}

func TestReconcileAbsentButTouchedKeyKeepsFeedValue(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 4, `"live"`, "ops"))
	settled(t, e, scope)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	e.onEvent(disconnectEvent(scope))

	release := heldList(fs)
	e.onEvent(resyncEvent(scope))

	// The feed publishes revision 7 and the row then leaves the store, so the
	// snapshot carries nothing for this key.
	fs.seed(scope, jsonRow(nk, 7, `"feed"`, "ops"))
	e.onEvent(upsertEvent(scope, nk, 7))
	fs.remove(scope, nk)

	release()
	waitReconcileIdle(t, e, scope)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after the reconcile")
	}

	if got.Value != "feed" || got.Revision != 7 {
		t.Errorf("after the reconcile: got (%v, rev %d), want (\"feed\", rev 7)", got.Value, got.Revision)
	}

	for _, ch := range deliveries(&rec) {
		if ch.Revision == 0 {
			t.Error("the reconcile published the default over a value the feed had just published")
		}
	}
}

func TestReconcileFailureKeepsCacheAndLeavesScopeStale(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}

	t.Run("a later reconcile", func(t *testing.T) {
		fs := newFakeStore()
		e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

		fs.seed(scope, jsonRow(nk, 2, `"live"`, "ops"))
		settled(t, e, scope)

		var rec recorder

		unsub := e.OnChange(nk, rec.record)
		defer unsub()

		fs.onList(func(store.Scope) error { return errList })
		e.onEvent(disconnectEvent(scope))
		e.onEvent(resyncEvent(scope))
		waitReconcileIdle(t, e, scope)

		got, ok := e.Lookup(scope, nk)
		if !ok {
			t.Fatal("Lookup reports a miss after a failed reconcile")
		}

		if got.Value != "live" || got.Revision != 2 {
			t.Errorf("after a failed reconcile: got (%v, rev %d), want the cached (\"live\", rev 2)",
				got.Value, got.Revision)
		}

		if !got.Stale {
			t.Error("Stale is false after a failed reconcile, want true")
		}

		if n := rec.len(); n != 0 {
			t.Errorf("deliveries: got %d, want 0: a failed reconcile publishes nothing", n)
		}
	})

	t.Run("the first reconcile", func(t *testing.T) {
		fs := newFakeStore()
		e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

		fs.onList(func(store.Scope) error { return errList })
		e.onEvent(resyncEvent(scope))

		// The completion path runs on failure too, or a Start waiting on it
		// would block until its context expired.
		err := waitFirstReconcile(t, e, scope)
		if !errors.Is(err, errList) {
			t.Errorf("first reconcile outcome: got %v, want %v", err, errList)
		}
	})
}

func TestStaleIsTrueBetweenDisconnectAndCompletedReconcile(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 1, `"live"`, "ops"))
	settled(t, e, scope)

	assertStale := func(what string, want bool) {
		t.Helper()

		got, ok := e.Lookup(scope, nk)
		if !ok {
			t.Fatalf("%s: Lookup reports a miss", what)
		}

		if got.Stale != want {
			t.Errorf("%s: Stale is %t, want %t", what, got.Stale, want)
		}
	}

	assertStale("after the first reconcile", false)

	e.onEvent(disconnectEvent(scope))
	assertStale("after OpDisconnect", true)

	fs.seed(scope, jsonRow(nk, 2, `"gapped"`, "ops"))
	assertStale("while the store moves behind the engine's back", true)

	release := heldList(fs)
	e.onEvent(resyncEvent(scope))
	assertStale("while the reconcile's List is in flight", true)

	release()
	waitReconcileIdle(t, e, scope)
	assertStale("after the reconcile completed", false)

	got, _ := e.Lookup(scope, nk)
	if got.Value != "gapped" || got.Revision != 2 {
		t.Errorf("after the reconcile: got (%v, rev %d), want (\"gapped\", rev 2)", got.Value, got.Revision)
	}
}

func TestFirstReconcileAnnouncesEveryRegisteredKey(t *testing.T) {
	scope := store.Scope{}
	seeded := NSKey{Namespace: "billing", Key: "limits"}
	absentA := NSKey{Namespace: "billing", Key: "retries"}
	absentB := NSKey{Namespace: "risk", Key: "threshold"}

	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{
		seeded:  {Default: "seeded-default"},
		absentA: {Default: "retries-default"},
		absentB: {Default: 7.0},
	}, fs, 0)

	var rec recorder

	for _, nk := range []NSKey{seeded, absentA, absentB} {
		unsub := e.OnChange(nk, rec.record)
		defer unsub()
	}

	fs.seed(scope, jsonRow(seeded, 4, `"stored"`, "ops"))

	settled(t, e, scope)

	countByKey := func() map[NSKey]int {
		counts := map[NSKey]int{}
		for _, ch := range deliveries(&rec) {
			counts[NSKey{Namespace: ch.Namespace, Key: ch.Key}]++
		}

		return counts
	}

	waitFor(t, 2*time.Second, "one delivery per registered key", func() bool {
		return rec.len() == 3
	})

	for nk, want := range map[NSKey]int{seeded: 1, absentA: 1, absentB: 1} {
		if got := countByKey()[nk]; got != want {
			t.Errorf("deliveries for %s/%s: got %d, want %d", nk.Namespace, nk.Key, got, want)
		}
	}

	for _, ch := range deliveries(&rec) {
		switch (NSKey{Namespace: ch.Namespace, Key: ch.Key}) {
		case seeded:
			if ch.Revision != 4 || ch.Value != "stored" {
				t.Errorf("seeded key announced as (%v, rev %d), want (\"stored\", rev 4)", ch.Value, ch.Revision)
			}
		case absentA:
			if ch.Revision != 0 || ch.Value != "retries-default" {
				t.Errorf("absent key announced as (%v, rev %d), want (\"retries-default\", rev 0)", ch.Value, ch.Revision)
			}
		case absentB:
			if ch.Revision != 0 || ch.Value != 7.0 {
				t.Errorf("absent key announced as (%v, rev %d), want (7, rev 0)", ch.Value, ch.Revision)
			}
		}
	}

	// A second reconnect with nothing changed announces nothing: every key is
	// already at the revision the store reports.
	e.onEvent(disconnectEvent(scope))
	e.onEvent(resyncEvent(scope))
	waitReconcileIdle(t, e, scope)

	time.Sleep(50 * time.Millisecond)

	for nk, want := range map[NSKey]int{seeded: 1, absentA: 1, absentB: 1} {
		if got := countByKey()[nk]; got != want {
			t.Errorf("after a second resync, deliveries for %s/%s: got %d, want %d",
				nk.Namespace, nk.Key, got, want)
		}
	}
}

func TestReconcileKeepsCachedValueWhenRereadWasUnusable(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}

	cases := map[string]struct {
		def     KeyDef
		arrange func(fs *fakeStore)
	}{
		"the reread errors": {
			def: KeyDef{Default: "fallback"},
			arrange: func(fs *fakeStore) {
				fs.onGet(func(store.Scope, NSKey) error { return errors.New("read failed") })
			},
		},
		"the reread is rejected by the validator": {
			def: KeyDef{
				Default: "fallback",
				Validate: func(v any) error {
					if v == "poison" {
						return errors.New("rejected")
					}

					return nil
				},
			},
			arrange: func(fs *fakeStore) {
				fs.seed(scope, jsonRow(nk, 3, `"poison"`, "ops"))
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fs := newFakeStore()
			e := feedEngine(t, map[NSKey]KeyDef{nk: tc.def}, fs, 0)

			fs.seed(scope, jsonRow(nk, 2, `"live"`, "ops"))
			settled(t, e, scope)

			var rec recorder

			unsub := e.OnChange(nk, rec.record)
			defer unsub()

			e.onEvent(disconnectEvent(scope))

			release := heldList(fs)
			e.onEvent(resyncEvent(scope))

			// The feed hears about the key but learns nothing usable, and the
			// snapshot happens not to carry the row.
			tc.arrange(fs)
			e.onEvent(upsertEvent(scope, nk, 3))
			fs.onGet(nil)
			fs.remove(scope, nk)

			release()
			waitReconcileIdle(t, e, scope)

			got, ok := e.Lookup(scope, nk)
			if !ok {
				t.Fatal("Lookup reports a miss: the reconcile erased the cache")
			}

			if got.Value != "live" || got.Revision != 2 {
				t.Errorf("after the reconcile: got (%v, rev %d), want the cached (\"live\", rev 2)",
					got.Value, got.Revision)
			}

			if n := rec.len(); n != 0 {
				t.Errorf("deliveries: got %d, want 0: nothing usable was learned", n)
			}
		})
	}
}

func TestReconcileUnusableKeyWithEmptyCacheStillGetsDefault(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	release := heldList(fs)
	e.onEvent(resyncEvent(scope))

	// Nothing is cached for the key and the feed's reread fails, so the
	// reconcile has nothing to protect: the registered default is announced.
	fs.onGet(func(store.Scope, NSKey) error { return errors.New("read failed") })
	e.onEvent(upsertEvent(scope, nk, 3))
	fs.onGet(nil)

	release()

	if err := waitFirstReconcile(t, e, scope); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss: the registered default was never announced")
	}

	if got.Value != "fallback" || got.Revision != 0 {
		t.Errorf("after the reconcile: got (%v, rev %d), want (\"fallback\", rev 0)", got.Value, got.Revision)
	}

	waitFor(t, time.Second, "the default delivery", func() bool { return rec.len() == 1 })
}

func TestReconcileUnusableKeyStillAcceptsListRow(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 2, `"live"`, "ops"))
	settled(t, e, scope)

	e.onEvent(disconnectEvent(scope))

	release := heldList(fs)
	e.onEvent(resyncEvent(scope))

	// The feed's reread fails, so the key is unusable — but the snapshot does
	// carry a row for it, and unusable must not suppress that.
	fs.onGet(func(store.Scope, NSKey) error { return errors.New("read failed") })
	e.onEvent(upsertEvent(scope, nk, 9))
	fs.onGet(nil)
	fs.seed(scope, jsonRow(nk, 9, `"snapshot"`, "ops"))

	release()
	waitReconcileIdle(t, e, scope)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after the reconcile")
	}

	if got.Value != "snapshot" || got.Revision != 9 {
		t.Errorf("after the reconcile: got (%v, rev %d), want the snapshot (\"snapshot\", rev 9)",
			got.Value, got.Revision)
	}
}

func TestStaleSurvivesDisconnectDuringReconcile(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 1, `"live"`, "ops"))
	settled(t, e, scope)

	e.onEvent(disconnectEvent(scope))

	release := heldList(fs)
	e.onEvent(resyncEvent(scope))

	// The connection drops again while the reconcile is in flight: whatever
	// the snapshot carries may already be behind.
	fs.seed(scope, jsonRow(nk, 2, `"applied"`, "ops"))
	e.onEvent(disconnectEvent(scope))

	release()

	waitFor(t, 2*time.Second, "the reconcile to apply its snapshot", func() bool {
		got, ok := e.Lookup(scope, nk)

		return ok && got.Revision == 2
	})

	got, _ := e.Lookup(scope, nk)
	if !got.Stale {
		t.Error("Stale is false after a reconcile that spanned a disconnect, want true")
	}

	// The new connection's own reconcile is what clears it.
	e.onEvent(resyncEvent(scope))
	waitReconcileIdle(t, e, scope)

	got, _ = e.Lookup(scope, nk)
	if got.Stale {
		t.Error("Stale is true after the new connection reconciled, want false")
	}
}

func TestReconcileClearsStaleOnSuccess(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 1, `"live"`, "ops"))
	settled(t, e, scope)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after the first reconcile")
	}

	if got.Stale {
		t.Error("Stale is true after a successful reconcile, want false")
	}
}
