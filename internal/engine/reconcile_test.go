//go:build unit

package engine

import (
	"context"
	"errors"
	"strconv"
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
	case <-time.After(hangGuard):
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

	waitFor(t, hangGuard, "the reconcile to finish", func() bool {
		sc.reconcileMu.Lock()
		defer sc.reconcileMu.Unlock()

		return len(sc.windows) == 0
	})
}

// settled brings a scope up the way Start will: the first OpResync, its
// reconcile completed, the scope no longer stale.
func settled(t *testing.T, e *Engine, scope store.Scope) {
	t.Helper()

	bringUp(t, e, scope)
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

	quiesce(t, e)

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

func TestOverlappingReconcilesKeepFeedValue(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	settled(t, e, scope)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	// Two Lists, each held: the older reconcile's is released first, so if it
	// applies its snapshot at all it does so while the newer one is still
	// waiting for its own photograph. Nothing here is a timing accident.
	first, second := make(chan struct{}), make(chan struct{})

	fs.onList(func(store.Scope) error {
		fs.onList(func(store.Scope) error {
			fs.onList(nil)
			<-second

			return nil
		})
		<-first

		return nil
	})

	// The older reconcile photographs a store in which the key does not exist.
	fs.freezeNextList(nil)

	e.onEvent(disconnectEvent(scope))
	e.onEvent(resyncEvent(scope))

	// The world moves on behind that photograph: the row is created and the
	// reconnected feed publishes it.
	fs.seed(scope, jsonRow(nk, 9, `"live"`, "ops"))
	e.onEvent(upsertEvent(scope, nk, 9))

	waitFor(t, time.Second, "the feed publication", func() bool { return rec.len() == 1 })

	// The feed drops and comes back, so a second reconcile owns the scope from
	// here on. The first one is now holding a photograph of a dead connection.
	e.onEvent(disconnectEvent(scope))
	e.onEvent(resyncEvent(scope))

	close(first)
	close(second)

	waitReconcileIdle(t, e, scope)

	// A superseded reconcile has had every chance to publish its stale
	// snapshot by the time the engine is quiet again.
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss: a superseded reconcile erased the cache")
	}

	if got.Value != "live" || got.Revision != 9 {
		t.Errorf("after two overlapping reconciles: got (%v, rev %d), want (\"live\", rev 9): "+
			"the older snapshot reset the key to its registered default", got.Value, got.Revision)
	}

	for _, ch := range deliveries(&rec) {
		if ch.Revision == 0 {
			t.Fatalf("a Revision 0 default was delivered: deliveries = %v", rec.revisions())
		}
	}

	if n := rec.len(); n != 1 {
		t.Errorf("deliveries: got %d (%v), want 1: only the feed's revision 9", n, rec.revisions())
	}
}

func TestFirstReconcileRejectsInvalidSnapshotRow(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	def := KeyDef{
		Default: "fallback",
		Validate: func(v any) error {
			if _, ok := v.(string); !ok {
				return errors.New("want a string")
			}

			return nil
		},
	}

	t.Run("nothing cached yet", func(t *testing.T) {
		fs := newFakeStore()
		e := feedEngine(t, map[NSKey]KeyDef{nk: def}, fs, 0)

		var rec recorder

		unsub := e.OnChange(nk, rec.record)
		defer unsub()

		// The key's only row is one an operator hand-edited to the wrong type.
		fs.seed(scope, jsonRow(nk, 7, `{"limit":10}`, "operator"))
		settled(t, e, scope)

		waitFor(t, time.Second, "the first-reconcile announcement", func() bool {
			return rec.len() == 1
		})

		// FC-11 as amended: with nothing cached, a row the ingress refuses on
		// the FIRST reconcile is announced exactly like an absent row — the
		// registered default at Revision 0 — so reads serve the default
		// instead of a miss. The rejection itself was logged at WARN.
		got, ok := e.Lookup(scope, nk)
		if !ok {
			t.Fatal("Lookup reports a miss: the key was never announced")
		}

		if got.Value != "fallback" || got.Revision != 0 {
			t.Errorf("announced: got (%v, rev %d), want (\"fallback\", rev 0)", got.Value, got.Revision)
		}

		if delivered := deliveries(&rec); len(delivered) != 1 ||
			delivered[0].Value != "fallback" || delivered[0].Revision != 0 {
			t.Errorf("deliveries: got %v, want one carrying the default at rev 0", delivered)
		}

		// The row stays rejected, so a later reconcile announces nothing: the
		// value in force stays in force (D-G4).
		e.onEvent(resyncEvent(scope))
		waitReconcileIdle(t, e, scope)
		quiesce(t, e)

		if n := rec.len(); n != 1 {
			t.Errorf("deliveries after a second reconcile: got %d (%v), want 1", n, rec.revisions())
		}
	})

	t.Run("a valid value is already cached", func(t *testing.T) {
		fs := newFakeStore()
		e := feedEngine(t, map[NSKey]KeyDef{nk: def}, fs, 0)

		fs.seed(scope, jsonRow(nk, 2, `"live"`, "ops"))
		settled(t, e, scope)

		var rec recorder

		unsub := e.OnChange(nk, rec.record)
		defer unsub()

		// The row is rewritten out from under the engine while the feed is
		// down, so only the reconcile's snapshot carries it.
		e.onEvent(disconnectEvent(scope))
		fs.seed(scope, jsonRow(nk, 3, `{"limit":10}`, "operator"))
		e.onEvent(resyncEvent(scope))

		waitReconcileIdle(t, e, scope)
		quiesce(t, e)

		got, ok := e.Lookup(scope, nk)
		if !ok {
			t.Fatal("Lookup reports a miss: the rejected snapshot row erased the cache")
		}

		if got.Value != "live" || got.Revision != 2 {
			t.Errorf("after the reconcile: got (%v, rev %d), want the cached (\"live\", rev 2)",
				got.Value, got.Revision)
		}

		if n := rec.len(); n != 0 {
			t.Errorf("deliveries: got %d (%v), want 0", n, rec.revisions())
		}
	})
}

// TestRejectedRowIsAnnouncedAfterATransientListFailure pins FC-11 against the
// first reconcile that actually SEES the row, not against the first one that
// ran.
//
// A backend can be up long enough to connect and gone before the snapshot, and
// that failed reconcile is still the one Start waited on: it completes, with
// an error. Gating the announcement on "no reconcile has completed yet" then
// silenced it for the life of the process — the key stayed uncached, every
// read reported a miss, and no subscriber ever heard the default that is in
// force for it.
func TestRejectedRowIsAnnouncedAfterATransientListFailure(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {
		Default: "fallback",
		Validate: func(v any) error {
			if _, ok := v.(string); !ok {
				return errors.New("want a string")
			}

			return nil
		},
	}}, fs, 0)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	// The key's only row is one an operator hand-edited to the wrong type.
	fs.seed(scope, jsonRow(nk, 7, `{"limit":10}`, "operator"))

	// The reconcile Start waits on never gets its photograph.
	fs.onList(func(store.Scope) error { return errList })
	bringUp(t, e, scope)
	e.onEvent(resyncEvent(scope))

	if err := waitFirstReconcile(t, e, scope); !errors.Is(err, errList) {
		t.Fatalf("first reconcile: got %v, want one wrapping %v", err, errList)
	}

	// The feed reconnects, and this reconcile does see the row.
	fs.onList(nil)
	e.onEvent(resyncEvent(scope))
	waitReconcileIdle(t, e, scope)

	waitFor(t, time.Second, "the rejected row's announcement", func() bool {
		return rec.len() == 1
	})

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss: the rejected row was never announced")
	}

	if got.Value != "fallback" || got.Revision != 0 {
		t.Errorf("announced: got (%v, rev %d), want (\"fallback\", rev 0)", got.Value, got.Revision)
	}

	// The row stays rejected, so the announcement does not repeat: the default
	// is cached by now, and the value in force stays in force (D-G4).
	e.onEvent(resyncEvent(scope))
	waitReconcileIdle(t, e, scope)
	quiesce(t, e)

	if n := rec.len(); n != 1 {
		t.Errorf("deliveries over the same rejected row: got %d (%v), want 1", n, rec.revisions())
	}

	// A row the validator accepts publishes over the announced default.
	fs.seed(scope, jsonRow(nk, 9, `"live"`, "ops"))
	e.onEvent(resyncEvent(scope))
	waitReconcileIdle(t, e, scope)

	waitFor(t, time.Second, "the repaired row's delivery", func() bool {
		return rec.len() == 2
	})

	if repaired, _ := e.Lookup(scope, nk); repaired.Value != "live" || repaired.Revision != 9 {
		t.Errorf("after the repair: got (%v, rev %d), want (\"live\", rev 9)",
			repaired.Value, repaired.Revision)
	}
}

func TestReconcileSkipsUndecodableSnapshotRow(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 2, `"live"`, "ops"))
	settled(t, e, scope)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	e.onEvent(disconnectEvent(scope))
	fs.seed(scope, jsonRow(nk, 4, `{not json`, "operator"))
	e.onEvent(resyncEvent(scope))

	waitReconcileIdle(t, e, scope)
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss: an undecodable snapshot row erased the cache")
	}

	if got.Value != "live" || got.Revision != 2 {
		t.Errorf("after the reconcile: got (%v, rev %d), want the cached (\"live\", rev 2)",
			got.Value, got.Revision)
	}

	if n := rec.len(); n != 0 {
		t.Errorf("deliveries: got %d (%v), want 0", n, rec.revisions())
	}
}

func TestReconcileSurvivesPanickingValidator(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()

	// The validator every consumer writes: an unchecked type assertion. It
	// panics on a row an operator hand-edited to the wrong JSON type, and v4
	// runs it on the engine's own reconcile goroutine.
	def := KeyDef{
		Default:  "fallback",
		Validate: func(v any) error { _ = v.(string); return nil },
	}

	e := feedEngine(t, map[NSKey]KeyDef{nk: def}, fs, 0)

	fs.seed(scope, jsonRow(nk, 2, `"live"`, "ops"))
	settled(t, e, scope)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	e.onEvent(disconnectEvent(scope))
	fs.seed(scope, jsonRow(nk, 5, `{"limit":10}`, "operator"))
	e.onEvent(resyncEvent(scope))

	waitReconcileIdle(t, e, scope)
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after a panicking validator")
	}

	if got.Value != "live" || got.Revision != 2 {
		t.Errorf("after the reconcile: got (%v, rev %d), want the cached (\"live\", rev 2): "+
			"a panicking validator must reject, not erase", got.Value, got.Revision)
	}

	if n := rec.len(); n != 0 {
		t.Errorf("deliveries: got %d (%v), want 0", n, rec.revisions())
	}
}

// gate returns a channel a store or registry hook can block on and the
// idempotent release that opens it, so a test can always unblock in a defer
// without risking a double close.
func gate() (ch chan struct{}, release func()) {
	ch = make(chan struct{})

	var once sync.Once

	return ch, func() { once.Do(func() { close(ch) }) }
}

// hookedRegistry wraps a Registry with the two seams the concurrency tests
// need: onKeys fires when a reconcile asks which keys are registered, onLookup
// when one value is about to be decided. Both hooks are swappable under a
// mutex, so a test installs them only after the first reconcile has settled.
type hookedRegistry struct {
	Registry

	mu       sync.Mutex
	onKeys   func()
	onLookup func(NSKey)
}

func (r *hookedRegistry) hookKeys(fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.onKeys = fn
}

func (r *hookedRegistry) hookLookup(fn func(NSKey)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.onLookup = fn
}

func (r *hookedRegistry) Keys() []NSKey {
	r.mu.Lock()
	fn := r.onKeys
	r.mu.Unlock()

	if fn != nil {
		fn()
	}

	return r.Registry.Keys()
}

func (r *hookedRegistry) Lookup(namespace, key string) (KeyDef, bool) {
	r.mu.Lock()
	fn := r.onLookup
	r.mu.Unlock()

	if fn != nil {
		fn(NSKey{Namespace: namespace, Key: key})
	}

	return r.Registry.Lookup(namespace, key)
}

// pauseOnce returns a lookup hook that opens the gate and then holds the
// reconcile at its decision point long enough for a feed event to reach the
// engine. It fires for nk only, and only for the first caller: the gate
// channel itself is the guard, never a sync.Once, because Once.Do makes every
// later caller WAIT for the first one — which would serialize the very two
// goroutines the test needs to interleave.
func pauseOnce(nk NSKey, opened <-chan struct{}, open func(), hold time.Duration) func(NSKey) {
	return func(got NSKey) {
		if got != nk {
			return
		}

		select {
		case <-opened:
			return
		default:
		}

		open()
		time.Sleep(hold)
	}
}

// TestOverlappingReconcilesApplyFresherListRow is the RED test for fences
// shared between reconcile windows. The feed publishes revision 3 inside the
// first window; the store then moves to revision 9 during a second outage. A
// second window that inherits the first one's touched set skips its own,
// fresher snapshot row and leaves the cache three revisions behind, reporting
// Stale false while it does so.
func TestOverlappingReconcilesApplyFresherListRow(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	settled(t, e, scope)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	first, releaseFirst := gate()
	second, releaseSecond := gate()

	defer releaseSecond()
	defer releaseFirst()

	fs.onList(func(store.Scope) error {
		fs.onList(func(store.Scope) error {
			fs.onList(nil)
			<-second

			return nil
		})
		<-first

		return nil
	})

	// The first window photographs a store in which the key does not exist.
	fs.freezeNextList(nil)

	e.onEvent(disconnectEvent(scope))
	e.onEvent(resyncEvent(scope))

	// The feed speaks inside the first window: that is what fills its fence.
	fs.seed(scope, jsonRow(nk, 3, `"three"`, "ops"))
	e.onEvent(upsertEvent(scope, nk, 3))

	waitFor(t, time.Second, "the feed publication", func() bool { return rec.len() == 1 })

	// A second outage, and the store moves on behind the engine's back. The
	// second window's own List is the only thing that can see revision 9.
	e.onEvent(disconnectEvent(scope))
	e.onEvent(resyncEvent(scope))

	fs.seed(scope, jsonRow(nk, 9, `"nine"`, "ops"))

	releaseFirst()
	releaseSecond()

	waitReconcileIdle(t, e, scope)
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after two overlapping reconciles")
	}

	if got.Value != "nine" || got.Revision != 9 {
		t.Errorf("after the second reconcile: got (%v, rev %d), want (\"nine\", rev 9): "+
			"the second window inherited the first one's touched set and skipped its own fresher row",
			got.Value, got.Revision)
	}

	if got.Stale {
		t.Error("the scope reports Stale true after a completed reconcile of the live connection")
	}
}

// TestPublishRecordsItsKeyAgainstAConcurrentReconcile is the RED test for the
// Set ingress. Publish is the only way into a scope's cache that never told a
// reconcile in flight that it had spoken, so a reconcile whose snapshot
// predates the write publishes the registered default at revision 0 over the
// value the caller just wrote — and revision 0 always wins the fence.
func TestPublishRecordsItsKeyAgainstAConcurrentReconcile(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	settled(t, e, scope)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	release := heldList(fs)
	defer release()

	// The photograph has no row for the key: it was taken before the write.
	fs.freezeNextList(nil)

	e.onEvent(disconnectEvent(scope))
	e.onEvent(resyncEvent(scope))

	waitFor(t, time.Second, "the reconcile to reach its List", func() bool { return fs.listCount() >= 2 })

	// What Client.Set does: persist, then publish to the caller's own scope.
	row := jsonRow(nk, 5, `"written"`, "ops")
	fs.seed(scope, row)
	e.Publish(context.Background(), scope, row)

	release()

	waitReconcileIdle(t, e, scope)
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss: the reconcile erased a value Set had just written")
	}

	if got.Value != "written" || got.Revision != 5 {
		t.Errorf("after the reconcile: got (%v, rev %d), want (\"written\", rev 5): "+
			"the snapshot published the registered default over the caller's own write",
			got.Value, got.Revision)
	}

	for _, ch := range deliveries(&rec) {
		if ch.Revision == 0 {
			t.Fatalf("a Revision 0 default was delivered over the write: deliveries = %v", rec.revisions())
		}
	}
}

// TestFeedDeleteDuringReconcileIsNotResurrected is the unconditional half of
// the delete fence: no registry hook and no decision-point gate, just an
// operator's delete landing while a reconcile's List is held open. The
// photograph still carries the row, and the only thing standing between it and
// the cache is the feed recording the delete against every open window — the
// snapshot row is NEWER than the revision-0 default a delete publishes, so the
// revision fence alone lets it back in.
func TestFeedDeleteDuringReconcileIsNotResurrected(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 5, `"five"`, "ops"))
	settled(t, e, scope)

	release := heldList(fs)
	defer release()

	// The photograph carries the row; the live store is about to stop doing so.
	fs.freezeNextList([]store.Entry{jsonRow(nk, 5, `"five"`, "ops")})

	e.onEvent(disconnectEvent(scope))
	e.onEvent(resyncEvent(scope))

	waitFor(t, hangGuard, "the reconcile to reach its List", func() bool { return fs.listCount() >= 2 })

	fs.remove(scope, nk)
	e.onEvent(deleteEvent(scope, nk))

	release()
	waitReconcileIdle(t, e, scope)
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after a delete that spanned a reconcile")
	}

	if got.Value != "fallback" || got.Revision != 0 {
		t.Errorf("after the delete: got (%v, rev %d), want the registered default at rev 0: "+
			"the reconcile's snapshot resurrected the key an operator deleted", got.Value, got.Revision)
	}
}

// TestSupersededReconcileClosesItsWindow pins which exit releases a reconcile's
// fences. A reconcile that finds itself superseded publishes nothing, and the
// temptation is to let it return before the cleanup — it changed nothing, after
// all. But the window it armed is still registered on the scope, and the feed
// goes on writing every event into it for the life of the scope: a leak that
// grows with every reconnect and silently teaches later reconciles to skip keys
// no live reconcile ever asked about.
func TestSupersededReconcileClosesItsWindow(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, newFakeStore(), 0)

	sc := e.scopeFor(scope)

	stale := sc.beginReconcile()
	newer := sc.beginReconcile()

	// The newer reconcile finishes first, so the only window left is the one
	// the superseded reconcile has to release itself.
	sc.closeWindow(newer)

	if got := openWindows(e, scope); got != 1 {
		t.Fatalf("windows open before the superseded reconcile ran: got %d, want 1", got)
	}

	e.reconcileScope(sc, stale)

	if got := openWindows(e, scope); got != 0 {
		t.Errorf("windows open after a superseded reconcile returned: got %d, want 0: "+
			"its fences keep collecting every feed event for the life of the scope", got)
	}
}

// TestConcurrentFeedDeleteIsNotResurrected is the RED test for the atomicity
// of the feed's publish-and-record pair. The reconcile reads the fence, finds
// it empty, and is held at its decision point while the feed publishes a
// delete and records it. The snapshot row then lands on top and brings the
// deleted key back.
func TestConcurrentFeedDeleteIsNotResurrected(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	reg := &hookedRegistry{Registry: fakeRegistry{defs: map[NSKey]KeyDef{nk: {Default: "fallback"}}}}
	e := registryEngine(t, reg, fs, 0)

	fs.seed(scope, jsonRow(nk, 5, `"five"`, "ops"))
	settled(t, e, scope)

	release := heldList(fs)
	defer release()

	// The photograph still carries the row; the live store no longer does.
	fs.freezeNextList([]store.Entry{jsonRow(nk, 5, `"five"`, "ops")})

	e.onEvent(disconnectEvent(scope))
	e.onEvent(resyncEvent(scope))

	waitFor(t, time.Second, "the reconcile to reach its List", func() bool { return fs.listCount() >= 2 })

	fs.remove(scope, nk)

	atDecision, openDecision := gate()
	defer openDecision()

	reg.hookLookup(pauseOnce(nk, atDecision, openDecision, 20*time.Millisecond))

	deleted := make(chan struct{})

	go func() {
		defer close(deleted)

		<-atDecision

		e.onEvent(deleteEvent(scope, nk))
	}()

	release()

	<-deleted
	waitReconcileIdle(t, e, scope)
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after a delete concurrent with a reconcile")
	}

	if got.Value != "fallback" || got.Revision != 0 {
		t.Errorf("after the delete: got (%v, rev %d), want the registered default at rev 0: "+
			"the reconcile's snapshot resurrected the deleted row", got.Value, got.Revision)
	}
}

// TestConcurrentFeedUpsertSurvivesReconcileDefault is the RED test for the
// other half of the same pair. The reconcile decides a key absent from its
// snapshot while the feed publishes a fresh revision for it; the registered
// default at revision 0 then lands on top, and revision 0 never loses the
// fence.
func TestConcurrentFeedUpsertSurvivesReconcileDefault(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	reg := &hookedRegistry{Registry: fakeRegistry{defs: map[NSKey]KeyDef{nk: {Default: "fallback"}}}}
	e := registryEngine(t, reg, fs, 0)

	fs.seed(scope, jsonRow(nk, 2, `"two"`, "ops"))
	settled(t, e, scope)

	release := heldList(fs)
	defer release()

	// The photograph has no row for the key at all.
	fs.freezeNextList(nil)

	e.onEvent(disconnectEvent(scope))
	e.onEvent(resyncEvent(scope))

	waitFor(t, time.Second, "the reconcile to reach its List", func() bool { return fs.listCount() >= 2 })

	fs.seed(scope, jsonRow(nk, 7, `"seven"`, "ops"))

	atDecision, openDecision := gate()
	defer openDecision()

	reg.hookLookup(pauseOnce(nk, atDecision, openDecision, 20*time.Millisecond))

	published := make(chan struct{})

	go func() {
		defer close(published)

		<-atDecision

		e.onEvent(upsertEvent(scope, nk, 7))
	}()

	release()

	<-published
	waitReconcileIdle(t, e, scope)
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after an upsert concurrent with a reconcile")
	}

	if got.Value != "seven" || got.Revision != 7 {
		t.Errorf("after the upsert: got (%v, rev %d), want (\"seven\", rev 7): "+
			"the reconcile published the registered default over a value the feed had just published",
			got.Value, got.Revision)
	}
}

// TestSupersededWindowStopsApplyingItsSnapshot is the RED test for the per-row
// window fence of step 3. A reconcile whose connection dropped part-way
// through applying its photograph is holding rows from a dead connection:
// every one it has not applied yet must be abandoned, or the cache ends up
// carrying a revision the live store does not have and the fence then rejects
// the truth that follows.
//
// The window moves BETWEEN two rows of one snapshot, a gap the engine holds
// its own lock across, so it is exercised directly rather than raced for: a
// test that tried to win that race would be telling the truth only sometimes.
func TestSupersededWindowStopsApplyingItsSnapshot(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	sc := e.scopeFor(scope)
	ctx := e.dispatchContext()

	// The control: a window that still owns the scope applies its own row.
	current := sc.beginReconcile()

	if superseded := e.applySnapshotRow(ctx, sc, current, jsonRow(nk, 7, `"seven"`, "ops")); superseded {
		t.Fatal("the window that owns the scope reported itself superseded")
	}

	got, ok := e.Lookup(scope, nk)
	if !ok || got.Revision != 7 {
		t.Fatalf("the owning window did not apply its own row: got (%v, rev %d), ok=%v", got.Value, got.Revision, ok)
	}

	sc.closeWindow(current)

	// A newer OpResync takes the scope while the older window still holds
	// rows it has not applied.
	stale := sc.beginReconcile()
	newer := sc.beginReconcile()

	defer sc.closeWindow(newer)
	defer sc.closeWindow(stale)

	if superseded := e.applySnapshotRow(ctx, sc, stale, jsonRow(nk, 9, `"nine"`, "ops")); !superseded {
		t.Error("a superseded window went on applying its photograph")
	}

	got, _ = e.Lookup(scope, nk)
	if got.Value != "seven" || got.Revision != 7 {
		t.Errorf("after the superseded row: got (%v, rev %d), want (\"seven\", rev 7): "+
			"a photograph of a dropped connection was published", got.Value, got.Revision)
	}
}

// TestSupersededReconcileDoesNotDefaultAbsentKeys is the step-4 twin of the
// test above. The absent-key publication is the one no revision fence can undo
// — revision 0 always wins — so a window that has moved must stop before it.
func TestSupersededReconcileDoesNotDefaultAbsentKeys(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	reg := &hookedRegistry{Registry: fakeRegistry{defs: map[NSKey]KeyDef{nk: {Default: "fallback"}}}}
	e := registryEngine(t, reg, fs, 0)

	fs.seed(scope, jsonRow(nk, 4, `"four"`, "ops"))
	settled(t, e, scope)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	// The photograph has no row, so the key is decided by the absent-key loop.
	fs.freezeNextList(nil)

	var once sync.Once

	reg.hookKeys(func() {
		once.Do(func() {
			e.onEvent(disconnectEvent(scope))
			e.onEvent(resyncEvent(scope))
		})
	})

	e.onEvent(disconnectEvent(scope))
	e.onEvent(resyncEvent(scope))

	waitReconcileIdle(t, e, scope)
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss: a superseded reconcile erased the cache")
	}

	if got.Value != "four" || got.Revision != 4 {
		t.Errorf("after the superseded reconcile: got (%v, rev %d), want (\"four\", rev 4): "+
			"a superseded window published the registered default over a live value",
			got.Value, got.Revision)
	}

	if n := rec.len(); n != 0 {
		t.Errorf("deliveries: got %d (%v), want 0", n, rec.revisions())
	}
}

// TestQueuedReconcileAbandonsBeforeListing asserts a reconcile superseded
// while it waits its turn never reaches the store at all. Listing for a
// connection that has already dropped costs a full scope read and produces a
// photograph nothing may apply.
func TestQueuedReconcileAbandonsBeforeListing(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 1, `"one"`, "ops"))
	settled(t, e, scope)

	release := heldList(fs)
	defer release()

	// The first reconcile holds the scope; the next two queue behind it.
	e.onEvent(resyncEvent(scope))
	waitFor(t, time.Second, "the reconcile to reach its List", func() bool { return fs.listCount() == 2 })

	e.onEvent(resyncEvent(scope))
	e.onEvent(resyncEvent(scope))

	release()

	waitReconcileIdle(t, e, scope)
	quiesce(t, e)

	// One List for the first reconcile, one for the newest. The middle one was
	// superseded before it ran and must never have asked the store.
	if n := fs.listCount(); n != 3 {
		t.Errorf("List calls: got %d, want 3: a superseded reconcile listed the scope anyway", n)
	}
}

// TestFailedReconcileLeavesTheNewerWindowArmed asserts a reconcile whose List
// fails disarms nobody but itself. Clearing the shared fence on the way out
// would leave the newer window blind to everything the feed published while it
// was open, and its snapshot would then overwrite those publications.
func TestFailedReconcileLeavesTheNewerWindowArmed(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	settled(t, e, scope)

	failing, releaseFailing := gate()
	newer, releaseNewer := gate()

	defer releaseNewer()
	defer releaseFailing()

	fs.onList(func(store.Scope) error {
		fs.onList(func(store.Scope) error {
			fs.onList(nil)
			<-newer

			return nil
		})
		<-failing

		return errList
	})

	e.onEvent(disconnectEvent(scope))
	e.onEvent(resyncEvent(scope))

	waitFor(t, time.Second, "the failing reconcile to reach its List", func() bool { return fs.listCount() == 2 })

	// A newer window opens while the first reconcile is still inside its List.
	e.onEvent(disconnectEvent(scope))
	e.onEvent(resyncEvent(scope))

	// The feed speaks: both open windows record it.
	fs.seed(scope, jsonRow(nk, 6, `"six"`, "ops"))
	e.onEvent(upsertEvent(scope, nk, 6))

	releaseFailing()

	// Once the newer reconcile is inside its own List, the failed one is gone
	// and whatever is still recorded belongs to the newer window alone.
	waitFor(t, time.Second, "the newer reconcile to reach its List", func() bool { return fs.listCount() == 3 })

	touched, _ := recordedSets(e, scope)
	if len(touched) != 1 || touched[0] != nk {
		t.Errorf("fences of the newer window: touched = %v, want [%v]: "+
			"the failed reconcile disarmed a window it did not own", touched, nk)
	}

	releaseNewer()
	waitReconcileIdle(t, e, scope)
}

// TestPublishRecordsARejectedWriteAgainstAConcurrentReconcile is the RED test
// for the asymmetry between the two halves of the ingress.
//
// The changefeed's re-read records BOTH outcomes: a value it published, and a
// row it could not use. Set recorded only the first, so a write whose stored
// value the registered validator rejects left both fences empty — and a
// reconcile holding a photograph taken before that write then found the key
// absent, concluded the row was gone, and published the registered default at
// revision 0 over the value that was cached. One transient rejection, one
// silent config reset, announced to every subscriber.
func TestPublishRecordsARejectedWriteAgainstAConcurrentReconcile(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {
		Default: "fallback",
		Validate: func(v any) error {
			if _, ok := v.(string); !ok {
				return errors.New("want a string")
			}

			return nil
		},
	}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 3, `"cached"`, "ops"))
	settled(t, e, scope)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	release := heldList(fs)
	defer release()

	// The photograph is taken before the write: it carries no row for the key.
	fs.freezeNextList(nil)

	e.onEvent(disconnectEvent(scope))
	e.onEvent(resyncEvent(scope))

	waitFor(t, time.Second, "the reconcile to reach its List", func() bool { return fs.listCount() >= 2 })

	// A Set whose persisted value the registered validator refuses: the row is
	// in the store, and the engine learned nothing usable from it.
	row := jsonRow(nk, 4, `42`, "ops")
	fs.seed(scope, row)
	e.Publish(context.Background(), scope, row)

	release()

	waitReconcileIdle(t, e, scope)
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss: the reconcile erased the cached value")
	}

	if got.Value != "cached" || got.Revision != 3 {
		t.Errorf("after the reconcile: got (%v, rev %d), want (\"cached\", rev 3): the snapshot "+
			"published the registered default over a value only a rejected write touched",
			got.Value, got.Revision)
	}

	for _, ch := range deliveries(&rec) {
		if ch.Revision == 0 {
			t.Fatalf("a Revision 0 default was announced to subscribers: deliveries = %v", rec.revisions())
		}
	}
}

// TestReconcileListIsBoundedAndTheScopeRecovers pins the timeout on the
// whole-scope List.
//
// The lifecycle context alone is not a bound — only Close cancels it — so a
// backend that accepts the call and answers far too late would hold the
// scope's ONE reconcile goroutine, and with it every later OpResync and any
// Start waiting on the first reconcile. The bound turns that into an ordinary
// reconcile failure: cache kept, scope Stale, next OpResync converges.
func TestReconcileListIsBoundedAndTheScopeRecovers(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 3, `"cached"`, "ops"))
	settled(t, e, scope)

	// Lowering the bound here is ordered against the reconcile goroutine: the
	// first reconcile completed before settled returned, and the next one only
	// reads the field after receiving the resync this test sends below.
	e.reconcileTimeout = 20 * time.Millisecond

	fs.onList(func(store.Scope) error {
		fs.onList(nil)
		// Fifteen times the bound: a backend that took the call and answers
		// long after anyone is still waiting for it.
		time.Sleep(300 * time.Millisecond)

		return nil
	})

	fs.seed(scope, jsonRow(nk, 4, `"written"`, "ops"))
	e.onEvent(resyncEvent(scope))

	waitReconcileIdle(t, e, scope)
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss: a reconcile that never got its snapshot erased the cache")
	}

	if got.Value != "cached" || got.Revision != 3 {
		t.Errorf("after the timed-out reconcile: got (%v, rev %d), want (\"cached\", rev 3)",
			got.Value, got.Revision)
	}

	if !got.Stale {
		t.Error("Stale is false after a reconcile whose List never answered: the scope is " +
			"reporting a cache nothing confirmed as current")
	}

	// The scope's one reconcile goroutine is free again, so the next OpResync
	// converges normally instead of queueing behind a call nobody bounded.
	e.reconcileTimeout = defaultReconcileTimeout

	e.onEvent(resyncEvent(scope))
	waitReconcileIdle(t, e, scope)
	quiesce(t, e)

	got, ok = e.Lookup(scope, nk)
	if !ok || got.Value != "written" || got.Revision != 4 {
		t.Errorf("after the following resync: got (%v, rev %d, ok %v), want (\"written\", rev 4, true): "+
			"the scope never reconciled again", got.Value, got.Revision, ok)
	}

	if got.Stale {
		t.Error("Stale is still true after a reconcile that succeeded")
	}
}

// TestConcurrentResyncsCannotStrandAScope drives two OpResync events into one
// scope at the same instant, which is what a flapping connection does.
//
// Arming a reconcile and queueing it were two separate steps under two
// separate locks, so the two could land in the mailbox in the opposite order
// to the one they armed in: the mailbox then held the OLDER arming, which the
// reconcile goroutine drops as superseded, while the newer window had already
// been released as the one it displaced. Nothing reconciled, and the scope
// stayed stale until some later resync happened to arrive — for a knob nobody
// touches again, never.
//
// The assertion is the product-level one: after a reconnect, the scope ends up
// confirmed against the store. Which of the two armings runs is the engine's
// business.
func TestConcurrentResyncsCannotStrandAScope(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0, 5*time.Second)

	fs.seed(scope, jsonRow(nk, 1, `"one"`, "ops"))

	if _, err := e.bringUpScope(scope); err != nil {
		t.Fatalf("bringUpScope: %v", err)
	}

	// One plain reconnect first, so the scope starts this test converged and
	// its reconcile goroutine is already running.
	e.onEvent(resyncEvent(scope))
	waitFor(t, 5*time.Second, "the first reconcile to confirm the scope",
		func() bool { return !scopeStale(t, e, scope) })

	for round := range 500 {
		start := make(chan struct{})

		var wg sync.WaitGroup

		wg.Add(2)

		for range 2 {
			go func() {
				defer wg.Done()
				<-start

				e.onEvent(resyncEvent(scope))
			}()
		}

		close(start)
		wg.Wait()

		waitFor(t, 5*time.Second,
			"round "+strconv.Itoa(round)+": the scope to reconcile after two simultaneous resyncs",
			func() bool { return !scopeStale(t, e, scope) })
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
}

// TestClearStaleRespectsANewerResync pins the last step of a reconcile against
// the first step of the next one.
//
// A finishing reconcile asked "has a newer OpResync taken the scope?" under
// one lock and wrote the stale flag under another. Between the two, a resync
// could arrive, mark the scope stale and arm its own window — and the older
// reconcile would then clear the flag that resync had just set, reporting a
// scope as confirmed against a connection that had already dropped.
//
// Whatever order the two land in, a scope with a reconcile armed and not yet
// run is stale.
func TestClearStaleRespectsANewerResync(t *testing.T) {
	for round := range 2000 {
		sc := newScopeState(store.Scope{})
		arm := sc.beginReconcile()

		start := make(chan struct{})

		var wg sync.WaitGroup

		wg.Add(2)

		go func() {
			defer wg.Done()
			<-start

			sc.clearStale(arm)
		}()

		go func() {
			defer wg.Done()
			<-start

			sc.beginReconcile()
		}()

		close(start)
		wg.Wait()

		sc.mu.RLock()
		stale := sc.stale
		sc.mu.RUnlock()

		if !stale {
			t.Fatalf("round %d: a finishing reconcile cleared the stale flag a newer OpResync had just set: "+
				"the scope reports itself confirmed while nothing has reconciled it", round)
		}
	}
}
