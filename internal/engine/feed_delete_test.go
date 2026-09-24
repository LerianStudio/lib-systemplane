//go:build unit

package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// deleteWindow is the real quiet window this file's tests run at, and the one
// forEachWindow pairs with WithDebounce(0). A real window is what separates
// the two behaviours: at zero the feed's re-read runs inline on the changefeed
// goroutine, which serialises everything these tests are about, so a test that
// only ever ran at zero would pin none of the regressions below.
//
// Every test whose claim is about a re-read's OUTCOME runs at both windows,
// through forEachWindow. The two that hold Store.Get open with a gate —
// TestFeedDeleteFencesAnInFlightReReadOnArrival and
// TestFeedDeleteDoesNotRevertAWritePublishedDuringItsReRead — cannot: at zero
// the goroutine their gate blocks inside Store.Get IS the changefeed
// goroutine, so the very event they need to deliver while the read is in
// flight can never arrive, and the test would deadlock rather than fail. They
// stay pinned at the real window, which is the only one where the race they
// describe exists.
const deleteWindow = 20 * time.Millisecond

// noRevision fails the test when any Change delivered carries revision, naming
// what that revision would have meant.
func noRevision(t *testing.T, rec *recorder, revision int64, what string) {
	t.Helper()

	for _, ch := range rec.changes() {
		if ch.Revision == revision {
			t.Fatalf("delivered revisions %v, want none at revision %d: %s", rec.revisions(), revision, what)
		}
	}
}

// TestFeedDeleteFencesAnInFlightReReadOnArrival pins the half of a delete that
// cannot wait for the key's quiet window.
//
// A re-read armed by an earlier notification is still inside Store.Get when
// the DELETE commits. Under READ COMMITTED it holds a snapshot taken before
// that commit, so it comes back with the removed row, and every revision beats
// the 0 a delete publishes. Unless the key is fenced the instant the event
// ARRIVES — not when the delete finally publishes, a window later — that
// reader republishes a row an operator deleted, to the cache and to every
// subscriber.
func TestFeedDeleteFencesAnInFlightReReadOnArrival(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, deleteWindow)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	fs.seed(scope, jsonRow(nk, 5, `"five"`, "ops"))

	inGet, enteredGet := gate()
	held, releaseGet := gate()

	defer enteredGet()
	defer releaseGet()

	fs.onGet(func(store.Scope, NSKey) error {
		fs.onGet(nil)
		enteredGet()
		<-held

		return nil
	})

	e.onEvent(upsertEvent(scope, nk, 5))
	mustReceive(t, inGet, "the upsert's re-read to enter Store.Get")

	// The DELETE commits. The reader above keeps the snapshot it started on;
	// a read that begins now finds nothing.
	fs.remove(scope, nk)
	e.onEvent(deleteEvent(scope, nk))

	// Released well inside the delete's quiet window, so the stale row reaches
	// the ingress before the delete has published anything at all.
	releaseGet()

	waitFor(t, time.Second, "the delete's re-read to publish the default", func() bool {
		got, ok := e.Lookup(scope, nk)

		return ok && got.Revision == 0 && got.Value == "fallback"
	})
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok || got.Value != "fallback" || got.Revision != 0 {
		t.Errorf("after the delete: got (%v, rev %d, cached %t), want the registered default at rev 0",
			got.Value, got.Revision, ok)
	}

	noRevision(t, &rec, 5, "a re-read holding the pre-delete row republished it on top of the delete")
}

// TestFeedDeleteRePublishesTheRowAWriteLeftBehind is the regression for the
// inversion a self-describing delete caused whenever the write that followed
// it was not close enough to coalesce with it.
//
// A caller deletes a key and writes it again. Both are published locally as
// they return (D4), so the value in force is already the new row when the
// delete's echo reaches the feed. Publishing the registered default for that
// echo reverted a write that had succeeded — and did so for a whole quiet
// window, or forever if no later notification repaired it. Re-reading the
// store instead answers it: the row is there, so the row is what gets
// published.
func TestFeedDeleteRePublishesTheRowAWriteLeftBehind(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}

	forEachWindow(t, func(t *testing.T, window time.Duration) {
		fs := newFakeStore()
		e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, window)

		var rec recorder

		unsub := e.OnChange(nk, rec.record)
		defer unsub()

		// The caller's own Delete, then its own Set, each published as it returned.
		if err := e.PublishDelete(scope, nk); err != nil {
			t.Fatalf("PublishDelete: %v", err)
		}

		row := jsonRow(nk, 9, `"rewritten"`, "actor")
		fs.seed(scope, row)
		e.Publish(context.Background(), scope, row)

		waitFor(t, time.Second, "the write's delivery", func() bool {
			revs := rec.revisions()

			return len(revs) > 0 && revs[len(revs)-1] == 9
		})

		delivered := rec.len()
		readsBefore := fs.getCount()

		// The delete's echo, arriving after the write is already in force.
		e.onEvent(deleteEvent(scope, nk))

		waitFor(t, time.Second, "the delete's re-read", func() bool { return fs.getCount() > readsBefore })
		quiesce(t, e)

		got, ok := e.Lookup(scope, nk)
		if !ok || got.Value != "rewritten" || got.Revision != 9 {
			t.Errorf("after the delete echo: got (%v, rev %d, cached %t), want (\"rewritten\", rev 9): "+
				"the echo of a delete reverted the write made after it", got.Value, got.Revision, ok)
		}

		for _, ch := range rec.changes()[delivered:] {
			if ch.Revision == 0 {
				t.Errorf("delivered revisions %v: subscribers were handed the registered default for a key "+
					"the caller had just written", rec.revisions())

				break
			}
		}
	})
}

// TestFeedDeleteDoesNotRevertAWritePublishedDuringItsReRead pins the fence on
// the other outcome: the re-read came back EMPTY.
//
// The registered default it then publishes carries revision 0, which wins the
// publish fence unconditionally, so it must not land on top of a value
// published while the read was in flight — the echo of a Set the caller made
// right after the delete, read-your-writes, before the row was visible to this
// reader.
func TestFeedDeleteDoesNotRevertAWritePublishedDuringItsReRead(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, deleteWindow)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	inGet, enteredGet := gate()
	held, releaseGet := gate()

	defer enteredGet()
	defer releaseGet()

	fs.onGet(func(store.Scope, NSKey) error {
		fs.onGet(nil)
		enteredGet()
		<-held

		return nil
	})

	e.onEvent(deleteEvent(scope, nk))
	mustReceive(t, inGet, "the delete's re-read to enter Store.Get")

	// The caller's own write, published as Set returned. The row is not yet
	// visible to the reader above, which is the whole hazard.
	e.Publish(context.Background(), scope, jsonRow(nk, 9, `"rewritten"`, "actor"))

	waitFor(t, time.Second, "the write's delivery", func() bool {
		revs := rec.revisions()

		return len(revs) > 0 && revs[len(revs)-1] == 9
	})

	releaseGet()
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok || got.Value != "rewritten" || got.Revision != 9 {
		t.Errorf("after the delete's empty re-read: got (%v, rev %d, cached %t), want (\"rewritten\", rev 9): "+
			"the registered default landed on top of a write published while the re-read was in flight",
			got.Value, got.Revision, ok)
	}

	noRevision(t, &rec, 0, "the delete reverted a write the caller had already been told had landed")
}

// TestFeedDeleteAfterARecreatePublishesTheRecreatedRow covers the echoes that
// land further apart than one quiet window, which is every pair a busy feed
// produces and the case coalescing alone could never fix.
//
// The recreate's echo has already been answered and its row is in force. The
// delete's echo arrives a window later; re-reading answers it with the row the
// store actually holds, so nothing reverts to the registered default even for
// an instant.
func TestFeedDeleteAfterARecreatePublishesTheRecreatedRow(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}

	forEachWindow(t, func(t *testing.T, window time.Duration) {
		fs := newFakeStore()
		e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, window)

		var rec recorder

		unsub := e.OnChange(nk, rec.record)
		defer unsub()

		fs.seed(scope, jsonRow(nk, 9, `"recreated"`, "actor"))
		e.onEvent(upsertEvent(scope, nk, 9))

		waitFor(t, time.Second, "the recreate's delivery", func() bool {
			revs := rec.revisions()

			return len(revs) > 0 && revs[len(revs)-1] == 9
		})

		readsBefore := fs.getCount()

		// More than one window after the upsert echo, so nothing coalesces
		// these two. At WithDebounce(0) there is no window to coalesce in at
		// all and the sleep is a no-op, which is the same starting state.
		time.Sleep(2 * window)
		e.onEvent(deleteEvent(scope, nk))

		waitFor(t, time.Second, "the delete's re-read", func() bool { return fs.getCount() > readsBefore })
		quiesce(t, e)

		got, ok := e.Lookup(scope, nk)
		if !ok || got.Value != "recreated" || got.Revision != 9 {
			t.Errorf("after a delete echo whose row is present: got (%v, rev %d, cached %t), want "+
				"(\"recreated\", rev 9)", got.Value, got.Revision, ok)
		}

		noRevision(t, &rec, 0, "a delete echo reverted a recreated row to the registered default")
	})
}

// TestFeedDeleteOfAMissingRowPublishesTheDefaultOnce is the ordinary delete:
// the row really is gone, and the registered default goes in force at revision
// 0 with no row's provenance behind it — exactly once.
func TestFeedDeleteOfAMissingRowPublishesTheDefaultOnce(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}

	forEachWindow(t, func(t *testing.T, window time.Duration) {
		fs := newFakeStore()
		e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, window)

		var rec recorder

		unsub := e.OnChange(nk, rec.record)
		defer unsub()

		fs.seed(scope, jsonRow(nk, 3, `"live"`, "ops"))
		e.onEvent(upsertEvent(scope, nk, 3))
		waitFor(t, time.Second, "the upsert delivery", func() bool { return rec.len() == 1 })

		fs.remove(scope, nk)
		e.onEvent(deleteEvent(scope, nk))

		waitFor(t, time.Second, "the delete delivery", func() bool { return rec.len() == 2 })
		quiesce(t, e)

		got, ok := e.Lookup(scope, nk)
		if !ok || got.Value != "fallback" || got.Revision != 0 {
			t.Errorf("after the delete: got (%v, rev %d, cached %t), want the registered default at rev 0",
				got.Value, got.Revision, ok)
		}

		if !got.UpdatedAt.IsZero() || got.UpdatedBy != "" {
			t.Errorf("provenance after delete: got (%s, %q), want (zero time, \"\")", got.UpdatedAt, got.UpdatedBy)
		}

		if revs := rec.revisions(); len(revs) != 2 || revs[1] != 0 {
			t.Errorf("delivered revisions: got %v, want exactly two, the second at revision 0", revs)
		}
	})
}

// TestPendingDeleteReReadAfterCloseNeverReachesTheStore is the delete twin of
// the upsert's pending-re-read drop. A delete's deferred work is now a store
// call like any other, so it takes the same door: Close refuses it, waits for
// nothing it never let start, and no goroutine outlives the engine.
func TestPendingDeleteReReadAfterCloseNeverReachesTheStore(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := storeEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 200*time.Millisecond, 2*time.Second)

	fs.seed(scope, jsonRow(nk, 1, `"live"`, "ops"))

	e.onEvent(deleteEvent(scope, nk))

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	// Well past the quiet window: the window IS what this test is about, so
	// waiting it out is the assertion rather than a guess at one.
	time.Sleep(400 * time.Millisecond)

	if got := fs.getCount(); got != 0 {
		t.Errorf("Store.Get called %d times after Close, want 0", got)
	}
}

// TestFailedRereadIsRetriedThenReportsStale closes the hole the delete re-read
// opened: answering a delete from the store made the removal conditional on
// that one read succeeding, and nothing retried it.
//
// A delete publishes nothing of its own — what goes in force is whatever the
// re-read finds — so a re-read that errors drops the removal entirely. The key
// keeps the deleted row at its old revision, the feed has already recorded it
// as touched so a reconcile in flight skips it, and a reconcile is armed only
// by an OpResync: on a connection that never drops, a row an operator deleted
// stayed in force for the life of the process while every read reported the
// scope as current.
func TestFailedRereadIsRetriedThenReportsStale(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}

	t.Run("a transient failure converges on the retry", func(t *testing.T) {
		fs := newFakeStore()
		e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, deleteWindow)

		fs.seed(scope, jsonRow(nk, 5, `"five"`, "ops"))
		settled(t, e, scope)

		// The row really is gone; the first re-read of the delete simply
		// cannot say so — one pool checkout that failed.
		fs.remove(scope, nk)
		fs.onGet(func(store.Scope, NSKey) error {
			fs.onGet(nil)

			return errors.New("pool exhausted")
		})

		e.onEvent(deleteEvent(scope, nk))

		waitFor(t, hangGuard, "the retry to put the registered default in force", func() bool {
			got, ok := e.Lookup(scope, nk)

			return ok && got.Revision == 0 && got.Value == "fallback"
		})

		if got, _ := e.Lookup(scope, nk); got.Stale {
			t.Error("the scope reports itself unconfirmed after a re-read that converged on its retry")
		}
	})

	t.Run("a failure that repeats leaves the scope stale until the next resync", func(t *testing.T) {
		fs := newFakeStore()
		e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, deleteWindow)

		fs.seed(scope, jsonRow(nk, 5, `"five"`, "ops"))
		settled(t, e, scope)

		fs.remove(scope, nk)
		fs.onGet(func(store.Scope, NSKey) error { return errors.New("pool exhausted") })

		e.onEvent(deleteEvent(scope, nk))

		// Two failures are not a blip. The cached value still stands — a read
		// that learned nothing is no reason to discard the last value that
		// did — but it stands as unconfirmed, which is the one thing a caller
		// can act on.
		waitFor(t, hangGuard, "the scope to report itself unconfirmed", func() bool {
			got, ok := e.Lookup(scope, nk)

			return ok && got.Stale
		})

		if got, _ := e.Lookup(scope, nk); got.Value != "five" || got.Revision != 5 {
			t.Errorf("after two failed re-reads: got (%v, rev %d), want the cached (\"five\", rev 5)",
				got.Value, got.Revision)
		}

		// And the repair the stale flag points at actually lands.
		fs.onGet(nil)
		e.onEvent(resyncEvent(scope))
		waitReconcileIdle(t, e, scope)

		got, ok := e.Lookup(scope, nk)
		if !ok || got.Value != "fallback" || got.Revision != 0 || got.Stale {
			t.Errorf("after the resync: got (%v, rev %d, stale %t, cached %t), want the registered default at rev 0, confirmed",
				got.Value, got.Revision, got.Stale, ok)
		}
	})

	// A reconcile whose List was taken BEFORE the delete never decides this
	// key — the feed recorded it as touched at event arrival, so the snapshot
	// skips it — yet it used to clear the scope-wide flag the twice-failed
	// re-read had raised. The deleted row then went on being served, at its
	// old revision, reported as confirmed by a reconcile that had said nothing
	// about it.
	forEachWindow(t, func(t *testing.T, window time.Duration) {
		fs := newFakeStore()
		e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, window)

		fs.seed(scope, jsonRow(nk, 5, `"five"`, "ops"))
		settled(t, e, scope)

		// The reconcile is armed and its photograph is not taken yet, so
		// everything below happens while it is in flight.
		// Released on cleanup as well as below: a failed assertion between the
		// two would otherwise leave the reconcile goroutine parked on the gate
		// forever, and the engine's own drain would hang the package run
		// instead of reporting the failure.
		release := heldList(fs)
		t.Cleanup(release)

		e.onEvent(resyncEvent(scope))
		waitFor(t, hangGuard, "the reconcile to reach its List", func() bool {
			return fs.listCount() >= 2
		})

		fs.remove(scope, nk)
		fs.onGet(func(store.Scope, NSKey) error { return errors.New("pool exhausted") })

		e.onEvent(deleteEvent(scope, nk))

		waitFor(t, hangGuard, "both re-reads to fail and record the key unconfirmed", func() bool {
			return scopeUnconfirmed(t, e, scope) == 1
		})

		fs.onGet(nil)
		release()
		waitReconcileIdle(t, e, scope)

		if got, _ := e.Lookup(scope, nk); got.Stale != true {
			t.Error("a reconcile that skipped the key cleared the flag its failed re-read raised: " +
				"the deleted row is served as confirmed")
		}
	})
}

// forEachWindow runs fn at the two quiet windows the feed behaves differently
// at: a real one, where a re-read runs on a debouncer timer goroutine, and
// WithDebounce(0), where Submit runs it inline on the changefeed goroutine.
// Every claim in this file about a re-read's outcome must hold at both.
func forEachWindow(t *testing.T, fn func(t *testing.T, window time.Duration)) {
	t.Helper()

	for _, tc := range []struct {
		name   string
		window time.Duration
	}{
		{name: "a real quiet window", window: deleteWindow},
		{name: "WithDebounce(0)", window: 0},
	} {
		t.Run(tc.name, func(t *testing.T) { fn(t, tc.window) })
	}
}

// TestRecoveredKeyClearsUnconfirmedWithoutAResync pins the other half: nothing
// on a CONNECTED changefeed ever emits OpResync, so a scope-wide flag raised
// by one failed re-read had no writer left to clear it. Five successful
// upserts later the key held the newest value and every read still reported it
// unconfirmed, for the life of the process.
func TestRecoveredKeyClearsUnconfirmedWithoutAResync(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}

	forEachWindow(t, func(t *testing.T, window time.Duration) {
		fs := newFakeStore()
		e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, window)

		fs.seed(scope, jsonRow(nk, 3, `"three"`, "ops"))
		settled(t, e, scope)

		fs.onGet(func(store.Scope, NSKey) error { return errors.New("pool exhausted") })
		e.onEvent(upsertEvent(scope, nk, 4))

		waitFor(t, hangGuard, "the scope to report itself unconfirmed", func() bool {
			got, _ := e.Lookup(scope, nk)

			return got.Stale
		})

		// The store recovers. No disconnect, so no OpResync will ever arrive:
		// the re-read of the next notification is the only thing that can
		// confirm this key again.
		fs.onGet(nil)
		fs.seed(scope, jsonRow(nk, 7, `"seven"`, "ops"))
		e.onEvent(upsertEvent(scope, nk, 7))

		waitFor(t, hangGuard, "the recovered key to report itself confirmed", func() bool {
			got, ok := e.Lookup(scope, nk)

			return ok && got.Value == "seven" && got.Revision == 7 && !got.Stale
		})

		if got := fs.listCount(); got != 1 {
			t.Errorf("List called %d times, want 1: the recovery must not need an OpResync", got)
		}
	})
}

// TestUnconfirmedIsPerKey pins the granularity. One key nobody could re-read
// makes the scope report Stale; a second key converging normally does not
// clear it; and the scope goes back to confirmed when the FIRST key converges,
// not when any key does.
func TestUnconfirmedIsPerKey(t *testing.T) {
	a := NSKey{Namespace: "billing", Key: "limits"}
	b := NSKey{Namespace: "billing", Key: "retries"}
	scope := store.Scope{}

	forEachWindow(t, func(t *testing.T, window time.Duration) {
		fs := newFakeStore()
		e := feedEngine(t, map[NSKey]KeyDef{a: {Default: "fallback-a"}, b: {Default: "fallback-b"}}, fs, window)

		fs.seed(scope, jsonRow(a, 1, `"a1"`, "ops"))
		fs.seed(scope, jsonRow(b, 1, `"b1"`, "ops"))
		settled(t, e, scope)

		fs.onGet(func(_ store.Scope, nk NSKey) error {
			if nk == a {
				return errors.New("pool exhausted")
			}

			return nil
		})

		e.onEvent(upsertEvent(scope, a, 2))

		waitFor(t, hangGuard, "key A to be recorded unconfirmed", func() bool {
			return scopeUnconfirmed(t, e, scope) == 1
		})

		// B converges while A is unconfirmed, and that must not answer for A.
		fs.seed(scope, jsonRow(b, 2, `"b2"`, "ops"))
		e.onEvent(upsertEvent(scope, b, 2))

		waitFor(t, hangGuard, "key B to take its new value", func() bool {
			got, ok := e.Lookup(scope, b)

			return ok && got.Value == "b2"
		})

		if got, _ := e.Lookup(scope, b); !got.Stale {
			t.Error("the scope reports itself confirmed while key A could not be read back")
		}

		fs.onGet(nil)
		fs.seed(scope, jsonRow(a, 3, `"a3"`, "ops"))
		e.onEvent(upsertEvent(scope, a, 3))

		waitFor(t, hangGuard, "the scope to report itself confirmed once A converged", func() bool {
			got, ok := e.Lookup(scope, a)

			return ok && got.Value == "a3" && !got.Stale
		})
	})
}

// TestRetryAfterCloseNeverReachesTheStore pins that the retry is engine work
// Close accounts for: it waits out its delay on the lifecycle context, so a
// Close during that delay ends it without a store call and without a survivor
// for goleak to find.
func TestRetryAfterCloseNeverReachesTheStore(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}

	forEachWindow(t, func(t *testing.T, window time.Duration) {
		fs := newFakeStore()
		e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, window)

		fs.seed(scope, jsonRow(nk, 1, `"one"`, "ops"))
		settled(t, e, scope)

		fs.onGet(func(store.Scope, NSKey) error { return errors.New("pool exhausted") })
		e.onEvent(upsertEvent(scope, nk, 2))

		waitFor(t, hangGuard, "the first re-read to fail", func() bool {
			return fs.getCount() >= 1
		})

		start := time.Now()

		if err := e.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		// Close accounts for the retry, so it can only return this fast by
		// ENDING it: a retry that merely waits out its delay makes Close block
		// for retryDelay before the WaitGroup drops.
		if waited := time.Since(start); waited >= retryDelay {
			t.Errorf("Close blocked for %s, want well under %s: the retry is waiting out its "+
				"delay instead of ending on the lifecycle context", waited, retryDelay)
		}

		after := fs.getCount()

		// Well past the retry delay: the delay IS what this test is about.
		time.Sleep(2 * retryDelay)

		if got := fs.getCount(); got != after {
			t.Errorf("Store.Get called %d times after Close, want the %d already counted", got, after)
		}
	})
}

// TestZeroWindowRetryDoesNotHoldTheFeedGoroutine pins the head-of-line cost of
// WithDebounce(0). Submit runs the re-read inline on the scope's single
// changefeed goroutine, so a retry submitted the same way doubled the outage
// every other key waited through: two store calls, back to back, on the
// goroutine that delivers every notification of the scope.
//
// The retry runs off that goroutine now, so one failing event costs one store
// call of head-of-line time, not two.
func TestZeroWindowRetryDoesNotHoldTheFeedGoroutine(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}

	const stall = 200 * time.Millisecond

	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 1, `"one"`, "ops"))
	settled(t, e, scope)

	fs.onGet(func(store.Scope, NSKey) error {
		time.Sleep(stall)

		return errors.New("pool exhausted")
	})

	start := time.Now()

	e.onEvent(upsertEvent(scope, nk, 2))

	if elapsed := time.Since(start); elapsed > 2*stall-stall/4 {
		t.Errorf("onEvent held the changefeed goroutine for %s, want about one %s store call: "+
			"the retry is running inline behind the first read", elapsed, stall)
	}
}

// TestRetryAfterAConvergenceLeavesTheKeyConfirmed pins the half of the
// unconfirmed record that no fence used to guard: WHEN the retry's failure is
// allowed to raise it.
//
// The retry runs a quarter of a second after the read it repeats, and a
// changefeed does not stop delivering meanwhile. A fresh notification whose
// re-read succeeds converges the key inside that pause, and the retry then
// wakes into a key something else already decided. Recording it unconfirmed
// there is the same permanent false alarm the per-key set was built to remove:
// the value in force is correct and current, nothing on a connected feed ever
// emits an OpResync, and no later event arrives for a key nobody writes again,
// so every read of the scope reports Stale for the life of the process.
//
// The store answers exactly one of the three reads, so which read converges
// the key does not depend on timing.
func TestRetryAfterAConvergenceLeavesTheKeyConfirmed(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}

	forEachWindow(t, func(t *testing.T, window time.Duration) {
		fs := newFakeStore()
		e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, window)

		fs.seed(scope, jsonRow(nk, 1, `"one"`, "ops"))
		settled(t, e, scope)

		fs.seed(scope, jsonRow(nk, 2, `"two"`, "ops"))

		var reads atomic.Int64

		fs.onGet(func(store.Scope, NSKey) error {
			if reads.Add(1) == 2 {
				return nil
			}

			return errors.New("pool exhausted")
		})

		// Read 1: the first attempt fails and arms the retry.
		e.onEvent(upsertEvent(scope, nk, 2))

		waitFor(t, hangGuard, "the first re-read to fail", func() bool {
			return reads.Load() >= 1
		})

		// Read 2: a fresh notification lands while the retry is still waiting
		// out its delay, and converges the key on the row the failed read was
		// sent for.
		e.onEvent(upsertEvent(scope, nk, 2))

		waitFor(t, hangGuard, "the key to converge on the new row", func() bool {
			got, ok := e.Lookup(scope, nk)

			return ok && got.Value == "two" && !got.Stale
		})

		// Read 3: the retry wakes and fails, which is the terminal branch
		// running over a key a later ingress already decided.
		waitFor(t, hangGuard, "the retry to run and fail", func() bool {
			return reads.Load() >= 3
		})

		got, _ := e.Lookup(scope, nk)
		if got.Stale {
			t.Error("a retry that failed after the key had already converged recorded it unconfirmed: " +
				"nothing will ever clear that on a connected feed, so every read of the scope reports " +
				"Stale for the life of the process over a value that is correct")
		}

		if got.Value != "two" || got.Revision != 2 {
			t.Errorf("value in force = (%v, rev %d), want (%q, rev 2)", got.Value, got.Revision, "two")
		}
	})
}

// TestReconcileAgreeingWithTheCacheConfirmsTheKey pins the one reconcile
// outcome that decides a key without publishing anything.
//
// A deleted key sits at its registered default with no row behind it, which is
// the ordinary post-delete state. A later event for it whose re-read fails
// twice records it unconfirmed. The whole-scope reconcile that follows READS
// THAT KEY BACK and finds it absent — agreeing exactly with what the cache
// holds — and so returns early, publishing nothing, because republishing
// revision 0 would deliver a second Change for a key that never changed.
//
// Early is not the same as undecided. Without the confirmation on that branch
// nothing can ever take the record back: no changefeed event arrives for a row
// that does not exist, and every later reconcile takes this same return, so
// the whole scope reports Stale forever over a converged value.
func TestReconcileAgreeingWithTheCacheConfirmsTheKey(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}

	forEachWindow(t, func(t *testing.T, window time.Duration) {
		fs := newFakeStore()
		e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, window)

		fs.seed(scope, jsonRow(nk, 5, `"five"`, "ops"))
		settled(t, e, scope)

		fs.remove(scope, nk)
		e.onEvent(deleteEvent(scope, nk))

		waitFor(t, hangGuard, "the delete to put the registered default in force", func() bool {
			got, ok := e.Lookup(scope, nk)

			return ok && got.Value == "fallback" && got.Revision == 0
		})

		fs.onGet(func(store.Scope, NSKey) error { return errors.New("pool exhausted") })
		e.onEvent(upsertEvent(scope, nk, 6))

		waitFor(t, hangGuard, "both re-reads to fail and record the key unconfirmed", func() bool {
			return scopeUnconfirmed(t, e, scope) == 1
		})

		fs.onGet(nil)
		e.onEvent(resyncEvent(scope))
		waitReconcileIdle(t, e, scope)

		got, ok := e.Lookup(scope, nk)
		if !ok || got.Stale {
			t.Errorf("after a reconcile that read the key back: (cached %t, stale %t), want confirmed",
				ok, got.Stale)
		}

		if got.Value != "fallback" || got.Revision != 0 {
			t.Errorf("value in force = (%v, rev %d), want the registered default at rev 0",
				got.Value, got.Revision)
		}
	})
}
