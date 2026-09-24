//go:build unit

package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// deleteWindow is the quiet window every test in this file runs at. A real one
// is the point: with WithDebounce(0) the feed's re-read runs inline on the
// changefeed goroutine, which serialises everything these tests are about and
// hides the four regressions they pin.
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
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, deleteWindow)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	// The caller's own Delete, then its own Set, each published as it returned.
	e.PublishDelete(scope, nk)

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
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, deleteWindow)

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

	// More than one window after the upsert echo: nothing coalesces these two.
	time.Sleep(2 * deleteWindow)
	e.onEvent(deleteEvent(scope, nk))

	waitFor(t, time.Second, "the delete's re-read", func() bool { return fs.getCount() > readsBefore })
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok || got.Value != "recreated" || got.Revision != 9 {
		t.Errorf("after a delete echo whose row is present: got (%v, rev %d, cached %t), want "+
			"(\"recreated\", rev 9)", got.Value, got.Revision, ok)
	}

	noRevision(t, &rec, 0, "a delete echo reverted a recreated row to the registered default")
}

// TestFeedDeleteOfAMissingRowPublishesTheDefaultOnce is the ordinary delete:
// the row really is gone, and the registered default goes in force at revision
// 0 with no row's provenance behind it — exactly once.
func TestFeedDeleteOfAMissingRowPublishesTheDefaultOnce(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, deleteWindow)

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
}
