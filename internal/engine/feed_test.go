//go:build unit

package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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
//
// It goes through New rather than building the struct: New derives fields from
// Config that a literal silently leaves zero. debounceAsync is the one that
// mattered — a hand-built engine with a non-zero window ran every debounced
// re-read inline on the caller's goroutine, so every feed and reconcile test
// with a real quiet window exercised a path production never takes.
func registryEngine(t *testing.T, reg Registry, fs *fakeStore, window time.Duration) *Engine {
	t.Helper()

	e := New(Config{Store: fs, Registry: reg, Debounce: window})

	track(t, e, store.Scope{})

	t.Cleanup(func() {
		e.debouncer.Close()
		e.lifecycleCancel()
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

// armWindow opens a reconcile window on the scope the way an OpResync does and
// hands back the arming that names it, so a test can assert what the feed
// records during that window without owning the reconcile itself.
//
// It empties the mailbox afterwards because arming and queueing are one step
// in production: a test that wants the window and no reconcile takes the
// arming back out rather than leaving it for a goroutine to run. The window
// stays open; the caller closes it.
func armWindow(sc *scopeState) reconcileArming {
	sc.armReconcile()

	arm, _ := sc.takeReconcile()

	return arm
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

// TestFeedDeleteDoesNotRevertTheWriteThatFollowedIt is the regression for the
// inversion a self-describing delete used to cause.
//
// A caller deletes a key and writes it again. Both are published locally the
// moment the store acknowledges them (D4), so the value in force is already
// the new row when their echoes arrive on the feed, in the order the store
// produced them: OpDelete, then OpUpsert. The delete carries revision 0, which
// always wins the publish fence, so applied on arrival it reverted a write
// that had already succeeded — Lookup served the registered default for a
// quiet window, and every subscriber took a spurious Revision 0 delivery — and
// only the upsert's debounced re-read repaired it.
//
// Sharing that window is what orders the two: the upsert replaces the pending
// delete instead of landing behind it, so the pair collapses to the write.
func TestFeedDeleteDoesNotRevertTheWriteThatFollowedIt(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 20*time.Millisecond)

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	// The caller's own Delete, then its own Set, each published as it returned.
	e.PublishDelete(scope, nk)

	row := jsonRow(nk, 7, `"written"`, "actor")
	fs.seed(scope, row)
	e.Publish(context.Background(), scope, row)

	waitFor(t, time.Second, "the write's delivery", func() bool {
		revs := rec.revisions()

		return len(revs) > 0 && revs[len(revs)-1] == 7
	})

	delivered := rec.len()

	// Their echoes, in the order the store produced them. The delete arrives
	// first and carries revision 0.
	e.onEvent(deleteEvent(scope, nk))

	if got, _ := e.Lookup(scope, nk); got.Value != "written" || got.Revision != 7 {
		t.Fatalf("value in force the moment the delete echo arrived: got (%v, rev %d), want "+
			"(\"written\", rev 7): the echo of a delete reverted a write made after it", got.Value, got.Revision)
	}

	e.onEvent(upsertEvent(scope, nk, 7))

	waitFor(t, time.Second, "the coalesced re-read", func() bool { return fs.getCount() > 0 })
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after the echoes of a delete and the write that followed it")
	}

	if got.Value != "written" || got.Revision != 7 {
		t.Errorf("value in force after both echoes: got (%v, rev %d), want (\"written\", rev 7)", got.Value, got.Revision)
	}

	for _, ch := range rec.changes()[delivered:] {
		if ch.Revision == 0 {
			t.Errorf("delivered revisions: got %v, want no Revision 0 after the write: subscribers "+
				"were handed the registered default for a key the caller had just written", rec.revisions())

			break
		}
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

	_ = armWindow(e.scopeFor(store.Scope{}))

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

// TestReReadRejectedByRevisionFenceLandsInTouchedNotUnusable pins the one
// outcome the two fences would otherwise disagree about. A re-read that decoded
// and passed the registered validator DID teach the engine the key's value; the
// fence then found it no newer than what is cached and published nothing.
//
// That is "the cache is already current", not "the engine learned nothing", and
// only the first keeps an older photograph off the key: a reconcile deciding a
// row its own List DID carry consults touched alone, so a key filed as unusable
// takes the snapshot row — which is exactly the row the fence just refused.
func TestReReadRejectedByRevisionFenceLandsInTouchedNotUnusable(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 5, `"five"`, "ops"))
	e.onEvent(upsertEvent(scope, nk, 5))

	arm := armWindow(e.scopeFor(scope))
	defer e.scopeFor(scope).closeWindow(arm)

	// A row the fence refuses: it decodes and it validates, it is simply older
	// than what the cache already holds.
	fs.seed(scope, jsonRow(nk, 3, `"three"`, "ops"))
	e.onEvent(upsertEvent(scope, nk, 3))

	touched, unusable := recordedSets(e, scope)
	if len(touched) != 1 || touched[0] != nk {
		t.Errorf("touched: got %v, want [%v]: a value the fence merely deduplicated is still a value the feed read",
			touched, nk)
	}

	if len(unusable) != 0 {
		t.Errorf("unusable: got %v, want empty: filing it there tells a reconcile the feed learned nothing "+
			"about the key, and its older snapshot row then lands on top of the newer cached value", unusable)
	}

	got, ok := e.Lookup(scope, nk)
	if !ok || got.Revision != 5 || got.Value != "five" {
		t.Errorf("cached after the rejected re-read: got (%v, rev %d, found %t), want (\"five\", rev 5, true)",
			got.Value, got.Revision, ok)
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
// path. A delete is self-describing, so the debouncer it shares with an upsert
// has no store read to schedule: at the zero quiet window this test uses it is
// applied inline on the changefeed goroutine, which makes it the one operation
// able to park that goroutine in a subscriber.
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

// TestSlowValidatorDoesNotBlockTheChangefeedGoroutine pins the consumer's
// validator OUT of the scope's reconcile mutex.
//
// The validator is consumer code: it may read a file, call a remote service,
// or simply be slow. Running it while the scope's reconcile mutex is held puts
// every other event for that scope behind it — a delete of an unrelated key
// waits on a validator that has nothing to do with it, and so does every
// reconcile of the scope. Decode and validate therefore run before the mutex
// is taken, and the mutex covers only the publish-and-record pair the fence
// needs to be atomic.
func TestSlowValidatorDoesNotBlockTheChangefeedGoroutine(t *testing.T) {
	scope := store.Scope{}
	slow := NSKey{Namespace: "billing", Key: "slow"}
	other := NSKey{Namespace: "billing", Key: "other"}

	entered := make(chan struct{})
	release := make(chan struct{})

	var once sync.Once

	fs := newFakeStore()
	fs.seed(scope, jsonRow(slow, 1, `"v"`, "ops"))

	e := feedEngine(t, map[NSKey]KeyDef{
		slow: {Default: "fallback", Validate: func(context.Context, any) error {
			once.Do(func() { close(entered) })
			<-release

			return nil
		}},
		other: {Default: "fallback"},
	}, fs, 0)

	delivered := make(chan Change, 1)
	unsubscribe := e.OnChange(other, func(_ context.Context, ch Change) { delivered <- ch })

	defer unsubscribe()

	reread := make(chan struct{})

	go func() {
		defer close(reread)

		e.onEvent(upsertEvent(scope, slow, 1))
	}()

	<-entered

	applied := make(chan struct{})

	go func() {
		defer close(applied)

		e.onEvent(deleteEvent(scope, other))
	}()

	timeout := time.After(200 * time.Millisecond)

	select {
	case <-applied:
	case <-timeout:
		close(release)
		<-reread
		<-applied
		t.Fatal("the delete of a second key waited on a validator holding the scope's reconcile mutex")
	}

	select {
	case ch := <-delivered:
		if ch.Revision != 0 || ch.Value != "fallback" {
			t.Errorf("delete delivered (rev %d, %v), want the registered default at revision 0", ch.Revision, ch.Value)
		}
	case <-timeout:
		close(release)
		<-reread
		t.Fatal("the delete of a second key was never delivered while a validator held the scope's reconcile mutex")
	}

	close(release)
	<-reread
}

// TestForeignUpsertEventCostsNoStoreRead pins the filter that keeps another
// service's traffic off this engine's store. `systemplane_entries` is one
// table per database, so every consumer sharing it emits notifications for
// keys this process never registered; answering each one with a debounce timer
// and a pooled connection buys a row the ingress throws away.
//
// Both quiet windows are covered because they take different paths out of
// onEvent: zero runs the re-read inline on the changefeed goroutine, non-zero
// arms a timer and a tracked goroutine.
func TestForeignUpsertEventCostsNoStoreRead(t *testing.T) {
	foreign := NSKey{Namespace: "billing", Key: "another-service"}
	mine := NSKey{Namespace: "billing", Key: "limits"}

	for _, tc := range []struct {
		name   string
		window time.Duration
	}{
		{name: "inline", window: 0},
		{name: "debounced", window: 20 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFakeStore()
			fs.seed(store.Scope{}, jsonRow(foreign, 1, `"theirs"`, "them"))

			e := feedEngine(t, map[NSKey]KeyDef{mine: {Default: "fallback"}}, fs, tc.window)

			e.onEvent(upsertEvent(store.Scope{}, foreign, 1))

			// The sentinel goes through the same debouncer with the same
			// window and is armed after the event above, so its delivery
			// proves any window that event opened has already closed. It does
			// NOT prove the re-read that window would have scheduled has
			// finished: on the debounced path the timer hands the re-read to a
			// tracked goroutine, so a Store.Get could still be in flight here
			// and the count below would read zero for a filter that is not
			// there. Close is what drains it — the same wait a consumer's own
			// shutdown gets — so the count is read against finished work.
			quiesce(t, e)

			if err := e.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			if reads := fs.getCount(); reads != 0 {
				t.Errorf("store reads for an upsert on a key this process never registered: got %d, want 0", reads)
			}
		})
	}
}

// TestPublishDeleteOnUntrackedScopeCreatesNoScope covers the path the Client's
// own Delete takes in multi-tenant mode and before Start: there is no tracked
// scope, and an exported publication must drop rather than bring one up. A
// scope created here would have no changefeed behind it, no reconcile to
// confirm it and a delivery worker registered in the WaitGroup Close drains —
// a cache that reads as current forever.
func TestPublishDeleteOnUntrackedScopeCreatesNoScope(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e := untrackedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs)

	e.PublishDelete(store.Scope{}, nk)

	if got := scopeCount(e); got != 0 {
		t.Errorf("scopes tracked after PublishDelete: got %d, want 0", got)
	}

	if got, ok := e.Lookup(store.Scope{}, nk); ok || got != (Entry{}) {
		t.Errorf("Lookup after PublishDelete on an untracked scope: got (%+v, %t), want (Entry{}, false)", got, ok)
	}
}

// untrackedEngine is registryEngine without the scope: an engine that has been
// built but never brought a scope up, which is what the Client holds in
// multi-tenant mode and before Start.
func untrackedEngine(t *testing.T, defs map[NSKey]KeyDef, fs *fakeStore) *Engine {
	t.Helper()

	e := New(Config{Store: fs, Registry: fakeRegistry{defs: defs}, Debounce: 0})

	t.Cleanup(func() {
		e.debouncer.Close()
		e.lifecycleCancel()
		e.closeWorkers()
		e.dispatchWG.Wait()
	})

	return e
}

// TestPublishDeleteRecordsTheKeyAsTouched is the exported entry point standing
// in for the feed in the scenario the delete fence exists for: a reconcile's
// photograph still carries the row, and the caller deletes the key while that
// List is held open. The snapshot row is NEWER than the revision-0 default a
// delete publishes, so the revision fence alone lets it back in; only the
// touched record keeps the deleted key dead. A Client Delete has to write that
// record too, or a delete issued during a reconnect is undone by the snapshot.
func TestPublishDeleteRecordsTheKeyAsTouched(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 5, `"five"`, "ops"))
	settled(t, e, scope)

	release := heldList(fs)
	defer release()

	fs.freezeNextList([]store.Entry{jsonRow(nk, 5, `"five"`, "ops")})

	e.onEvent(disconnectEvent(scope))
	e.onEvent(resyncEvent(scope))

	waitFor(t, hangGuard, "the reconcile to reach its List", func() bool { return fs.listCount() >= 2 })

	fs.remove(scope, nk)
	e.PublishDelete(scope, nk)

	release()
	waitReconcileIdle(t, e, scope)
	quiesce(t, e)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss after a delete that spanned a reconcile")
	}

	if got.Value != "fallback" || got.Revision != 0 {
		t.Errorf("after the delete: got (%v, rev %d), want the registered default at rev 0: "+
			"the reconcile's snapshot resurrected a key the caller deleted", got.Value, got.Revision)
	}
}
