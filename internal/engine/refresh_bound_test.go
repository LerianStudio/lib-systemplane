//go:build unit

package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// getGate parks every Store.Get of a fakeStore until open, counting how many
// are parked at once and the most that ever were.
type getGate struct {
	mu        sync.Mutex
	inside    int
	highWater int
	gate      chan struct{}
	open      func()
}

func gateGets(t *testing.T, fs *fakeStore) *getGate {
	t.Helper()

	g := &getGate{gate: make(chan struct{})}
	g.open = sync.OnceFunc(func() { close(g.gate) })
	t.Cleanup(g.open)

	fs.onGet(func(store.Scope, NSKey) error {
		g.mu.Lock()
		g.inside++
		g.highWater = max(g.highWater, g.inside)
		g.mu.Unlock()

		<-g.gate

		g.mu.Lock()
		g.inside--
		g.mu.Unlock()

		return nil
	})

	return g
}

func (g *getGate) parked() int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.inside
}

func (g *getGate) peak() int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.highWater
}

// inBackground runs fn on its own goroutine and hands back the channel its
// return closes.
func inBackground(fn func()) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)

		fn()
	}()

	return done
}

// mustCoalesce delivers one more notification for nk while its re-read is
// parked: it must return at once, having started no Get of its own.
func mustCoalesce(t *testing.T, e *Engine, g *getGate, nk NSKey, deleted bool) {
	t.Helper()

	select {
	case <-inBackground(func() { e.trackedRefresh(store.Scope{}, nk, deleted) }):
	case <-time.After(2 * time.Second):
		g.open()
		t.Fatal("a notification for a key whose re-read is in flight did not return: " +
			"it started a second, overlapping Store.Get instead of coalescing")
	}
}

// rereadOwed reports whether nk's in-flight tracked re-read owes a trailing
// read: a notification under a real quiet window was coalesced behind it.
func rereadOwed(e *Engine, scope store.Scope, nk NSKey) bool {
	sc := e.scopeFor(scope)

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	_, owed := sc.again[nk]

	return owed
}

func assertCached(t *testing.T, e *Engine, nk NSKey, value any, revision int64) {
	t.Helper()

	got, ok := e.Lookup(store.Scope{}, nk)
	if !ok || got.Value != value || got.Revision != revision {
		t.Fatalf("cached: got (%v, rev %d, hit %v), want (%v, rev %d)", got.Value, got.Revision, ok, value, revision)
	}
}

func TestRefreshCoalescesOverlappingNotifications(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 1, `"v1"`, "ops"))

	g := gateGets(t, fs)
	first := inBackground(func() { e.trackedRefresh(scope, nk, false) })

	waitFor(t, hangGuard, "the first re-read to park inside Store.Get", func() bool { return g.parked() == 1 })

	// The write lands after the parked Get took its snapshot: only a trailing
	// read can see it.
	fs.seed(scope, jsonRow(nk, 2, `"v2"`, "ops"))
	mustCoalesce(t, e, g, nk, false)
	mustCoalesce(t, e, g, nk, false)

	g.open()
	mustReceive(t, first, "the in-flight re-read and its trailing read")

	if got := fs.getCount(); got != 2 {
		t.Errorf("Store.Get calls for two notifications behind one in-flight read: got %d, want 2 "+
			"(the in-flight read plus exactly one trailing read)", got)
	}

	if got := g.peak(); got != 1 {
		t.Errorf("concurrent Store.Get calls for one key: got %d, want 1", got)
	}

	assertCached(t, e, nk, "v2", 2)

	// The claim is released with the trailing read: the next notification
	// reads the store again.
	e.trackedRefresh(scope, nk, false)

	if got := fs.getCount(); got != 3 {
		t.Errorf("Store.Get calls after the coalesced batch finished: got %d, want 3", got)
	}
}

func TestRefreshCoalescedDeletePublishesTheDefault(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 1, `"v1"`, "ops"))
	e.trackedRefresh(scope, nk, false)
	assertCached(t, e, nk, "v1", 1)

	g := gateGets(t, fs)
	first := inBackground(func() { e.trackedRefresh(scope, nk, false) })

	waitFor(t, hangGuard, "the upsert re-read to park inside Store.Get", func() bool { return g.parked() == 1 })

	// The delete arrives behind the parked upsert read, the way onEvent hands
	// it over: counted at arrival, then its re-read submitted.
	fs.remove(scope, nk)
	e.recordFeedDelete(scope, nk)
	mustCoalesce(t, e, g, nk, true)
	mustCoalesce(t, e, g, nk, false)

	g.open()
	mustReceive(t, first, "the in-flight re-read and its trailing read")

	assertCached(t, e, nk, "fallback", 0)
}

func TestRefreshEmptyReadKeepsTheCachedValue(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e := feedEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs, 0)

	fs.seed(scope, jsonRow(nk, 1, `"v1"`, "ops"))
	e.trackedRefresh(scope, nk, false)
	clearStale(e, scope)

	g := gateGets(t, fs)
	first := inBackground(func() { e.trackedRefresh(scope, nk, false) })

	waitFor(t, hangGuard, "the upsert re-read to park inside Store.Get", func() bool { return g.parked() == 1 })

	// Not visible to the trailing reader: an upsert's empty read is a
	// non-answer, never a removal.
	fs.remove(scope, nk)
	mustCoalesce(t, e, g, nk, false)

	g.open()
	mustReceive(t, first, "the in-flight re-read and its trailing read")

	if got := fs.getCount(); got != 3 {
		t.Errorf("Store.Get calls: got %d, want 3 (the seed read, the in-flight read, one trailing read)", got)
	}

	assertCached(t, e, nk, "v1", 1)

	if got, _ := e.Lookup(scope, nk); got.Stale {
		t.Error("an upsert's empty first-attempt read left the key Stale; it must record nothing")
	}
}

func TestRefreshSemaphoreCapsConcurrentGets(t *testing.T) {
	const keys = 3 * refreshGetLimit

	scope := store.Scope{}
	fs := newFakeStore()
	defs := make(map[NSKey]KeyDef, keys)

	for i := range keys {
		defs[NSKey{Namespace: "bulk", Key: fmt.Sprintf("k%d", i)}] = KeyDef{Default: "fallback"}
	}

	e := feedEngine(t, defs, fs, 0)
	g := gateGets(t, fs)

	var started, finished sync.WaitGroup

	// A bulk delete: one re-read per key, all of them at once.
	for nk := range defs {
		started.Add(1)
		finished.Add(1)

		go func() {
			defer finished.Done()

			started.Done()
			e.trackedRefresh(scope, nk, true)
		}()
	}

	started.Wait()
	waitFor(t, hangGuard, "the cap's worth of Store.Get calls to park", func() bool {
		return g.parked() >= refreshGetLimit
	})

	// Every re-read is running; an uncapped scope would park all of them.
	time.Sleep(20 * time.Millisecond)

	g.open()
	finished.Wait()

	if got := g.peak(); got != refreshGetLimit {
		t.Errorf("concurrent Store.Get calls for %d deleted keys in one scope: got %d, want %d",
			keys, got, refreshGetLimit)
	}

	if got := fs.getCount(); got != keys {
		t.Errorf("Store.Get calls: got %d, want %d (one per key, none lost to the cap)", got, keys)
	}
}

func TestRefreshSemaphoreReleasesOnClose(t *testing.T) {
	scope := store.Scope{}
	fs := newFakeStore()
	defs := make(map[NSKey]KeyDef, refreshGetLimit+1)

	for i := range refreshGetLimit + 1 {
		defs[NSKey{Namespace: "bulk", Key: fmt.Sprintf("k%d", i)}] = KeyDef{Default: "fallback"}
	}

	e := storeEngine(t, defs, fs, 0, 2*time.Second)
	g := gateGets(t, fs)

	var holders sync.WaitGroup

	// Inline re-reads, on changefeed goroutines Close does not wait for, hold
	// every slot of the scope.
	for i := range refreshGetLimit {
		holders.Add(1)

		go func() {
			defer holders.Done()

			e.onEvent(upsertEvent(scope, NSKey{Namespace: "bulk", Key: fmt.Sprintf("k%d", i)}, 1))
		}()
	}

	waitFor(t, hangGuard, "every slot to be held", func() bool { return g.parked() == refreshGetLimit })

	queuedKey := NSKey{Namespace: "bulk", Key: fmt.Sprintf("k%d", refreshGetLimit)}
	queued := inBackground(func() { e.trackedRefresh(scope, queuedKey, false) })

	// Claimed, the tracked re-read is work Close waits for, headed for the full cap.
	waitFor(t, hangGuard, "the tracked re-read to claim its key", func() bool {
		sc := e.trackedScope(scope)

		sc.mu.RLock()
		defer sc.mu.RUnlock()

		_, running := sc.inflight[queuedKey]

		return running
	})

	if err := e.Close(); err != nil {
		t.Errorf("Close() = %v with a re-read queued on a full semaphore, want nil", err)
	}

	mustReceive(t, queued, "the queued re-read to return once Close began")

	g.open()
	holders.Wait()
}
