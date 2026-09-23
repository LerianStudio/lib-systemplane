//go:build unit

package engine

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// dispatchEngine returns an Engine whose dispatch workers stop when the test
// ends, so goleak sees no survivor. It tears down with lifecycleCancel plus a
// wait on the dispatch WaitGroup rather than with Close — which exists, and is
// covered in close_test.go — because this engine is hand-built with no store,
// no registry and no debouncer, and Close would exercise that wiring instead
// of the dispatch behavior these tests are about.
func dispatchEngine(t *testing.T) *Engine {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		scopes:          map[store.Scope]*scopeState{},
		lifecycleCtx:    ctx,
		lifecycleCancel: cancel,
	}

	track(t, e, store.Scope{})

	t.Cleanup(func() {
		cancel()
		e.dispatchWG.Wait()
	})

	return e
}

// recorder collects every delivery a subscriber receives.
type recorder struct {
	mu  sync.Mutex
	got []Change
}

func (r *recorder) record(_ context.Context, ch Change) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.got = append(r.got, ch)
}

func (r *recorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.got)
}

func (r *recorder) changes() []Change {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]Change(nil), r.got...)
}

func (r *recorder) revisions() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	revs := make([]int64, len(r.got))
	for i, ch := range r.got {
		revs[i] = ch.Revision
	}

	return revs
}

// waitFor polls cond until it holds or the timeout expires. Dispatch is
// asynchronous by design, so every assertion about a delivery is a wait.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func pub(nk NSKey, revision int64, value any) publication {
	return publication{NSKey: nk, Revision: revision, Value: value}
}

func TestDispatchIsolatesKeys(t *testing.T) {
	e := dispatchEngine(t)
	keyA := NSKey{Namespace: "billing", Key: "a"}
	keyB := NSKey{Namespace: "billing", Key: "b"}

	blocked := make(chan struct{})
	release := make(chan struct{})

	unsubA := e.OnChange(keyA, func(ctx context.Context, _ Change) {
		close(blocked)

		select {
		case <-release:
		case <-ctx.Done():
		}
	})
	defer unsubA()

	var recB recorder

	unsubB := e.OnChange(keyB, recB.record)
	defer unsubB()

	e.publishInto(pub(keyA, 1, "a1"))
	<-blocked

	e.publishInto(pub(keyB, 1, "b1"))
	waitFor(t, 500*time.Millisecond, "key b delivered while key a is blocked", func() bool {
		return recB.len() == 1
	})

	close(release)
}

func TestSubscriberMutationDoesNotAffectCache(t *testing.T) {
	e := dispatchEngine(t)
	nk := NSKey{Namespace: "billing", Key: "limits"}
	delivered := make(chan struct{})

	unsub := e.OnChange(nk, func(_ context.Context, ch Change) {
		defer close(delivered)

		m, ok := ch.Value.(map[string]any)
		if !ok {
			t.Errorf("delivered value: got %T, want map[string]any", ch.Value)

			return
		}

		m["injected"] = true
	})
	defer unsub()

	e.publishInto(pub(nk, 1, map[string]any{"max": float64(10)}))
	<-delivered

	got, ok := e.Lookup(store.Scope{}, nk)
	if !ok {
		t.Fatal("Lookup reports a miss for a published key")
	}

	cached, ok := got.Value.(map[string]any)
	if !ok {
		t.Fatalf("cached value: got %T, want map[string]any", got.Value)
	}

	if _, mutated := cached["injected"]; mutated {
		t.Errorf("subscriber mutation reached the cache: %v", cached)
	}
}

func TestTwoSubscribersGetIndependentCopies(t *testing.T) {
	e := dispatchEngine(t)
	nk := NSKey{Namespace: "billing", Key: "limits"}
	delivered := make(chan map[string]any, 2)

	marker := func(who string) func(context.Context, Change) {
		return func(_ context.Context, ch Change) {
			m, ok := ch.Value.(map[string]any)
			if !ok {
				t.Errorf("delivered value: got %T, want map[string]any", ch.Value)
				delivered <- nil

				return
			}

			m[who] = true
			delivered <- m
		}
	}

	unsubFirst := e.OnChange(nk, marker("first"))
	defer unsubFirst()

	unsubSecond := e.OnChange(nk, marker("second"))
	defer unsubSecond()

	e.publishInto(pub(nk, 1, map[string]any{"max": float64(10)}))

	for i := 0; i < 2; i++ {
		got := <-delivered
		if got == nil {
			continue
		}

		_, first := got["first"]
		_, second := got["second"]

		if first && second {
			t.Errorf("a subscriber observed the other's mutation: %v", got)
		}

		if !first && !second {
			t.Errorf("delivered copy carries no marker: %v", got)
		}
	}
}

func TestDispatchCoalescesToLatestRevision(t *testing.T) {
	e := dispatchEngine(t)
	nk := NSKey{Namespace: "billing", Key: "limits"}

	started := make(chan struct{})
	release := make(chan struct{})

	var rec recorder

	unsub := e.OnChange(nk, func(ctx context.Context, ch Change) {
		rec.record(ctx, ch)

		if ch.Revision == 1 {
			close(started)

			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	})
	defer unsub()

	e.publishInto(pub(nk, 1, "v1"))
	<-started

	for rev := int64(2); rev <= 10; rev++ {
		e.publishInto(pub(nk, rev, fmt.Sprintf("v%d", rev)))
	}

	close(release)

	waitFor(t, time.Second, "the coalesced delivery", func() bool { return rec.len() >= 2 })
	quiesce(t, e)

	got := rec.revisions()
	if len(got) != 2 || got[0] != 1 || got[1] != 10 {
		t.Errorf("delivered revisions: got %v, want [1 10]", got)
	}
}

func TestDispatchDeliversInRevisionOrder(t *testing.T) {
	e := dispatchEngine(t)
	nk := NSKey{Namespace: "billing", Key: "limits"}

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	const last = 100
	for rev := int64(1); rev <= last; rev++ {
		e.publishInto(pub(nk, rev, rev))
	}

	waitFor(t, 2*time.Second, "the newest revision to be delivered", func() bool {
		revs := rec.revisions()

		return len(revs) > 0 && revs[len(revs)-1] == last
	})

	revs := rec.revisions()
	for i := 1; i < len(revs); i++ {
		if revs[i] < revs[i-1] {
			t.Fatalf("revisions delivered out of order at %d: %v", i, revs)
		}
	}
}

// TestConcurrentPublishesDeliverInRevisionOrder pins the one thing that keeps
// deliveries in revision order when several goroutines publish the same key:
// dispatch's mailbox write happens under the same acquisition of the scope's
// write lock that made the fence decision. Move e.dispatch out of that
// critical section and a publication that LOST the fence can overtake the
// winner on the way to the slot, so a subscriber is handed an older revision
// after a newer one.
//
// Neither neighbour covers it. TestDispatchDeliversInRevisionOrder publishes
// from a single goroutine, which cannot produce that interleaving, and
// TestPublishIsSerializedUnderRace watches Lookup, which stays monotonic
// whatever order the mailboxes were written in.
//
// One run of the choreography catches that regression about one time in five:
// the losing publication has to be descheduled between the unlock and the
// mailbox write, and usually it is not. Rounds are what turn a coin flip into
// a guard — at that rate 50 independent rounds miss only once in 10^5 — so
// each round builds its own engine, subscriber and recorder, and a single
// out-of-order delivery in any of them fails the test.
func TestConcurrentPublishesDeliverInRevisionOrder(t *testing.T) {
	const (
		last   = 200
		rounds = 50
	)

	nk := NSKey{Namespace: "billing", Key: "limits"}

	for round := range rounds {
		e := dispatchEngine(t)

		var rec recorder

		unsub := e.OnChange(nk, rec.record)

		var wg sync.WaitGroup

		for writer := range 2 {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for rev := int64(1); rev <= last; rev++ {
					e.publishInto(pub(nk, rev, fmt.Sprintf("writer%d-rev%d", writer, rev)))
				}
			}()
		}

		wg.Wait()

		waitFor(t, hangGuard, "the newest revision to be delivered", func() bool {
			revs := rec.revisions()

			return len(revs) > 0 && revs[len(revs)-1] == last
		})

		revs := rec.revisions()
		for i := 1; i < len(revs); i++ {
			if revs[i] < revs[i-1] {
				t.Fatalf("round %d: revisions delivered out of order at %d: %v", round, i, revs)
			}
		}

		unsub()
	}
}

func TestPanickingSubscriberDoesNotStopLaterDeliveries(t *testing.T) {
	e := dispatchEngine(t)
	nk := NSKey{Namespace: "billing", Key: "limits"}

	var rec recorder

	unsub := e.OnChange(nk, func(ctx context.Context, ch Change) {
		rec.record(ctx, ch)

		if ch.Revision == 1 {
			panic("subscriber blew up")
		}
	})
	defer unsub()

	e.publishInto(pub(nk, 1, "v1"))
	waitFor(t, time.Second, "the delivery that panics", func() bool { return rec.len() == 1 })

	e.publishInto(pub(nk, 2, "v2"))
	waitFor(t, time.Second, "the delivery after the panic", func() bool { return rec.len() == 2 })

	if got := rec.revisions(); got[1] != 2 {
		t.Errorf("delivered revisions after the panic: got %v, want the second to be 2", got)
	}
}

func TestUnsubscribeIsIdempotentAndStopsDelivery(t *testing.T) {
	e := dispatchEngine(t)
	nk := NSKey{Namespace: "billing", Key: "limits"}

	var first recorder

	unsub := e.OnChange(nk, first.record)

	e.publishInto(pub(nk, 1, "v1"))
	waitFor(t, time.Second, "the first delivery", func() bool { return first.len() == 1 })

	unsub()
	unsub()

	var second recorder

	unsubSecond := e.OnChange(nk, second.record)
	defer unsubSecond()

	e.publishInto(pub(nk, 2, "v2"))
	waitFor(t, time.Second, "the delivery to the surviving subscriber", func() bool {
		return second.len() == 1
	})

	if got := first.len(); got != 1 {
		t.Errorf("unsubscribed subscriber received %d deliveries, want 1", got)
	}
}

func TestSameRevisionPublishedTwiceDeliversOnce(t *testing.T) {
	e := dispatchEngine(t)
	nk := NSKey{Namespace: "billing", Key: "limits"}

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	e.publishInto(pub(nk, 7, "same"))
	waitFor(t, time.Second, "the first delivery", func() bool { return rec.len() == 1 })

	e.publishInto(pub(nk, 7, "same"))
	quiesce(t, e)

	if got := rec.revisions(); len(got) != 1 || got[0] != 7 {
		t.Errorf("delivered revisions: got %v, want [7]", got)
	}
}

// TestUnsubscribeRemovesOnlyItsOwnSubscription pins that unsubscribe is
// identity-based. Two subscriptions of one key are indistinguishable by
// function value — the same func may be registered twice — so a removal that
// drops whichever subscription happens to sit first silently cancels a
// bystander. Here the dropped subscription is the SECOND one registered, so a
// drop-the-first removal takes the survivor instead and the survivor's first
// delivery never arrives.
//
// The second unsubscribe is the other half: repeating it must remove nothing,
// not the next subscriber that inherited the freed slot.
func TestUnsubscribeRemovesOnlyItsOwnSubscription(t *testing.T) {
	e := dispatchEngine(t)
	nk := NSKey{Namespace: "billing", Key: "limits"}

	var kept, dropped recorder

	unsubKept := e.OnChange(nk, kept.record)
	defer unsubKept()

	unsubDropped := e.OnChange(nk, dropped.record)
	unsubDropped()

	e.publishInto(pub(nk, 1, "v1"))
	waitFor(t, time.Second, "the surviving subscriber's first delivery", func() bool {
		return kept.len() == 1
	})

	// Deliveries for one key run serially in registration order, so the
	// survivor's delivery already proves the dropped one was skipped; the
	// quiesce only covers a delivery ordered after it.
	quiesce(t, e)

	if got := dropped.len(); got != 0 {
		t.Fatalf("the unsubscribed subscriber received %d deliveries, want 0", got)
	}

	unsubDropped()

	e.publishInto(pub(nk, 2, "v2"))
	waitFor(t, time.Second, "the surviving subscriber's second delivery", func() bool {
		return kept.len() == 2
	})

	if got := dropped.len(); got != 0 {
		t.Errorf("after a repeated unsubscribe the dropped subscriber received %d deliveries, want 0", got)
	}
}

// TestDispatchIsolatesScopesAndNamesTheTenant pins the two halves of FC-4's
// scope rule with one subscription. OnChange covers a key in EVERY scope, so
// the callback is told which one fired through Change.Tenant — hard-wiring it
// to "" leaves a tenant subscriber unable to tell whose configuration changed.
// And the delivery workers are keyed by (scope, key), not by key alone, so a
// blocked single-tenant delivery cannot hold a tenant's delivery of the same
// key behind it.
func TestDispatchIsolatesScopesAndNamesTheTenant(t *testing.T) {
	e := dispatchEngine(t)
	nk := NSKey{Namespace: "billing", Key: "limits"}
	tenant := store.Scope{Tenant: "t1"}

	blocked := make(chan struct{})
	release := make(chan struct{})

	var tenantRec recorder

	var once sync.Once

	unsub := e.OnChange(nk, func(ctx context.Context, ch Change) {
		if ch.Tenant == "" {
			once.Do(func() { close(blocked) })

			select {
			case <-release:
			case <-ctx.Done():
			}

			return
		}

		tenantRec.record(ctx, ch)
	})
	defer unsub()

	track(t, e, tenant)

	e.publishInto(publication{Scope: store.Scope{}, NSKey: nk, Revision: 1, Value: "single"})
	<-blocked

	e.publishInto(publication{Scope: tenant, NSKey: nk, Revision: 1, Value: "t1"})

	waitFor(t, 500*time.Millisecond, "the tenant delivery while the single-tenant one is blocked", func() bool {
		return tenantRec.len() == 1
	})

	got := tenantRec.changes()[0]
	if got.Tenant != "t1" || got.Namespace != nk.Namespace || got.Key != nk.Key {
		t.Errorf("tenant delivery: got (%q, %q/%q), want (\"t1\", %q/%q)",
			got.Tenant, got.Namespace, got.Key, nk.Namespace, nk.Key)
	}

	close(release)
}

// quiesceKey is the key the quiesce helper publishes its sentinel on. Nothing
// in this package registers it, so no reconcile enumerates it and no
// assertion can see it.
var quiesceKey = NSKey{Namespace: "quiesce", Key: "sentinel"}

// quiesceRev keeps every sentinel strictly newer than the last, so the publish
// fence accepts it and the sentinel is always delivered.
var quiesceRev atomic.Int64

// quiesce blocks until the engine has finished everything the test already set
// in motion, so an "and nothing more arrived" assertion cannot pass merely
// because a loaded runner had not got round to the extra delivery yet. A fixed
// time.Sleep gambles on exactly that and goes green when it loses.
//
// It waits for two POSITIVE signals, in this order:
//
//  1. A sentinel publication on a key of its own, submitted through the
//     engine's own debouncer and waited for on a subscriber of that key. With
//     a real quiet window the sentinel's trailing-edge timer is armed after
//     every re-read the test submitted and runs for the same duration, so the
//     sentinel cannot fire first: its delivery proves those windows closed and
//     their re-reads went through the store.
//  2. Every dispatch worker holding an empty mailbox and sitting in no
//     subscriber. Dispatch is one goroutine per (scope, key) by design, so the
//     sentinel's own worker can only speak for itself; this is what covers the
//     key the assertion is actually about.
//
// It is for engines that are still open. A closed engine accepts no
// publication, so there is no signal to wait for and nothing left in flight to
// wait on.
func quiesce(t *testing.T, e *Engine) {
	t.Helper()

	var rec recorder

	unsub := e.OnChange(quiesceKey, rec.record)
	defer unsub()

	fire := func() {
		e.publishInto(publication{
			NSKey:    quiesceKey,
			Revision: quiesceRev.Add(1),
			Value:    "sentinel",
		})
	}

	// Not dead: dispatchEngine hand-builds an Engine with no debouncer (the
	// dispatch tests are not about the feed), and Submit is nil-receiver safe,
	// so routing the sentinel through it unconditionally would drop it in
	// silence and hang every quiesce in this file.
	if e.debouncer == nil {
		fire()
	} else {
		e.debouncer.Submit(scopeNSKey{Namespace: quiesceKey.Namespace, Key: quiesceKey.Key}, fire)
	}

	waitFor(t, hangGuard, "the quiesce sentinel to be delivered", func() bool {
		return rec.len() > 0
	})

	// Idleness is sampled TWICE, with a yield between. workersIdle reads the
	// busy marker and then the mailboxes, so one sample taken while a worker
	// sits between waking and marking itself busy reports idle for a delivery
	// about to start. The second sample's marker read happens after the first
	// sample's mailbox read, hence after the worker took the Change and set
	// the marker, so a worker mid-delivery can no longer read as idle twice.
	waitFor(t, hangGuard, "every dispatch worker to go idle", func() bool {
		if !workersIdle(e) {
			return false
		}

		runtime.Gosched()

		return workersIdle(e)
	})
}

// workersIdle reports whether no dispatch worker is holding a Change: none
// waiting in a mailbox, none inside a subscriber.
func workersIdle(e *Engine) bool {
	busy := false

	e.running.Range(func(_, _ any) bool {
		busy = true

		return false
	})

	if busy {
		return false
	}

	scopes := e.trackedScopes()

	e.workersMu.Lock()

	var ws []*dispatchWorker

	for _, sc := range scopes {
		for _, w := range sc.workers {
			ws = append(ws, w)
		}
	}

	e.workersMu.Unlock()

	for _, w := range ws {
		w.mu.Lock()
		pending := w.hasPending
		w.mu.Unlock()

		if pending {
			return false
		}
	}

	return true
}

// TestQuiesceOutlastsASlowDelivery is the check on the helper every negative
// assertion in this package now depends on. The subscriber is slower than any
// fixed pause a test would have written, which is what a loaded CI runner
// looks like from the outside: if quiesce returns first, every "and nothing
// more arrived" assertion in the package can pass falsely.
func TestQuiesceOutlastsASlowDelivery(t *testing.T) {
	e := dispatchEngine(t)
	nk := NSKey{Namespace: "billing", Key: "limits"}

	var rec recorder

	unsub := e.OnChange(nk, func(ctx context.Context, ch Change) {
		// A subscriber on a runner with nothing to spare. Modelled, not
		// asserted: the assertion is that quiesce outlasts it.
		time.Sleep(200 * time.Millisecond)
		rec.record(ctx, ch)
	})
	defer unsub()

	e.publishInto(pub(nk, 1, "v1"))

	quiesce(t, e)

	if got := rec.len(); got != 1 {
		t.Fatalf("deliveries after quiesce: got %d, want 1 — quiesce returned before a slow delivery landed", got)
	}
}

// TestQuiesceNeverReturnsBeforeAPendingDelivery pins the ordering inside a
// dispatch worker that every "and nothing more arrived" assertion in this
// package rests on.
//
// quiesce reads exactly two things: the mailbox, and the marker saying a worker
// is inside a subscriber. A worker that empties its mailbox BEFORE setting that
// marker leaves a gap where the Change is in neither, so quiesce reports an
// idle engine for a delivery that is about to start and the assertion after it
// passes before the delivery lands.
//
// Holding the worker's own mutex freezes it in that gap deterministically —
// no sleep, no load: the mailbox is full, the worker has woken, and the only
// question left is whether it marked itself busy before reaching for the slot.
func TestQuiesceNeverReturnsBeforeAPendingDelivery(t *testing.T) {
	e := dispatchEngine(t)
	nk := NSKey{Namespace: "billing", Key: "limits"}

	var rec recorder

	unsub := e.OnChange(nk, rec.record)
	defer unsub()

	// One delivery first, so the worker exists and is parked on its select
	// rather than being started by the submit below.
	e.publishInto(pub(nk, 1, "v1"))
	waitFor(t, hangGuard, "the worker's first delivery", func() bool { return rec.len() == 1 })
	quiesce(t, e)

	// running is keyed by the worker itself and carries its (scope, key) as the
	// value: it is what a timed-out Close reads, and that message has to name
	// the tenant.
	w := scopeWorker(e, e.trackedScope(store.Scope{}), nk)
	if w == nil {
		t.Fatal("no dispatch worker for the key a delivery just went to")
	}

	// Deliberately not w.submit: it releases the mutex BEFORE signalling, so
	// the worker it wakes races this goroutine for the lock and can finish the
	// whole delivery — marker set and cleared — before the poll below takes
	// its first sample. That made this test fail against correct code under
	// load, blaming the engine for the very defect it guards. Filling the slot
	// and signalling under the lock parks the worker inside take() on every
	// run, which is the gap under test.
	w.mu.Lock()
	w.pending = Change{Namespace: nk.Namespace, Key: nk.Key, Revision: 2, Value: "v2"}
	w.hasPending = true

	select {
	case w.signal <- struct{}{}:
	default:
	}

	// Polled inline rather than through waitFor: waitFor fails with Fatalf,
	// and failing while this mutex is held would park the worker in take()
	// for good and hang the cleanup that waits for it instead of reporting
	// the defect.
	marked := false

	for deadline := time.Now().Add(hangGuard); time.Now().Before(deadline); {
		if _, busy := e.running.Load(w); busy {
			marked = true

			break
		}

		time.Sleep(time.Millisecond)
	}

	w.mu.Unlock()

	if !marked {
		t.Fatal("the worker emptied its mailbox before marking itself busy: quiesce can " +
			"observe an idle engine while a delivery is about to start")
	}

	quiesce(t, e)

	if got := rec.len(); got != 2 {
		t.Fatalf("deliveries after quiesce: got %d, want 2 — quiesce returned before a pending delivery landed", got)
	}
}

// TestUnsubscribeReleasesTheCallback pins that a removal drops the closure as
// well as the subscription. Re-slicing with append copies the survivors down
// and leaves the removed entry sitting in the backing array's tail slot, so
// the callback — and everything a consumer captured in it, usually a whole
// service struct — stays reachable for the life of the Engine although nothing
// can ever invoke it again.
func TestUnsubscribeReleasesTheCallback(t *testing.T) {
	e := dispatchEngine(t)
	nk := NSKey{Namespace: "billing", Key: "limits"}

	var kept, dropped recorder

	unsubKept := e.OnChange(nk, kept.record)
	defer unsubKept()

	unsubDropped := e.OnChange(nk, dropped.record)
	unsubDropped()

	e.subsMu.RLock()
	defer e.subsMu.RUnlock()

	subs := e.subscribers[nk]
	for i, slot := range subs[:cap(subs)] {
		if i < len(subs) {
			continue
		}

		if slot.fn != nil || slot.id != 0 {
			t.Errorf("backing slot %d past the %d live subscriptions still holds subscription %d: "+
				"the unsubscribed callback is still reachable", i, len(subs), slot.id)
		}
	}
}
