//go:build unit

package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// dispatchEngine returns an Engine whose dispatch workers stop when the test
// ends, so goleak sees no survivor. Engine.Close arrives with Task 1.3.2;
// until then canceling the lifecycle context is what stops a worker, and
// waiting on the dispatch WaitGroup is what proves it stopped.
func dispatchEngine(t *testing.T) *Engine {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		scopes:          map[store.Scope]*scopeState{},
		lifecycleCtx:    ctx,
		lifecycleCancel: cancel,
	}

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

	e.publish(pub(keyA, 1, "a1"))
	<-blocked

	e.publish(pub(keyB, 1, "b1"))
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

	e.publish(pub(nk, 1, map[string]any{"max": float64(10)}))
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

	e.publish(pub(nk, 1, map[string]any{"max": float64(10)}))

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

	e.publish(pub(nk, 1, "v1"))
	<-started

	for rev := int64(2); rev <= 10; rev++ {
		e.publish(pub(nk, rev, fmt.Sprintf("v%d", rev)))
	}

	close(release)

	waitFor(t, time.Second, "the coalesced delivery", func() bool { return rec.len() >= 2 })
	time.Sleep(50 * time.Millisecond)

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
		e.publish(pub(nk, rev, rev))
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

	e.publish(pub(nk, 1, "v1"))
	waitFor(t, time.Second, "the delivery that panics", func() bool { return rec.len() == 1 })

	e.publish(pub(nk, 2, "v2"))
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

	e.publish(pub(nk, 1, "v1"))
	waitFor(t, time.Second, "the first delivery", func() bool { return first.len() == 1 })

	unsub()
	unsub()

	var second recorder

	unsubSecond := e.OnChange(nk, second.record)
	defer unsubSecond()

	e.publish(pub(nk, 2, "v2"))
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

	e.publish(pub(nk, 7, "same"))
	waitFor(t, time.Second, "the first delivery", func() bool { return rec.len() == 1 })

	e.publish(pub(nk, 7, "same"))
	time.Sleep(50 * time.Millisecond)

	if got := rec.revisions(); len(got) != 1 || got[0] != 7 {
		t.Errorf("delivered revisions: got %v, want [7]", got)
	}
}
