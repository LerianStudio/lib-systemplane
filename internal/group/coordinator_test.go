//go:build unit

package group

import (
	"context"
	"sync"
	"testing"
	"time"
)

type coordDoc struct {
	Name string `json:"name"`
}

// document builds the untyped shape a store or a cache publishes, so every
// test drives the coordinator through the real decode path.
func document(name string) any {
	return map[string]any{"name": name}
}

func publication(tenant string, revision int64, name string) Publication {
	return Publication{Tenant: tenant, Revision: revision, Value: document(name)}
}

func newCoordinator(t *testing.T) *Coordinator[coordDoc] {
	t.Helper()

	return NewCoordinator[coordDoc](nil, Decode[coordDoc], nil)
}

type received struct {
	current  Decoded[coordDoc]
	previous *Decoded[coordDoc]
}

// recorder is an applier that records every delivery it receives.
type recorder struct {
	mu         sync.Mutex
	deliveries []received
}

func (r *recorder) apply(_ context.Context, current Decoded[coordDoc], previous *Decoded[coordDoc]) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.deliveries = append(r.deliveries, received{current: current, previous: previous})

	return nil
}

func (r *recorder) all() []received {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]received(nil), r.deliveries...)
}

func (r *recorder) names() []string {
	out := []string{}
	for _, d := range r.all() {
		out = append(out, d.current.Value.Name)
	}

	return out
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestCoordinatorRegisterReplaysEveryObservedScope(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	c.Publish(ctx, publication("t1", 1, "one"))
	c.Publish(ctx, publication("t2", 2, "two"))

	var rec recorder

	unsubscribe := c.Register(rec.apply)
	defer unsubscribe()

	got := rec.all()
	if len(got) != 2 {
		t.Fatalf("replay delivered %d times, want 2: %v", len(got), rec.names())
	}

	seen := map[string]int64{}

	for _, d := range got {
		if d.previous != nil {
			t.Errorf("replay for %q carried previous %#v, want nil", d.current.Tenant, *d.previous)
		}

		seen[d.current.Tenant] = d.current.Revision
	}

	if seen["t1"] != 1 || seen["t2"] != 2 {
		t.Errorf("replayed revisions = %#v, want t1=1 t2=2", seen)
	}
}

func TestCoordinatorDeliversTheNewestObservationOnly(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	c.Publish(ctx, publication("t1", 1, "one"))
	c.Publish(ctx, publication("t1", 2, "two"))
	c.Publish(ctx, publication("t1", 3, "three"))

	var rec recorder

	unsubscribe := c.Register(rec.apply)
	defer unsubscribe()

	got := rec.all()
	if len(got) != 1 {
		t.Fatalf("replay delivered %d times, want 1: %v", len(got), rec.names())
	}

	if got[0].current.Value.Name != "three" || got[0].current.Revision != 3 {
		t.Errorf("replay delivered %#v, want the newest observation (revision 3, %q)", got[0].current, "three")
	}

	status := c.Status()
	if len(status) != 1 || status[0].Desired != 3 {
		t.Errorf("Status() = %#v, want one scope with Desired 3", status)
	}
}

func TestCoordinatorCoalescesWhileAnApplierIsBusy(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	var (
		rec      recorder
		once     sync.Once
		entered  = make(chan struct{})
		release  = make(chan struct{})
		blocking = func(fnCtx context.Context, current Decoded[coordDoc], previous *Decoded[coordDoc]) error {
			once.Do(func() {
				close(entered)
				<-release
			})

			return rec.apply(fnCtx, current, previous)
		}
	)

	unsubscribe := c.Register(blocking)
	defer unsubscribe()

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		c.Publish(ctx, publication("t1", 1, "v1"))
	}()

	waitFor(t, entered, "the applier to block on its first delivery")

	for revision := int64(2); revision <= 50; revision++ {
		c.Publish(ctx, publication("t1", revision, "v"+itoa(revision)))
	}

	close(release)
	wg.Wait()

	got := rec.all()
	if len(got) >= 50 {
		t.Fatalf("delivered %d times for 50 publications, want far fewer", len(got))
	}

	last := got[len(got)-1]
	if last.current.Revision != 50 {
		t.Errorf("last delivery was revision %d, want 50 (the newest publication must always land)", last.current.Revision)
	}
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}

	digits := ""

	for v > 0 {
		digits = string(rune('0'+v%10)) + digits
		v /= 10
	}

	return digits
}

func TestCoordinatorSerializesDeliveriesPerScope(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	// inFlight is deliberately unguarded: two deliveries running concurrently
	// have no happens-before edge between them, so -race reports the overlap.
	inFlight := false
	overlapped := false

	serialized := func(_ context.Context, _ Decoded[coordDoc], _ *Decoded[coordDoc]) error {
		if inFlight {
			overlapped = true
		}

		inFlight = true
		time.Sleep(time.Microsecond)
		inFlight = false

		return nil
	}

	for range 2 {
		unsubscribe := c.Register(serialized)
		defer unsubscribe()
	}

	var wg sync.WaitGroup

	for revision := int64(1); revision <= 20; revision++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			c.Publish(ctx, publication("t1", revision, "v"))
		}()
	}

	wg.Wait()

	if overlapped {
		t.Error("two deliveries for one scope ran concurrently")
	}
}

func TestCoordinatorDeliversScopesIndependently(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	var (
		once      sync.Once
		entered   = make(chan struct{})
		release   = make(chan struct{})
		delivered = make(chan string, 8)
	)

	applier := func(_ context.Context, current Decoded[coordDoc], _ *Decoded[coordDoc]) error {
		if current.Tenant == "t2" {
			once.Do(func() {
				close(entered)
				<-release
			})
		}

		delivered <- current.Tenant

		return nil
	}

	unsubscribe := c.Register(applier)
	defer unsubscribe()

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		c.Publish(ctx, publication("t2", 1, "two"))
	}()

	waitFor(t, entered, "the t2 applier to block")

	c.Publish(ctx, publication("t1", 1, "one"))

	select {
	case tenant := <-delivered:
		if tenant != "t1" {
			t.Errorf("first delivery came from %q, want t1 (t2 is blocked)", tenant)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("t1 never delivered while t2 was blocked: scopes are not independent")
	}

	close(release)
	wg.Wait()
}

func TestCoordinatorNeverDeliversTheSameObservationTwice(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	c.Publish(ctx, publication("t1", 1, "one"))

	var (
		rec recorder
		wg  sync.WaitGroup
	)

	wg.Add(1)

	go func() {
		defer wg.Done()

		c.Publish(ctx, publication("t1", 2, "two"))
	}()

	unsubscribe := c.Register(rec.apply)

	wg.Wait()
	unsubscribe()

	var previous int64

	for i, d := range rec.all() {
		if i > 0 && d.current.Revision <= previous {
			t.Errorf("delivery %d carried revision %d after %d: an observation was repeated or inverted",
				i, d.current.Revision, previous)
		}

		previous = d.current.Revision
	}
}

func TestCoordinatorDeliversTheSameRevisionAgainWhenTheValueChanged(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	var rec recorder

	unsubscribe := c.Register(rec.apply)
	defer unsubscribe()

	c.Publish(ctx, publication("t1", 7, "first"))
	c.Publish(ctx, publication("t1", 7, "second"))

	got := rec.names()
	if len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Errorf("deliveries = %v, want both documents at revision 7", got)
	}
}

func TestCoordinatorRevisionZeroIsAlwaysDelivered(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	var rec recorder

	unsubscribe := c.Register(rec.apply)
	defer unsubscribe()

	for range 3 {
		c.Publish(ctx, publication("t1", 0, "default"))
	}

	if got := rec.names(); len(got) != 3 {
		t.Errorf("revision 0 delivered %d times, want 3: %v", len(got), got)
	}
}

func TestCoordinatorPreviousIsTheLastAcceptedValue(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	var rec recorder

	unsubscribe := c.Register(rec.apply)
	defer unsubscribe()

	c.Publish(ctx, publication("t1", 1, "one"))
	c.Publish(ctx, publication("t1", 2, "two"))

	got := rec.all()
	if len(got) != 2 {
		t.Fatalf("delivered %d times, want 2: %v", len(got), rec.names())
	}

	if got[0].previous != nil {
		t.Errorf("first delivery carried previous %#v, want nil", *got[0].previous)
	}

	if got[1].previous == nil {
		t.Fatal("second delivery carried no previous, want the first accepted value")
	}

	if got[1].previous.Value.Name != "one" || got[1].previous.Revision != 1 {
		t.Errorf("previous = %#v, want revision 1 %q", *got[1].previous, "one")
	}
}

func TestCoordinatorPublishFromInsideAnApplierIsDeliveredAfterIt(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	var (
		events []string
		once   sync.Once
	)

	applier := func(_ context.Context, current Decoded[coordDoc], _ *Decoded[coordDoc]) error {
		events = append(events, "enter:"+current.Value.Name)

		once.Do(func() {
			c.Publish(ctx, publication("t1", 2, "inner"))
		})

		events = append(events, "exit:"+current.Value.Name)

		return nil
	}

	unsubscribe := c.Register(applier)
	defer unsubscribe()

	done := make(chan struct{})

	go func() {
		defer close(done)

		c.Publish(ctx, publication("t1", 1, "outer"))
	}()

	waitFor(t, done, "the re-entrant publish to finish without deadlocking")

	want := []string{"enter:outer", "exit:outer", "enter:inner", "exit:inner"}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}

	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events = %v, want %v", events, want)
		}
	}
}

func TestCoordinatorRegisterRefusesANilApplyFunc(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	unsubscribe := c.Register(nil)
	if unsubscribe == nil {
		t.Fatal("Register(nil) returned a nil unsubscribe")
	}

	c.Publish(ctx, publication("t1", 1, "one"))
	unsubscribe()
}

func TestCoordinatorNilReceiverIsSafe(t *testing.T) {
	var c *Coordinator[coordDoc]

	c.Publish(context.Background(), publication("t1", 1, "one"))

	unsubscribe := c.Register(func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error { return nil })
	if unsubscribe == nil {
		t.Fatal("Register on a nil coordinator returned a nil unsubscribe")
	}

	unsubscribe()

	if got := c.Status(); got != nil {
		t.Errorf("Status() on a nil coordinator = %#v, want nil", got)
	}
}
