//go:build unit

package group

import (
	"context"
	goruntime "runtime"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
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
	// delivered is unguarded for the same reason, and it is what stops this test
	// passing on a coordinator that never invokes an applier at all: overlapped
	// starts false and is only ever written from inside a delivery.
	inFlight := false
	overlapped := false
	delivered := [2]int{}

	serialized := func(which int) ApplyFunc[coordDoc] {
		return func(_ context.Context, _ Decoded[coordDoc], _ *Decoded[coordDoc]) error {
			if inFlight {
				overlapped = true
			}

			inFlight = true
			delivered[which]++
			time.Sleep(time.Microsecond)
			inFlight = false

			return nil
		}
	}

	for which := range 2 {
		unsubscribe := c.Register(serialized(which))
		defer unsubscribe()
	}

	// One publication on this goroutine, before any concurrency: its fan-out
	// runs to completion here, so both counters are readable without a race and
	// a coordinator that delivers nothing fails on the spot.
	c.Publish(ctx, publication("t1", 1, "warm"))

	for which, count := range delivered {
		if count == 0 {
			t.Fatalf("applier %d received no delivery at all, so the serialization this test guards was never exercised", which)
		}
	}

	var wg sync.WaitGroup

	for revision := int64(2); revision <= 21; revision++ {
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

	got := rec.all()
	if len(got) == 0 {
		t.Fatal("the registration delivered nothing at all, so the dedupe this test guards was never exercised")
	}

	// Deterministic on every interleaving: whoever drains reads the scope's
	// newest observation, and only one goroutine drains a scope at a time, so
	// revision 2 is always the last thing to land.
	if last := got[len(got)-1].current.Revision; last != 2 {
		t.Errorf("final delivery carried revision %d, want 2: the newest observation must always be the one left in force", last)
	}

	var previous int64

	for i, d := range got {
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

// blowUpOnMarshal is a document whose MarshalJSON panics. A group carries the
// consumer's own type — Group.Set persists the caller's struct verbatim and the
// client's value clone preserves its concrete type — so it is the consumer's
// MarshalJSON that runs inside the coordinator, and inside its state mutex.
type blowUpOnMarshal struct{}

func (blowUpOnMarshal) MarshalJSON() ([]byte, error) { panic("MarshalJSON exploded") }

func constantDecode(any) (coordDoc, error) { return coordDoc{Name: "decoded"}, nil }

func noopApply(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error { return nil }

func seedOf(pub Publication) func() (Publication, bool) {
	return func() (Publication, bool) { return pub, true }
}

func mustPanic(t *testing.T, what string, fn func()) {
	t.Helper()

	defer func() {
		if recover() == nil {
			t.Fatalf("%s did not panic, so this test proves nothing", what)
		}
	}()

	fn()
}

// stillResponsive is the whole assertion: a coordinator whose mutex was
// released can still be read and published to.
func stillResponsive(t *testing.T, c *Coordinator[coordDoc]) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		defer close(done)

		c.Status()
		c.Publish(context.Background(), publication("", 9, "after"))
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the state mutex is still held after a panic under it: every later publication and every Status blocks forever, hot reload stops, and a health check calling Status leaks a goroutine per call")
	}
}

// TestCoordinatorPanicUnderTheStateMutexDoesNotWedgeTheGroup covers both
// critical sections that run consumer-controlled code: the marshal the seed
// watermark does on a publication, and the seed read itself.
func TestCoordinatorPanicUnderTheStateMutexDoesNotWedgeTheGroup(t *testing.T) {
	t.Run("publish", func(t *testing.T) {
		c := NewCoordinator[coordDoc](nil, constantDecode, seedOf(publication("", 1, "seeded")))

		unsubscribe := c.Register(noopApply)
		defer unsubscribe()

		// Registering took the seed, which armed the watermark, so this
		// publication is marshalled under the state mutex.
		mustPanic(t, "Publish of a document whose MarshalJSON panics", func() {
			c.Publish(context.Background(), Publication{Revision: 1, Value: blowUpOnMarshal{}})
		})

		stillResponsive(t, c)
	})

	t.Run("register", func(t *testing.T) {
		c := NewCoordinator[coordDoc](nil, constantDecode, func() (Publication, bool) {
			panic("the seed read exploded")
		})

		mustPanic(t, "Register whose seed read panics", func() { c.Register(noopApply) })

		stillResponsive(t, c)
	})
}

// alwaysPanickingLogger is a consumer logger that panics on every line. The
// coordinator's recovery path writes a recovered applier panic through the
// consumer's own logger, so a logger like this raises a second panic from
// inside the recovery itself — the one place a scope could be left marked as
// delivering forever.
type alwaysPanickingLogger struct {
	*log.NopLogger
}

func (*alwaysPanickingLogger) Log(context.Context, int, string, ...any) {
	panic("the consumer logger exploded")
}

func publishWithoutPanicking(t *testing.T, c *Coordinator[coordDoc], pub Publication) {
	t.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Errorf("Publish unwound into the publisher: %v", recovered)
		}
	}()

	c.Publish(context.Background(), pub)
}

func TestCoordinatorAPanickingLoggerDoesNotWedgeTheScope(t *testing.T) {
	c := NewCoordinator[coordDoc](&alwaysPanickingLogger{NopLogger: &log.NopLogger{}}, Decode[coordDoc], nil)

	var (
		mu       sync.Mutex
		names    []string
		exploded bool
	)

	applier := func(_ context.Context, current Decoded[coordDoc], _ *Decoded[coordDoc]) error {
		mu.Lock()
		defer mu.Unlock()

		names = append(names, current.Value.Name)

		if !exploded {
			exploded = true

			panic("the apply function exploded")
		}

		return nil
	}

	unsubscribe := c.Register(applier)
	defer unsubscribe()

	publishWithoutPanicking(t, c, publication("t1", 1, "one"))
	publishWithoutPanicking(t, c, publication("t1", 2, "two"))

	mu.Lock()
	defer mu.Unlock()

	if len(names) != 2 || names[0] != "one" || names[1] != "two" {
		t.Fatalf("deliveries = %v, want [one two]: hot reload for the scope stopped after the recovery panicked", names)
	}
}

type publisherCtxKey struct{}

func TestCoordinatorDeliversUnderThePublishersContext(t *testing.T) {
	c := newCoordinator(t)

	var (
		mu     sync.Mutex
		values []any
		once   sync.Once
	)

	entered := make(chan struct{})
	release := make(chan struct{})

	applier := func(ctx context.Context, _ Decoded[coordDoc], _ *Decoded[coordDoc]) error {
		mu.Lock()
		values = append(values, ctx.Value(publisherCtxKey{}))
		mu.Unlock()

		once.Do(func() {
			close(entered)
			<-release
		})

		return nil
	}

	c.Publish(context.Background(), publication("t1", 1, "one"))

	registered := make(chan struct{})

	var unsubscribe func()

	go func() {
		defer close(registered)

		unsubscribe = c.Register(applier)
	}()

	waitFor(t, entered, "the registration replay to reach the applier")

	// The publisher's context must reach the applier even though another
	// goroutine's fan-out is the one that picks the publication up.
	c.Publish(context.WithValue(context.Background(), publisherCtxKey{}, "publisher"), publication("t1", 2, "two"))

	close(release)
	waitFor(t, registered, "the registration to return")

	defer unsubscribe()

	mu.Lock()
	defer mu.Unlock()

	if len(values) != 2 {
		t.Fatalf("deliveries = %d, want 2", len(values))
	}

	if values[1] != "publisher" {
		t.Errorf("second delivery ran under ctx value %v, want %q: a publication delivered by another goroutine's fan-out lost its publisher's context", values[1], "publisher")
	}
}

func TestCoordinatorDiscardsAPublicationThatDecodedAfterANewerOne(t *testing.T) {
	var once sync.Once

	entered := make(chan struct{})
	release := make(chan struct{})

	slowDecode := func(value any) (coordDoc, error) {
		doc, err := Decode[coordDoc](value)
		if err != nil {
			return doc, err
		}

		if doc.Name == "old" {
			once.Do(func() {
				close(entered)
				<-release
			})
		}

		return doc, nil
	}

	c := NewCoordinator[coordDoc](nil, slowDecode, nil)

	var rec recorder

	unsubscribe := c.Register(rec.apply)
	defer unsubscribe()

	done := make(chan struct{})

	go func() {
		defer close(done)

		c.Publish(context.Background(), publication("t1", 1, "old"))
	}()

	waitFor(t, entered, "the older publication to reach its decode")

	c.Publish(context.Background(), publication("t1", 2, "new"))

	close(release)
	waitFor(t, done, "the older publication to finish committing")

	if names := rec.names(); len(names) != 1 || names[0] != "new" {
		t.Fatalf("deliveries = %v, want [new]: an older publication whose decode finished late overwrote the newer one", names)
	}

	if st := statusOf(t, c, "t1"); st.Desired != 2 {
		t.Errorf("Desired = %d, want 2: a late older publication moved the scope backwards", st.Desired)
	}
}

// TestCoordinatorAnAbandonedApplierReleasesTheScope covers the exit path no
// recover can catch: an applier that ends its goroutine outright, which is what
// a t.Fatal inside a consumer's own apply hook does. Deferred bookkeeping still
// runs on that path, so the scope must not be left marked as delivering with
// hot reload stopped for good.
func TestCoordinatorAnAbandonedApplierReleasesTheScope(t *testing.T) {
	c := newCoordinator(t)

	var (
		rec  recorder
		once sync.Once
	)

	abandoning := func(ctx context.Context, current Decoded[coordDoc], previous *Decoded[coordDoc]) error {
		abandon := false
		once.Do(func() { abandon = true })

		if abandon {
			goruntime.Goexit()
		}

		return rec.apply(ctx, current, previous)
	}

	unsubscribe := c.Register(abandoning)
	defer unsubscribe()

	done := make(chan struct{})

	go func() {
		defer close(done)

		c.Publish(context.Background(), publication("t1", 1, "one"))
	}()

	waitFor(t, done, "the abandoned delivery to end its goroutine")

	c.Publish(context.Background(), publication("t1", 2, "two"))

	if names := rec.names(); len(names) != 1 || names[0] != "two" {
		t.Fatalf("deliveries after the abandoned one = %v, want [two]: the scope stayed marked as delivering", names)
	}
}
