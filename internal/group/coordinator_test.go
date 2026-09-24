//go:build unit

package group

import (
	"context"
	"errors"
	goruntime "runtime"
	"slices"
	"strconv"
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

	return NewCoordinator[coordDoc](nil, coordNamespace, coordKey, false, false, Decode[coordDoc], nil)
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

	unsubscribe := mustRegister(t, c, rec.apply)
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

	unsubscribe := mustRegister(t, c, rec.apply)
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

	unsubscribe := mustRegister(t, c, blocking)
	defer unsubscribe()

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		c.Publish(ctx, publication("t1", 1, "v1"))
	}()

	waitFor(t, entered, "the applier to block on its first delivery")

	for revision := int64(2); revision <= 50; revision++ {
		c.Publish(ctx, publication("t1", revision, "v"+strconv.FormatInt(revision, 10)))
	}

	close(release)
	wg.Wait()

	// The count is exact rather than a bound, and deterministic on every
	// interleaving: the applier is provably inside its first delivery before
	// any of revisions 2..50 is published, so every one of them is recorded
	// against a scope already marked delivering and they collapse into the
	// single trailing delivery the drain makes once the applier unblocks.
	// A bound of "fewer than 50" would also pass on a coordinator that
	// delivered nothing at all.
	got := rec.all()
	if len(got) != 2 {
		t.Fatalf("delivered %d times for 50 publications, want exactly 2 — the first and the newest: %v", len(got), rec.names())
	}

	if got[0].current.Revision != 1 {
		t.Errorf("first delivery was revision %d, want 1", got[0].current.Revision)
	}

	if got[1].current.Revision != 50 {
		t.Errorf("trailing delivery was revision %d, want 50 (the newest publication must always land)", got[1].current.Revision)
	}
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
		unsubscribe := mustRegister(t, c, serialized(which))
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

	unsubscribe := mustRegister(t, c, applier)
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

	unsubscribe := mustRegister(t, c, rec.apply)

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

	unsubscribe := mustRegister(t, c, rec.apply)
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

	unsubscribe := mustRegister(t, c, rec.apply)
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

	unsubscribe := mustRegister(t, c, rec.apply)
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

	unsubscribe := mustRegister(t, c, applier)
	defer unsubscribe()

	done := make(chan struct{})

	go func() {
		defer close(done)

		c.Publish(ctx, publication("t1", 1, "outer"))
	}()

	waitFor(t, done, "the re-entrant publish to finish without deadlocking")

	assertEvents(t, events, []string{"enter:outer", "exit:outer", "enter:inner", "exit:inner"})
}

// TestCoordinatorRegisterFromInsideAnApplierIsDeliveredAfterIt pins A7: a
// registration made while its own scope is already delivering cannot deliver on
// the spot, because the fan-out is inside an applier. The new function is
// appended and its first delivery is deferred to the drain's next iteration, on
// the delivering goroutine, after the applier that registered it returns — and
// an unsubscribe reaching the coordinator before that iteration cancels the
// delivery outright, so the function never runs at all.
//
// events is deliberately unguarded in both cases. Every delivery here must run
// on the publishing goroutine, so -race is what proves the same-goroutine half
// of the claim: a delivery from anywhere else has no happens-before edge to the
// read at the end and is reported as a race.
func TestCoordinatorRegisterFromInsideAnApplierIsDeliveredAfterIt(t *testing.T) {
	t.Run("deferred to the next iteration", func(t *testing.T) {
		c := newCoordinator(t)
		ctx := context.Background()

		var (
			events []string
			once   sync.Once
		)

		second := func(_ context.Context, current Decoded[coordDoc], previous *Decoded[coordDoc]) error {
			events = append(events, "second:"+current.Value.Name)

			if previous != nil {
				t.Errorf("previous on a deferred first delivery = %#v, want nil", *previous)
			}

			return nil
		}

		first := func(_ context.Context, current Decoded[coordDoc], _ *Decoded[coordDoc]) error {
			events = append(events, "enter:"+current.Value.Name)

			once.Do(func() { c.Register(second) })

			events = append(events, "exit:"+current.Value.Name)

			return nil
		}

		unsubscribe := mustRegister(t, c, first)
		defer unsubscribe()

		done := make(chan struct{})

		go func() {
			defer close(done)

			c.Publish(ctx, publication("t1", 1, "one"))
		}()

		waitFor(t, done, "the re-entrant registration to finish without deadlocking")

		assertEvents(t, events, []string{"enter:one", "exit:one", "second:one"})
	})

	t.Run("unsubscribed before the next iteration", func(t *testing.T) {
		c := newCoordinator(t)
		ctx := context.Background()

		var (
			events []string
			once   sync.Once
		)

		second := func(_ context.Context, current Decoded[coordDoc], _ *Decoded[coordDoc]) error {
			events = append(events, "second:"+current.Value.Name)

			return nil
		}

		first := func(_ context.Context, current Decoded[coordDoc], _ *Decoded[coordDoc]) error {
			events = append(events, "enter:"+current.Value.Name)

			once.Do(func() {
				// Not mustRegister: this runs on the publishing goroutine,
				// where t.Fatalf is not allowed.
				drop, _ := c.Register(second)
				drop()
			})

			events = append(events, "exit:"+current.Value.Name)

			return nil
		}

		unsubscribe := mustRegister(t, c, first)
		defer unsubscribe()

		done := make(chan struct{})

		go func() {
			defer close(done)

			c.Publish(ctx, publication("t1", 1, "one"))
		}()

		waitFor(t, done, "the cancelled registration to finish without deadlocking")

		assertEvents(t, events, []string{"enter:one", "exit:one"})
	})

	// A re-entrant registration is the only moment an applier exists and has
	// not been offered a scope the coordinator already published: the fan-out
	// picks it up on its next iteration. Applied has to read 0 in that window
	// rather than the revision the incumbent applier already accepted, because
	// the document is demonstrably not in force everywhere and reporting it
	// would call a half-applied group converged.
	t.Run("a newly registered applier leaves the scope unapplied until it is offered", func(t *testing.T) {
		c := newCoordinator(t)
		ctx := context.Background()

		var (
			duringDelivery []Status
			once           sync.Once
		)

		second := func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error { return nil }

		first := func(_ context.Context, current Decoded[coordDoc], _ *Decoded[coordDoc]) error {
			if current.Revision == 2 {
				once.Do(func() {
					// Not mustRegister: t.Fatalf is not allowed here, and the
					// coordinator takes no seed, so this cannot fail.
					c.Register(second)

					duringDelivery = c.Status()
				})
			}

			return nil
		}

		unsubscribe := mustRegister(t, c, first)
		defer unsubscribe()

		c.Publish(ctx, publication("t1", 1, "one")) // accepted by the only applier
		c.Publish(ctx, publication("t1", 2, "two")) // registers the second one mid-delivery

		if len(duringDelivery) != 1 || duringDelivery[0].Tenant != "t1" {
			t.Fatalf("Status() during the delivery = %+v, want the one scope t1", duringDelivery)
		}

		if duringDelivery[0].Applied != 0 {
			t.Errorf("Applied during the delivery = %d, want 0: the applier registered a moment earlier has never been offered this scope, so revision 1 is not in force everywhere", duringDelivery[0].Applied)
		}

		if got := statusOf(t, c, "t1"); got.Desired != 2 || got.Applied != 2 || got.LastErr != nil {
			t.Errorf("Status after the fan-out drained = %+v, want Desired 2 Applied 2 and no error: the deferred registration must be offered and recorded on the drain's next iteration", got)
		}
	})
}

// assertEvents compares a delivery trace in order, so a test reads as the
// sequence it is pinning rather than as a loop.
func assertEvents(t *testing.T, got, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}
}

func TestCoordinatorRegisterRefusesANilApplyFunc(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	unsubscribe := mustRegister(t, c, nil)
	if unsubscribe == nil {
		t.Fatal("Register(nil) returned a nil unsubscribe")
	}

	c.Publish(ctx, publication("t1", 1, "one"))

	if got := statusOf(t, c, "t1"); got.LastErr != nil {
		t.Errorf("LastErr = %v, want nil: a nil apply function is refused at Register, so nothing can reject a publication", got.LastErr)
	}

	unsubscribe()
}

func TestCoordinatorNilReceiverIsSafe(t *testing.T) {
	var c *Coordinator[coordDoc]

	c.Publish(context.Background(), publication("t1", 1, "one"))

	unsubscribe := mustRegister(t, c, func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error { return nil })
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

// refuseMarshal is a document whose MarshalJSON FAILS rather than panicking, so
// the seed watermark has nothing to compare and can never prove a publication
// identical to the seed it anticipates. Like blowUpOnMarshal it is the
// consumer's own type: a group persists the caller's struct verbatim, so it is
// the consumer's MarshalJSON that the watermark runs.
type refuseMarshal struct{}

func (refuseMarshal) MarshalJSON() ([]byte, error) { return nil, errors.New("marshal refused") }

// decodeRefusing maps refuseMarshal onto the same document every other value in
// the same test decodes to, so a delivery can only be explained by the
// watermark refusing to spend itself and never by the two documents differing.
// Decode goes through JSON, which refuseMarshal fails, so it cannot decode one.
func decodeRefusing(v any) (coordDoc, error) {
	if _, ok := v.(refuseMarshal); ok {
		return coordDoc{Name: unmarshallableName}, nil
	}

	return Decode[coordDoc](v)
}

// unmarshallableName is what both sides of an unprovable watermark comparison
// decode to.
const unmarshallableName = "unmarshallable"

func constantDecode(any) (coordDoc, error) { return coordDoc{Name: "decoded"}, nil }

func noopApply(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error { return nil }

func seedOf(pub Publication) func() (Publication, bool, error) {
	return func() (Publication, bool, error) { return pub, true, nil }
}

// mustRegister registers fn and fails the test when the seed read behind the
// registration failed, which is the uninteresting case in every test that is
// not about seed errors.
func mustRegister(t *testing.T, c *Coordinator[coordDoc], fn ApplyFunc[coordDoc]) func() {
	t.Helper()

	unsubscribe, err := c.Register(fn)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	return unsubscribe
}

// applierScopeKeys reports every scope key any registered applier keeps
// bookkeeping under, sorted and deduplicated. Per-applier state is keyed by the
// SCOPE on the way in and on the way out alike, so the only key that may ever
// appear is the scope's own tenant — never the delivered payload's, which is
// empty on the zero observation and would file a real scope under "".
func applierScopeKeys(t *testing.T, c *Coordinator[coordDoc]) []string {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	var keys []string

	for _, ap := range c.appliers {
		for tenant := range ap.state {
			if !slices.Contains(keys, tenant) {
				keys = append(keys, tenant)
			}
		}
	}

	slices.Sort(keys)

	return keys
}

// scopeObserved reports the scope's observed flag, which is what decides
// whether a later registration seeds and what a replay iterates.
func scopeObserved(t *testing.T, c *Coordinator[coordDoc], tenant string) bool {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	sc, ok := c.scopes[tenant]

	return ok && sc.observed
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
		c := NewCoordinator[coordDoc](nil, coordNamespace, coordKey, false, false, constantDecode, seedOf(publication("", 1, "seeded")))

		unsubscribe := mustRegister(t, c, noopApply)
		defer unsubscribe()

		// Registering took the seed, which armed the watermark, so this
		// publication is marshalled under the state mutex.
		mustPanic(t, "Publish of a document whose MarshalJSON panics", func() {
			c.Publish(context.Background(), Publication{Revision: 1, Value: blowUpOnMarshal{}})
		})

		stillResponsive(t, c)
	})

	t.Run("register", func(t *testing.T) {
		c := NewCoordinator[coordDoc](nil, coordNamespace, coordKey, false, false, constantDecode, func() (Publication, bool, error) {
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
	c := NewCoordinator[coordDoc](&alwaysPanickingLogger{NopLogger: &log.NopLogger{}}, coordNamespace, coordKey, false, false, Decode[coordDoc], nil)

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

	unsubscribe := mustRegister(t, c, applier)
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

		// Not mustRegister: t.Fatalf is not allowed off the test goroutine.
		var err error

		unsubscribe, err = c.Register(applier)
		if err != nil {
			t.Errorf("Register: %v", err)
		}
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

	c := NewCoordinator[coordDoc](nil, coordNamespace, coordKey, false, false, slowDecode, nil)

	var rec recorder

	unsubscribe := mustRegister(t, c, rec.apply)
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

	unsubscribe := mustRegister(t, c, abandoning)
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

// TestCoordinatorReplayDoesNotRunUnderADeadPublisherContext pins the one case
// where the observation's own context must be refused. A registration replays
// the last observation, which keeps the context of whoever published it — and
// through the Client that is the dispatch context Close cancels. Nothing is
// ever retried, so a hook that honours cancellation would reject the replay and
// leave the scope permanently unconverged, on a key nobody may write again.
func TestCoordinatorReplayDoesNotRunUnderADeadPublisherContext(t *testing.T) {
	c := newCoordinator(t)

	publisherCtx, cancel := context.WithCancel(context.Background())
	c.Publish(publisherCtx, publication("t1", 1, "one"))
	cancel()

	var (
		seen    int
		lastErr error
	)

	unsubscribe := mustRegister(t, c, func(ctx context.Context, _ Decoded[coordDoc], _ *Decoded[coordDoc]) error {
		seen++
		lastErr = ctx.Err()

		return ctx.Err()
	})
	defer unsubscribe()

	if seen != 1 {
		t.Fatalf("deliveries = %d, want the replay", seen)
	}

	if lastErr != nil {
		t.Fatalf("the replay ran under a context reporting %v, want a live one: the publisher is gone and the delivery is not retried", lastErr)
	}

	if got := statusOf(t, c, "t1"); got.LastErr != nil {
		t.Errorf("LastErr = %v, want nil: the replay was applied", got.LastErr)
	}
}

// TestCoordinatorRegisterWhoseSeedPanicsRegistersNothing pins the ordering
// inside add: the seed runs consumer code, and a panic there escapes Register
// before the caller holds an unsubscribe, so an applier appended BEFORE the
// seed would stay registered with no way to remove it and receive every later
// delivery.
func TestCoordinatorRegisterWhoseSeedPanicsRegistersNothing(t *testing.T) {
	c := NewCoordinator[coordDoc](nil, coordNamespace, coordKey, false, false, constantDecode, func() (Publication, bool, error) {
		panic("the seed read exploded")
	})

	var rec recorder

	mustPanic(t, "Register whose seed read panics", func() { c.Register(rec.apply) })

	c.mu.Lock()
	registered := len(c.appliers)
	c.mu.Unlock()

	if registered != 0 {
		t.Fatalf("appliers registered after the seed panicked = %d, want 0: the caller never received an unsubscribe for it", registered)
	}

	c.Publish(context.Background(), publication("", 1, "after"))

	if got := rec.names(); len(got) != 0 {
		t.Fatalf("deliveries to an applier whose registration panicked = %v, want none", got)
	}
}

// TestCoordinatorUnsubscribeMidBatchSkipsTheApplierNotYetInvoked pins what a
// returned unsubscribe promises: the function is not started again. A batch
// is snapshotted under the state mutex and delivered without it, so a sibling
// applier can unsubscribe another one between the snapshot and that one's
// turn; the one that left must be skipped, and its absence must not be
// recorded as a rejection or hold Applied back.
func TestCoordinatorUnsubscribeMidBatchSkipsTheApplierNotYetInvoked(t *testing.T) {
	c := newCoordinator(t)

	var (
		first, second recorder
		unsubSecond   func()
	)

	firstApply := func(ctx context.Context, current Decoded[coordDoc], previous *Decoded[coordDoc]) error {
		if current.Revision == 2 {
			unsubSecond()
		}

		return first.apply(ctx, current, previous)
	}

	unsubFirst := mustRegister(t, c, firstApply)
	defer unsubFirst()

	unsubSecond = mustRegister(t, c, second.apply)
	defer unsubSecond()

	c.Publish(context.Background(), publication("", 1, "one"))
	c.Publish(context.Background(), publication("", 2, "two"))

	if got := first.names(); !slices.Equal(got, []string{"one", "two"}) {
		t.Fatalf("first applier saw %v, want [one two]", got)
	}

	if got := second.names(); !slices.Equal(got, []string{"one"}) {
		t.Fatalf("second applier saw %v after its unsubscribe returned mid-batch, want [one]", got)
	}

	status := c.Status()
	if len(status) != 1 || status[0].Applied != 2 || status[0].LastErr != nil {
		t.Fatalf("Status = %+v, want Applied 2 with no error: the skipped applier is gone and records nothing", status)
	}
}

// TestCoordinatorRegisterWhoseSeedPanicsKeepsTheSeedRetryable pins the order
// inside seedLocked: the decoder and the document's MarshalJSON are consumer
// code that can panic, and a seed marked taken BEFORE they ran would leave
// nothing observed and nothing replayable, so the next registration would
// return success and deliver nothing. The seed must stay untaken until both
// have returned, so the next registration reads it again.
func TestCoordinatorRegisterWhoseSeedPanicsKeepsTheSeedRetryable(t *testing.T) {
	cases := []struct {
		name  string
		build func() *Coordinator[coordDoc]
	}{
		{
			name: "decoder panics once",
			build: func() *Coordinator[coordDoc] {
				calls := 0
				decode := func(v any) (coordDoc, error) {
					calls++
					if calls == 1 {
						panic("the decoder exploded")
					}

					return Decode[coordDoc](v)
				}

				return NewCoordinator[coordDoc](nil, coordNamespace, coordKey, false, false, decode, seedOf(publication("", 1, "seeded")))
			},
		},
		{
			name: "MarshalJSON panics once",
			build: func() *Coordinator[coordDoc] {
				calls := 0
				seed := func() (Publication, bool, error) {
					calls++
					if calls == 1 {
						return Publication{Revision: 1, Value: blowUpOnMarshal{}}, true, nil
					}

					return publication("", 1, "seeded"), true, nil
				}

				return NewCoordinator[coordDoc](nil, coordNamespace, coordKey, false, false, constantDecode, seed)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.build()

			var rec recorder

			mustPanic(t, "Register whose seed panics", func() { c.Register(rec.apply) })

			unsubscribe := mustRegister(t, c, rec.apply)
			defer unsubscribe()

			if got := rec.names(); len(got) != 1 {
				t.Fatalf("second registration delivered %v, want the seeded document: the seed was spent by a registration that never completed", got)
			}

			status := c.Status()
			if len(status) != 1 || status[0].Desired != 1 || status[0].Applied != 1 || status[0].LastErr != nil {
				t.Fatalf("Status = %+v, want Desired 1, Applied 1, no error", status)
			}
		})
	}
}
