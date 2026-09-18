//go:build unit

package group

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
)

var errRejected = errors.New("applier rejected the document")

// recordingLogger keeps the lines the coordinator writes so a test can prove a
// recovered panic reached the logger as well as ApplyStatus.
type recordingLogger struct {
	*log.NopLogger

	mu    sync.Mutex
	lines []string
}

func (r *recordingLogger) Log(_ context.Context, level int, msg string, _ ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.lines = append(r.lines, log.LevelName(level)+": "+msg)
}

func (r *recordingLogger) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.lines...)
}

func newRecordingLogger() *recordingLogger {
	return &recordingLogger{NopLogger: &log.NopLogger{}}
}

// rejectingDecode decodes normally but refuses the document carrying the given
// name, so a test can drive the decode-failure branch with one publication.
func rejectingDecode(bad string) func(any) (coordDoc, error) {
	return func(value any) (coordDoc, error) {
		doc, err := Decode[coordDoc](value)
		if err != nil {
			return doc, err
		}

		if doc.Name == bad {
			return coordDoc{}, errors.New("decode: refused " + bad)
		}

		return doc, nil
	}
}

func statusOf(t *testing.T, c *Coordinator[coordDoc], tenant string) Status {
	t.Helper()

	for _, st := range c.Status() {
		if st.Tenant == tenant {
			return st
		}
	}

	t.Fatalf("Status() = %#v, want an entry for tenant %q", c.Status(), tenant)

	return Status{}
}

func TestCoordinatorApplierErrorRecordsARejection(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	unsubscribe := c.Register(func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error {
		return errRejected
	})
	defer unsubscribe()

	c.Publish(ctx, publication("t1", 5, "five"))

	got := statusOf(t, c, "t1")
	if got.Desired != 5 {
		t.Errorf("Desired = %d, want 5: a rejection must not stop the newest revision being desired", got.Desired)
	}

	if got.Applied != 0 {
		t.Errorf("Applied = %d, want 0: a rejected revision is never applied", got.Applied)
	}

	if !errors.Is(got.LastErr, errRejected) {
		t.Errorf("LastErr = %v, want the applier's error", got.LastErr)
	}
}

func TestCoordinatorApplierPanicIsRecordedLikeAnError(t *testing.T) {
	logger := newRecordingLogger()
	c := NewCoordinator[coordDoc](logger, Decode[coordDoc], nil)
	ctx := context.Background()

	unsubscribe := c.Register(func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error {
		panic("boom")
	})
	defer unsubscribe()

	c.Publish(ctx, publication("t1", 3, "three"))

	got := statusOf(t, c, "t1")
	if got.Applied != 0 || got.Desired != 3 {
		t.Errorf("Status = %#v, want Desired 3 and Applied 0", got)
	}

	if got.LastErr == nil || !strings.Contains(got.LastErr.Error(), "panicked") {
		t.Errorf("LastErr = %v, want an error naming the panic", got.LastErr)
	}

	if !strings.Contains(got.LastErr.Error(), "boom") {
		t.Errorf("LastErr = %v, want the recovered value in the message", got.LastErr)
	}

	lines := logger.recorded()
	if len(lines) == 0 {
		t.Fatal("the recovered panic was never logged")
	}

	if !strings.HasPrefix(lines[0], "error: ") {
		t.Errorf("logged %q, want an error-level line", lines[0])
	}
}

func TestCoordinatorRejectionLeavesPreviousUntouched(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	var rec recorder

	unsubscribe := c.Register(func(fnCtx context.Context, current Decoded[coordDoc], previous *Decoded[coordDoc]) error {
		if current.Revision == 2 {
			return errRejected
		}

		return rec.apply(fnCtx, current, previous)
	})
	defer unsubscribe()

	c.Publish(ctx, publication("t1", 1, "one"))
	c.Publish(ctx, publication("t1", 2, "two"))
	c.Publish(ctx, publication("t1", 3, "three"))

	got := rec.all()
	if len(got) != 2 {
		t.Fatalf("accepted %d deliveries, want 2 (revision 2 was rejected): %v", len(got), rec.names())
	}

	last := got[1]
	if last.previous == nil {
		t.Fatal("the delivery after a rejection carried no previous, want the last ACCEPTED value")
	}

	if last.previous.Revision != 1 || last.previous.Value.Name != "one" {
		t.Errorf("previous = %#v, want revision 1 %q: a rejection must not become previous", *last.previous, "one")
	}
}

func TestCoordinatorRejectionIsNeverRetried(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	var (
		mu    sync.Mutex
		calls int
	)

	unsubscribe := c.Register(func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error {
		mu.Lock()
		defer mu.Unlock()

		calls++

		return errRejected
	})
	defer unsubscribe()

	c.Publish(ctx, publication("t1", 9, "nine"))

	mu.Lock()
	defer mu.Unlock()

	if calls != 1 {
		t.Errorf("the applier ran %d times for one publication, want 1: a rejection is never retried", calls)
	}
}

func TestCoordinatorDecodeFailureIsRecordedAndNeverDelivered(t *testing.T) {
	logger := newRecordingLogger()
	c := NewCoordinator[coordDoc](logger, rejectingDecode("bad"), nil)
	ctx := context.Background()

	var rec recorder

	unsubscribe := c.Register(rec.apply)
	defer unsubscribe()

	c.Publish(ctx, publication("t1", 1, "good"))
	c.Publish(ctx, publication("t1", 2, "bad"))

	if got := rec.names(); len(got) != 1 || got[0] != "good" {
		t.Errorf("deliveries = %v, want only the decodable document: garbage must never reach an applier", got)
	}

	got := statusOf(t, c, "t1")
	if got.Desired != 2 {
		t.Errorf("Desired = %d, want 2: a revision rejected at decode still advances Desired", got.Desired)
	}

	if got.Applied != 1 {
		t.Errorf("Applied = %d, want 1: the last good revision stays applied", got.Applied)
	}

	if got.LastErr == nil {
		t.Error("LastErr = nil, want the decode failure")
	}

	// The last good publication must stay replayable for a later Register.
	var late recorder

	unsubscribeLate := c.Register(late.apply)
	defer unsubscribeLate()

	if names := late.names(); len(names) != 1 || names[0] != "good" {
		t.Errorf("replay = %v, want the last decodable document", names)
	}
}

func TestCoordinatorAppliedIsTheMinimumAcrossAppliers(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	accepting := func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error { return nil }
	picky := func(_ context.Context, current Decoded[coordDoc], _ *Decoded[coordDoc]) error {
		if current.Revision == 2 {
			return errRejected
		}

		return nil
	}

	unsubscribeA := c.Register(accepting)
	defer unsubscribeA()

	unsubscribeB := c.Register(picky)
	defer unsubscribeB()

	c.Publish(ctx, publication("t1", 1, "one"))

	if got := statusOf(t, c, "t1"); got.Applied != 1 || got.LastErr != nil {
		t.Errorf("Status after both accepted = %#v, want Applied 1 and no error", got)
	}

	c.Publish(ctx, publication("t1", 2, "two"))

	got := statusOf(t, c, "t1")
	if got.Desired != 2 {
		t.Errorf("Desired = %d, want 2", got.Desired)
	}

	if got.Applied != 1 {
		t.Errorf("Applied = %d, want 1: applied is the minimum across appliers, not the fastest one", got.Applied)
	}

	if !errors.Is(got.LastErr, errRejected) {
		t.Errorf("LastErr = %v, want the rejection", got.LastErr)
	}
}

func TestCoordinatorStatusWithNoApplierReportsConverged(t *testing.T) {
	c := newCoordinator(t)

	c.Publish(context.Background(), publication("t1", 4, "four"))

	got := statusOf(t, c, "t1")
	if got.Desired != 4 || got.Applied != 4 {
		t.Errorf("Status = %#v, want Desired and Applied both 4: nothing can lag when nothing is hooked", got)
	}

	if got.LastErr != nil {
		t.Errorf("LastErr = %v, want nil", got.LastErr)
	}
}

func TestCoordinatorLastErrClearsWhenAppliedCatchesUp(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	unsubscribe := c.Register(func(_ context.Context, current Decoded[coordDoc], _ *Decoded[coordDoc]) error {
		if current.Revision == 5 {
			return errRejected
		}

		return nil
	})
	defer unsubscribe()

	c.Publish(ctx, publication("t1", 5, "five"))

	if got := statusOf(t, c, "t1"); got.LastErr == nil {
		t.Fatalf("Status after the rejection = %#v, want LastErr set", got)
	}

	c.Publish(ctx, publication("t1", 6, "six"))

	got := statusOf(t, c, "t1")
	if got.Desired != 6 || got.Applied != 6 {
		t.Errorf("Status = %#v, want Desired and Applied both 6", got)
	}

	if got.LastErr != nil {
		t.Errorf("LastErr = %v, want nil once applied caught up", got.LastErr)
	}
}

func TestCoordinatorUnsubscribeStopsDeliveryAndReleasesStatus(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	var (
		rec    recorder
		leaver recorder
	)

	unsubscribeKeeper := c.Register(rec.apply)
	defer unsubscribeKeeper()

	unsubscribeLeaver := c.Register(func(fnCtx context.Context, current Decoded[coordDoc], previous *Decoded[coordDoc]) error {
		_ = leaver.apply(fnCtx, current, previous)

		return errRejected
	})

	c.Publish(ctx, publication("t1", 1, "one"))

	if got := statusOf(t, c, "t1"); got.Applied != 0 || got.LastErr == nil {
		t.Fatalf("Status while the rejecting applier is registered = %#v, want Applied 0 and an error", got)
	}

	unsubscribeLeaver()

	got := statusOf(t, c, "t1")
	if got.Applied != 1 {
		t.Errorf("Applied = %d, want 1: an unsubscribed applier must stop holding status down", got.Applied)
	}

	if got.LastErr != nil {
		t.Errorf("LastErr = %v, want nil once the remaining appliers are caught up", got.LastErr)
	}

	c.Publish(ctx, publication("t1", 2, "two"))

	if delivered := len(leaver.all()); delivered != 1 {
		t.Errorf("the unsubscribed applier received %d deliveries, want 1", delivered)
	}

	if names := rec.names(); len(names) != 2 {
		t.Errorf("the remaining applier received %v, want both publications", names)
	}
}

func TestCoordinatorUnsubscribeFromInsideAnApplierDoesNotDeadlock(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	var (
		rec         recorder
		unsubscribe func()
		once        sync.Once
	)

	unsubscribe = c.Register(func(fnCtx context.Context, current Decoded[coordDoc], previous *Decoded[coordDoc]) error {
		once.Do(unsubscribe)

		return rec.apply(fnCtx, current, previous)
	})
	defer unsubscribe()

	done := make(chan struct{})

	go func() {
		defer close(done)

		c.Publish(ctx, publication("t1", 1, "one"))
	}()

	waitFor(t, done, "the self-unsubscribing applier to return without deadlocking")

	c.Publish(ctx, publication("t1", 2, "two"))

	if names := rec.names(); len(names) != 1 || names[0] != "one" {
		t.Errorf("deliveries = %v, want only the one that was already in flight", names)
	}
}

func TestCoordinatorStatusIsSortedByTenant(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	for _, tenant := range []string{"t3", "t1", "t2"} {
		c.Publish(ctx, publication(tenant, 1, tenant))
	}

	got := c.Status()
	if len(got) != 3 {
		t.Fatalf("Status() = %#v, want three scopes", got)
	}

	for i, want := range []string{"t1", "t2", "t3"} {
		if got[i].Tenant != want {
			t.Fatalf("Status() tenants = %#v, want them sorted t1, t2, t3", got)
		}
	}
}

// TestCoordinatorRejectionAtRevisionZeroReadsAsConverged pins a hole this lane
// cannot close on its own. FC-7 freezes ApplyStatus.LastErr as "nil when
// Desired == Applied", and an applier that has accepted nothing reports Applied
// 0 — the very value a publication at Revision 0 desires. A rejected Revision-0
// document therefore reads back as a converged, error-free scope, and through
// the wave-1 facade EVERY publication is Revision 0, so an applier refusing a
// configuration is invisible on the only error surface FC-7 gives a consumer.
//
// Closing it means either letting LastErr outlive Desired == Applied, or giving
// Applied a "nothing accepted yet" value that is not a revision. Both change
// what FC-7 promises, so it is the orchestrator's call, not this lane's. The
// behaviour is pinned here instead of fixed: this test flips the day FC-7 is
// amended, which is exactly when it should.
func TestCoordinatorRejectionAtRevisionZeroReadsAsConverged(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	invocations := 0

	unsubscribe := c.Register(func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error {
		invocations++

		return errRejected
	})
	defer unsubscribe()

	c.Publish(ctx, publication("t1", 0, "zero"))

	if invocations != 1 {
		t.Fatalf("the applier ran %d times, want 1: without a real rejection this test proves nothing", invocations)
	}

	got := statusOf(t, c, "t1")
	if got.Desired != 0 || got.Applied != 0 {
		t.Fatalf("Status = %#v, want Desired 0 and Applied 0", got)
	}

	if got.LastErr != nil {
		t.Fatalf("Status.LastErr = %v, but FC-7 requires nil when Desired == Applied; changing this is a contract amendment, not a test fix", got.LastErr)
	}
}
