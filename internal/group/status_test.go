//go:build unit

package group

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
)

var errRejected = errors.New("applier rejected the document")

// logLine is one line the coordinator wrote. The fields are normalized through
// log.Fields because the two call sites shape them differently: the coordinator
// hands one []log.Field, while the panic handler in lib-observability passes
// several separate Fields.
type logLine struct {
	level  int
	msg    string
	fields map[string]any
}

// recordingLogger keeps the lines the coordinator writes so a test can prove a
// rejection and a recovered panic reached the logger as well as ApplyStatus.
type recordingLogger struct {
	*log.NopLogger

	mu    sync.Mutex
	lines []logLine
}

func (r *recordingLogger) Log(_ context.Context, level int, msg string, fields ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	line := logLine{level: level, msg: msg, fields: map[string]any{}}
	for _, f := range log.Fields(fields...) {
		line.fields[f.Key] = f.Value
	}

	r.lines = append(r.lines, line)
}

func (r *recordingLogger) recorded() []logLine {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]logLine(nil), r.lines...)
}

// lineContaining returns the recorded line whose message contains want, and
// fails the test when nothing matched.
func (r *recordingLogger) lineContaining(t *testing.T, want string) logLine {
	t.Helper()

	for _, line := range r.recorded() {
		if strings.Contains(line.msg, want) {
			return line
		}
	}

	t.Fatalf("logged %v, want a line whose message contains %q", r.recorded(), want)

	return logLine{}
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

	unsubscribe := mustRegister(t, c, func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error {
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

	unsubscribe := mustRegister(t, c, func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error {
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

	// The recovered value is whatever the hook was holding — routinely the
	// decoded document, with whatever endpoints and credentials it carries —
	// and LastErr is read, logged and surfaced by operators. It stays in the
	// panic log line, which redacts it in production; it never goes in here.
	if strings.Contains(got.LastErr.Error(), "boom") {
		t.Errorf("LastErr = %v, want no recovered value in the message: that undoes the redaction the panic log applies", got.LastErr)
	}

	line := logger.lineContaining(t, "panic recovered")
	if line.level != log.LevelError {
		t.Errorf("the recovered panic logged at %s level, want error", log.LevelName(line.level))
	}

	if line.fields["source"] != "group.apply" {
		t.Errorf("source = %v, want group.apply: the line must name the hook that panicked", line.fields["source"])
	}

	if value, _ := line.fields["value"].(string); value != "boom" {
		t.Errorf("value = %v, want the recovered value", line.fields["value"])
	}

	if stack, _ := line.fields["stack_trace"].(string); stack == "" {
		t.Error("stack_trace is empty, want the panicking goroutine's stack")
	}
}

// TestCoordinatorApplierPanicIsRedactedInProductionMode pins the other half of
// the panic path: in production mode the recovered value and the stack stay out
// of the log line, and out of Status with them. Status is the surface operators
// read and log, so republishing the value there would hand back in the clear
// exactly what the log line just redacted — and a panic value is whatever the
// hook was holding, routinely the decoded document with its endpoints and its
// credentials. Status still reports the rejection; only the payload is gone.
func TestCoordinatorApplierPanicIsRedactedInProductionMode(t *testing.T) {
	runtime.SetProductionMode(true)

	defer runtime.SetProductionMode(false)

	logger := newRecordingLogger()
	c := NewCoordinator[coordDoc](logger, Decode[coordDoc], nil)

	unsubscribe := mustRegister(t, c, func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error {
		panic("boom")
	})
	defer unsubscribe()

	c.Publish(context.Background(), publication("t1", 3, "three"))

	line := logger.lineContaining(t, "panic recovered")
	if value, _ := line.fields["value"].(string); !strings.Contains(value, "redacted") {
		t.Errorf("value = %v, want the redacted placeholder in production mode", line.fields["value"])
	}

	if _, logged := line.fields["stack_trace"]; logged {
		t.Error("stack_trace was logged in production mode, want it withheld")
	}

	got := statusOf(t, c, "t1")
	if got.LastErr == nil || !strings.Contains(got.LastErr.Error(), "panicked") {
		t.Errorf("LastErr = %v, want the rejection recorded", got.LastErr)
	}

	if strings.Contains(got.LastErr.Error(), "boom") {
		t.Errorf("LastErr = %v, want no recovered value: Status would republish in the clear what the log line redacts", got.LastErr)
	}
}

// TestCoordinatorApplierErrorIsLogged pins the operational half of a rejection:
// Status is a pull surface nobody reads at 3am, so an applier refusing a
// configuration must also reach the consumer's logger, naming the scope, the
// revision and the error.
func TestCoordinatorApplierErrorIsLogged(t *testing.T) {
	logger := newRecordingLogger()
	c := NewCoordinator[coordDoc](logger, Decode[coordDoc], nil)

	unsubscribe := mustRegister(t, c, func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error {
		return errRejected
	})
	defer unsubscribe()

	c.Publish(context.Background(), publication("t1", 5, "five"))

	line := logger.lineContaining(t, "rejected")
	if line.level != log.LevelError {
		t.Errorf("the rejection logged at %s level, want error", log.LevelName(line.level))
	}

	if got := line.fields[constants.AttrKeyTenantID]; got != "t1" {
		t.Errorf("%s = %v, want the rejecting scope", constants.AttrKeyTenantID, got)
	}

	if revision, _ := line.fields["revision"].(int64); revision != 5 {
		t.Errorf("revision = %v, want 5", line.fields["revision"])
	}

	if err, _ := line.fields["error"].(error); !errors.Is(err, errRejected) {
		t.Errorf("error = %v, want the applier's rejection", line.fields["error"])
	}
}

func TestCoordinatorRejectionLeavesPreviousUntouched(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	var rec recorder

	unsubscribe := mustRegister(t, c, func(fnCtx context.Context, current Decoded[coordDoc], previous *Decoded[coordDoc]) error {
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

	unsubscribe := mustRegister(t, c, func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error {
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

	unsubscribe := mustRegister(t, c, rec.apply)
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

	unsubscribeLate := mustRegister(t, c, late.apply)
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

	unsubscribeA := mustRegister(t, c, accepting)
	defer unsubscribeA()

	unsubscribeB := mustRegister(t, c, picky)
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

	unsubscribe := mustRegister(t, c, func(_ context.Context, current Decoded[coordDoc], _ *Decoded[coordDoc]) error {
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

	unsubscribeKeeper := mustRegister(t, c, rec.apply)
	defer unsubscribeKeeper()

	unsubscribeLeaver := mustRegister(t, c, func(fnCtx context.Context, current Decoded[coordDoc], previous *Decoded[coordDoc]) error {
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

	unsubscribe = mustRegister(t, c, func(fnCtx context.Context, current Decoded[coordDoc], previous *Decoded[coordDoc]) error {
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

// TestCoordinatorRejectionAtRevisionZeroStaysVisible drives the case the
// wave-1 facade makes universal: every publication it makes carries Revision 0,
// so an applier that has accepted nothing reports the very revision the scope
// desires. Convergence is decided by which observation each applier accepted,
// never by that arithmetic, so the rejection stays visible until the applier
// accepts something (A12) — otherwise the only error surface FC-7 gives a
// consumer is erased the instant it is written.
func TestCoordinatorRejectionAtRevisionZeroStaysVisible(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	invocations := 0
	reject := true

	unsubscribe := mustRegister(t, c, func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error {
		invocations++

		if reject {
			return errRejected
		}

		return nil
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

	if !errors.Is(got.LastErr, errRejected) {
		t.Fatalf("Status.LastErr = %v, want the rejection to survive Desired == Applied == 0", got.LastErr)
	}

	// Only an acceptance clears it.
	reject = false

	c.Publish(ctx, publication("t1", 0, "zero again"))

	if got := statusOf(t, c, "t1"); got.LastErr != nil {
		t.Errorf("Status.LastErr = %v, want nil once the applier accepted a later publication", got.LastErr)
	}
}

// TestCoordinatorAppliedIsTheOldestObservationNotTheLowestRevision drives a
// delete past two appliers where one refuses it. The refusing applier still has
// the pre-delete document in force, so the scope is applied at that revision —
// reporting the delete's Revision 0 because it is the lower number would call a
// half-applied delete convergence.
func TestCoordinatorAppliedIsTheOldestObservationNotTheLowestRevision(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	accepting := func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error { return nil }
	refusesTheDelete := func(_ context.Context, current Decoded[coordDoc], _ *Decoded[coordDoc]) error {
		if current.Revision == 0 {
			return errRejected
		}

		return nil
	}

	unsubscribeA := mustRegister(t, c, accepting)
	defer unsubscribeA()

	unsubscribeB := mustRegister(t, c, refusesTheDelete)
	defer unsubscribeB()

	c.Publish(ctx, publication("t1", 7, "seven"))
	c.Publish(ctx, publication("t1", 0, "deleted"))

	got := statusOf(t, c, "t1")
	if got.Desired != 0 {
		t.Errorf("Desired = %d, want 0: a delete is the newest state of the scope", got.Desired)
	}

	if got.Applied != 7 {
		t.Errorf("Applied = %d, want 7: the applier furthest behind still has revision 7 in force", got.Applied)
	}

	if !errors.Is(got.LastErr, errRejected) {
		t.Errorf("LastErr = %v, want the refused delete", got.LastErr)
	}
}
