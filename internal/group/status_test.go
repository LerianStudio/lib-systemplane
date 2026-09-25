//go:build unit

package group

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"

	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/logguard"
	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/panicmetric"
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

// String renders one recorded line for a failure message. Every assertion in
// this file that cannot find what it expected dumps the recorded lines, and a
// bare struct print gives "{2 msg map[...]}" - the level as a naked integer
// and no cue which of the four coordinator reports the line is. That dump is
// the only evidence a rare failure leaves behind, so it is rendered, not
// printed.
func (l logLine) String() string {
	return fmt.Sprintf("%s %q %v", log.LevelName(l.level), l.msg, l.fields)
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

// assertPanicScopeLine pins the line that names WHICH scope and WHICH revision
// the panicking applier was handed. runtime.HandlePanicValue carries the
// recovered value and the stack but neither the tenant nor the revision, and in
// production mode it redacts even those, so without this second line an
// operator reading the log learns that something panicked and nothing about
// what stopped being applied. The recovered value stays out of it: redaction is
// the panic handler's job and duplicating the payload here would undo it.
func assertPanicScopeLine(t *testing.T, logger *recordingLogger, tenant string, revision int64) {
	t.Helper()

	line := logger.lineContaining(t, "apply function panicked")
	if line.level != log.LevelError {
		t.Errorf("the panic scope line logged at %s level, want error", log.LevelName(line.level))
	}

	if got := line.fields[constants.AttrKeyTenantID]; got != tenant {
		t.Errorf("%s = %v, want %q: the line must name the scope that stopped being applied", constants.AttrKeyTenantID, got, tenant)
	}

	if got, _ := line.fields["revision"].(int64); got != revision {
		t.Errorf("revision = %v, want %d", line.fields["revision"], revision)
	}

	for key, value := range line.fields {
		if text, _ := value.(string); strings.Contains(text, "boom") {
			t.Errorf("%s = %v, want no recovered value on this line: production redaction lives with the panic handler", key, value)
		}
	}
}

// coordNamespace and coordKey are the group every unit coordinator in this
// package is built for. They are distinctive so an assertion that reads them
// back out of a log line is reading what NewCoordinator was handed.
const (
	coordNamespace = "grpns"
	coordKey       = "grpkey"
)

// assertNamesTheGroup pins the two fields that say WHICH group a coordinator
// report is about. Without them a report reads "apply function panicked
// tenant.id=t1 revision=0" and an operator has nothing to act on. "keyname",
// not "key": the latter is an exact entry in lib-observability's
// sensitive-field list, and it is the same field name the engine's twin
// reports emit.
//
// It hangs off the recorder rather than taking a bare line so that a miss can
// dump every line with its fields. One run in twenty of the unit suite once
// failed all five call sites of this helper at once with "namespace = <nil>",
// and the investigation that followed (2026-09-24) reproduced it in none of
// 150 shuffled race runs and found no mechanism by reading: the four
// coordinator sites emit the field unconditionally and identically, and no
// other message in this package or in lib-observability's panic handler
// matches any substring lineContaining selects on. So the next occurrence has
// to carry its own evidence - which line was read, what else was recorded and
// with what fields - because a lone "<nil>" says only that the field was
// absent from the one line the assertion happened to read.
func (r *recordingLogger) assertNamesTheGroup(t *testing.T, line logLine, namespace, key string) {
	t.Helper()

	named := true

	if got := line.fields["namespace"]; got != namespace {
		t.Errorf("namespace = %v, want %q: the report must name the group", got, namespace)

		named = false
	}

	if got := line.fields["keyname"]; got != key {
		t.Errorf("keyname = %v, want %q: the report must name the group", got, key)

		named = false
	}

	if !named {
		t.Errorf("the line read was %v; every line recorded: %v", line, r.recorded())
	}
}

// assertDecodeFailureRendering pins how a document nobody could parse is
// reported: the decode cause as produced, naming the group. The library adds
// nothing to the line and withholds nothing from it.
func assertDecodeFailureRendering(t *testing.T, logger *recordingLogger) {
	t.Helper()

	line := logger.lineContaining(t, "failed to decode")
	logger.assertNamesTheGroup(t, line, coordNamespace, coordKey)

	if detail := fmt.Sprint(line.fields["error"]); !strings.Contains(detail, payloadMarker) {
		t.Errorf("error = %q, want the decode cause verbatim", detail)
	}
}

// TestNoLoggedFieldNameIsRedacted reads this package's own source and refuses
// any field name lib-observability erases. The coordinator's reports are what
// name the group that stopped being applied, so a log.String("key", …)
// slipping in here would hand an operator namespace=grpns key=[REDACTED].
// It runs in parallel: it only parses this package's source. Go pauses a
// t.Parallel() top-level test and resumes it only after every sequential
// top-level test in the package has returned, so it can never overlap the
// process-global toggles the sequential panic tests below set.
func TestNoLoggedFieldNameIsRedacted(t *testing.T) {
	t.Parallel()

	logguard.AssertNoneRedacted(t, ".")
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
	c := NewCoordinator[coordDoc](logger, coordNamespace, coordKey, true, Decode[coordDoc], nil)
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

	if !errors.Is(got.LastErr, ErrApplyPanicked) {
		t.Errorf("LastErr = %v, want ErrApplyPanicked: a consumer matches the panic with errors.Is, not by parsing the message", got.LastErr)
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

	assertPanicScopeLine(t, logger, "t1", 3)
}

// TestCoordinatorApplierPanicIsRedactedInProductionMode pins the other half of
// the panic path: in production mode the recovered value and the stack stay out
// of the log line, and out of Status with them. Status is the surface operators
// read and log, so republishing the value there would hand back in the clear
// exactly what the log line just redacted — and a panic value is whatever the
// hook was holding, routinely the decoded document with its endpoints and its
// credentials. Status still reports the rejection; only the payload is gone.
func TestCoordinatorApplierPanicIsRedactedInProductionMode(t *testing.T) {
	// Process-global. Safe while this test stays sequential: a t.Parallel()
	// top-level test only runs after every sequential one has returned, so no
	// parallel test can observe the toggle. What would break it is this test
	// itself calling t.Parallel(), or running parallel subtests under it.
	runtime.SetProductionMode(true)

	defer runtime.SetProductionMode(false)

	logger := newRecordingLogger()
	c := NewCoordinator[coordDoc](logger, coordNamespace, coordKey, true, Decode[coordDoc], nil)

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
	if !errors.Is(got.LastErr, ErrApplyPanicked) {
		t.Errorf("LastErr = %v, want ErrApplyPanicked recorded", got.LastErr)
	}

	if strings.Contains(got.LastErr.Error(), "boom") {
		t.Errorf("LastErr = %v, want no recovered value: Status would republish in the clear what the log line redacts", got.LastErr)
	}

	assertPanicScopeLine(t, logger, "t1", 3)
}

// TestCoordinatorAPanickingLoggerStillRecordsThePanic pins the observability
// half of a recovered applier panic against the consumer's own logger failing:
// the panic handler logs BEFORE it records the counter, the span event and the
// error report, so a logger that panics on that line used to take all three
// with it and the coordinator's own recovery then swallowed the unwind — a
// fleet whose hot reload stopped, with the panic counter flat.
//
// Process-global like the production-mode toggle above, and safe for the same
// reason: this test is sequential, so no t.Parallel() test overlaps it. It must
// not call t.Parallel() itself or run parallel subtests.
func TestCoordinatorAPanickingLoggerStillRecordsThePanic(t *testing.T) {
	counter := panicmetric.Install(t)

	c := NewCoordinator[coordDoc](&alwaysPanickingLogger{NopLogger: &log.NopLogger{}}, coordNamespace, coordKey, false, Decode[coordDoc], nil)

	unsubscribe := mustRegister(t, c, func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error {
		panic("the apply function exploded")
	})
	defer unsubscribe()

	publishWithoutPanicking(t, c, publication("t1", 1, "one"))

	// Both lines the broken logger drops are counted by log.Guard, then the
	// applier's panic under its own site.
	loggerPanic := panicmetric.Increment{Component: "log", Name: "Log"}
	want := []panicmetric.Increment{loggerPanic, loggerPanic, {Component: "systemplane", Name: "group.apply"}}

	if got := counter.Increments(); !slices.Equal(got, want) {
		t.Errorf("panic counter increments = %+v, want %+v", got, want)
	}

	if got := statusOf(t, c, "t1"); !errors.Is(got.LastErr, ErrApplyPanicked) {
		t.Errorf("LastErr = %v, want ErrApplyPanicked", got.LastErr)
	}
}

// TestCoordinatorApplierErrorIsLogged pins the operational half of a rejection:
// Status is a pull surface nobody reads at 3am, so an applier refusing a
// configuration must also reach the consumer's logger, naming the scope, the
// revision and the error the applier returned.
func TestCoordinatorApplierErrorIsLogged(t *testing.T) {
	logger := newRecordingLogger()
	c := NewCoordinator[coordDoc](logger, coordNamespace, coordKey, true, Decode[coordDoc], nil)

	// The applier names what it refused, which is what a consumer's
	// apply hook does when it wants the log to be actionable.
	rejection := fmt.Errorf("refused %s", payloadMarker)

	unsubscribe := mustRegister(t, c, func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error {
		return rejection
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

	logger.assertNamesTheGroup(t, line, coordNamespace, coordKey)
	assertRejectionRendering(t, logger, rejection)
}

// assertRejectionRendering is assertDecodeFailureRendering's twin for the cause
// an APPLIER returns: the rejection itself, which is also the error FC-7's
// Status keeps.
func assertRejectionRendering(t *testing.T, logger *recordingLogger, rejection error) {
	t.Helper()

	line := logger.lineContaining(t, "rejected")
	if err, _ := line.fields["error"].(error); !errors.Is(err, rejection) {
		t.Errorf("error = %v, want the applier's rejection verbatim", line.fields["error"])
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

// TestCoordinatorDecodeFailureIsRecordedAndNeverDelivered pins a published
// document nobody could parse: it never reaches an applier, the last good one
// stays replayable, the scope keeps reporting the failure, and the log names
// the group exactly once.
func TestCoordinatorDecodeFailureIsRecordedAndNeverDelivered(t *testing.T) {
	logger := newRecordingLogger()
	c := NewCoordinator[coordDoc](logger, coordNamespace, coordKey, false, rejectingDecode(payloadMarker), nil)
	ctx := context.Background()

	var rec recorder

	unsubscribe := mustRegister(t, c, rec.apply)
	defer unsubscribe()

	c.Publish(ctx, publication("t1", 1, "good"))
	c.Publish(ctx, publication("t1", 2, payloadMarker))

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

	assertDecodeFailureRendering(t, logger)

	// The last good publication must stay replayable for a later Register.
	var late recorder

	unsubscribeLate := mustRegister(t, c, late.apply)
	defer unsubscribeLate()

	if names := late.names(); len(names) != 1 || names[0] != "good" {
		t.Errorf("replay = %v, want the last decodable document", names)
	}

	// The replay hands revision 1 to a second applier, and that acceptance must
	// not read as convergence: the newest thing the scope observed is the
	// malformed revision 2, which nobody applied.
	got = statusOf(t, c, "t1")
	if got.Desired != 2 {
		t.Errorf("Desired after the replay = %d, want 2: the malformed revision is still the newest observation", got.Desired)
	}

	if got.LastErr == nil {
		t.Error("LastErr after the replay = nil, want the decode failure: replaying an older revision is not an acceptance of the newest one")
	}
}

// TestCoordinatorSupersededDecodeFailureIsStillLogged pins the one malformed
// document that used to vanish: a publication that fails to decode AND is
// superseded before it commits reaches no applier, records nothing on the scope
// (a newer observation already owns it) and so has only the log left to name it.
// Dropping that line left an operator with a document nobody could parse and no
// trace of it anywhere.
func TestCoordinatorSupersededDecodeFailureIsStillLogged(t *testing.T) {
	logger := newRecordingLogger()

	entered := make(chan struct{})
	release := make(chan struct{})

	// The malformed document parks inside the decoder, which runs outside the
	// coordinator's mutex, so the good publication behind it commits first and
	// the malformed one arrives at commit already superseded.
	decode := func(value any) (coordDoc, error) {
		doc, err := Decode[coordDoc](value)
		if err != nil {
			return doc, err
		}

		if doc.Name == "bad" {
			close(entered)
			<-release

			return coordDoc{}, errors.New("decode: refused bad")
		}

		return doc, nil
	}

	c := NewCoordinator[coordDoc](logger, coordNamespace, coordKey, true, decode, nil)
	ctx := context.Background()

	var rec recorder

	unsubscribe := mustRegister(t, c, rec.apply)
	defer unsubscribe()

	done := make(chan struct{})

	go func() {
		defer close(done)

		c.Publish(ctx, publication("t1", 1, "bad"))
	}()

	waitFor(t, entered, "the malformed publication to reach the decoder")

	c.Publish(ctx, publication("t1", 2, "good"))

	close(release)
	waitFor(t, done, "the superseded publication to commit")

	var lines int

	for _, line := range logger.recorded() {
		if strings.Contains(line.msg, "failed to decode") {
			lines++

			if got := line.fields[constants.AttrKeyTenantID]; got != "t1" {
				t.Errorf("%s = %v, want the scope the malformed document was published to", constants.AttrKeyTenantID, got)
			}

			if got, _ := line.fields["revision"].(int64); got != 1 {
				t.Errorf("revision = %v, want 1", line.fields["revision"])
			}
		}
	}

	if lines != 1 {
		t.Errorf("decode-failure lines = %d, want exactly 1: every malformed document is named once", lines)
	}

	if got := rec.names(); len(got) != 1 || got[0] != "good" {
		t.Errorf("deliveries = %v, want only the decodable document", got)
	}
}

// TestCoordinatorDecodeFailureOnAFreshScopeIsObserved pins A6 on a scope whose
// FIRST publication is the one that fails: the rejection counts as an
// observation, so a later registration neither spends a read on the seed nor
// decodes the same unparseable document a second time. Status still reports the
// scope and its error.
func TestCoordinatorDecodeFailureOnAFreshScopeIsObserved(t *testing.T) {
	var seeds int

	seed := func() (Publication, bool, error) {
		seeds++

		return Publication{}, false, nil
	}

	c := NewCoordinator[coordDoc](newRecordingLogger(), coordNamespace, coordKey, false, rejectingDecode("bad"), seed)

	c.Publish(context.Background(), publication("t1", 4, "bad"))

	var rec recorder

	unsubscribe := mustRegister(t, c, rec.apply)
	defer unsubscribe()

	if seeds != 0 {
		t.Errorf("seed taken %d times, want 0: a scope whose publication was rejected has been observed", seeds)
	}

	if got := rec.names(); len(got) != 0 {
		t.Errorf("deliveries = %v, want none: nothing decodable was ever published", got)
	}

	got := statusOf(t, c, "t1")
	if got.Desired != 4 {
		t.Errorf("Desired = %d, want 4", got.Desired)
	}

	if got.LastErr == nil {
		t.Error("LastErr = nil, want the decode failure")
	}

	// The replay of a scope holding no decodable document delivers nothing, and
	// it must record nothing either: per-applier bookkeeping is keyed by the
	// scope, so keying it on the zero observation's payload invents an entry for
	// a tenant that does not exist.
	if keys := applierScopeKeys(t, c); !slices.Equal(keys, []string{"t1"}) {
		t.Errorf(`applier scope keys = %q, want ["t1"]: bookkeeping is keyed by the scope, never by a payload`, keys)
	}

	// A document that DOES decode now delivers, which is the write-back half of
	// the same rule: an acceptance must land under the scope's own tenant and
	// leave no second key behind.
	c.Publish(context.Background(), publication("t1", 5, "good"))

	if got := rec.names(); len(got) != 1 || got[0] != "good" {
		t.Errorf("deliveries = %v, want the decodable document delivered once", got)
	}

	if keys := applierScopeKeys(t, c); !slices.Equal(keys, []string{"t1"}) {
		t.Errorf(`applier scope keys after a delivery = %q, want ["t1"]`, keys)
	}
}

// TestCoordinatorNullValueIsRejectedByTheCodecAndNeverDelivered pins the
// coordinator half of the typed group's null rule: a published null reaches
// the codec as a nil value, and a codec that refuses it (which is what the
// group's own decodePublished does for a struct-shaped document) turns that
// publication into a recorded rejection rather than a blank document handed to
// an applier. The coordinator itself never special-cases nil.
func TestCoordinatorNullValueIsRejectedByTheCodecAndNeverDelivered(t *testing.T) {
	refuseNull := func(value any) (coordDoc, error) {
		if value == nil {
			return coordDoc{}, errors.New("decode: null document")
		}

		return Decode[coordDoc](value)
	}

	c := NewCoordinator[coordDoc](newRecordingLogger(), coordNamespace, coordKey, false, refuseNull, nil)
	ctx := context.Background()

	var rec recorder

	unsubscribe := mustRegister(t, c, rec.apply)
	defer unsubscribe()

	c.Publish(ctx, publication("t1", 1, "good"))
	c.Publish(ctx, Publication{Tenant: "t1", Revision: 2, Value: nil})

	if got := rec.names(); len(got) != 1 || got[0] != "good" {
		t.Errorf("deliveries = %v, want only the decodable document: a null must never reach an applier", got)
	}

	got := statusOf(t, c, "t1")
	if got.Desired != 2 {
		t.Errorf("Desired = %d, want 2: a null rejected at decode still advances Desired", got.Desired)
	}

	if got.Applied != 1 {
		t.Errorf("Applied = %d, want 1: the last good revision stays applied", got.Applied)
	}

	if got.LastErr == nil {
		t.Error("LastErr = nil, want the refused null document")
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

// TestCoordinatorUnsubscribingTheLastApplierKeepsTheRejection pins A12 at the
// boundary where the convergence test reads as vacuously true: with nobody
// registered, "every applier has accepted the newest observation" holds by
// default, so clearing the scope's error on the way out erases a decode failure
// or a refusal that nothing ever applied — and the next reader is told a group
// nobody is applying is healthy. LastErr clears when an applier ACCEPTS, and
// leaving is not accepting.
func TestCoordinatorUnsubscribingTheLastApplierKeepsTheRejection(t *testing.T) {
	c := NewCoordinator[coordDoc](newRecordingLogger(), coordNamespace, coordKey, false, rejectingDecode("bad"), nil)

	c.Publish(context.Background(), publication("t1", 4, "bad"))

	var rec recorder

	unsubscribe := mustRegister(t, c, rec.apply)
	unsubscribe()

	if got := statusOf(t, c, "t1"); got.LastErr == nil {
		t.Errorf("Status after the only applier unsubscribed = %#v, want the decode failure still readable", got)
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

// TestCoordinatorLastApplierRejectingAndLeavingKeepsTheRejection covers the
// re-entrant half of A12 that TestCoordinatorUnsubscribingTheLastApplierKeepsTheRejection
// leaves open: the only applier unsubscribes itself from inside its own
// delivery — a pattern OnApply's godoc explicitly blesses — and then refuses the
// document. Its rejection is written and the convergence test that follows runs
// against an empty applier list, where "every applier accepted" is vacuously
// true. Clearing there tells the next reader a group nobody applied is healthy,
// and a crashed hook becomes indistinguishable from a successful one.
func TestCoordinatorLastApplierRejectingAndLeavingKeepsTheRejection(t *testing.T) {
	// A consumer that configured no logger is the common case in tests and the
	// default in a service that has not wired observability yet. The panic
	// handler serializes the recovered value through that logger, so a nil one
	// has to be absorbed at the boundary: reaching the assertions is the proof
	// that Publish returned instead of unwinding into the publisher.
	cases := []struct {
		name   string
		logger log.Logger
		apply  func() error
		want   error
	}{
		{name: "returns an error", logger: newRecordingLogger(), apply: func() error { return errRejected }, want: errRejected},
		{name: "panics", logger: newRecordingLogger(), apply: func() error { panic("boom") }, want: ErrApplyPanicked},
		{name: "panics with no logger configured", logger: nil, apply: func() error { panic("boom") }, want: ErrApplyPanicked},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewCoordinator[coordDoc](tc.logger, coordNamespace, coordKey, false, Decode[coordDoc], nil)

			var (
				unsubscribe func()
				once        sync.Once
			)

			unsubscribe = mustRegister(t, c, func(context.Context, Decoded[coordDoc], *Decoded[coordDoc]) error {
				once.Do(unsubscribe)

				return tc.apply()
			})
			defer unsubscribe()

			c.Publish(context.Background(), publication("t1", 4, "four"))

			got := statusOf(t, c, "t1")
			if !errors.Is(got.LastErr, tc.want) {
				t.Fatalf("Status after the only applier rejected and left = %#v, want LastErr matching %v", got, tc.want)
			}

			if strings.Contains(got.LastErr.Error(), "boom") {
				t.Errorf("LastErr = %q, want the panic value kept out of it: a panicking hook is routinely holding the decoded document with its endpoints and credentials", got.LastErr)
			}
		})
	}
}

// groupReportMessages are the four report lines a coordinator writes about a
// scope: two decode failures, an apply rejection and an apply panic.
var groupReportMessages = []string{
	"systemplane.group: seeded document failed to decode",
	"systemplane.group: published document failed to decode",
	"systemplane.group: apply function rejected the published document",
	"systemplane.group: apply function panicked",
}

// driveEveryGroupReport makes c write each of groupReportMessages once, every
// one about tenant: a seeded document that fails to decode, then a published
// one, then a document the applier rejects and one it panics on.
func driveEveryGroupReport(t *testing.T, c *Coordinator[coordDoc], tenant string) {
	t.Helper()

	unsubscribe := mustRegister(t, c, func(_ context.Context, current Decoded[coordDoc], _ *Decoded[coordDoc]) error {
		switch current.Value.Name {
		case "boom":
			panic("boom")
		case "no":
			return errors.New("refused")
		}

		return nil
	})
	defer unsubscribe()

	ctx := context.Background()
	c.Publish(ctx, publication(tenant, 2, "bad"))
	c.Publish(ctx, publication(tenant, 3, "no"))
	c.Publish(ctx, publication(tenant, 4, "boom"))
}

// TestMultiTenantGroupReportsNameTheTenant pins the multi-tenant half: every
// report names the publication's tenant, and a publication that names none is
// stamped unresolved rather than rendering an empty tenant.id that reads like a
// single-tenant line.
func TestMultiTenantGroupReportsNameTheTenant(t *testing.T) {
	for _, tc := range []struct {
		name, tenant, want string
	}{
		{name: "resolved", tenant: "acme", want: "acme"},
		{name: "unresolved", tenant: "", want: "unresolved"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := newRecordingLogger()
			c := NewCoordinator[coordDoc](logger, coordNamespace, coordKey, true, rejectingDecode("bad"),
				seedOf(publication(tc.tenant, 1, "bad")))

			driveEveryGroupReport(t, c, tc.tenant)

			for _, msg := range groupReportMessages {
				line := logger.lineContaining(t, msg)
				if got := line.fields[constants.AttrKeyTenantID]; got != tc.want {
					t.Errorf("%q carries %s = %v, want %q", msg, constants.AttrKeyTenantID, got, tc.want)
				}
			}
		})
	}
}

// TestSingleTenantGroupReportsCarryNoTenant pins the single-tenant half of the
// tenant stamp: a single-tenant scope has no tenant, so an empty tenant.id on
// its reports reads like a value that went missing. None of the four reports
// may carry the key at all.
func TestSingleTenantGroupReportsCarryNoTenant(t *testing.T) {
	logger := newRecordingLogger()
	c := NewCoordinator[coordDoc](logger, coordNamespace, coordKey, false, rejectingDecode("bad"), seedOf(publication("", 1, "bad")))

	driveEveryGroupReport(t, c, "")

	for _, msg := range groupReportMessages {
		line := logger.lineContaining(t, msg)
		if got, ok := line.fields[constants.AttrKeyTenantID]; ok {
			t.Errorf("%q carries %s = %q, want no tenant field on a single-tenant report", msg, constants.AttrKeyTenantID, got)
		}
	}
}
