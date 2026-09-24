//go:build unit

package systemplane_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
	systemplane "github.com/LerianStudio/lib-systemplane/v4"
)

// redactProbeMarker is the sentinel a panicking applier carries inside the
// document it was handed. It is the shape the matcher pilot stores in a
// RedactFull group — hmac_secret, secret_access_key, a tenant API key — and the
// only thing an assertion here can match on that a look-alike could not produce.
const redactProbeMarker = "probe-secret-Xk93Qz"

// redactRecorder is a consumer logger that keeps every line, so a test can
// prove what did and did not reach it. It implements systemplane.Logger, the
// boundary interface WithLogger takes.
type redactRecorder struct {
	mu    sync.Mutex
	lines []string
	// levels runs parallel to lines, so a failure dump can say at what level
	// each line went out without the level leading the rendered line that
	// lineStarting matches by prefix.
	levels []int
	value  map[string]any
}

func (r *redactRecorder) Log(_ context.Context, level int, msg string, fields ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rendered := msg

	for _, f := range log.Fields(fields...) {
		rendered += fmt.Sprintf(" %s=%v", f.Key, f.Value)

		if f.Key == "value" && strings.Contains(msg, "panic recovered") {
			if r.value == nil {
				r.value = map[string]any{}
			}

			r.value[f.Key] = f.Value
		}
	}

	r.lines = append(r.lines, rendered)
	r.levels = append(r.levels, level)
}

// dump renders every recorded line with its level, for a failure message.
func (r *redactRecorder) dump() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var b strings.Builder

	for i, line := range r.lines {
		fmt.Fprintf(&b, "\n  [%s] %s", log.LevelName(r.levels[i]), line)
	}

	return b.String()
}

func (r *redactRecorder) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.lines...)
}

func (r *redactRecorder) panicValue(t *testing.T) string {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	v, ok := r.value["value"]
	if !ok {
		t.Fatalf("no \"panic recovered\" line carries a value field; logged %v", r.lines)
	}

	return fmt.Sprint(v)
}

// redactGroup binds a group under redaction whose applier fails the way apply
// hooks fail — naming the document it was handed — and returns everything the
// consumer's logger saw. The failure mode is the caller's, because a panicking
// hook and one that returns an error reach two different log lines and both of
// them used to carry the document.
func redactGroup(
	t *testing.T,
	redaction systemplane.RedactPolicy,
	apply func(context.Context, systemplane.Applied[groupConfig]) error,
) *redactRecorder {
	t.Helper()

	rec := &redactRecorder{}

	c, err := systemplane.NewForTesting(newGroupMemoryStore(),
		systemplane.WithDebounce(0), systemplane.WithLogger(rec))
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	// Not discarded: every applier this helper serves either panics or refuses,
	// and a worker killed by a panicking applier is exactly what Close reports
	// as ErrCloseTimeout. Swallowing it here would hide the one failure the
	// redaction tests are most likely to cause.
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close after a panicking applier: %v, want nil", err)
		}
	})

	defaults := groupConfig{Name: redactProbeMarker, Retries: 1}

	g, err := systemplane.Bind(c, redactGroupNamespace, redactGroupKeyName, defaults, nil,
		systemplane.WithRedaction(redaction))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	startGroupClient(t, c)

	unsubscribe, err := g.OnApply(apply)
	if err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	t.Cleanup(unsubscribe)

	return rec
}

// redactPolicyCases is the whole enum, every time. RedactMask was the hole the
// Phase 2 review found: the gate is "any policy at all", but end to end only
// RedactFull and RedactNone were ever exercised, so narrowing the gate to
// RedactFull alone left the suite green and would have published a masked
// group's document at ERROR.
var redactPolicyCases = []struct {
	name         string
	policy       systemplane.RedactPolicy
	wantVerbatim bool
}{
	{name: "none", policy: systemplane.RedactNone, wantVerbatim: true},
	{name: "mask", policy: systemplane.RedactMask, wantVerbatim: false},
	{name: "full", policy: systemplane.RedactFull, wantVerbatim: false},
}

// TestGroupApplierPanicHonorsTheKeyRedaction is the end-to-end probe through
// the public Bind/OnApply surface. A redacted group is exactly where a consumer
// puts the credentials it never wants logged — the matcher pilot keeps
// hmac_secret, secret_access_key and a tenant API key in one — and an apply
// hook that panics naming what it could not apply is the ordinary shape of a
// panic. lib-observability's canonical handler logs log.Any("value", recovered)
// whenever production mode is off, and off is what ships, so the whole document
// reached ERROR in the clear.
func TestGroupApplierPanicHonorsTheKeyRedaction(t *testing.T) {
	t.Parallel()

	for _, tt := range redactPolicyCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := redactGroup(t, tt.policy, func(_ context.Context, a systemplane.Applied[groupConfig]) error {
				panic(fmt.Sprintf("cannot apply %+v", a.Value))
			})

			rec.assertNamesTheGroup(t, rec.lineStarting(t, "systemplane.group: apply function panicked"))

			value := rec.panicValue(t)

			if tt.wantVerbatim {
				if !strings.Contains(value, redactProbeMarker) {
					t.Errorf("panic value field = %q, want the panic value verbatim for an unredacted group", value)
				}

				return
			}

			assertNoLineCarriesTheDocument(t, rec)

			if !strings.Contains(value, "(string,") || !strings.Contains(value, "value withheld") {
				t.Errorf("panic value field = %q, want the panic value's type and no document", value)
			}
		})
	}
}

// TestGroupApplierErrorHonorsTheKeyRedaction is the returned-error twin. An
// apply hook that REJECTS a document names it as freely as one that panics —
// fmt.Errorf("cannot apply %+v", a.Value) — and that error reached a second
// ERROR line eleven lines below the one the panic gate closed. FC-7's Status
// keeps the error untouched: that is the consumer's own surface, not a log sink.
func TestGroupApplierErrorHonorsTheKeyRedaction(t *testing.T) {
	t.Parallel()

	for _, tt := range redactPolicyCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := redactGroup(t, tt.policy, func(_ context.Context, a systemplane.Applied[groupConfig]) error {
				return fmt.Errorf("cannot apply %+v", a.Value)
			})

			line := rec.lineStarting(t, "systemplane.group: apply function rejected the published document")

			rec.assertNamesTheGroup(t, line)

			if tt.wantVerbatim {
				if !strings.Contains(line, redactProbeMarker) {
					t.Errorf("rejection line = %q, want the error verbatim for an unredacted group", line)
				}

				return
			}

			assertNoLineCarriesTheDocument(t, rec)

			if !strings.Contains(line, "(*errors.errorString)") {
				t.Errorf("rejection line = %q, want the cause named by its dynamic type", line)
			}
		})
	}
}

// redactGroupNamespace and redactGroupKeyName are the group redactGroup binds.
// Two distinct non-empty literals, so an assertion that reads them back out of
// a coordinator report is reading what Bind actually handed NewCoordinator.
const (
	redactGroupNamespace = "runtime"
	redactGroupKeyName   = "ingest"
)

// lineStarting returns the last recorded line whose message is msg, and fails
// the test when nothing carries it.
func (r *redactRecorder) lineStarting(t *testing.T, msg string) string {
	t.Helper()

	var found string

	for _, line := range r.recorded() {
		if strings.HasPrefix(line, msg) {
			found = line
		}
	}

	if found == "" {
		t.Fatalf("no %q line was logged; logged %v", msg, r.recorded())
	}

	return found
}

// assertNamesTheGroup pins the identity Bind hands the coordinator. Nothing
// else does: swap the namespace and the key arguments at the NewCoordinator
// call and the whole unit suite stays green, while every report a redacted
// group produces names the wrong group — and for a redacted group that report
// is all an operator gets, because the document that would have identified it
// is exactly what redaction withheld.
//
// It hangs off the recorder, like its internal/group twin, so a miss dumps
// every recorded line with its level and fields rather than only the one read.
func (r *redactRecorder) assertNamesTheGroup(t *testing.T, line string) {
	t.Helper()

	// The leading space is load-bearing: it keeps "keyname=" from matching
	// inside the rendered value of some other field.
	for _, want := range []string{" namespace=" + redactGroupNamespace, " keyname=" + redactGroupKeyName} {
		if !strings.Contains(line, want) {
			t.Errorf("line = %q, want it to carry %q: the report must name the group Bind was given; every line recorded:%s",
				line, want, r.dump())
		}
	}
}

// assertNoLineCarriesTheDocument is the assertion both tests exist for.
func assertNoLineCarriesTheDocument(t *testing.T, rec *redactRecorder) {
	t.Helper()

	for _, line := range rec.recorded() {
		if strings.Contains(line, redactProbeMarker) {
			t.Errorf("a log line carries the document of a redacted group: %s", line)
		}
	}
}
