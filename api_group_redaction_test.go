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

// redactProbeSecret is the sentinel a panicking applier carries inside the
// document it was handed. It is the shape the matcher pilot stores in a
// RedactFull group — hmac_secret, secret_access_key, a tenant API key — and the
// only thing an assertion here can match on that a look-alike could not produce.
const redactProbeSecret = "probe-secret-Xk93Qz"

// redactRecorder is a consumer logger that keeps every line, so a test can
// prove what did and did not reach it. It implements systemplane.Logger, the
// boundary interface WithLogger takes.
type redactRecorder struct {
	mu    sync.Mutex
	lines []string
	value map[string]any
}

func (r *redactRecorder) Log(_ context.Context, _ int, msg string, fields ...any) {
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

// redactPanickingGroup binds a group whose applier panics naming the document
// it was handed, and returns everything the consumer's logger saw.
func redactPanickingGroup(t *testing.T, redaction systemplane.RedactPolicy) *redactRecorder {
	t.Helper()

	rec := &redactRecorder{}

	c, err := systemplane.NewForTesting(newGroupMemoryStore(),
		systemplane.WithDebounce(0), systemplane.WithLogger(rec))
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	defaults := groupConfig{Name: redactProbeSecret, Retries: 1}

	g, err := systemplane.Bind(c, "runtime", "ingest", defaults, nil,
		systemplane.WithRedaction(redaction))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	startGroupClient(t, c)

	unsubscribe, err := g.OnApply(func(_ context.Context, a systemplane.Applied[groupConfig]) error {
		panic(fmt.Sprintf("cannot apply %+v", a.Value))
	})
	if err != nil {
		t.Fatalf("OnApply: %v", err)
	}

	t.Cleanup(unsubscribe)

	return rec
}

// TestGroupApplierPanicOnARedactedKeyWithholdsTheDocument is the end-to-end
// probe through the public Bind/OnApply surface. A group registered
// RedactFull is exactly where a consumer puts the credentials it never wants
// logged — the matcher pilot keeps hmac_secret, secret_access_key and a tenant
// API key in one — and an apply hook that panics naming what it could not
// apply is the ordinary shape of a panic. lib-observability's canonical
// handler logs log.Any("value", recovered) whenever production mode is off,
// and off is what ships, so the whole document reached ERROR in the clear.
func TestGroupApplierPanicOnARedactedKeyWithholdsTheDocument(t *testing.T) {
	t.Parallel()

	rec := redactPanickingGroup(t, systemplane.RedactFull)

	for _, line := range rec.recorded() {
		if strings.Contains(line, redactProbeSecret) {
			t.Errorf("a log line carries the document of a redacted group: %s", line)
		}
	}

	value := rec.panicValue(t)
	if !strings.Contains(value, "(string,") || !strings.Contains(value, "value withheld") {
		t.Errorf("panic value field = %q, want the panic value's type and no document", value)
	}
}

// TestGroupApplierPanicOnAnUnredactedKeyIsReportedVerbatim is the twin: the
// withholding is the redaction policy speaking, not a blanket loss of the one
// thing that tells two panicking hooks apart.
func TestGroupApplierPanicOnAnUnredactedKeyIsReportedVerbatim(t *testing.T) {
	t.Parallel()

	rec := redactPanickingGroup(t, systemplane.RedactNone)

	if value := rec.panicValue(t); !strings.Contains(value, redactProbeSecret) {
		t.Errorf("panic value field = %q, want the panic value verbatim for an unredacted group", value)
	}
}
