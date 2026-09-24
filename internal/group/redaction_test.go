//go:build unit

package group

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// redactionMarker is the sentinel the payload carries: an applier panics with
// it, and it is also the name of the document the decode-failure tests refuse,
// so the rejection's own message quotes it the way encoding/json quotes a row
// it cannot parse. Nothing but the real payload can produce it, so an
// assertion that never sees it is an assertion about the payload itself.
const redactionMarker = "probe-secret-Vt71Qm"

// recordingSpan is a span that records, so a test can read what the panic
// handler stamped on it. lib-observability writes the panic value into the
// span event's panic.value attribute and into the error it records, both of
// them exports a tracing backend keeps, and neither of them covered by an
// assertion on the log lines alone.
type recordingSpan struct {
	noop.Span

	mu      sync.Mutex
	written []string
}

func (s *recordingSpan) IsRecording() bool { return true }

func (s *recordingSpan) AddEvent(name string, opts ...trace.EventOption) {
	cfg := trace.NewEventConfig(opts...)

	rendered := name
	for _, attr := range cfg.Attributes() {
		rendered += fmt.Sprintf(" %s=%s", attr.Key, attr.Value.Emit())
	}

	s.write(rendered)
}

func (s *recordingSpan) RecordError(err error, _ ...trace.EventOption) {
	s.write(err.Error())
}

func (s *recordingSpan) write(line string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.written = append(s.written, line)
}

func (s *recordingSpan) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.written...)
}

// panickingApply publishes one document into a coordinator whose only applier
// panics naming what it was handed, and returns everything the logger and the
// span saw.
func panickingApply(t *testing.T, redacted bool) (*recordingLogger, *recordingSpan) {
	t.Helper()

	logger := newRecordingLogger()
	span := &recordingSpan{}

	c := NewCoordinator[coordDoc](logger, coordNamespace, coordKey, redacted, false, Decode[coordDoc], nil)

	unsubscribe := mustRegister(t, c, func(_ context.Context, current Decoded[coordDoc], _ *Decoded[coordDoc]) error {
		panic(fmt.Sprintf("cannot apply %+v", current.Value))
	})
	defer unsubscribe()

	c.Publish(trace.ContextWithSpan(context.Background(), span), publication("t1", 4, redactionMarker))

	return logger, span
}

// TestApplierPanicOnARedactedGroupWithholdsTheDocument pins what a group
// registered with a redaction policy costs a panicking applier: the document
// itself. lib-observability's canonical handler logs log.Any("value",
// recovered) and stamps the same rendering on the span event and the recorded
// error whenever production mode is off, and off is what it ships, so a hook
// that panics naming what it could not apply published the whole document —
// for the matcher pilot, an hmac secret and a tenant API key — at ERROR and
// into the trace.
func TestApplierPanicOnARedactedGroupWithholdsTheDocument(t *testing.T) {
	logger, span := panickingApply(t, true)

	for _, line := range logger.recorded() {
		for key, value := range line.fields {
			if text := fmt.Sprint(value); strings.Contains(text, redactionMarker) {
				t.Errorf("log field %s carries the document of a redacted group: %s", key, text)
			}
		}

		if strings.Contains(line.msg, redactionMarker) {
			t.Errorf("a log message carries the document of a redacted group: %s", line.msg)
		}
	}

	for _, written := range span.recorded() {
		if strings.Contains(written, redactionMarker) {
			t.Errorf("the span carries the document of a redacted group: %s", written)
		}
	}

	// The document is withheld, so these two fields are all that is left to
	// say which group stopped being applied.
	logger.assertNamesTheGroup(t, logger.lineContaining(t, "apply function panicked"), coordNamespace, coordKey)

	reported := logger.lineContaining(t, "panic recovered")

	value := fmt.Sprint(reported.fields["value"])
	if !strings.Contains(value, "(string,") || !strings.Contains(value, "value withheld") {
		t.Errorf("panic value field = %q, want the panic value's type and no document", value)
	}
}

// TestApplierPanicOnAnUnredactedGroupIsReportedVerbatim is the twin. The
// withholding is the key's registration speaking, not a blanket loss of the
// one thing that tells two panicking hooks apart.
func TestApplierPanicOnAnUnredactedGroupIsReportedVerbatim(t *testing.T) {
	logger, span := panickingApply(t, false)

	reported := logger.lineContaining(t, "panic recovered")
	if value := fmt.Sprint(reported.fields["value"]); !strings.Contains(value, redactionMarker) {
		t.Errorf("panic value field = %q, want the panic value verbatim for an unredacted group", value)
	}

	if !strings.Contains(strings.Join(span.recorded(), "\n"), redactionMarker) {
		t.Errorf("span recorded %v, want the panic value verbatim for an unredacted group", span.recorded())
	}
}
