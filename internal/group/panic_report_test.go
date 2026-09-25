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

// payloadMarker is the sentinel a published document carries: an applier panics
// with it, and it is also the name of the document the decode-failure tests
// refuse, so the rejection's own message quotes it the way encoding/json quotes
// a row it cannot parse. Nothing but the real payload can produce it, so an
// assertion that sees it is an assertion about the payload itself.
const payloadMarker = "probe-payload-Vt71Qm"

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

// TestApplierPanicIsReportedVerbatim pins that a recovered applier panic
// reaches lib-observability's handler with the value it was raised with, on
// the log line and on the span alike. This package substitutes nothing: what a
// panic report renders, and whether production mode withholds it, is the
// handler's decision.
func TestApplierPanicIsReportedVerbatim(t *testing.T) {
	logger := newRecordingLogger()
	span := &recordingSpan{}

	c := NewCoordinator[coordDoc](logger, coordNamespace, coordKey, false, Decode[coordDoc], nil)

	unsubscribe := mustRegister(t, c, func(_ context.Context, current Decoded[coordDoc], _ *Decoded[coordDoc]) error {
		panic(fmt.Sprintf("cannot apply %+v", current.Value))
	})
	defer unsubscribe()

	c.Publish(trace.ContextWithSpan(context.Background(), span), publication("t1", 4, payloadMarker))

	logger.assertNamesTheGroup(t, logger.lineContaining(t, "apply function panicked"), coordNamespace, coordKey)

	reported := logger.lineContaining(t, "panic recovered")
	if value := fmt.Sprint(reported.fields["value"]); !strings.Contains(value, payloadMarker) {
		t.Errorf("panic value field = %q, want the panic value verbatim", value)
	}

	if !strings.Contains(strings.Join(span.recorded(), "\n"), payloadMarker) {
		t.Errorf("span recorded %v, want the panic value verbatim", span.recorded())
	}
}
