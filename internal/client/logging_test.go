//go:build unit

package client

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/redaction"
)

// fields unwraps the structured attributes of a recorded line. The logger
// interface takes ...any, so a call site may pass Fields one by one or as a
// slice; both shapes are flattened here.
func (l logLine) structured() []log.Field {
	out := make([]log.Field, 0, len(l.fields))

	for _, arg := range l.fields {
		switch v := arg.(type) {
		case []log.Field:
			out = append(out, v...)
		case log.Field:
			out = append(out, v)
		}
	}

	return out
}

// TestLogLinesNameTheKeyUnderKeyname pins the field name the client publishes
// the configuration key under. "key" is an exact entry in lib-observability's
// default sensitive-field list, so log.String("key", ...) renders as
// key=[REDACTED] and the line reaches the operator naming the namespace and
// withholding the one thing it exists to publish. internal/engine already uses
// "keyname" for the same value; this guard keeps the client from drifting back.
func TestLogLinesNameTheKeyUnderKeyname(t *testing.T) {
	if redaction.IsSensitiveField("keyname") {
		t.Fatal(`lib-observability now redacts "keyname" too: every line below ships its key as [REDACTED]`)
	}

	m := newMemStore(false)
	logger := &recordingLogger{}
	c := newSingleTenantClientWithLogger(t, m, logger)

	validator := func(_ context.Context, value any) error {
		if value == rejectedSecret {
			return errSchemeRefused
		}

		return nil
	}

	if err := c.Register("ns", "k", "default", WithContextValidator(validator)); err != nil {
		t.Fatalf("register: %v", err)
	}

	seedEntry(t, m, "ns", "k", rejectedSecret)

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	warns := logger.warns("stored value rejected by validator, keeping default")
	if len(warns) != 1 {
		t.Fatalf("got %d WARN lines for the rejected stored value, want exactly 1", len(warns))
	}

	var named bool

	for _, f := range warns[0].structured() {
		if redaction.IsSensitiveField(f.Key) {
			t.Errorf("the line carries field %q, which lib-observability redacts: the operator reads it as [REDACTED]", f.Key)
		}

		if f.Key == "keyname" {
			named = true

			if f.Value != "k" {
				t.Errorf(`field "keyname" = %v, want the configuration key`, f.Value)
			}
		}
	}

	if !named {
		t.Errorf(`the line carries no "keyname" field, so an operator cannot tell which key was rejected: %v`, warns[0])
	}
}
