//go:build unit

package client

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/redaction"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/logguard"
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

// TestNoLoggedFieldNameIsRedacted reads this package's own source and refuses
// any field name lib-observability erases. The test above drives ONE of the
// lines that rename touched; this reaches every call site.
func TestNoLoggedFieldNameIsRedacted(t *testing.T) {
	logguard.AssertNoneRedacted(t, ".")
}

// TestRefreshPanicNamesTheKey pins the identity line a panicking store driver
// leaves behind under the single-tenant changefeed.
//
// The debouncer's guard catches the panic either way, and that is what it
// cannot say: runtime.RecoverAndLog logs source="debounce" and nothing else,
// and in production mode the recovered value and the stack are redacted out of
// that line (lib-observability/v4 runtime/recover.go logPanicWithStack), so an
// operator learns something under the debouncer blew up and never which
// namespace or key. internal/engine.(*Engine).recoverRefresh is the same guard
// on the v4 path.
func TestRefreshPanicNamesTheKey(t *testing.T) {
	m := newMemStore(false)
	logger := &recordingLogger{}
	c := newSingleTenantClientWithLogger(t, m, logger)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	m.mu.Lock()
	m.getHook = func(_, _ string) (store.Entry, bool, bool) { panic("store driver blew up") }
	m.mu.Unlock()

	m.fire(store.Event{Op: store.OpUpsert, Namespace: "ns", Key: "k"})

	lines := logger.errs("systemplane: changefeed re-read panicked")
	if len(lines) != 1 {
		t.Fatalf("got %d ERROR lines naming the panicking re-read, want exactly 1: %s", len(lines), logger.rendered())
	}

	var namespaced, named bool

	for _, f := range lines[0].structured() {
		if redaction.IsSensitiveField(f.Key) {
			t.Errorf("the line carries field %q, which lib-observability redacts: the operator reads it as [REDACTED]", f.Key)
		}

		switch f.Key {
		case "namespace":
			namespaced = f.Value == "ns"
		case "keyname":
			named = f.Value == "k"
		}
	}

	if !namespaced || !named {
		t.Errorf("the line does not name the key the re-read panicked on: %v", lines[0])
	}

	// The identity line says which key; this one says the panic was COUNTED.
	// runtime.HandlePanicValue is what records panic_recovered_total and the
	// span event, and it is also what logs this line, so the line is the only
	// in-process evidence the counter moved: a recoverRefresh that re-panicked
	// into the debouncer's RecoverAndLog instead would leave the identity line
	// standing above and the counter at zero, and nothing else here would
	// notice.
	accounted := logger.errs("panic recovered")
	if len(accounted) != 1 {
		t.Fatalf("got %d ERROR lines accounting for the panic, want exactly 1: %s", len(accounted), logger.rendered())
	}

	var source any

	for _, f := range accounted[0].structured() {
		if f.Key == "source" {
			source = f.Value
		}
	}

	if source != "refresh" {
		t.Errorf("the accounting line carries source = %v, want \"refresh\": the panic was counted under another site, or reported without being counted", source)
	}
}
