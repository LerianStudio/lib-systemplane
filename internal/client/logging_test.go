//go:build unit

package client

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-observability/v4/constants"
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

// TestLogLinesNameTheKeyUnderKeyname drives one rejection end to end: a stored
// value the consumer's validator refuses, read back by the engine during the
// first reconcile, reaching the operator through the logger the consumer handed
// the Client — and naming the refused configuration key under "keyname".
//
// The name is the subject. "key" is an exact entry in lib-observability's
// default sensitive-field list, so the same line under that name renders as
// key=[REDACTED]: the operator is told a row was rejected and never which one.
//
// One line is all this test drives, so it is not the guarantee that no OTHER
// line drifted back to "key". That is TestNoLoggedFieldNameIsRedacted below,
// which reads every call site in this package rather than the ones a suite
// happens to reach.
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

	warns := logger.warns("stored value rejected by validator, keeping cached value")
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
// The debouncer's guard would count and trace the panic too, but under its own
// name and without the key; the engine's recovery is what names the key.
//
// source on the accounting line stays "refresh": runtime.HandlePanicValue puts
// its NAME argument there and its component nowhere on the line, so "refresh"
// is what names the call site that was recovered.
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
	// One-shot: a re-read that could not answer is retried once, so a hook
	// that kept exploding would report the same panic twice and say nothing
	// this single report does not.
	m.getHook = func(_, _ string) (store.Entry, bool, bool) {
		m.mu.Lock()
		m.getHook = nil
		m.mu.Unlock()

		panic("store driver blew up")
	}
	m.mu.Unlock()

	m.fire(store.Event{Op: store.OpUpsert, Namespace: "ns", Key: "k"})

	lines := logger.errs("systemplane.engine: changefeed re-read panicked")
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
	// span event, and it is also what logs this line: source "refresh" proves the
	// engine counted it, not the debouncer's guard (source "invoke").
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

// TestMultiTenantDecodeFailureNamesTheTenant pins the one identifier a
// multi-tenant read-through failure used to withhold. The line named the
// namespace and the key, which on a multi-tenant deployment is the same
// namespace and the same key for every tenant in the fleet: an operator
// reading it learned that SOMEBODY's row was unreadable and had no way to tell
// whose. The tenant travels on the caller's context, so it is stamped centrally
// on every ERROR the Client logs in multi-tenant mode, and named in the error
// the caller receives beside the namespace and key.
func TestMultiTenantDecodeFailureNamesTheTenant(t *testing.T) {
	m := newMemStore(true)
	logger := &recordingLogger{}
	c := newMultiTenantClientWithLogger(t, m, logger)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	seedRaw(m, "ns", "k", []byte(`{not json`))

	ctx := tmcore.ContextWithTenantID(context.Background(), "acme")

	if _, _, err := c.Get(ctx, "ns", "k"); err == nil {
		t.Fatal("Get: want a decode error, got nil")
	} else if !strings.Contains(err.Error(), "acme") {
		t.Errorf("the decode error does not name the tenant whose row failed: %v", err)
	}

	lines := logger.errs("failed to unmarshal stored value")
	if len(lines) != 1 {
		t.Fatalf("got %d ERROR lines for the undecodable row, want exactly 1: %s", len(lines), logger.rendered())
	}

	var tenant string

	for _, f := range lines[0].structured() {
		if f.Key == constants.AttrKeyTenantID {
			tenant, _ = f.Value.(string)
		}
	}

	if tenant != "acme" {
		t.Errorf("the line carries %s = %q, so an operator cannot tell whose row is broken: %s",
			constants.AttrKeyTenantID, tenant, logger.rendered())
	}
}

// TestMultiTenantDecodeFailureWithNoTenantIDSaysSo pins the other half of the
// tenant stamp. The tenant database and the tenant id ride independent context
// keys, so a read can resolve a database while carrying no id. An empty
// tenant.id on the line reads exactly like a single-tenant line, and `in
// tenant ""` in the error reads like a tenant named nothing; both must say the
// tenant was unresolved instead.
func TestMultiTenantDecodeFailureWithNoTenantIDSaysSo(t *testing.T) {
	m := newMemStore(true)
	logger := &recordingLogger{}
	c := newMultiTenantClientWithLogger(t, m, logger)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	seedRaw(m, "ns", "k", []byte(`{not json`))

	// The memStore resolves a tenant store for any ctx, which is the
	// resolved-database-without-an-id shape this test is about.
	_, _, err := c.Get(context.Background(), "ns", "k")
	if err == nil {
		t.Fatal("Get: want a decode error, got nil")
	}

	if !strings.Contains(err.Error(), "in an unresolved tenant") || strings.Contains(err.Error(), `in tenant ""`) {
		t.Errorf("the decode error does not say the tenant was unresolved: %v", err)
	}

	// Nothing here masks a value, so the json cause that locates the bad byte
	// stays in the chain for the caller debugging the row.
	var syntax *json.SyntaxError
	if !errors.As(err, &syntax) {
		t.Errorf("the json cause callers debug the row with is gone: %v", err)
	}

	lines := logger.errs("failed to unmarshal stored value")
	if len(lines) != 1 {
		t.Fatalf("got %d ERROR lines for the undecodable row, want exactly 1: %s", len(lines), logger.rendered())
	}

	var tenant any

	for _, f := range lines[0].structured() {
		if f.Key == constants.AttrKeyTenantID {
			tenant = f.Value
		}
	}

	if tenant != "unresolved" {
		t.Errorf("the line carries %s = %v, want \"unresolved\": %s",
			constants.AttrKeyTenantID, tenant, logger.rendered())
	}
}

// TestMultiTenantValidatorPanicNamesTheTenantAndKey pins the identity on the
// one consumer panic the engine still reported anonymously.
//
// A validator that panics is recovered and turned into a validation refusal,
// and the report carried the source, the panic value and a stack — no tenant,
// no namespace, no key. On a multi-tenant deployment one namespace and one key
// serve every tenant in the fleet, so an operator paged by that line learned
// that SOMEBODY's write was refused by an exploding validator and had no way
// to tell whose or which key. The changefeed re-read already named all three;
// this is the same line, emitted from the one place every consumer panic is
// reported.
func TestMultiTenantValidatorPanicNamesTheTenantAndKey(t *testing.T) {
	m := newMemStore(true)
	logger := &recordingLogger{}
	c := newMultiTenantClientWithLogger(t, m, logger)

	// The registered default is graded at Register with context.Background(),
	// so the validator has to pass it and blow up only on the write below.
	if err := c.Register("ns", "k", "default", WithValidator(func(v any) error {
		if v == "default" {
			return nil
		}

		panic("validator blew up")
	})); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	ctx := tmcore.ContextWithTenantID(context.Background(), "acme")

	if err := c.Set(ctx, "ns", "k", "new", "ops"); err == nil {
		t.Fatal("Set: want a validation refusal from the panicking validator, got nil")
	}

	lines := logger.errs("systemplane.engine: validator panicked")
	if len(lines) != 1 {
		t.Fatalf("got %d ERROR lines naming the panicking validator, want exactly 1: %s", len(lines), logger.rendered())
	}

	want := map[string]string{constants.AttrKeyTenantID: "acme", "namespace": "ns", "keyname": "k"}

	for _, f := range lines[0].structured() {
		if redaction.IsSensitiveField(f.Key) {
			t.Errorf("the line carries field %q, which lib-observability redacts", f.Key)
		}

		if expected, ok := want[f.Key]; ok {
			if f.Value != expected {
				t.Errorf("the line carries %s = %v, want %q", f.Key, f.Value, expected)
			}

			delete(want, f.Key)
		}
	}

	for missing := range want {
		t.Errorf("the line carries no %q field, so an operator cannot tell whose write was refused: %v", missing, lines[0])
	}
}
