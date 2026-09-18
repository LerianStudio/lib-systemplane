//go:build unit

package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/redaction"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// logRecord is one entry the engine emitted. Args is kept raw so a test can
// assert on the whole rendering — lib-observability's own recovery pipeline
// passes shapes this package never constructs — while fields() unwraps the
// one argument the engine itself passes, a []log.Field.
type logRecord struct {
	Level int
	Msg   string
	Args  []any
	// Ctx is the context the engine logged under. It is captured because a
	// line severed from the caller's trace is indistinguishable from one
	// attached to it by message and fields alone.
	Ctx context.Context
}

func (r logRecord) fields() []log.Field {
	out := make([]log.Field, 0, len(r.Args))

	for _, arg := range r.Args {
		switch v := arg.(type) {
		case []log.Field:
			out = append(out, v...)
		case log.Field:
			out = append(out, v)
		}
	}

	return out
}

func (r logRecord) field(key string) (log.Field, bool) {
	for _, f := range r.fields() {
		if f.Key == key {
			return f, true
		}
	}

	return log.Field{}, false
}

func (r logRecord) String() string {
	return fmt.Sprintf("level=%d msg=%q fields=%v", r.Level, r.Msg, r.Args)
}

// recordingLogger captures every entry the engine emits. It is the only way a
// test can see a rejection the engine deliberately swallows: an ingress that
// skips a row publishes nothing and returns nothing, so the log line IS the
// operator-visible outcome, and its level is what decides whether an operator
// ever finds it.
type recordingLogger struct {
	log.Logger

	mu      sync.Mutex
	records []logRecord
}

func (r *recordingLogger) Log(ctx context.Context, level int, msg string, fields ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.records = append(r.records, logRecord{Level: level, Msg: msg, Args: fields, Ctx: ctx})
}

func (r *recordingLogger) snapshot() []logRecord {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]logRecord(nil), r.records...)
}

func (r *recordingLogger) all() []string {
	entries := make([]string, 0)
	for _, rec := range r.snapshot() {
		entries = append(entries, rec.String())
	}

	return entries
}

// requireLogged finds the single entry whose message is msg and asserts it was
// emitted at level naming nk. Level is the load-bearing half: a rejection
// logged at DEBUG is invisible on a production logger, and one logged at WARN
// for an ordinary condition trains operators to ignore the channel.
func requireLogged(t *testing.T, r *recordingLogger, level int, msg string, nk NSKey) {
	t.Helper()

	matches := make([]logRecord, 0, 1)

	for _, rec := range r.snapshot() {
		if rec.Msg == msg {
			matches = append(matches, rec)
		}
	}

	if len(matches) != 1 {
		t.Fatalf("entries with message %q: got %d, want 1; all entries: %v", msg, len(matches), r.all())
	}

	got := matches[0]
	if got.Level != level {
		t.Errorf("%q logged at level %s, want %s", msg, log.LevelName(got.Level), log.LevelName(level))
	}

	requireNotRedacted(t, got)

	for key, want := range map[string]string{"namespace": nk.Namespace, "keyname": nk.Key} {
		f, ok := got.field(key)
		if !ok {
			t.Errorf("%q carries no %q field, so an operator cannot tell which key it is about: %s", msg, key, got)

			continue
		}

		if f.Value != want {
			t.Errorf("%q field %q: got %v, want %q", msg, key, f.Value, want)
		}
	}
}

// requireNotRedacted fails when a field name the engine chose is one
// lib-observability erases before an operator ever reads it.
//
// "key" is an exact entry in its default sensitive-field list, so
// log.String("key", ...) renders as key=[REDACTED] on both the stdlib logger
// and the production zap logger — every line below would then name the
// namespace and withhold the key, which is the only thing that line exists to
// publish. The check runs over every field of every entry these tests assert
// on, so a rename back into that list turns them red instead of silently
// blinding the operator.
func requireNotRedacted(t *testing.T, rec logRecord) {
	t.Helper()

	for _, f := range rec.fields() {
		if redaction.IsSensitiveField(f.Key) {
			t.Errorf("%q carries field %q, which lib-observability redacts: the line reaches "+
				"the operator with its value replaced by [REDACTED]", rec.Msg, f.Key)
		}
	}
}

// loggingEngine builds the engine the way the Client does — through New, with
// the logger arriving on Config — so these tests pin the wiring as well as the
// levels.
func loggingEngine(t *testing.T, defs map[NSKey]KeyDef, fs *fakeStore) (*Engine, *recordingLogger) {
	t.Helper()

	rec := &recordingLogger{Logger: log.NewNop()}
	e := New(Config{Store: fs, Registry: fakeRegistry{defs: defs}, Logger: rec})

	track(t, e, store.Scope{})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	return e, rec
}

// TestUnregisteredKeyIsLoggedAtDebug pins the one rejection that is ordinary
// rather than wrong. A store legitimately holds rows this process never
// registered — another service's keys in the same table — so announcing each
// one at WARN would bury the rejections that matter.
func TestUnregisteredKeyIsLoggedAtDebug(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "unknown"}
	e, rec := loggingEngine(t, map[NSKey]KeyDef{}, newFakeStore())

	ingestRow(e, jsonRow(nk, 1, `"v"`, "ops"))

	requireLogged(t, rec, log.LevelDebug, "value for unregistered key, skipping", nk)
}

// TestUndecodableValueIsLoggedAtWarn pins the corrupt-row rejection. The cache
// keeps what it held, so nothing downstream changes — which is exactly why the
// log line has to be loud enough for an operator to find the broken row.
func TestUndecodableValueIsLoggedAtWarn(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, newFakeStore())

	ingestRow(e, jsonRow(nk, 1, `{not json`, "ops"))

	requireLogged(t, rec, log.LevelWarn, "failed to unmarshal stored value, keeping cached value", nk)
}

// TestValidatorRejectionIsLoggedAtWarn pins the wrong-typed-row rejection. The
// key keeps its last valid value rather than reverting to the default, so an
// operator who hand-edited a row to the wrong shape sees no effect at all
// except this line.
func TestValidatorRejectionIsLoggedAtWarn(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {
		Default:  "fallback",
		Validate: func(any) error { return errors.New("want a string") },
	}}, newFakeStore())

	ingestRow(e, jsonRow(nk, 1, `42`, "ops"))

	requireLogged(t, rec, log.LevelWarn, "stored value rejected by validator, keeping cached value", nk)
}

// TestReReadErrorIsLoggedAtWarn pins the changefeed's read failure. The engine
// learned nothing about the key and keeps the cached value, so this line is
// the only trace that a notification was dropped on the floor.
func TestReReadErrorIsLoggedAtWarn(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs)

	fs.onGet(func(store.Scope, NSKey) error { return errors.New("connection reset") })

	e.refreshKey(store.Scope{}, nk)

	requireLogged(t, rec, log.LevelWarn, "changefeed re-read failed, keeping current value", nk)
}

// TestReReadCanceledByCloseIsLoggedAtDebug pins the other half of the same
// path: a read that fails because the engine is shutting down is a shutdown,
// not an incident. Logging it at WARN would make every clean Close emit
// warnings for whatever re-reads were in flight.
func TestReReadCanceledByCloseIsLoggedAtDebug(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs)

	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The re-read a debounce timer already fired, reaching the store after
	// Close canceled the lifecycle context.
	e.refreshKey(store.Scope{}, nk)

	requireLogged(t, rec, log.LevelDebug, "changefeed re-read canceled during shutdown", nk)
}

// requireLoggedAt asserts the single entry with this message was emitted at
// level. It is requireLogged for the lines that name a scope rather than a key:
// a reconcile failure carries the tenant and the error, not a namespace.
func requireLoggedAt(t *testing.T, r *recordingLogger, level int, msg string) {
	t.Helper()

	matches := make([]logRecord, 0, 1)

	for _, rec := range r.snapshot() {
		if rec.Msg == msg {
			matches = append(matches, rec)
		}
	}

	if len(matches) != 1 {
		t.Fatalf("entries with message %q: got %d, want 1; all entries: %v", msg, len(matches), r.all())
	}

	if got := matches[0].Level; got != level {
		t.Errorf("%q logged at level %s, want %s", msg, log.LevelName(got), log.LevelName(level))
	}

	requireNotRedacted(t, matches[0])
}

// TestReconcileListFailureLevelsSplitOnShutdown pins the one level that is
// noise. Every ordinary Close with a reconcile in flight cancels that List, so
// logging it at WARN made a clean shutdown look like an incident — and trained
// operators to ignore the channel the real failure uses.
func TestReconcileListFailureLevelsSplitOnShutdown(t *testing.T) {
	const msg = "scope reconcile failed to list, keeping cached values"

	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}

	t.Run("canceled by Close", func(t *testing.T) {
		fs := newFakeStore()
		e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs)

		release := heldList(fs)

		e.onEvent(resyncEvent(scope))
		waitFor(t, time.Second, "the reconcile to reach its List", func() bool { return fs.listCount() == 1 })

		done := closeInBackground(e)

		// The cancellation must land BEFORE the List is released, or the List
		// succeeds and there is no failure to log at any level.
		waitFor(t, time.Second, "Close to cancel the lifecycle context", func() bool {
			return e.lifecycleCtx.Err() != nil
		})

		release()
		mustCloseCleanly(t, done)

		requireLoggedAt(t, rec, log.LevelDebug, msg)
	})

	t.Run("a real List failure", func(t *testing.T) {
		fs := newFakeStore()
		e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs)

		fs.onList(func(store.Scope) error { return errList })

		e.onEvent(resyncEvent(scope))

		if err := waitFirstReconcile(t, e, scope); !errors.Is(err, errList) {
			t.Fatalf("first reconcile outcome: got %v, want %v", err, errList)
		}

		requireLoggedAt(t, rec, log.LevelWarn, msg)
	})
}

// requireOneRecord returns the single entry whose message is msg, failing when
// the engine emitted none or several: every assertion below is about WHAT one
// line carries, which is meaningless if the line is ambiguous.
func requireOneRecord(t *testing.T, r *recordingLogger, msg string) logRecord {
	t.Helper()

	matches := make([]logRecord, 0, 1)

	for _, rec := range r.snapshot() {
		if rec.Msg == msg {
			matches = append(matches, rec)
		}
	}

	if len(matches) != 1 {
		t.Fatalf("entries with message %q: got %d, want 1; all entries: %v", msg, len(matches), r.all())
	}

	return matches[0]
}

// TestPublishLogsUnderTheCallerContext pins the write path to the caller's
// context. A Set arrives on the consumer's own goroutine, inside the consumer's
// own span; logging its ingress under the engine's background context detaches
// every rejection an operator would use to explain why a write did not take
// effect from the request that caused it.
func TestPublishLogsUnderTheCallerContext(t *testing.T) {
	type ctxKey struct{}

	nk := NSKey{Namespace: "billing", Key: "limits"}
	e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {
		Default:  "fallback",
		Validate: func(any) error { return errors.New("want a string") },
	}}, newFakeStore())

	ctx := context.WithValue(context.Background(), ctxKey{}, "caller-span")

	e.Publish(ctx, store.Scope{}, jsonRow(nk, 1, `42`, "ops"))

	got := requireOneRecord(t, rec, "stored value rejected by validator, keeping cached value")
	if got.Ctx == nil || got.Ctx.Value(ctxKey{}) != "caller-span" {
		t.Errorf("the write path logged under a context that is not the caller's: %s", got)
	}
}

// validatorError is the shape a real validator returns: a consumer-defined
// error whose message names what it refused — which is how the refused value
// itself ends up in the message.
type validatorError struct{ msg string }

func (e validatorError) Error() string { return e.msg }

// TestValidatorErrorIsRedactedByKeyPolicy pins the one place a consumer-built
// string reaches the log stream carrying a configuration value: the message of
// an error the registered validator returned. A validator that interpolates
// the value it refused ("password %q is too short") publishes that value at
// WARN, past every redaction the key was registered with.
//
// The error returned to the caller of Set is unchanged in both cases; only the
// log line is redacted.
func TestValidatorErrorIsRedactedByKeyPolicy(t *testing.T) {
	const (
		sentinel = "s3cr3t-value"
		msg      = "stored value rejected by validator, keeping cached value"
	)

	rejecting := func(any) error { return validatorError{"rejected " + sentinel} }

	visible := NSKey{Namespace: "billing", Key: "limits"}
	secret := NSKey{Namespace: "billing", Key: "apitoken"}

	t.Run("RedactNone keeps the validator's own message", func(t *testing.T) {
		e, rec := loggingEngine(t, map[NSKey]KeyDef{
			visible: {Default: "fallback", Validate: rejecting},
		}, newFakeStore())

		ingestRow(e, jsonRow(visible, 1, `42`, "ops"))

		got := requireOneRecord(t, rec, msg)
		requireNotRedacted(t, got)

		if !strings.Contains(got.String(), sentinel) {
			t.Errorf("a key registered without redaction lost its validator's message: %s", got)
		}
	})

	t.Run("RedactFull withholds it", func(t *testing.T) {
		e, rec := loggingEngine(t, map[NSKey]KeyDef{
			secret: {Default: "fallback", Validate: rejecting, Redacted: true},
		}, newFakeStore())

		ingestRow(e, jsonRow(secret, 1, `42`, "ops"))

		got := requireOneRecord(t, rec, msg)
		requireNotRedacted(t, got)

		if strings.Contains(got.String(), sentinel) {
			t.Errorf("a redacted key published its value through the validator's error message: %s", got)
		}

		if !strings.Contains(got.String(), "engine.validatorError") {
			t.Errorf("the redacted line names no error type, so an operator cannot tell the rejections apart: %s", got)
		}
	})
}
