//go:build unit

package engine

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/v4/constants"
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

	// debugOff makes the recorder report DEBUG as disabled, the way a
	// production logger configured at INFO does. The zero value reports every
	// level enabled, because a recorder that claimed otherwise would make the
	// engine skip the very lines these tests read.
	debugOff bool

	mu      sync.Mutex
	records []logRecord
}

// Enabled overrides the embedded no-op logger, which reports every level
// disabled. A caller that asks before building its fields would then never
// emit anything into this recorder.
func (r *recordingLogger) Enabled(level int) bool {
	return !r.debugOff || level != log.LevelDebug
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
// emitted at level naming the whole identity of the key: tenant, namespace and
// keyname. Level is one load-bearing half: a rejection logged at DEBUG is
// invisible on a production logger, and one logged at WARN for an ordinary
// condition trains operators to ignore the channel.
//
// The tenant is the other. A line that names a namespace and a key and no
// tenant is not half an answer in a multi-tenant deployment — it sends an
// operator through every tenant's logs to find which one holds the rejected
// row, and it reads as complete in the source, so only this assertion keeps
// the gap from reopening one line at a time.
func requireLogged(t *testing.T, r *recordingLogger, level int, msg string, scope store.Scope, nk NSKey) {
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

	for key, want := range map[string]string{
		constants.AttrKeyTenantID: scope.Tenant,
		"namespace":               nk.Namespace,
		"keyname":                 nk.Key,
	} {
		f, ok := got.field(key)
		if !ok {
			t.Errorf("%q carries no %q field, so an operator cannot tell which tenant's key it is about: %s", msg, key, got)

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

// logFieldConstructors are the log.Field constructors whose first argument is
// the field NAME — the string an operator greps by and the string
// lib-observability matches its sensitive list against. log.Err is absent
// because it names no field.
var logFieldConstructors = map[string]bool{
	"String":   true,
	"Any":      true,
	"Int":      true,
	"Int64":    true,
	"Bool":     true,
	"Duration": true,
	"Float64":  true,
	"Strings":  true,
}

// TestNoLoggedFieldNameIsRedacted reads the package's own source and refuses
// any field name lib-observability erases.
//
// requireNotRedacted only sees the lines a test happens to drive, and this
// package emits from a dozen sites the suite reaches unevenly. "key" is an
// exact entry in the default sensitive-field list, so one log.String("key", …)
// slipping back in publishes namespace=billing key=[REDACTED] to an operator
// hunting a rejected row — a line that survives review because it reads
// correctly in the source and is only wrong in production.
func TestNoLoggedFieldNameIsRedacted(t *testing.T) {
	names := loggedFieldNames(t)

	if len(names) == 0 {
		t.Fatal("no log field names found in this package: the scan matched nothing, so it proves nothing")
	}

	for name, pos := range names {
		if redaction.IsSensitiveField(name) {
			t.Errorf("%s: field name %q is on lib-observability's sensitive list, so the line "+
				"reaches the operator with its value replaced by [REDACTED]", pos, name)
		}
	}
}

// loggedFieldNames parses every non-test file of the package under test — the
// test binary runs with its package directory as cwd — and returns each
// literal field name a log.Field constructor is called with, keyed by name so
// the same name reported twice fails once. A name built from a constant or a
// variable is skipped: it is not a literal this scan can read, and
// constants.AttrKeyTenantID is the library's own and already checked there.
func loggedFieldNames(t *testing.T) map[string]token.Position {
	t.Helper()

	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list package sources: %v", err)
	}

	fset := token.NewFileSet()
	names := make(map[string]token.Position)

	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, source, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", source, err)
		}

		ast.Inspect(file, func(n ast.Node) bool {
			name, lit, ok := logFieldName(n)
			if ok {
				names[name] = fset.Position(lit.Pos())
			}

			return true
		})
	}

	return names
}

// logFieldName reports the literal field name of a log.<Constructor>("name",
// …) call, and false for every other node.
func logFieldName(n ast.Node) (string, *ast.BasicLit, bool) {
	call, ok := n.(*ast.CallExpr)
	if !ok || len(call.Args) == 0 {
		return "", nil, false
	}

	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !logFieldConstructors[sel.Sel.Name] {
		return "", nil, false
	}

	if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "log" {
		return "", nil, false
	}

	lit, ok := call.Args[0].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", nil, false
	}

	name, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", nil, false
	}

	return name, lit, true
}

// loggingEngine builds the engine the way the Client does — through New, with
// the logger arriving on Config — so these tests pin the wiring as well as the
// levels.
func loggingEngine(t *testing.T, defs map[NSKey]KeyDef, fs *fakeStore) (*Engine, *recordingLogger) {
	t.Helper()

	rec := &recordingLogger{Logger: log.NewNop()}

	return loggingEngineWith(t, defs, fs, rec), rec
}

// loggingEngineWith is loggingEngine over a recorder the caller configured —
// the one knob being whether it reports DEBUG enabled.
func loggingEngineWith(t *testing.T, defs map[NSKey]KeyDef, fs *fakeStore, rec *recordingLogger) *Engine {
	t.Helper()

	e := New(Config{Store: fs, Registry: fakeRegistry{defs: defs}, Logger: rec})

	track(t, e, store.Scope{})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	return e
}

// TestUnregisteredKeyIsLoggedAtDebug pins the one rejection that is ordinary
// rather than wrong. A store legitimately holds rows this process never
// registered — another service's keys in the same table — so announcing each
// one at WARN would bury the rejections that matter.
func TestUnregisteredKeyIsLoggedAtDebug(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "unknown"}
	e, rec := loggingEngine(t, map[NSKey]KeyDef{}, newFakeStore())

	ingestRow(e, jsonRow(nk, 1, `"v"`, "ops"))

	requireLogged(t, rec, log.LevelDebug, "value for unregistered key, skipping", store.Scope{}, nk)
}

// TestUndecodableValueIsLoggedAtWarn pins the corrupt-row rejection. The cache
// keeps what it held, so nothing downstream changes — which is exactly why the
// log line has to be loud enough for an operator to find the broken row.
func TestUndecodableValueIsLoggedAtWarn(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, newFakeStore())

	ingestRow(e, jsonRow(nk, 1, `{not json`, "ops"))

	requireLogged(t, rec, log.LevelWarn, "failed to unmarshal stored value, keeping cached value", store.Scope{}, nk)
}

// TestValidatorRejectionIsLoggedAtWarn pins the wrong-typed-row rejection. The
// key keeps its last valid value rather than reverting to the default, so an
// operator who hand-edited a row to the wrong shape sees no effect at all
// except this line.
func TestValidatorRejectionIsLoggedAtWarn(t *testing.T) {
	const msg = "stored value rejected by validator, keeping cached value"

	nk := NSKey{Namespace: "billing", Key: "limits"}
	row := jsonRow(nk, 1, `42`, "ops")

	e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {
		Default:  "fallback",
		Validate: func(context.Context, any) error { return errors.New("want a string") },
	}}, newFakeStore())

	ingestRow(e, row)

	requireLogged(t, rec, log.LevelWarn, msg, store.Scope{}, nk)

	// The line is announced once per INGESTION ATTEMPT, not once per key, and
	// KeyDef.Validate says so. A changefeed that flaps re-reads the same
	// unusable row on every resync, and an operator waiting for someone to fix
	// that row needs each attempt to report itself: deduplicating the line
	// would make a key that has been refused for an hour look like a key that
	// was refused once, long ago.
	ingestRow(e, row)

	warns := 0

	for _, got := range rec.snapshot() {
		if got.Msg != msg {
			continue
		}

		warns++

		if got.Level != log.LevelWarn {
			t.Errorf("rejection %d logged at level %s, want %s",
				warns, log.LevelName(got.Level), log.LevelName(log.LevelWarn))
		}
	}

	if warns != 2 {
		t.Errorf("entries with message %q after two ingestion attempts: got %d, want 2; all entries: %v",
			msg, warns, rec.all())
	}
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

	requireLogged(t, rec, log.LevelWarn, "changefeed re-read failed, keeping current value", store.Scope{}, nk)
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

	requireLogged(t, rec, log.LevelDebug, "changefeed re-read canceled during shutdown", store.Scope{}, nk)
}

// TestLogLevelReReadWithNoRowIsDebug pins the re-read that finds nothing. The
// write may simply not be visible to this reader yet, and a real removal
// arrives as its own delete event, so this is ordinary rather than wrong: at
// WARN every routine race between a notification and its row becoming readable
// would reach an operator as a fault.
func TestLogLevelReReadWithNoRowIsDebug(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, newFakeStore())

	e.refreshKey(store.Scope{}, nk)

	requireLogged(t, rec, log.LevelDebug, "changefeed re-read found no row, keeping current value", store.Scope{}, nk)
}

// TestLogLevelUnregisteredFeedEventIsDebug pins the feed's registry filter for
// both operations. A store legitimately holds another service's keys in the
// same table, so notifications about them are ordinary traffic — at WARN they
// would bury the rejections that matter, once per foreign write.
func TestLogLevelUnregisteredFeedEventIsDebug(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "unknown"}

	for _, tc := range []struct {
		name string
		evt  store.Event
	}{
		{name: "upsert", evt: upsertEvent(store.Scope{}, nk, 1)},
		{name: "delete", evt: deleteEvent(store.Scope{}, nk)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, rec := loggingEngine(t, map[NSKey]KeyDef{}, newFakeStore())

			e.onEvent(tc.evt)

			requireLogged(t, rec, log.LevelDebug, "changefeed event for unregistered key, skipping", store.Scope{}, nk)
		})
	}
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

	// The third case is the one the split gets wrong by accident: a
	// context.Canceled that is NOT this engine shutting down — a connection
	// pool aborting a checkout, a driver cancelling internally. Keying the
	// level on the error alone would file it under "clean shutdown" and hide
	// the one trace that a whole-scope reload failed on a live engine.
	t.Run("canceled outside shutdown", func(t *testing.T) {
		fs := newFakeStore()
		e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs)

		fs.onList(func(store.Scope) error {
			return fmt.Errorf("pool checkout aborted: %w", context.Canceled)
		})

		e.onEvent(resyncEvent(scope))

		if err := waitFirstReconcile(t, e, scope); !errors.Is(err, context.Canceled) {
			t.Fatalf("first reconcile outcome: got %v, want a wrapped context.Canceled", err)
		}

		requireLoggedAt(t, rec, log.LevelWarn, msg)
	})
}

// requireOneRecord returns the single entry whose message is msg, failing when
// the engine emitted none or several: every assertion below is about WHAT one
// line carries, which is meaningless if the line is ambiguous.
//
// It runs requireNotRedacted for the same reason requireLogged and
// requireLoggedAt do — a caller that forgot to repeat the check by hand read a
// line whose fields lib-observability erases before an operator sees them.
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

	requireNotRedacted(t, matches[0])

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
		Validate: func(context.Context, any) error { return errors.New("want a string") },
	}}, newFakeStore())

	ctx := context.WithValue(context.Background(), ctxKey{}, "caller-span")

	e.Publish(ctx, store.Scope{}, jsonRow(nk, 1, `42`, "ops"))

	got := requireOneRecord(t, rec, "stored value rejected by validator, keeping cached value")
	if got.Ctx == nil || got.Ctx.Value(ctxKey{}) != "caller-span" {
		t.Errorf("the write path logged under a context that is not the caller's: %s", got)
	}
}

// TestTenantIsLoggedUnderTheCanonicalKey pins the field key every tenant-scoped
// line in this package uses. Three spellings were live in one repository at
// once — a bare "tenant" here, "tenant_id" in internal/manager, and
// lib-observability's own constants.AttrKeyTenantID — so an operator filtering
// a log stream by tenant matched two of the three and silently lost the rest.
// The engine follows the library constant; this assertion is what stops the
// literal coming back.
func TestTenantIsLoggedUnderTheCanonicalKey(t *testing.T) {
	const tenant = "acme"

	nk := NSKey{Namespace: "billing", Key: "limits"}
	e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, newFakeStore())

	// loggingEngine tracks only the zero scope, so a write addressed to a
	// tenant is dropped — and the drop names the tenant it was addressed to.
	e.Publish(context.Background(), store.Scope{Tenant: tenant}, jsonRow(nk, 1, `"5"`, "ops"))

	got := requireOneRecord(t, rec, "write for an untracked scope, dropping")

	f, ok := got.field(constants.AttrKeyTenantID)
	if !ok {
		t.Fatalf("no %q field, so a tenant filter never matches the engine's lines: %s",
			constants.AttrKeyTenantID, got)
	}

	if f.Value != tenant {
		t.Errorf("field %q: got %v, want %q", constants.AttrKeyTenantID, f.Value, tenant)
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

	rejecting := func(context.Context, any) error { return validatorError{"rejected " + sentinel} }

	visible := NSKey{Namespace: "billing", Key: "limits"}
	secret := NSKey{Namespace: "billing", Key: "apitoken"}

	t.Run("RedactNone keeps the validator's own message", func(t *testing.T) {
		e, rec := loggingEngine(t, map[NSKey]KeyDef{
			visible: {Default: "fallback", Validate: rejecting},
		}, newFakeStore())

		ingestRow(e, jsonRow(visible, 1, `42`, "ops"))

		got := requireOneRecord(t, rec, msg)

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

		if strings.Contains(got.String(), sentinel) {
			t.Errorf("a redacted key published its value through the validator's error message: %s", got)
		}

		if !strings.Contains(got.String(), "engine.validatorError") {
			t.Errorf("the redacted line names no error type, so an operator cannot tell the rejections apart: %s", got)
		}
	})
}

// TestUndecodableValueIsRedactedByKeyPolicy pins the SECOND place a
// configuration value reaches the log stream: the message of the decode error
// itself. encoding/json reports an unparsable row as "invalid character 'h'
// looking for beginning of value", quoting the offending byte — so a key
// registered RedactFull whose row is a raw secret publishes that secret's
// first byte at WARN, past every redaction the key was registered with.
//
// It is reachable rather than theoretical: the MongoDB backend stores value as
// a BSON string it never validates as JSON, so a foreign writer or a
// hand-edited document produces exactly this error.
//
// The error returned to the caller is unchanged in both cases; only the log
// line is redacted.
func TestUndecodableValueIsRedactedByKeyPolicy(t *testing.T) {
	const (
		secret = "hunter2-s3cret"
		msg    = "failed to unmarshal stored value, keeping cached value"
	)

	visible := NSKey{Namespace: "billing", Key: "limits"}
	sensitive := NSKey{Namespace: "billing", Key: "apitoken"}

	t.Run("RedactNone keeps the decoder's own message", func(t *testing.T) {
		e, rec := loggingEngine(t, map[NSKey]KeyDef{visible: {Default: "fallback"}}, newFakeStore())

		ingestRow(e, jsonRow(visible, 1, secret, "ops"))

		got := requireOneRecord(t, rec, msg)

		if !strings.Contains(got.String(), "invalid character") {
			t.Errorf("a key registered without redaction lost the decoder's message: %s", got)
		}
	})

	t.Run("RedactFull withholds every byte of the value", func(t *testing.T) {
		e, rec := loggingEngine(t, map[NSKey]KeyDef{
			sensitive: {Default: "fallback", Redacted: true},
		}, newFakeStore())

		ingestRow(e, jsonRow(sensitive, 1, secret, "ops"))

		got := requireOneRecord(t, rec, msg)

		rendered := fmt.Sprint(got.fields())

		// The first byte, the prefix it starts, the whole value and its
		// length: a decode error that carries any of them tells a log reader
		// something about the secret it was not entitled to.
		for _, leak := range []string{secret, "hunter", "'h'", strconv.Itoa(len(secret))} {
			if strings.Contains(rendered, leak) {
				t.Errorf("the redacted decode line carries %q from the stored value: %s", leak, rendered)
			}
		}

		if !strings.Contains(rendered, "json.SyntaxError") {
			t.Errorf("the redacted line names no error type, so an operator cannot tell the failures apart: %s", rendered)
		}
	})
}

// TestReReadCanceledOutsideShutdownIsLoggedAtWarn is the companion of
// TestReReadCanceledByCloseIsLoggedAtDebug: a store that surfaces a wrapped
// context.Canceled for a reason that is NOT this engine shutting down — a
// connection-pool checkout aborted, a driver-internal cancellation — is a real
// read failure. The cache goes on serving a value nothing confirmed, so
// logging it below WARN hides the one trace that it happened.
func TestReReadCanceledOutsideShutdownIsLoggedAtWarn(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	fs := newFakeStore()
	e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, fs)

	fs.onGet(func(store.Scope, NSKey) error {
		return fmt.Errorf("pool checkout aborted: %w", context.Canceled)
	})

	e.refreshKey(store.Scope{}, nk)

	requireLogged(t, rec, log.LevelWarn, "changefeed re-read failed, keeping current value", store.Scope{}, nk)
}

// requireNotLogged fails when the engine emitted msg at all. It is the
// assertion a line's VOLUME needs: requireLogged pins the one line an event
// must produce, and this pins the ones it must not, which is the only way a
// duplicate announcement shows up as a test failure rather than as noise in a
// production log.
func requireNotLogged(t *testing.T, r *recordingLogger, msg string) {
	t.Helper()

	for _, rec := range r.snapshot() {
		if rec.Msg == msg {
			t.Fatalf("entries with message %q: got at least 1, want 0; all entries: %v", msg, r.all())
		}
	}
}

// TestReconcileLogsAnUnregisteredSnapshotRowOnce pins the volume of the one
// rejection that is both ordinary and repeated on every reconcile. A single
// systemplane_entries table serves every consumer of a database, so a scope's
// snapshot carries every foreign namespace's rows — and the reconcile used to
// announce each of them twice: once by the ingress that refused the row, then
// again by a no-row fallback that looked the key up a second time and reported
// it missing for a key the snapshot plainly carried.
func TestReconcileLogsAnUnregisteredSnapshotRowOnce(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "unknown"}
	scope := store.Scope{}
	fs := newFakeStore()
	e, rec := loggingEngine(t, map[NSKey]KeyDef{}, fs)

	fs.seed(scope, jsonRow(nk, 1, `"v"`, "ops"))

	e.onEvent(resyncEvent(scope))

	if err := waitFirstReconcile(t, e, scope); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	requireLogged(t, rec, log.LevelDebug, "value for unregistered key, skipping", scope, nk)
	requireNotLogged(t, rec, "no-row event for unregistered key, skipping")
}

// TestReconcileAnnouncesTheDefaultForARefusedSnapshotRow is the other side of
// that branch, and the behaviour the volume fix must not touch: a row the
// engine refused for a key the consumer DID register still announces the
// registered default at revision 0 (FC-11), with the rejection reported once
// at WARN.
func TestReconcileAnnouncesTheDefaultForARefusedSnapshotRow(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {
		Default:  "fallback",
		Validate: func(context.Context, any) error { return errors.New("want a string") },
	}}, fs)

	fs.seed(scope, jsonRow(nk, 7, `42`, "ops"))

	e.onEvent(resyncEvent(scope))

	if err := waitFirstReconcile(t, e, scope); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss: the registered default was never announced for the refused row")
	}

	if got.Value != "fallback" || got.Revision != 0 {
		t.Errorf("after the reconcile: got (%v, rev %d), want (\"fallback\", rev 0)", got.Value, got.Revision)
	}

	requireLogged(t, rec, log.LevelWarn, "stored value rejected by validator, keeping cached value", scope, nk)
}

// TestScopeDropDiagnosticsAreDebug pins the level of the three lines a dropped
// scope emits for work that was already moving when it was dropped: a
// reconcile sitting in the mailbox, a write on the consumer's goroutine that
// had already resolved its scope, and a notification the backend's own
// changefeed goroutine was already carrying — its key is registered, so the
// feed's unregistered-key filter does not reject it first and the drop is what
// stops it. All three are the ordinary shape of a tenant being suspended or
// deleted under load — the engine is refusing to act on state nothing tracks,
// which is the guard working — so at WARN a single dropped tenant with queued
// work would report a burst of faults for a correctly handled drop, in the
// same channel the real failures use.
func TestScopeDropDiagnosticsAreDebug(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}

	tests := []struct {
		name  string
		msg   string
		drive func(t *testing.T, e *Engine)
	}{
		{
			name: "a reconcile queued behind the drop",
			msg:  "reconcile for a scope state the engine no longer tracks, abandoning",
			drive: func(t *testing.T, e *Engine) {
				sc := e.trackedScope(store.Scope{})

				sc.armReconcile()

				pending, ok := sc.takeReconcile()
				if !ok {
					t.Fatal("arming a reconcile left the mailbox empty")
				}

				e.dropScope(store.Scope{})
				e.runOneReconcile(e.dispatchContext(), sc, pending)
			},
		},
		{
			name: "a write addressed to a scope the engine never tracked",
			msg:  "write for an untracked scope, dropping",
			drive: func(t *testing.T, e *Engine) {
				e.Publish(context.Background(), store.Scope{Tenant: "acme"}, jsonRow(nk, 1, `"5"`, "ops"))
			},
		},
		{
			// The third path into the same drop, and the one that runs on the
			// backend's own changefeed goroutine: a notification the feed was
			// already carrying when the scope went away. The key is
			// registered, so nothing earlier rejects it — the drop is what
			// stops it.
			name: "a changefeed notification that outlived its scope",
			msg:  "changefeed work for an untracked scope, dropping",
			drive: func(_ *testing.T, e *Engine) {
				e.dropScope(store.Scope{})
				e.onEvent(upsertEvent(store.Scope{}, nk, 1))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, newFakeStore())

			tt.drive(t, e)

			requireLoggedAt(t, rec, log.LevelDebug, tt.msg)
		})
	}
}

// rereadPanicMsg is the one line a panic under a changefeed re-read produces,
// whichever of the two re-read paths raised it. It says "changefeed" rather
// than "debounced" because one of those paths is not debounced at all: the
// inline re-read a consumer on WithDebounce(0) takes.
const rereadPanicMsg = "systemplane.engine: changefeed re-read panicked"

// panicRecoveredMsg is the line lib-observability's own handler emits, and the
// only thing a test can see of the accounting that comes with it:
// HandlePanicValue records panic_recovered_total and a span event, neither of
// which is readable from here. Asserting the line is what keeps the call — and
// therefore the metric and the span event — from being dropped in favour of
// the engine's own identity line, which a reader mistakes for the whole
// report.
const panicRecoveredMsg = "panic recovered"

// requirePanicAccounted asserts the handler ran for the named source, so the
// panic counter and the span event were recorded and not only logged.
func requirePanicAccounted(t *testing.T, r *recordingLogger, source string) {
	t.Helper()

	requireLoggedAt(t, r, log.LevelError, panicRecoveredMsg)

	got, ok := findLogged(r, panicRecoveredMsg).field("source")
	if !ok || got.Value != source {
		t.Errorf("%q source field: got %v (present=%t), want %q: the accounting was recorded under "+
			"another site, or the engine reported the panic without it",
			panicRecoveredMsg, got.Value, ok, source)
	}
}

// TestReReadPanicNamesTheKey pins the identity on the one panic the debouncer
// alone would report anonymously.
//
// runtime.RecoverAndLog, the debouncer's generic guard, logs source="debounce"
// and — in production mode — a redacted value with no stack, so an operator
// paged by it learns something under the debouncer blew up and never which
// tenant, namespace or key. The engine's own recovery is what turns that into
// an actionable line.
//
// Both quiet windows are covered because they reach the recovery by different
// routes: zero runs the re-read inline on the changefeed goroutine — a
// documented production mode, not only a test convenience — and non-zero hands
// it to a timer goroutine the WaitGroup tracks. Neither may lose the identity.
//
// Each case then proves the engine survives its own panic rather than merely
// reporting it: the exploding hook is cleared, a newer row is seeded, and a
// second notification for the same key has to travel the whole path again —
// re-read, ingress, publish, delivery. A recovery that left the key fenced,
// the scope locked or the debouncer's timer entry behind would go silent here
// while the first half of the test still passed.
func TestReReadPanicNamesTheKey(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{Tenant: "acme"}

	for _, tc := range []struct {
		name   string
		window time.Duration
	}{
		{name: "inline", window: 0},
		{name: "debounced", window: time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFakeStore()
			rec := &recordingLogger{Logger: log.NewNop()}

			e := New(Config{
				Store:    fs,
				Registry: fakeRegistry{defs: map[NSKey]KeyDef{nk: {Default: "fallback"}}},
				Logger:   rec,
				Debounce: tc.window,
			})
			track(t, e, scope)

			t.Cleanup(func() { _ = e.Close() })

			fs.onGet(func(store.Scope, NSKey) error { panic("the store driver exploded") })

			e.onEvent(upsertEvent(scope, nk, 1))

			// Both lines, because the engine's own is emitted first: waiting
			// on it alone would read the accounting line before the handler
			// wrote it.
			waitFor(t, hangGuard, "the panicking re-read to be reported and accounted", func() bool {
				var reported, accounted bool

				for _, r := range rec.snapshot() {
					switch r.Msg {
					case rereadPanicMsg:
						reported = true
					case panicRecoveredMsg:
						accounted = true
					}
				}

				return reported && accounted
			})

			// requireLogged carries the tenant assertion: a panic naming a
			// namespace and a key and no tenant sends an operator through
			// every tenant's logs to find which one exploded.
			requireLogged(t, rec, log.LevelError, rereadPanicMsg, scope, nk)
			requirePanicAccounted(t, rec, "refresh")

			fs.onGet(nil)
			fs.seed(scope, jsonRow(nk, 2, `"after"`, "ops"))

			var delivered recorder

			unsub := e.OnChange(nk, delivered.record)
			defer unsub()

			e.onEvent(upsertEvent(scope, nk, 2))

			waitFor(t, hangGuard, "the key to be published after the panic", func() bool {
				return delivered.len() == 1
			})

			if got := delivered.changes()[0]; got.Value != "after" || got.Revision != 2 {
				t.Errorf("delivered (rev %d, %v) after a panicking re-read, want (rev 2, %q)",
					got.Revision, got.Value, "after")
			}
		})
	}
}

// slowLogger delays recording one message, so a test can tell "the line was
// written before Close returned" from "Close returned and the line landed a
// moment later" without racing on nanoseconds. A logger that takes a moment to
// reach its sink is what a production one does anyway.
type slowLogger struct {
	*recordingLogger

	msg   string
	delay time.Duration
}

func (s *slowLogger) Log(ctx context.Context, level int, msg string, fields ...any) {
	if msg == s.msg {
		time.Sleep(s.delay)
	}

	s.recordingLogger.Log(ctx, level, msg, fields...)
}

// TestPanicIdentityIsRecordedBeforeCloseReturns pins the ORDER, which is the
// half of the identity line that decides whether an operator ever sees it.
//
// The re-read's WaitGroup release and its panic recovery are both deferred on
// the tracked path, so their order is the registration order: a recovery
// registered outside the release lets Close return — and the process exit, or
// the test binary's logger teardown — while the line naming the tenant, the
// namespace and the key is still being written. Close waiting for the re-read
// has to mean waiting for its report too.
func TestPanicIdentityIsRecordedBeforeCloseReturns(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{Tenant: "acme"}
	fs := newFakeStore()
	rec := &recordingLogger{Logger: log.NewNop()}

	e := New(Config{
		Store:        fs,
		Registry:     fakeRegistry{defs: map[NSKey]KeyDef{nk: {Default: "fallback"}}},
		Logger:       &slowLogger{recordingLogger: rec, msg: rereadPanicMsg, delay: 50 * time.Millisecond},
		Debounce:     time.Millisecond,
		CloseTimeout: 2 * time.Second,
	})
	track(t, e, scope)

	t.Cleanup(func() { _ = e.Close() })

	inGet := make(chan struct{})

	fs.onGet(func(store.Scope, NSKey) error {
		close(inGet)

		panic("the store driver exploded")
	})

	e.onEvent(upsertEvent(scope, nk, 1))

	// Close only once the timer has fired and the re-read is inside the store
	// call: a Close that ran first would discard the pending timer, and there
	// would be no panic to report at all.
	<-inGet

	if err := e.Close(); err != nil {
		t.Fatalf("Close after a panicking re-read: %v, want nil", err)
	}

	requireLogged(t, rec, log.LevelError, rereadPanicMsg, scope, nk)
}

// findLogged returns the single entry carrying msg. requireLogged has already
// asserted there is exactly one by the time a caller reaches here.
func findLogged(r *recordingLogger, msg string) logRecord {
	for _, rec := range r.snapshot() {
		if rec.Msg == msg {
			return rec
		}
	}

	return logRecord{}
}

// TestPerEventDropLinesCostNothingWhenDebugIsOff pins the guard on the three
// drop lines that run per foreign row rather than per failure.
//
// `systemplane_entries` is one table per database, so every foreign write by
// any other consumer reaches this feed, every foreign ROW reaches the ingress
// on every reconcile, and a dropped tenant's subscription keeps delivering
// until it is released. All three lines are DEBUG — off in every production
// deployment — yet the fields are built at the call site, so without the guard
// each of those events still costs a []log.Field and its boxing before the
// logger throws the line away.
func TestPerEventDropLinesCostNothingWhenDebugIsOff(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}

	tests := []struct {
		name  string
		defs  map[NSKey]KeyDef
		drive func(e *Engine)
	}{
		{
			name: "an event for a key another consumer owns",
			defs: map[NSKey]KeyDef{},
			drive: func(e *Engine) {
				e.onEvent(upsertEvent(store.Scope{}, nk, 1))
			},
		},
		{
			name: "a snapshot row another consumer owns",
			defs: map[NSKey]KeyDef{},
			drive: func(e *Engine) {
				// The reconcile's path, not the feed's: the feed drops an
				// unregistered key before the ingress, so a foreign ROW only
				// reaches this line through a snapshot — once per reconcile,
				// for every namespace sharing the table.
				ingestRow(e, jsonRow(nk, 1, `"v"`, "ops"))
			},
		},
		{
			name: "a notification that outlived its scope",
			defs: map[NSKey]KeyDef{nk: {Default: "fallback"}},
			drive: func(e *Engine) {
				e.dropScope(store.Scope{})
				e.onEvent(upsertEvent(store.Scope{}, nk, 1))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingLogger{Logger: log.NewNop(), debugOff: true}
			e := loggingEngineWith(t, tt.defs, newFakeStore(), rec)

			tt.drive(e)

			if got := rec.all(); len(got) != 0 {
				t.Errorf("logger with DEBUG off received %d entries, want 0: %v", len(got), got)
			}
		})
	}
}

// hookLogger runs fn at the instant the engine emits msg, BEFORE the line
// reaches the recorder beneath it.
//
// It is how a test reads engine state at the exact moment the consumer's
// logger is handed control — the only way to pin that a fence was written
// BEFORE any consumer code could run — and how it holds that moment open long
// enough for a reconcile to run inside it.
type hookLogger struct {
	*recordingLogger

	msg string
	fn  func()
}

func (h *hookLogger) Log(ctx context.Context, level int, msg string, fields ...any) {
	if msg == h.msg {
		h.fn()
	}

	h.recordingLogger.Log(ctx, level, msg, fields...)
}

// TestFailedRereadFencesTheKeyBeforeLogging pins the ORDER of the two things a
// re-read that learned nothing does: it must tell every reconcile in flight
// that the key is unusable BEFORE it hands a line to the consumer's logger.
//
// The logger is consumer code and nothing bounds it. A reconcile that reaches
// the key while that code runs finds an empty fence, reads the key's absence
// from its snapshot as a deletion, and publishes the registered default at
// revision 0 — which never loses the fence. Both paths exist to stop exactly
// that, so both have to close it before anything reentrant runs.
func TestFailedRereadFencesTheKeyBeforeLogging(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}

	tests := []struct {
		name    string
		msg     string
		level   int
		arrange func(fs *fakeStore)
	}{
		{
			name:  "the re-read panicked",
			msg:   rereadPanicMsg,
			level: log.LevelError,
			arrange: func(fs *fakeStore) {
				fs.onGet(func(store.Scope, NSKey) error { panic("the store driver exploded") })
			},
		},
		{
			name:  "the re-read errored",
			msg:   "changefeed re-read failed, keeping current value",
			level: log.LevelWarn,
			arrange: func(fs *fakeStore) {
				fs.onGet(func(store.Scope, NSKey) error { return errors.New("backend down") })
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := newFakeStore()
			rec := &recordingLogger{Logger: log.NewNop()}

			var (
				e         *Engine
				fencedYet bool
			)

			e = New(Config{
				Store:    fs,
				Registry: fakeRegistry{defs: map[NSKey]KeyDef{nk: {Default: "fallback"}}},
				Logger: &hookLogger{recordingLogger: rec, msg: tt.msg, fn: func() {
					_, unusable := recordedSets(e, scope)
					fencedYet = len(unusable) == 1 && unusable[0] == nk
				}},
			})

			track(t, e, scope)

			t.Cleanup(func() {
				if err := e.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			})

			// The fence needs somewhere to land: the sets exist only for the
			// window between a reconcile's List and its application.
			_ = armWindow(e.scopeFor(scope))

			tt.arrange(fs)
			e.onEvent(upsertEvent(scope, nk, 1))

			requireLogged(t, rec, tt.level, tt.msg, scope, nk)

			if !fencedYet {
				t.Errorf("%q reached the consumer's logger with %v still unfenced: a reconcile running "+
					"inside that call treats the key as absent and publishes the registered default "+
					"over a live value", tt.msg, nk)
			}
		})
	}
}

// TestPanicUnderReReadCannotResetALiveValue is the failure that order prevents,
// end to end and on the clock: a key whose re-read panics while a reconcile
// applies a snapshot taken after the row was removed, with a consumer logger
// slow enough — 50ms, what shipping a line to a remote sink costs — for the
// whole reconcile to run inside the panic report.
func TestPanicUnderReReadCannotResetALiveValue(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}
	fs := newFakeStore()
	rec := &recordingLogger{Logger: log.NewNop()}
	logging := make(chan struct{})

	e := New(Config{
		Store:    fs,
		Registry: fakeRegistry{defs: map[NSKey]KeyDef{nk: {Default: "fallback"}}},
		Logger: &hookLogger{recordingLogger: rec, msg: rereadPanicMsg, fn: func() {
			close(logging)
			time.Sleep(50 * time.Millisecond)
		}},
	})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	fs.seed(scope, jsonRow(nk, 2, `"live"`, "ops"))
	settled(t, e, scope)

	var sub recorder

	unsub := e.OnChange(nk, sub.record)
	defer unsub()

	e.onEvent(disconnectEvent(scope))

	release := heldList(fs)
	e.onEvent(resyncEvent(scope))

	// The snapshot the held List is about to take no longer carries the row,
	// and the re-read that would have taught the engine about the key blows up.
	fs.remove(scope, nk)
	fs.onGet(func(store.Scope, NSKey) error { panic("the store driver exploded") })

	done := make(chan struct{})

	go func() {
		defer close(done)

		e.onEvent(upsertEvent(scope, nk, 3))
	}()

	// Release the snapshot the moment the panic report reaches the consumer's
	// logger: the reconcile then decides the key inside that call.
	<-logging

	release()
	waitReconcileIdle(t, e, scope)

	got, ok := e.Lookup(scope, nk)
	if !ok {
		t.Fatal("Lookup reports a miss: the reconcile erased the cache")
	}

	if got.Value != "live" || got.Revision != 2 {
		t.Errorf("with the panic report still inside the consumer's logger: got (%v, rev %d), want the "+
			"cached (\"live\", rev 2) — the reconcile read an empty fence and reset a live value to "+
			"its registered default", got.Value, got.Revision)
	}

	if n := sub.len(); n != 0 {
		t.Errorf("deliveries: got %d, want 0: every subscriber of the key was told it changed", n)
	}

	<-done
}
