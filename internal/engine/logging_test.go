//go:build unit

package engine

import (
	"context"
	"errors"
	"fmt"
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
	const msg = "stored value rejected by validator, keeping cached value"

	nk := NSKey{Namespace: "billing", Key: "limits"}
	row := jsonRow(nk, 1, `42`, "ops")

	e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {
		Default:  "fallback",
		Validate: func(context.Context, any) error { return errors.New("want a string") },
	}}, newFakeStore())

	ingestRow(e, row)

	requireLogged(t, rec, log.LevelWarn, msg, nk)

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

// TestLogLevelReReadWithNoRowIsDebug pins the re-read that finds nothing. The
// write may simply not be visible to this reader yet, and a real removal
// arrives as its own delete event, so this is ordinary rather than wrong: at
// WARN every routine race between a notification and its row becoming readable
// would reach an operator as a fault.
func TestLogLevelReReadWithNoRowIsDebug(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {Default: "fallback"}}, newFakeStore())

	e.refreshKey(store.Scope{}, nk)

	requireLogged(t, rec, log.LevelDebug, "changefeed re-read found no row, keeping current value", nk)
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

			requireLogged(t, rec, log.LevelDebug, "changefeed event for unregistered key, skipping", nk)
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
	requireNotRedacted(t, got)

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
		requireNotRedacted(t, got)

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
		requireNotRedacted(t, got)

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

	requireLogged(t, rec, log.LevelWarn, "changefeed re-read failed, keeping current value", nk)
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

	requireLogged(t, rec, log.LevelDebug, "value for unregistered key, skipping", nk)
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

	requireLogged(t, rec, log.LevelWarn, "stored value rejected by validator, keeping cached value", nk)
}

// TestScopeDropDiagnosticsAreDebug pins the level of the two lines a dropped
// scope emits for work that was already moving when it was dropped: a
// reconcile sitting in the mailbox, and a write on the consumer's goroutine
// that had already resolved its scope. Both are the ordinary shape of a tenant
// being suspended or deleted under load — the engine is refusing to act on
// state nothing tracks, which is the guard working — so at WARN a single
// dropped tenant with queued work would report a burst of faults for a
// correctly handled drop, in the same channel the real failures use.
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

// TestDebouncedReReadPanicNamesTheKey pins the identity on the one panic the
// debouncer alone would report anonymously.
//
// runtime.RecoverAndLog, the debouncer's generic guard, logs source="debounce"
// and — in production mode — a redacted value with no stack, so an operator
// paged by it learns something under the debouncer blew up and never which
// tenant, namespace or key. The engine's own recovery is what turns that into
// an actionable line, and the engine stays usable afterwards: the re-read is
// dropped, not the process.
func TestDebouncedReReadPanicNamesTheKey(t *testing.T) {
	const msg = "systemplane.engine: debounced re-read panicked"

	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{Tenant: "acme"}
	fs := newFakeStore()
	rec := &recordingLogger{Logger: log.NewNop()}

	e := New(Config{
		Store:    fs,
		Registry: fakeRegistry{defs: map[NSKey]KeyDef{nk: {Default: "fallback"}}},
		Logger:   rec,
		Debounce: time.Millisecond,
	})
	track(t, e, scope)

	fs.onGet(func(store.Scope, NSKey) error { panic("the store driver exploded") })

	e.onEvent(upsertEvent(scope, nk, 1))

	waitFor(t, time.Second, "the panicking re-read to be reported", func() bool {
		for _, r := range rec.snapshot() {
			if r.Msg == msg {
				return true
			}
		}

		return false
	})

	requireLogged(t, rec, log.LevelError, msg, nk)

	tenant, ok := findLogged(rec, msg).field(constants.AttrKeyTenantID)
	if !ok || tenant.Value != scope.Tenant {
		t.Errorf("%q tenant field: got %v (present=%t), want %q: a panic naming no tenant sends an "+
			"operator through every tenant's logs", msg, tenant.Value, ok, scope.Tenant)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("Close after a panicking re-read: %v, want nil: one exploding store call must not "+
			"strand shutdown", err)
	}
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
