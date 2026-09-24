//go:build unit

package engine

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// deadLogger is the consumer logger that is broken rather than slow: every
// entry panics, and so does every level check. A nil field dereferenced in a
// custom Log, a sink closed at shutdown and written to afterwards — it is the
// ordinary way a consumer's observability code fails, and it arrives at the
// engine on engine-owned goroutines where the consumer cannot recover it.
type deadLogger struct{}

func (deadLogger) Log(context.Context, int, string, ...any) { panic("the consumer's logger blew up") }

func (deadLogger) Enabled(int) bool { panic("the consumer's logger blew up") }

func (l deadLogger) With(...any) log.Logger { return l }

func (l deadLogger) WithGroup(string) log.Logger { return l }

func (deadLogger) Sync(context.Context) error { return nil }

// TestAPanickingConsumerLoggerNeverKillsTheEngine covers the three
// engine-owned goroutines that hand a recovered panic to the consumer's
// logger: the reconcile worker, the debounced changefeed re-read, and a
// delivery worker.
//
// Each of them recovers consumer code — a registered validator, a subscriber
// callback — and then REPORTS that recovery through the logger the consumer
// passed to WithLogger. A logger that panics on the report turns the recovered
// panic into an unrecovered one on a goroutine the consumer does not own, and
// takes the process with it. On the v3 path the same input panicked on the
// caller's own stack, where the caller could see it.
//
// The assertions are the outcomes the godoc promises, because "the process
// survived" alone would also pass with a silently broken engine: the key whose
// validator panicked keeps its registered default, the key that validates is
// published, the delivery whose subscriber panicked is dropped and the next
// one still arrives. goleak (TestMain) covers the goroutines.
func TestAPanickingConsumerLoggerNeverKillsTheEngine(t *testing.T) {
	bad := NSKey{Namespace: "billing", Key: "limits"}
	good := NSKey{Namespace: "billing", Key: "mode"}
	scope := store.Scope{}

	fs := newFakeStore()
	e := New(Config{
		Store: fs,
		Registry: fakeRegistry{defs: map[NSKey]KeyDef{
			bad: {Default: "fallback", Validate: func(context.Context, any) error {
				panic("the consumer's validator blew up")
			}},
			good: {Default: "off"},
		}},
		Logger:   deadLogger{},
		Debounce: 0,
	})
	track(t, e, scope)

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("Close: %v, want nil", err)
		}
	})

	// Reconcile path: the whole-scope snapshot runs the panicking validator on
	// the reconcile worker's goroutine.
	fs.seed(scope, jsonRow(bad, 1, `"live"`, "ops"))
	fs.seed(scope, jsonRow(good, 1, `"on"`, "ops"))

	e.onEvent(resyncEvent(scope))
	waitReconcileIdle(t, e, scope)

	requireValue(t, e, scope, bad, "fallback")
	requireValue(t, e, scope, good, "on")

	// Dispatch path: the subscriber panics, and the recovery reports it.
	var delivered recorder

	unsub := e.OnChange(good, func(ctx context.Context, ch Change) {
		if ch.Revision == 2 {
			panic("the consumer's subscriber blew up")
		}

		delivered.record(ctx, ch)
	})
	defer unsub()

	fs.seed(scope, jsonRow(good, 2, `"panicking"`, "ops"))
	e.onEvent(upsertEvent(scope, good, 2))

	// Changefeed re-read path: the panicking validator again, this time on the
	// debounced re-read rather than the reconcile.
	fs.seed(scope, jsonRow(bad, 2, `"newer"`, "ops"))
	e.onEvent(upsertEvent(scope, bad, 2))

	// The dropped delivery is proven by the one that follows it: a worker the
	// panic ended would go silent here.
	fs.seed(scope, jsonRow(good, 3, `"after"`, "ops"))
	e.onEvent(upsertEvent(scope, good, 3))

	waitFor(t, hangGuard, "the delivery after the panicking one", func() bool {
		return delivered.len() == 1
	})

	if got := deliveries(&delivered)[0]; got.Revision != 3 || got.Value != "after" {
		t.Errorf("delivered (rev %d, %v), want (rev 3, %q): the panicking delivery must be "+
			"dropped, not retried, and the next one must still arrive",
			got.Revision, got.Value, "after")
	}

	requireValue(t, e, scope, bad, "fallback")
	requireValue(t, e, scope, good, "after")
}

// requireValue asserts what a key holds in the engine's cache, which is what a
// consumer's Get reads.
func requireValue(t *testing.T, e *Engine, scope store.Scope, nk NSKey, want any) {
	t.Helper()

	waitFor(t, hangGuard, "the cache to settle on "+nk.Key, func() bool {
		got, ok := e.Lookup(scope, nk)

		return ok && got.Value == want
	})
}

// TestRecoverRefreshRetriesBeforeReportingThePanic pins the ORDER inside the
// changefeed re-read's own recovery.
//
// That recovery does two things: it reports the panic — through the consumer's
// logger, and through lib-observability's handler, which logs before it counts
// — and it schedules the one retry the re-read's godoc promises. Reporting
// first makes the repair conditional on the reporting surviving, and the
// engine's guard covers the logger it holds, not whatever the handler reaches.
// The retry is the part an operator cannot replace: without it the key stays
// on a value no read confirmed until the next notification for it, which for a
// knob nobody touches again is never.
//
// The raw panicking logger is installed after New deliberately: the debouncer
// keeps the guarded one, so the escaping report lands in its net the way it
// does in production, and this test asserts the retry rather than the guard.
func TestRecoverRefreshRetriesBeforeReportingThePanic(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}

	fs := newFakeStore()
	lg := &panicLogger{Logger: log.NewNop()}

	e := New(Config{
		Store:    fs,
		Registry: fakeRegistry{defs: map[NSKey]KeyDef{nk: {Default: "fallback"}}},
		Logger:   lg,
		Debounce: 0,
	})
	track(t, e, scope)

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("Close: %v, want nil", err)
		}
	})

	fs.seed(scope, jsonRow(nk, 1, `"live"`, "ops"))

	// The store blows up once, and the report of that panic blows up next.
	fs.onGet(func(store.Scope, NSKey) error {
		fs.onGet(nil)
		lg.armed.Store(true)

		panic("the store driver exploded")
	})

	e.logger = lg

	e.onEvent(upsertEvent(scope, nk, 1))

	waitFor(t, hangGuard, "the re-read to be retried after the panic", func() bool {
		return fs.getCount() == 2
	})

	requireValue(t, e, scope, nk, "live")
}
