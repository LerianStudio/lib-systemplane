//go:build unit

package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// fakeRegistry is the only test double this file needs: ingest consumes a
// store.Entry the caller already holds, so no fake store is involved.
type fakeRegistry struct {
	defs map[NSKey]KeyDef
}

func (r fakeRegistry) Lookup(namespace, key string) (KeyDef, bool) {
	def, ok := r.defs[NSKey{Namespace: namespace, Key: key}]

	return def, ok
}

// AnyRedacted scans the defs the way the Client scans its registry.
func (r fakeRegistry) AnyRedacted() bool {
	for _, def := range r.defs {
		if def.Redacted {
			return true
		}
	}

	return false
}

func (r fakeRegistry) Keys() []NSKey {
	keys := make([]NSKey, 0, len(r.defs))
	for nk := range r.defs {
		keys = append(keys, nk)
	}

	return keys
}

// engineWithRegistry tracks the single-tenant scope, the way Start leaves it:
// ingest publishes, and a publication into an untracked scope is dropped.
func engineWithRegistry(reg Registry) *Engine {
	e := &Engine{registry: reg, scopes: map[store.Scope]*scopeState{}}
	e.scopeFor(store.Scope{})

	return e
}

// ingestRow puts se through the ingress of the single-tenant scope, the way
// the changefeed re-read and a write both do: decode and validate first, then
// publish and record under the scope's reconcile mutex.
//
// The ingress reports nothing — every caller publishes and moves on — so what
// it made of the row is asserted where it is visible: the cache it did or did
// not change, and the line it logged.
func ingestRow(e *Engine, se store.Entry) {
	_ = e.ingest(context.Background(), e.scopeFor(store.Scope{}), se, feedFence{}, false)
}

func TestIngestRejectsInvalidValueKeepingPrevious(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	e := engineWithRegistry(fakeRegistry{defs: map[NSKey]KeyDef{
		nk: {
			Default: "default",
			Validate: func(_ context.Context, v any) error {
				if _, ok := v.(string); !ok {
					return errors.New("want a string")
				}

				return nil
			},
		},
	}})

	valid := store.Entry{Namespace: nk.Namespace, Key: nk.Key, Value: []byte(`"a"`), Revision: 1, UpdatedBy: "ops"}
	ingestRow(e, valid)

	rejected := store.Entry{Namespace: nk.Namespace, Key: nk.Key, Value: []byte(`42`), Revision: 2, UpdatedBy: "typo"}
	ingestRow(e, rejected)

	got := cachedEntry(t, e, store.Scope{}, nk)
	if got.Value != "a" || got.Revision != 1 {
		t.Errorf("cached after rejection: got (%v, rev %d), want (\"a\", rev 1)", got.Value, got.Revision)
	}

	if got.UpdatedBy != "ops" {
		t.Errorf("provenance after rejection: got %q, want %q", got.UpdatedBy, "ops")
	}
}

func TestIngestSkipsUnregisteredKey(t *testing.T) {
	e := engineWithRegistry(fakeRegistry{})

	unregistered := store.Entry{Namespace: "billing", Key: "unknown", Value: []byte(`"a"`), Revision: 1}
	ingestRow(e, unregistered)

	sc := e.trackedScope(store.Scope{})

	sc.mu.RLock()
	cached := len(sc.entries)
	sc.mu.RUnlock()

	if cached != 0 {
		t.Errorf("unregistered key cached %d entries, want 0: it must not reach publish", cached)
	}
}

func TestIngestSkipsUndecodableJSONKeepingPrevious(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	e := engineWithRegistry(fakeRegistry{defs: map[NSKey]KeyDef{nk: {Default: "default"}}})

	valid := store.Entry{Namespace: nk.Namespace, Key: nk.Key, Value: []byte(`"a"`), Revision: 1, UpdatedBy: "ops"}
	ingestRow(e, valid)

	corrupt := store.Entry{Namespace: nk.Namespace, Key: nk.Key, Value: []byte(`{not json`), Revision: 2, UpdatedBy: "corrupt"}
	ingestRow(e, corrupt)

	got := cachedEntry(t, e, store.Scope{}, nk)
	if got.Value != "a" || got.Revision != 1 || got.UpdatedBy != "ops" {
		t.Errorf("cached after undecodable row: got (%v, rev %d, %q), want (\"a\", rev 1, %q)",
			got.Value, got.Revision, got.UpdatedBy, "ops")
	}
}

func TestIngestDefaultPublishesAtRevisionZero(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	e := engineWithRegistry(fakeRegistry{defs: map[NSKey]KeyDef{nk: {Default: "fallback"}}})

	seeded := store.Entry{Namespace: nk.Namespace, Key: nk.Key, Value: []byte(`"a"`), Revision: 7, UpdatedBy: "ops"}
	ingestRow(e, seeded)

	if notify, err := e.ingestDefault(context.Background(), e.scopeFor(store.Scope{}), nk, true); !notify || err != nil {
		t.Errorf("no-row publication: (notify %t, err %v), want (true, nil)", notify, err)
	}

	got := cachedEntry(t, e, store.Scope{}, nk)
	if got.Value != "fallback" || got.Revision != 0 {
		t.Errorf("cached: got (%v, rev %d), want (\"fallback\", rev 0)", got.Value, got.Revision)
	}

	if !got.UpdatedAt.IsZero() || got.UpdatedBy != "" {
		t.Errorf("provenance: got (%s, %q), want (zero time, \"\")", got.UpdatedAt, got.UpdatedBy)
	}

	if notify, err := e.ingestDefault(context.Background(), e.scopeFor(store.Scope{}),
		NSKey{Namespace: "billing", Key: "unknown"}, true); notify || err == nil {
		t.Errorf("no-row publication for an unregistered key: (notify %t, err %v), want (false, an error)", notify, err)
	}
}

func TestIngestClonesRegisteredDefault(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	registered := map[string]any{"limit": float64(10)}
	e := engineWithRegistry(fakeRegistry{defs: map[NSKey]KeyDef{nk: {Default: registered}}})

	if notify, err := e.ingestDefault(context.Background(), e.scopeFor(store.Scope{}), nk, true); !notify || err != nil {
		t.Fatalf("no-row publication: (notify %t, err %v), want (true, nil)", notify, err)
	}

	cached, isMap := cachedEntry(t, e, store.Scope{}, nk).Value.(map[string]any)
	if !isMap {
		t.Fatalf("cached default: got %T, want map[string]any", cachedEntry(t, e, store.Scope{}, nk).Value)
	}

	cached["limit"] = float64(99)

	if registered["limit"] != float64(10) {
		t.Errorf("mutating the cached default reached the registry: got %v, want 10", registered["limit"])
	}
}

// TestIngestRejectsPanickingValidatorWithoutLeakingPanicValue pins the
// contract for a validator that panics while inspecting a row: the row is
// rejected, the key keeps the last value that passed, and the panic is
// reported through lib-observability's recovery pipeline, which redacts the
// panic value in production mode. A validator that panics on the secret it was
// handed must not be the thing that turns that secret into a log line.
func TestIngestRejectsPanickingValidatorWithoutLeakingPanicValue(t *testing.T) {
	// lib-observability's production mode is a process-global switch, which is why it is
	// restored in t.Cleanup and why this test must never run in parallel with another.
	productionMode(t)

	const secret = "validator-panic-sentinel-Zq7Xk"

	nk := NSKey{Namespace: "billing", Key: "limits"}
	logger := &recordingLogger{Logger: log.NewNop()}
	e := engineWithRegistry(fakeRegistry{defs: map[NSKey]KeyDef{
		nk: {
			Default: "default",
			Validate: func(_ context.Context, v any) error {
				if v == "a" {
					return nil
				}

				panic(secret)
			},
		},
	}})
	e.logger = logger

	valid := store.Entry{Namespace: nk.Namespace, Key: nk.Key, Value: []byte(`"a"`), Revision: 1, UpdatedBy: "ops"}
	ingestRow(e, valid)

	panicking := store.Entry{Namespace: nk.Namespace, Key: nk.Key, Value: []byte(`42`), Revision: 2, UpdatedBy: "typo"}
	ingestRow(e, panicking)

	if got := cachedEntry(t, e, store.Scope{}, nk); got.Value != "a" || got.Revision != 1 {
		t.Errorf("cached after a panicking validator: got (%v, rev %d), want (\"a\", rev 1)",
			got.Value, got.Revision)
	}

	entries := logger.all()
	recovered := false

	for _, entry := range entries {
		if strings.Contains(entry, "panic recovered") {
			recovered = true
		}

		if strings.Contains(entry, secret) {
			t.Errorf("a log entry carries the panic value: %s", entry)
		}
	}

	if !recovered {
		t.Errorf("no panic-recovery entry was logged, got %v", entries)
	}
}

// ctxMarkerKey is the request marker these tests stamp on a context. Each
// ingress either carries it to the registered validator or must not: the write
// path is the caller's own goroutine and keeps it, the two read-back paths run
// on engine-owned goroutines and carry nothing of the caller's.
type ctxMarkerKey struct{}

const ctxMarker = "request-42"

func markedContext() context.Context {
	return context.WithValue(context.Background(), ctxMarkerKey{}, ctxMarker)
}

// markerWatcher is a registered validator that records, per call, whether the
// context it was handed carried the request marker — and refuses the value
// when it did not, the way a tenant-aware validator refuses a row it cannot
// scope to a tenant.
type markerWatcher struct {
	mu   sync.Mutex
	sawn []bool
}

func (w *markerWatcher) validate(ctx context.Context, _ any) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	saw := ctx.Value(ctxMarkerKey{}) == ctxMarker
	w.sawn = append(w.sawn, saw)

	if !saw {
		return errors.New("no tenant in context")
	}

	return nil
}

func (w *markerWatcher) observations() []bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]bool(nil), w.sawn...)
}

// TestPublishDoesNotRegradeALocalWrite pins the write half of the per-ingress
// grading contract: a local publication has already been graded by the Client,
// against these same canonical bytes and under the caller's own context, so
// the engine does not run the registered validator over it a second time.
//
// The watcher here refuses whatever it is handed on a context carrying no
// request marker, which is what a second grading would be handed on this path.
// It is never called, so the write reaches the cache — and a validator whose
// answer moved between the two calls can no longer strand a write the Client
// already reported as landed.
func TestPublishDoesNotRegradeALocalWrite(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}

	var watcher markerWatcher

	e, _ := loggingEngine(t, map[NSKey]KeyDef{nk: {
		Default:  "fallback",
		Validate: watcher.validate,
	}}, newFakeStore())

	if err := e.Publish(context.Background(), store.Scope{}, jsonRow(nk, 1, `"accepted"`, "ops")); err != nil {
		t.Fatalf("Publish: %v, want nil", err)
	}

	if got := watcher.observations(); len(got) != 0 {
		t.Fatalf("the engine graded a local write the Client had already graded: %v", got)
	}

	entry, ok := e.Lookup(store.Scope{}, nk)
	if !ok || entry.Value != "accepted" {
		t.Errorf("the write did not reach the cache: %+v (ok=%v)", entry, ok)
	}
}

// TestIngestValidatorGetsNoTenantOnFeedAndReconcile pins the read-back half.
// A value that arrives from the changefeed re-read or from a reconcile
// snapshot is validated with the engine's dispatch context, which carries no
// tenant and no request — nothing of whatever goroutine happened to call
// Start. A validator that refuses without a tenant therefore refuses every
// stored row, and the contract is what happens then: the last value that
// passed stays in force, nothing is delivered, and the rejection is logged
// once so an operator can find it.
func TestIngestValidatorGetsNoTenantOnFeedAndReconcile(t *testing.T) {
	const rejection = "stored value rejected by validator, keeping cached value"

	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{}

	// arrange seeds the cache through the write path, which the Client has
	// already graded and the engine therefore does not grade again — so both
	// subtests start from a value in force with no observation recorded, and
	// the only grading either of them sees is its own read-back's.
	arrange := func(t *testing.T) (*Engine, *fakeStore, *recordingLogger, *markerWatcher) {
		t.Helper()

		watcher := &markerWatcher{}
		fs := newFakeStore()

		e, rec := loggingEngine(t, map[NSKey]KeyDef{nk: {
			Default:  "fallback",
			Validate: watcher.validate,
		}}, fs)

		if err := e.Publish(context.Background(), scope, jsonRow(nk, 1, `"in-force"`, "ops")); err != nil {
			t.Fatalf("seeding Publish: %v", err)
		}

		// The store moves on behind the engine's back, which is what both
		// read-back paths exist to notice.
		fs.seed(scope, jsonRow(nk, 2, `"from-the-store"`, "operator"))

		return e, fs, rec, watcher
	}

	requireValueInForce := func(t *testing.T, e *Engine, watcher *markerWatcher, rec *recordingLogger) {
		t.Helper()

		entry, ok := e.Lookup(scope, nk)
		if !ok || entry.Value != "in-force" || entry.Revision != 1 {
			t.Errorf("a row the validator refused replaced the value in force: %+v (ok=%v)", entry, ok)
		}

		if got := watcher.observations(); len(got) != 1 || got[0] {
			t.Errorf("validator contexts: got %v, want [false] — the only grading on either "+
				"path is the read-back's, and it carries no request", got)
		}

		requireOneRecord(t, rec, rejection)
	}

	t.Run("changefeed re-read", func(t *testing.T) {
		e, _, rec, watcher := arrange(t)

		e.onEvent(upsertEvent(scope, nk, 2))

		requireValueInForce(t, e, watcher, rec)
	})

	t.Run("reconcile", func(t *testing.T) {
		e, fs, rec, watcher := arrange(t)

		fs.resyncOnSubscribe()

		// Start is handed a context carrying the request marker. Nothing of it
		// may reach the reconcile: the engine subscribes and reconciles on its
		// own lifecycle context, so the marker must be invisible both to the
		// store's Subscribe and to the validator the reconcile runs.
		if err := e.Start(markedContext()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if got := fs.subscribeContext(); got == nil || got.Value(ctxMarkerKey{}) != nil {
			t.Errorf("the changefeed was opened on the caller's context, so it dies with the "+
				"request instead of with Close: %v", got)
		}

		requireValueInForce(t, e, watcher, rec)
	})
}

// refusalEngine builds an engine tracking the single-tenant scope over a
// registry the case may hook, the way the Client leaves it after Start.
func refusalEngine(t *testing.T, reg Registry) *Engine {
	t.Helper()

	e := New(Config{Store: newFakeStore(), Registry: reg, Logger: log.NewNop()})
	track(t, e, store.Scope{})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	return e
}

// requireRefusal grades one write path's return value against what the next
// read will serve: nil only when the cache holds the write or something newer,
// and otherwise an error an operator can both match and read.
func requireRefusal(t *testing.T, what string, err error, wantErr error, wantText string) {
	t.Helper()

	if wantErr == nil && wantText == "" {
		if err != nil {
			t.Fatalf("%s: %v, want nil", what, err)
		}

		return
	}

	if err == nil {
		t.Fatalf("%s reported success for a change the next read cannot serve", what)
	}

	if wantErr != nil && !errors.Is(err, wantErr) {
		t.Errorf("%s: got %v, want errors.Is %v", what, err, wantErr)
	}

	if wantText != "" && !strings.Contains(err.Error(), wantText) {
		t.Errorf("%s: %v does not name %q", what, err, wantText)
	}
}

// closeUnderTheCall and stopScopeUnderTheCall are the two seams that reach the
// drops publish makes AFTER the exported guard has already passed. Registry
// lookup is consumer code and runs between the two, which is exactly the
// window a Client's Close or a dropped scope lands in.
func closeUnderTheCall(e *Engine) func(NSKey) {
	return func(NSKey) { e.closed.Store(true) }
}

func stopScopeUnderTheCall(e *Engine) func(NSKey) {
	return func(NSKey) { e.trackedScope(store.Scope{}).stopReconcileWorker() }
}

// TestPublishReportsEveryRefusalItCanStillMake pins the write path's return
// value to what the next read will serve. A local write is graded by the
// Client before the store sees it, so the engine grades it no second time —
// but it can still refuse the publication, and a refusal it swallowed left the
// Client returning nil for a write nobody could read back.
//
// One table rather than a subtest each, with a string every refusal must carry:
// a refusal added without a row here fails the build of nothing, but a refusal
// added without a MESSAGE fails this test, which is the drift that matters.
func TestPublishReportsEveryRefusalItCanStillMake(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	foreign := NSKey{Namespace: "billing", Key: "unregistered"}
	defs := map[NSKey]KeyDef{nk: {Default: "fallback"}}

	cases := []struct {
		name     string
		engine   func(t *testing.T, reg *hookedRegistry) *Engine
		scope    store.Scope
		row      store.Entry
		wantErr  error
		wantText string
	}{
		{
			name:   "an accepted write reports nothing",
			engine: func(t *testing.T, reg *hookedRegistry) *Engine { return refusalEngine(t, reg) },
			row:    jsonRow(nk, 1, `"written"`, "ops"),
		},
		{
			name:     "a nil engine reports ErrClosed",
			engine:   func(*testing.T, *hookedRegistry) *Engine { return nil },
			row:      jsonRow(nk, 1, `"written"`, "ops"),
			wantErr:  ErrClosed,
			wantText: "engine is closed",
		},
		{
			name: "a closed engine reports ErrClosed",
			engine: func(t *testing.T, reg *hookedRegistry) *Engine {
				e := refusalEngine(t, reg)
				if err := e.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}

				return e
			},
			row:      jsonRow(nk, 1, `"written"`, "ops"),
			wantErr:  ErrClosed,
			wantText: "engine is closed",
		},
		{
			name: "an engine closed under the write reports ErrClosed",
			engine: func(t *testing.T, reg *hookedRegistry) *Engine {
				e := refusalEngine(t, reg)
				reg.hookLookup(closeUnderTheCall(e))

				return e
			},
			row:      jsonRow(nk, 1, `"written"`, "ops"),
			wantErr:  ErrClosed,
			wantText: "engine is closed",
		},
		{
			name: "a scope stopped under the write reports the drop",
			engine: func(t *testing.T, reg *hookedRegistry) *Engine {
				e := refusalEngine(t, reg)
				reg.hookLookup(stopScopeUnderTheCall(e))

				return e
			},
			row:      jsonRow(nk, 1, `"written"`, "ops"),
			wantErr:  ErrScopeNotTracked,
			wantText: "does not track",
		},
		{
			name:     "an untracked scope reports the drop",
			engine:   func(t *testing.T, reg *hookedRegistry) *Engine { return refusalEngine(t, reg) },
			scope:    store.Scope{Tenant: "acme"},
			row:      jsonRow(nk, 1, `"written"`, "ops"),
			wantErr:  ErrScopeNotTracked,
			wantText: "tenant acme",
		},
		{
			name:     "an unregistered key reports the skip",
			engine:   func(t *testing.T, reg *hookedRegistry) *Engine { return refusalEngine(t, reg) },
			row:      jsonRow(foreign, 1, `"written"`, "ops"),
			wantText: "billing/unregistered",
		},
		{
			name:     "undecodable bytes report the decode failure",
			engine:   func(t *testing.T, reg *hookedRegistry) *Engine { return refusalEngine(t, reg) },
			row:      jsonRow(nk, 1, `{not json`, "ops"),
			wantText: "billing/limits",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := &hookedRegistry{Registry: fakeRegistry{defs: defs}}

			err := tc.engine(t, reg).Publish(context.Background(), tc.scope, tc.row)

			requireRefusal(t, "Publish", err, tc.wantErr, tc.wantText)
		})
	}
}

// TestPublishDeleteReportsEveryRefusalItCanStillMake is the removal's half of
// the table above, and exists for the same reason: Client.Delete removes the
// row and then publishes the registered default so the caller's next read
// stops serving what it deleted, and a drop it never heard about left that
// read serving the deleted value behind a nil error.
func TestPublishDeleteReportsEveryRefusalItCanStillMake(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	foreign := NSKey{Namespace: "billing", Key: "unregistered"}
	defs := map[NSKey]KeyDef{nk: {Default: "fallback"}}

	cases := []struct {
		name     string
		engine   func(t *testing.T, reg *hookedRegistry) *Engine
		scope    store.Scope
		nk       NSKey
		wantErr  error
		wantText string
	}{
		{
			name:   "an accepted delete reports nothing",
			engine: func(t *testing.T, reg *hookedRegistry) *Engine { return refusalEngine(t, reg) },
			nk:     nk,
		},
		{
			name:     "a nil engine reports ErrClosed",
			engine:   func(*testing.T, *hookedRegistry) *Engine { return nil },
			nk:       nk,
			wantErr:  ErrClosed,
			wantText: "engine is closed",
		},
		{
			name: "a closed engine reports ErrClosed",
			engine: func(t *testing.T, reg *hookedRegistry) *Engine {
				e := refusalEngine(t, reg)
				if err := e.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}

				return e
			},
			nk:       nk,
			wantErr:  ErrClosed,
			wantText: "engine is closed",
		},
		{
			name: "an engine closed under the delete reports ErrClosed",
			engine: func(t *testing.T, reg *hookedRegistry) *Engine {
				e := refusalEngine(t, reg)
				reg.hookLookup(closeUnderTheCall(e))

				return e
			},
			nk:       nk,
			wantErr:  ErrClosed,
			wantText: "engine is closed",
		},
		{
			name: "a scope stopped under the delete reports the drop",
			engine: func(t *testing.T, reg *hookedRegistry) *Engine {
				e := refusalEngine(t, reg)
				reg.hookLookup(stopScopeUnderTheCall(e))

				return e
			},
			nk:       nk,
			wantErr:  ErrScopeNotTracked,
			wantText: "does not track",
		},
		{
			name:     "an untracked scope reports the drop",
			engine:   func(t *testing.T, reg *hookedRegistry) *Engine { return refusalEngine(t, reg) },
			scope:    store.Scope{Tenant: "acme"},
			nk:       nk,
			wantErr:  ErrScopeNotTracked,
			wantText: "tenant acme",
		},
		{
			name:     "an unregistered key reports the skip",
			engine:   func(t *testing.T, reg *hookedRegistry) *Engine { return refusalEngine(t, reg) },
			nk:       foreign,
			wantText: "billing/unregistered",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := &hookedRegistry{Registry: fakeRegistry{defs: defs}}

			err := tc.engine(t, reg).PublishDelete(context.Background(), tc.scope, tc.nk)

			requireRefusal(t, "PublishDelete", err, tc.wantErr, tc.wantText)
		})
	}
}

// TestRunValidator pins the exported door the Client grades through.
//
// Register and Set are the two places in the library that hand a validator a
// value without going through an ingress, so this wrapper is the only thing
// between a consumer's panicking validator and the consumer's own process. All
// four outcomes matter to a caller: a nil engine still grades (the Client
// reaches here before anything confirms an engine exists), no validator
// accepts, a returned error comes back untouched so the caller can match its
// own sentinel, and a panic comes back as a refusal that names the panic and
// never the value it panicked on — a configuration row is exactly where a
// secret lives.
func TestRunValidator(t *testing.T) {
	// lib-observability's production mode is a process-global switch: without
	// it the recovery pipeline prints the recovered value, which is the one
	// thing the panic case asserts is absent.
	productionMode(t)

	const secret = "run-validator-panic-sentinel-Wq4Nb"

	refused := errors.New("the scheme is not allowed")

	for _, tc := range []struct {
		name      string
		nilEngine bool
		validate  func(context.Context, any) error
		check     func(t *testing.T, err error, logger *recordingLogger)
	}{
		{
			name:      "a nil engine grades and survives a panic",
			nilEngine: true,
			validate:  func(context.Context, any) error { panic(secret) },
			check: func(t *testing.T, err error, _ *recordingLogger) {
				requireValidatorPanic(t, err)
			},
		},
		{
			name:     "no validator accepts every value",
			validate: nil,
			check: func(t *testing.T, err error, _ *recordingLogger) {
				if err != nil {
					t.Errorf("err = %v, want nil: an unvalidated key refuses nothing", err)
				}
			},
		},
		{
			name:     "a returned error comes back verbatim",
			validate: func(context.Context, any) error { return refused },
			check: func(t *testing.T, err error, _ *recordingLogger) {
				if !errors.Is(err, refused) {
					t.Errorf("err = %v, want the validator's own error", err)
				}

				if err != refused { //nolint:errorlint // verbatim is the contract: no wrapping.
					t.Errorf("err = %#v, want the validator's error unwrapped", err)
				}
			},
		},
		{
			name:     "a panic becomes a validation refusal that never names the value",
			validate: func(context.Context, any) error { panic(secret) },
			check: func(t *testing.T, err error, logger *recordingLogger) {
				requireValidatorPanic(t, err)

				recovered := false

				for _, entry := range logger.all() {
					if strings.Contains(entry, "panic recovered") {
						recovered = true
					}

					if strings.Contains(entry, secret) {
						t.Errorf("a log entry carries the panic value: %s", entry)
					}
				}

				if !recovered {
					t.Errorf("no panic-recovery entry was logged, got %v", logger.all())
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := &recordingLogger{Logger: log.NewNop()}

			var e *Engine
			if !tc.nilEngine {
				e = engineWithRegistry(fakeRegistry{defs: map[NSKey]KeyDef{}})
				e.logger = logger
			}

			tc.check(t, e.RunValidator(context.Background(), store.Scope{}, NSKey{}, tc.validate, secret, false), logger)
		})
	}
}

// requireValidatorPanic asserts the one shape a recovered validator panic ever
// comes back as.
func requireValidatorPanic(t *testing.T, err error) {
	t.Helper()

	if !errors.Is(err, store.ErrValidation) {
		t.Fatalf("err = %v, want store.ErrValidation", err)
	}

	if !strings.Contains(err.Error(), "validator panicked") {
		t.Errorf("err = %q, want it to name the panic", err)
	}
}

// TestValidatorPanicOnARedactedKeyWithholdsTheValue pins the one hole the
// rejection contract still had: a validator that panics NAMING the value it
// was handed.
//
// A returned error is already rendered under the key's redaction policy
// ([ErrorDetail]), but a panic is not the engine's line to write — it goes to
// lib-observability's canonical handler, which logs log.Any("value", panicked)
// whenever production mode is off, and off is the shipped default. The field
// key is "value", which is not on the sensitive-field list, so nothing
// downstream catches it either: a RedactFull key's secret reaches ERROR in the
// clear. Deliberately NOT run in production mode, because that mode is what
// the old assertions leaned on and nothing ships with it.
func TestValidatorPanicOnARedactedKeyWithholdsTheValue(t *testing.T) {
	const secret = "redacted-validator-panic-sentinel-Rk9Tz"

	nk := NSKey{Namespace: "billing", Key: "token"}
	logger := &recordingLogger{Logger: log.NewNop()}
	e := engineWithRegistry(fakeRegistry{defs: map[NSKey]KeyDef{
		nk: {
			Default:  "default",
			Redacted: true,
			Validate: func(_ context.Context, v any) error {
				panic(fmt.Sprintf("refusing %v", v))
			},
		},
	}})
	e.logger = logger

	ingestRow(e, store.Entry{
		Namespace: nk.Namespace,
		Key:       nk.Key,
		Value:     []byte(`"` + secret + `"`),
		Revision:  1,
		UpdatedBy: "ops",
	})

	requirePanicWithheld(t, logger, "validator", "string", secret)
}

// requirePanicWithheld asserts the report of a panic raised by consumer code
// over a redacted key: lib-observability's handler ran under the named source,
// so the panic counter and the span event were recorded and not only logged;
// the line names the panic value's dynamic type, which is enough to tell two
// panics apart; and no entry anywhere carries the value itself.
func requirePanicWithheld(t *testing.T, r *recordingLogger, source, wantType, secret string) {
	t.Helper()

	requirePanicAccounted(t, r, source)

	value, ok := findLogged(r, panicRecoveredMsg).field("value")
	if !ok {
		t.Fatalf("%q carries no value field, got %v", panicRecoveredMsg, r.all())
	}

	rendered := fmt.Sprint(value.Value)
	if !strings.Contains(rendered, "("+wantType+",") || !strings.Contains(rendered, "value withheld") {
		t.Errorf("%q value field: got %q, want the panic value's type and no value", panicRecoveredMsg, rendered)
	}

	for _, entry := range r.all() {
		if strings.Contains(entry, secret) {
			t.Errorf("a log entry carries the value of a redacted key: %s", entry)
		}
	}
}

// productionMode turns lib-observability's production mode on for one test and
// puts back whatever it held, rather than the literal false: the switch is
// process-global, so a test that restores a constant silently turns the mode
// off for every test that ran outside it.
func productionMode(t *testing.T) {
	t.Helper()

	previous := runtime.IsProductionMode()
	runtime.SetProductionMode(true)

	t.Cleanup(func() { runtime.SetProductionMode(previous) })
}
