//go:build unit

package engine

import (
	"context"
	"errors"
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

	if notify := e.ingestDefault(context.Background(), e.scopeFor(store.Scope{}), nk, true); !notify {
		t.Error("no-row publication: notify is false, want true")
	}

	got := cachedEntry(t, e, store.Scope{}, nk)
	if got.Value != "fallback" || got.Revision != 0 {
		t.Errorf("cached: got (%v, rev %d), want (\"fallback\", rev 0)", got.Value, got.Revision)
	}

	if !got.UpdatedAt.IsZero() || got.UpdatedBy != "" {
		t.Errorf("provenance: got (%s, %q), want (zero time, \"\")", got.UpdatedAt, got.UpdatedBy)
	}

	if notify := e.ingestDefault(context.Background(), e.scopeFor(store.Scope{}), NSKey{Namespace: "billing", Key: "unknown"}, true); notify {
		t.Error("no-row publication for an unregistered key: notify is true, want false")
	}
}

func TestIngestClonesRegisteredDefault(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	registered := map[string]any{"limit": float64(10)}
	e := engineWithRegistry(fakeRegistry{defs: map[NSKey]KeyDef{nk: {Default: registered}}})

	if notify := e.ingestDefault(context.Background(), e.scopeFor(store.Scope{}), nk, true); !notify {
		t.Fatal("no-row publication: notify is false, want true")
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
	runtime.SetProductionMode(true)
	t.Cleanup(func() { runtime.SetProductionMode(false) })

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

// TestPublishReportsEveryRefusalItCanStillMake pins the write path's return
// value to what the next read will serve. A local write is graded by the
// Client before the store sees it, so the engine grades it no second time —
// but it can still refuse the publication, and a refusal it swallowed left the
// Client returning nil for a write nobody could read back.
func TestPublishReportsEveryRefusalItCanStillMake(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	defs := map[NSKey]KeyDef{nk: {Default: "fallback"}}

	t.Run("an accepted write reports nothing", func(t *testing.T) {
		e, _ := loggingEngine(t, defs, newFakeStore())

		if err := e.Publish(context.Background(), store.Scope{}, jsonRow(nk, 1, `"written"`, "ops")); err != nil {
			t.Fatalf("Publish: %v, want nil", err)
		}
	})

	t.Run("a closed engine reports ErrClosed", func(t *testing.T) {
		e, _ := loggingEngine(t, defs, newFakeStore())

		if err := e.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		if err := e.Publish(context.Background(), store.Scope{}, jsonRow(nk, 1, `"written"`, "ops")); !errors.Is(err, ErrClosed) {
			t.Errorf("Publish on a closed engine: got %v, want ErrClosed", err)
		}
	})

	t.Run("a nil engine reports ErrClosed", func(t *testing.T) {
		var e *Engine

		if err := e.Publish(context.Background(), store.Scope{}, jsonRow(nk, 1, `"written"`, "ops")); !errors.Is(err, ErrClosed) {
			t.Errorf("Publish on a nil engine: got %v, want ErrClosed", err)
		}
	})

	t.Run("an untracked scope reports the drop", func(t *testing.T) {
		e, _ := loggingEngine(t, defs, newFakeStore())

		if err := e.Publish(context.Background(), store.Scope{Tenant: "acme"}, jsonRow(nk, 1, `"written"`, "ops")); err == nil {
			t.Error("Publish into an untracked scope reported success for a write nothing cached")
		}
	})

	t.Run("an unregistered key reports the skip", func(t *testing.T) {
		e, _ := loggingEngine(t, defs, newFakeStore())

		foreign := NSKey{Namespace: "billing", Key: "unregistered"}

		if err := e.Publish(context.Background(), store.Scope{}, jsonRow(foreign, 1, `"written"`, "ops")); err == nil {
			t.Error("Publish of an unregistered key reported success")
		}
	})

	t.Run("undecodable bytes report the decode failure", func(t *testing.T) {
		e, _ := loggingEngine(t, defs, newFakeStore())

		if err := e.Publish(context.Background(), store.Scope{}, jsonRow(nk, 1, `{not json`, "ops")); err == nil {
			t.Error("Publish of an undecodable value reported success for a write nothing cached")
		}
	})
}
