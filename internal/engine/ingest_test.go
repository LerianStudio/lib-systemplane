//go:build unit

package engine

import (
	"context"
	"errors"
	"strings"
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

func TestIngestRejectsInvalidValueKeepingPrevious(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	e := engineWithRegistry(fakeRegistry{defs: map[NSKey]KeyDef{
		nk: {
			Default: "default",
			Validate: func(v any) error {
				if _, ok := v.(string); !ok {
					return errors.New("want a string")
				}

				return nil
			},
		},
	}})

	valid := store.Entry{Namespace: nk.Namespace, Key: nk.Key, Value: []byte(`"a"`), Revision: 1, UpdatedBy: "ops"}
	if !e.ingest(context.Background(), store.Scope{}, valid) {
		t.Fatal("first valid row: usable is false, want true")
	}

	rejected := store.Entry{Namespace: nk.Namespace, Key: nk.Key, Value: []byte(`42`), Revision: 2, UpdatedBy: "typo"}
	if e.ingest(context.Background(), store.Scope{}, rejected) {
		t.Error("validator-rejected row: usable is true, want false")
	}

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
	if e.ingest(context.Background(), store.Scope{}, unregistered) {
		t.Error("unregistered key: usable is true, want false")
	}

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
	if !e.ingest(context.Background(), store.Scope{}, valid) {
		t.Fatal("first valid row: usable is false, want true")
	}

	corrupt := store.Entry{Namespace: nk.Namespace, Key: nk.Key, Value: []byte(`{not json`), Revision: 2, UpdatedBy: "corrupt"}
	if e.ingest(context.Background(), store.Scope{}, corrupt) {
		t.Error("undecodable JSON: usable is true, want false")
	}

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
	if !e.ingest(context.Background(), store.Scope{}, seeded) {
		t.Fatal("seeding row: usable is false, want true")
	}

	if notify := e.ingestDefault(context.Background(), store.Scope{}, nk); !notify {
		t.Error("no-row publication: notify is false, want true")
	}

	got := cachedEntry(t, e, store.Scope{}, nk)
	if got.Value != "fallback" || got.Revision != 0 {
		t.Errorf("cached: got (%v, rev %d), want (\"fallback\", rev 0)", got.Value, got.Revision)
	}

	if !got.UpdatedAt.IsZero() || got.UpdatedBy != "" {
		t.Errorf("provenance: got (%s, %q), want (zero time, \"\")", got.UpdatedAt, got.UpdatedBy)
	}

	if notify := e.ingestDefault(context.Background(), store.Scope{}, NSKey{Namespace: "billing", Key: "unknown"}); notify {
		t.Error("no-row publication for an unregistered key: notify is true, want false")
	}
}

func TestIngestClonesRegisteredDefault(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	registered := map[string]any{"limit": float64(10)}
	e := engineWithRegistry(fakeRegistry{defs: map[NSKey]KeyDef{nk: {Default: registered}}})

	if notify := e.ingestDefault(context.Background(), store.Scope{}, nk); !notify {
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
	runtime.SetProductionMode(true)
	t.Cleanup(func() { runtime.SetProductionMode(false) })

	const secret = "sk_live_7Rq2pAnIcKeDtOkEn"

	nk := NSKey{Namespace: "billing", Key: "limits"}
	logger := &recordingLogger{Logger: log.NewNop()}
	e := engineWithRegistry(fakeRegistry{defs: map[NSKey]KeyDef{
		nk: {
			Default: "default",
			Validate: func(v any) error {
				if v == "a" {
					return nil
				}

				panic(secret)
			},
		},
	}})
	e.logger = logger

	valid := store.Entry{Namespace: nk.Namespace, Key: nk.Key, Value: []byte(`"a"`), Revision: 1, UpdatedBy: "ops"}
	if !e.ingest(context.Background(), store.Scope{}, valid) {
		t.Fatal("first valid row: usable is false, want true")
	}

	panicking := store.Entry{Namespace: nk.Namespace, Key: nk.Key, Value: []byte(`42`), Revision: 2, UpdatedBy: "typo"}
	if e.ingest(context.Background(), store.Scope{}, panicking) {
		t.Error("row whose validator panicked: usable is true, want false")
	}

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
