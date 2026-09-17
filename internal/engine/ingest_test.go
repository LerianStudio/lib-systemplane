//go:build unit

package engine

import (
	"context"
	"errors"
	"testing"

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

func engineWithRegistry(reg Registry) *Engine {
	return &Engine{registry: reg, scopes: map[store.Scope]*scopeState{}}
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

	if len(e.scopes) != 0 {
		t.Errorf("unregistered key created %d scope(s), want 0: it must not reach publish", len(e.scopes))
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
