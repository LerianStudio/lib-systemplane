//go:build unit

package engine

import (
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// engineTracking returns an Engine that tracks sc and nothing else. Scope
// creation on demand arrives with the publish fence; these tests seed the
// cache directly because they exercise the read path alone.
func engineTracking(sc *scopeState) *Engine {
	return &Engine{scopes: map[store.Scope]*scopeState{sc.scope: sc}}
}

func TestLookupReturnsCachedEntryWithProvenance(t *testing.T) {
	updatedAt := time.Date(2026, time.September, 17, 12, 0, 0, 0, time.UTC)
	nk := NSKey{Namespace: "billing", Key: "limits"}

	sc := newScopeState(store.Scope{})
	sc.entries[nk] = entry{
		Value:     map[string]any{"limit": float64(10)},
		Revision:  7,
		UpdatedAt: updatedAt,
		UpdatedBy: "ops",
	}

	got, ok := engineTracking(sc).Lookup(store.Scope{}, nk)
	if !ok {
		t.Fatal("Lookup on a cached key: ok is false, want true")
	}

	if got.Revision != 7 {
		t.Errorf("revision: got %d, want 7", got.Revision)
	}

	if !got.UpdatedAt.Equal(updatedAt) {
		t.Errorf("updatedAt: got %s, want %s", got.UpdatedAt, updatedAt)
	}

	if got.UpdatedBy != "ops" {
		t.Errorf("updatedBy: got %q, want %q", got.UpdatedBy, "ops")
	}

	value, isMap := got.Value.(map[string]any)
	if !isMap {
		t.Fatalf("value: got %T, want map[string]any", got.Value)
	}

	value["limit"] = float64(99)

	again, _ := engineTracking(sc).Lookup(store.Scope{}, nk)
	if again.Value.(map[string]any)["limit"] != float64(10) {
		t.Errorf("mutating the returned value reached the cache: got %v, want 10", again.Value)
	}
}

func TestLookupOnUnknownScopeOrKeyReportsMiss(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}

	sc := newScopeState(store.Scope{})
	sc.entries[nk] = entry{Value: "cached", Revision: 1}

	e := engineTracking(sc)

	if got, ok := e.Lookup(store.Scope{Tenant: "t1"}, nk); ok || got != (Entry{}) {
		t.Errorf("Lookup on an untracked scope: got (%+v, %t), want (Entry{}, false)", got, ok)
	}

	// A miss inside a tracked scope still reports that scope's staleness: the
	// caller answers it with the registered default, and a default served out
	// of an unreconciled scope is not a confirmed value (FC-5).
	unknown := NSKey{Namespace: "billing", Key: "unregistered"}
	if got, ok := e.Lookup(store.Scope{}, unknown); ok || got != (Entry{Stale: true}) {
		t.Errorf("Lookup on an uncached key: got (%+v, %t), want (Entry{Stale: true}, false)", got, ok)
	}
}

func TestLookupOnNilEngineDoesNotPanic(t *testing.T) {
	var e *Engine

	got, ok := e.Lookup(store.Scope{}, NSKey{Namespace: "billing", Key: "limits"})
	if ok || got != (Entry{}) {
		t.Errorf("Lookup on a nil Engine: got (%+v, %t), want (Entry{}, false)", got, ok)
	}
}

func TestNewScopeStartsStale(t *testing.T) {
	sc := newScopeState(store.Scope{})

	if len(sc.entries) != 0 {
		t.Errorf("a new scope caches %d entries, want 0: registered defaults must not be pre-seeded", len(sc.entries))
	}

	nk := NSKey{Namespace: "billing", Key: "limits"}
	sc.entries[nk] = entry{Value: "published", Revision: 1}

	got, ok := engineTracking(sc).Lookup(store.Scope{}, nk)
	if !ok {
		t.Fatal("Lookup on a cached key: ok is false, want true")
	}

	if !got.Stale {
		t.Error("stale: got false, want true before the scope's first reconcile")
	}
}
