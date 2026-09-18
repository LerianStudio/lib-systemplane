//go:build unit

package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// cachedEntry reads the raw cached entry, bypassing Lookup's clone, so a test
// can assert that a rejected or refreshed publication left the stored value
// itself untouched.
func cachedEntry(t *testing.T, e *Engine, scope store.Scope, nk NSKey) entry {
	t.Helper()

	sc := e.scopes[scope]
	if sc == nil {
		t.Fatalf("engine tracks no scope %+v", scope)
	}

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	cached, ok := sc.entries[nk]
	if !ok {
		t.Fatalf("no cached entry for %+v", nk)
	}

	return cached
}

// emptyEngine holds nothing but the single-tenant scope, which exists in
// production from the moment Start has run. These tests drive publish
// directly, and publish no longer creates a scope for itself.
func emptyEngine() *Engine {
	e := &Engine{scopes: map[store.Scope]*scopeState{}}
	e.scopeFor(store.Scope{})

	return e
}

// publishInto resolves the publication's scope and publishes into it, which is
// what every production caller of publish does: resolve the scope once, then
// hand the state down. Tests address a publication by its scope alone, so the
// resolve lives here rather than in every case.
func (e *Engine) publishInto(pub publication) (notify bool) {
	sc := e.trackedScope(pub.Scope)
	if sc == nil {
		return false
	}

	return e.publish(sc, pub)
}

func TestPublishAcceptsHigherRevision(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	e := emptyEngine()

	if notify := e.publishInto(publication{NSKey: nk, Revision: 1, Value: "a", UpdatedBy: "ops"}); !notify {
		t.Error("first publication of a key: notify is false, want true")
	}

	at := time.Date(2026, time.September, 17, 12, 0, 0, 0, time.UTC)
	if notify := e.publishInto(publication{NSKey: nk, Revision: 2, Value: "b", UpdatedAt: at, UpdatedBy: "console"}); !notify {
		t.Error("higher revision: notify is false, want true")
	}

	got := cachedEntry(t, e, store.Scope{}, nk)
	if got.Value != "b" || got.Revision != 2 {
		t.Errorf("cached: got (%v, rev %d), want (\"b\", rev 2)", got.Value, got.Revision)
	}

	if !got.UpdatedAt.Equal(at) || got.UpdatedBy != "console" {
		t.Errorf("provenance: got (%s, %q), want (%s, %q)", got.UpdatedAt, got.UpdatedBy, at, "console")
	}
}

func TestPublishRejectsLowerRevision(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	at := time.Date(2026, time.September, 17, 12, 0, 0, 0, time.UTC)

	e := emptyEngine()
	e.publishInto(publication{NSKey: nk, Revision: 5, Value: "current", UpdatedAt: at, UpdatedBy: "ops"})

	stale := time.Date(2026, time.September, 17, 11, 0, 0, 0, time.UTC)
	if notify := e.publishInto(publication{NSKey: nk, Revision: 4, Value: "old", UpdatedAt: stale, UpdatedBy: "snapshot"}); notify {
		t.Error("lower revision: notify is true, want false")
	}

	got := cachedEntry(t, e, store.Scope{}, nk)
	if got.Value != "current" || got.Revision != 5 {
		t.Errorf("cached: got (%v, rev %d), want (\"current\", rev 5)", got.Value, got.Revision)
	}

	if !got.UpdatedAt.Equal(at) || got.UpdatedBy != "ops" {
		t.Errorf("a rejected publication overwrote provenance: got (%s, %q), want (%s, %q)", got.UpdatedAt, got.UpdatedBy, at, "ops")
	}
}

func TestPublishRefreshesProvenanceWithoutNotify(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	first := time.Date(2026, time.September, 17, 12, 0, 0, 0, time.UTC)

	e := emptyEngine()
	e.publishInto(publication{
		NSKey:     nk,
		Revision:  3,
		Value:     map[string]any{"limit": float64(10)},
		UpdatedAt: first,
		UpdatedBy: "ops",
	})

	// The same row read again: equal revision, and a value that is a distinct
	// object but deeply equal, which is exactly what a re-read of unchanged
	// JSON produces.
	second := time.Date(2026, time.September, 17, 13, 0, 0, 0, time.UTC)
	if notify := e.publishInto(publication{
		NSKey:     nk,
		Revision:  3,
		Value:     map[string]any{"limit": float64(10)},
		UpdatedAt: second,
		UpdatedBy: "console",
	}); notify {
		t.Error("equal revision with an equal value: notify is true, want false")
	}

	got := cachedEntry(t, e, store.Scope{}, nk)
	if !got.UpdatedAt.Equal(second) || got.UpdatedBy != "console" {
		t.Errorf("provenance was not refreshed: got (%s, %q), want (%s, %q)", got.UpdatedAt, got.UpdatedBy, second, "console")
	}

	if got.Revision != 3 {
		t.Errorf("revision: got %d, want 3", got.Revision)
	}

	if limit := got.Value.(map[string]any)["limit"]; limit != float64(10) {
		t.Errorf("value: got %v, want 10", limit)
	}
}

// TestPublishAcceptsSameRevisionWithChangedValue is D3's foreign-writer rule:
// MongoDB has no triggers, so a Console process writing the collection
// directly can change value and leave revision alone. Deduplicating on
// revision alone would make that write invisible forever.
func TestPublishAcceptsSameRevisionWithChangedValue(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	first := time.Date(2026, time.September, 17, 12, 0, 0, 0, time.UTC)

	e := emptyEngine()
	e.publishInto(publication{NSKey: nk, Revision: 3, Value: "a", UpdatedAt: first, UpdatedBy: "ops"})

	second := time.Date(2026, time.September, 17, 13, 0, 0, 0, time.UTC)
	if notify := e.publishInto(publication{NSKey: nk, Revision: 3, Value: "b", UpdatedAt: second, UpdatedBy: "console"}); !notify {
		t.Error("equal revision with a changed value: notify is false, want true")
	}

	got := cachedEntry(t, e, store.Scope{}, nk)
	if got.Value != "b" || got.Revision != 3 {
		t.Errorf("cached: got (%v, rev %d), want (\"b\", rev 3)", got.Value, got.Revision)
	}

	if !got.UpdatedAt.Equal(second) || got.UpdatedBy != "console" {
		t.Errorf("provenance: got (%s, %q), want (%s, %q)", got.UpdatedAt, got.UpdatedBy, second, "console")
	}
}

func TestPublishRevisionZeroAlwaysWinsAndResetsRevision(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}

	e := emptyEngine()
	e.publishInto(publication{
		NSKey:     nk,
		Revision:  9,
		Value:     "persisted",
		UpdatedAt: time.Date(2026, time.September, 17, 12, 0, 0, 0, time.UTC),
		UpdatedBy: "ops",
	})

	if notify := e.publishInto(publication{NSKey: nk, Revision: 0, Value: "default"}); !notify {
		t.Error("revision 0 over a cached revision 9: notify is false, want true")
	}

	got := cachedEntry(t, e, store.Scope{}, nk)
	if got.Value != "default" || got.Revision != 0 {
		t.Errorf("cached: got (%v, rev %d), want (\"default\", rev 0)", got.Value, got.Revision)
	}

	if !got.UpdatedAt.IsZero() || got.UpdatedBy != "" {
		t.Errorf("a no-row publication kept provenance: got (%s, %q), want (zero, \"\")", got.UpdatedAt, got.UpdatedBy)
	}

	// Revision 0 is never deduplicated: a second delete still notifies.
	if notify := e.publishInto(publication{NSKey: nk, Revision: 0, Value: "default"}); !notify {
		t.Error("a repeated revision 0 with an equal value: notify is false, want true")
	}

	// A recreate arrives with a fresh non-zero revision and is accepted over
	// the reset counter, which is why resetting it cannot swallow one.
	if notify := e.publishInto(publication{NSKey: nk, Revision: 1, Value: "recreated"}); !notify {
		t.Error("recreate at revision 1 after a delete: notify is false, want true")
	}

	if got := cachedEntry(t, e, store.Scope{}, nk); got.Value != "recreated" || got.Revision != 1 {
		t.Errorf("cached after recreate: got (%v, rev %d), want (\"recreated\", rev 1)", got.Value, got.Revision)
	}
}

// TestPublishRefusesAnUntrackedScope pins what a publication may NOT do:
// bring a scope into existence. A tenant that was never activated, or one that
// was dropped when it was suspended, must not get a cache with no changefeed
// behind it and no reconcile goroutine to confirm it — that cache would read
// as current forever.
func TestPublishRefusesAnUntrackedScope(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	scope := store.Scope{Tenant: "t1"}

	e := emptyEngine()
	if notify := e.publishInto(publication{Scope: scope, NSKey: nk, Revision: 1, Value: "a"}); notify {
		t.Error("publish into an untracked scope: notify is true, want false")
	}

	if _, ok := e.Lookup(scope, nk); ok {
		t.Error("Lookup after publish into an untracked scope: ok is true, want false")
	}

	if tracked := e.trackedScope(scope); tracked != nil {
		t.Error("a publication created a scope the engine never brought up")
	}

	if _, ok := e.Lookup(store.Scope{}, nk); ok {
		t.Error("publishing into tenant t1 reached the single-tenant scope")
	}
}

// TestPublishIsSerializedUnderRace pins the compare and the store to one
// acquisition of the scope's write lock: splitting them would let a lower
// revision land after a higher one.
func TestPublishIsSerializedUnderRace(t *testing.T) {
	const revisions = 100

	nk := NSKey{Namespace: "billing", Key: "limits"}
	e := emptyEngine()

	var wg sync.WaitGroup

	for writer := range 2 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			var highest int64

			for rev := int64(1); rev <= revisions; rev++ {
				e.publishInto(publication{
					NSKey:     nk,
					Revision:  rev,
					Value:     fmt.Sprintf("writer%d-rev%d", writer, rev),
					UpdatedBy: fmt.Sprintf("writer%d", writer),
				})

				got, ok := e.Lookup(store.Scope{}, nk)
				if !ok {
					t.Error("Lookup during concurrent publishes: ok is false, want true")

					return
				}

				if got.Revision < highest {
					t.Errorf("revision went backwards: observed %d after %d", got.Revision, highest)

					return
				}

				highest = got.Revision
			}
		}()
	}

	wg.Wait()

	if got := cachedEntry(t, e, store.Scope{}, nk); got.Revision != revisions {
		t.Errorf("final revision: got %d, want %d", got.Revision, revisions)
	}
}
