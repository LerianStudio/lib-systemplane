//go:build unit

package engine

import (
	"fmt"
	"reflect"
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

// TestPublishFence walks the four fence outcomes of publish, one subtest per
// case: accepted (a first publication, a higher revision, an equal revision
// with a changed value, any revision 0), refreshed without a notification (an
// equal revision with an equal value) and rejected (a lower revision).
func TestPublishFence(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	first := time.Date(2026, time.September, 17, 12, 0, 0, 0, time.UTC)
	second := time.Date(2026, time.September, 17, 13, 0, 0, 0, time.UTC)
	older := time.Date(2026, time.September, 17, 11, 0, 0, 0, time.UTC)

	type provenance struct {
		UpdatedAt time.Time
		UpdatedBy string
	}

	tests := []struct {
		name string
		// seed is published before the candidate and builds the cached state
		// the fence decides against.
		seed           []publication
		candidate      publication
		wantNotify     bool
		wantValue      any
		wantRevision   int64
		wantProvenance provenance
	}{
		{
			name:           "a key that is not cached yet is accepted",
			candidate:      publication{NSKey: nk, Revision: 1, Value: "a", UpdatedBy: "ops"},
			wantNotify:     true,
			wantValue:      "a",
			wantRevision:   1,
			wantProvenance: provenance{UpdatedBy: "ops"},
		},
		{
			name:           "a higher revision is accepted and carries its provenance",
			seed:           []publication{{NSKey: nk, Revision: 1, Value: "a", UpdatedBy: "ops"}},
			candidate:      publication{NSKey: nk, Revision: 2, Value: "b", UpdatedAt: first, UpdatedBy: "console"},
			wantNotify:     true,
			wantValue:      "b",
			wantRevision:   2,
			wantProvenance: provenance{UpdatedAt: first, UpdatedBy: "console"},
		},
		{
			name:           "a lower revision is rejected and overwrites nothing",
			seed:           []publication{{NSKey: nk, Revision: 5, Value: "current", UpdatedAt: first, UpdatedBy: "ops"}},
			candidate:      publication{NSKey: nk, Revision: 4, Value: "old", UpdatedAt: older, UpdatedBy: "snapshot"},
			wantValue:      "current",
			wantRevision:   5,
			wantProvenance: provenance{UpdatedAt: first, UpdatedBy: "ops"},
		},
		{
			// The same row read again: equal revision, and a value that is a
			// distinct object but deeply equal, which is exactly what a re-read
			// of unchanged JSON produces.
			name: "an equal revision with an equal value refreshes provenance only",
			seed: []publication{{
				NSKey: nk, Revision: 3, Value: map[string]any{"limit": float64(10)},
				UpdatedAt: first, UpdatedBy: "ops",
			}},
			candidate: publication{
				NSKey: nk, Revision: 3, Value: map[string]any{"limit": float64(10)},
				UpdatedAt: second, UpdatedBy: "console",
			},
			wantValue:      map[string]any{"limit": float64(10)},
			wantRevision:   3,
			wantProvenance: provenance{UpdatedAt: second, UpdatedBy: "console"},
		},
		{
			// D3's foreign-writer rule: MongoDB has no triggers, so a Console
			// process writing the collection directly can change value and
			// leave revision alone. Deduplicating on revision alone would make
			// that write invisible forever.
			name:           "an equal revision with a changed value is accepted",
			seed:           []publication{{NSKey: nk, Revision: 3, Value: "a", UpdatedAt: first, UpdatedBy: "ops"}},
			candidate:      publication{NSKey: nk, Revision: 3, Value: "b", UpdatedAt: second, UpdatedBy: "console"},
			wantNotify:     true,
			wantValue:      "b",
			wantRevision:   3,
			wantProvenance: provenance{UpdatedAt: second, UpdatedBy: "console"},
		},
		{
			name:         "revision 0 wins over a cached revision and resets the counter",
			seed:         []publication{{NSKey: nk, Revision: 9, Value: "persisted", UpdatedAt: first, UpdatedBy: "ops"}},
			candidate:    publication{NSKey: nk, Revision: 0, Value: "default"},
			wantNotify:   true,
			wantValue:    "default",
			wantRevision: 0,
		},
		{
			name: "a repeated revision 0 is never deduplicated",
			seed: []publication{
				{NSKey: nk, Revision: 9, Value: "persisted", UpdatedAt: first, UpdatedBy: "ops"},
				{NSKey: nk, Revision: 0, Value: "default"},
			},
			candidate:    publication{NSKey: nk, Revision: 0, Value: "default"},
			wantNotify:   true,
			wantValue:    "default",
			wantRevision: 0,
		},
		{
			// A recreate always arrives with a fresh non-zero revision, which
			// is why resetting the cached counter on a delete cannot swallow
			// one.
			name: "a recreate after a delete is accepted over the reset counter",
			seed: []publication{
				{NSKey: nk, Revision: 9, Value: "persisted", UpdatedAt: first, UpdatedBy: "ops"},
				{NSKey: nk, Revision: 0, Value: "default"},
			},
			candidate:    publication{NSKey: nk, Revision: 1, Value: "recreated"},
			wantNotify:   true,
			wantValue:    "recreated",
			wantRevision: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := emptyEngine()

			for _, seed := range tt.seed {
				e.publishInto(seed)
			}

			if notify := e.publishInto(tt.candidate); notify != tt.wantNotify {
				t.Errorf("notify: got %t, want %t", notify, tt.wantNotify)
			}

			got := cachedEntry(t, e, tt.candidate.Scope, nk)

			if !reflect.DeepEqual(got.Value, tt.wantValue) {
				t.Errorf("cached value: got %v, want %v", got.Value, tt.wantValue)
			}

			if got.Revision != tt.wantRevision {
				t.Errorf("cached revision: got %d, want %d", got.Revision, tt.wantRevision)
			}

			if !got.UpdatedAt.Equal(tt.wantProvenance.UpdatedAt) || got.UpdatedBy != tt.wantProvenance.UpdatedBy {
				t.Errorf("provenance: got (%s, %q), want (%s, %q)",
					got.UpdatedAt, got.UpdatedBy, tt.wantProvenance.UpdatedAt, tt.wantProvenance.UpdatedBy)
			}
		})
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
