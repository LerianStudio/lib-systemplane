//go:build unit

package engine

import (
	"bytes"
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

	notify, _ = e.publish(sc, pub)

	return notify
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
		// wantRaw, when set, is the byte spelling the cache must hold after
		// the candidate. A refreshed entry that kept the OLD spelling would
		// miss the fence's memcmp on every later re-read of that revision.
		wantRaw []byte
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
			// The echo of a write: the same row read back byte for byte, which
			// is what every Set produces once the changefeed re-reads the row
			// it just published with the store's revision. The fence must
			// recognise it without walking the decoded document.
			name: "an equal revision carrying identical bytes refreshes provenance only",
			seed: []publication{{
				NSKey: nk, Revision: 3, Value: map[string]any{"limit": float64(10), "burst": float64(2)},
				Raw:       []byte(`{"limit":10,"burst":2}`),
				UpdatedAt: first, UpdatedBy: "ops",
			}},
			candidate: publication{
				NSKey: nk, Revision: 3, Value: map[string]any{"limit": float64(10), "burst": float64(2)},
				Raw:       []byte(`{"limit":10,"burst":2}`),
				UpdatedAt: second, UpdatedBy: "console",
			},
			wantValue:      map[string]any{"limit": float64(10), "burst": float64(2)},
			wantRevision:   3,
			wantProvenance: provenance{UpdatedAt: second, UpdatedBy: "console"},
		},
		{
			// The reason the byte comparison can never be the only one: a
			// writer that reordered the object's keys changed every byte and
			// nothing else. It falls through to the decoded comparison and is
			// still the no-op it is.
			name: "an equal revision whose bytes were reordered still fires nothing",
			seed: []publication{{
				NSKey: nk, Revision: 3, Value: map[string]any{"limit": float64(10), "burst": float64(2)},
				Raw:       []byte(`{"limit":10,"burst":2}`),
				UpdatedAt: first, UpdatedBy: "ops",
			}},
			candidate: publication{
				NSKey: nk, Revision: 3, Value: map[string]any{"limit": float64(10), "burst": float64(2)},
				Raw:       []byte(`{"burst":2,"limit":10}`),
				UpdatedAt: second, UpdatedBy: "console",
			},
			wantValue:      map[string]any{"limit": float64(10), "burst": float64(2)},
			wantRevision:   3,
			wantProvenance: provenance{UpdatedAt: second, UpdatedBy: "console"},
			wantRaw:        []byte(`{"burst":2,"limit":10}`),
		},
		{
			// The publication that carries no bytes at all: a reconcile row
			// the snapshot decoded, or any candidate assembled in-process
			// rather than read back from the store. It must not erase the
			// spelling the cache already holds — an entry left with no bytes
			// misses the fence's memcmp on every later re-read of this
			// revision and walks the whole decoded document again, for the
			// life of the revision.
			name: "an equal revision carrying no bytes keeps the cached spelling",
			seed: []publication{{
				NSKey: nk, Revision: 3, Value: map[string]any{"limit": float64(10)},
				Raw:       []byte(`{"limit":10}`),
				UpdatedAt: first, UpdatedBy: "ops",
			}},
			candidate: publication{
				NSKey: nk, Revision: 3, Value: map[string]any{"limit": float64(10)},
				UpdatedAt: second, UpdatedBy: "console",
			},
			wantValue:      map[string]any{"limit": float64(10)},
			wantRevision:   3,
			wantProvenance: provenance{UpdatedAt: second, UpdatedBy: "console"},
			wantRaw:        []byte(`{"limit":10}`),
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

			if tt.wantRaw != nil && !bytes.Equal(got.Raw, tt.wantRaw) {
				t.Errorf("cached raw: got %s, want %s", got.Raw, tt.wantRaw)
			}
		})
	}
}

// TestIngestedRowCachesTheRowBytes pins the fast path's INPUT. Every other
// assertion on Raw is a hand-built publication literal, so dropping either
// assignment that carries the store's bytes into the cache — the ingress's or
// the fence's — leaves the suite green while the memcmp compares against nil
// forever. Put a real row through the ingress and read the bytes back.
func TestIngestedRowCachesTheRowBytes(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	e := engineWithRegistry(fakeRegistry{defs: map[NSKey]KeyDef{nk: {Default: map[string]any{}}}})

	row := jsonRow(nk, 1, `{"limit":10}`, "ops")
	ingestRow(e, row)

	if got := cachedEntry(t, e, store.Scope{}, nk); !bytes.Equal(got.Raw, row.Value) {
		t.Errorf("cached raw after ingesting a row: got %s, want %s", got.Raw, row.Value)
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
