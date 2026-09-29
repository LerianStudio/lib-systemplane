//go:build unit

package mongodb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var _ store.DeletionLister = (*Store)(nil)

func TestListDeletions_PreIOPaths(t *testing.T) {
	t.Parallel()

	var nilStore *Store
	if _, err := nilStore.ListDeletions(context.Background(), store.Scope{}, "ns", "k", 1); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("nil ListDeletions error = %v, want ErrClosed", err)
	}

	closed := newSubscribeStore()
	_ = closed.Close()

	if _, err := closed.ListDeletions(context.Background(), store.Scope{}, "ns", "k", 1); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("closed ListDeletions error = %v, want ErrClosed", err)
	}

	s, err := New(Config{MultiTenantEnabled: true, RecordDeletions: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := s.ListDeletions(context.Background(), store.Scope{}, "", "k", 1); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("empty namespace error = %v, want ErrValidation", err)
	}

	if _, err := s.ListDeletions(context.Background(), store.Scope{}, "ns", "k", 0); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("zero limit error = %v, want ErrValidation", err)
	}

	if _, err := s.ListDeletions(context.Background(), store.Scope{}, "ns", "k", 1); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("ListDeletions error = %v, want ErrTenantConnectionMissing", err)
	}
}

// TestDeletionRecordWrite_ShapeAndLiterals pins the record upsert: keyed on
// (namespace, key, revision) so a retry of the same delete writes nothing new,
// and the provenance set only on insert. It is a plain update, not a pipeline,
// so a "$"-leading actor is stored as written with no $literal wrapper.
func TestDeletionRecordWrite_ShapeAndLiterals(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	tomb := entryDoc{Namespace: "$ns", Key: "k", Revision: 7, UpdatedAt: at, UpdatedBy: "$value", Deleted: true}

	filter, update := deletionRecordWrite(tomb)

	wantFilter := bson.D{
		{Key: fieldNamespace, Value: "$ns"},
		{Key: fieldKey, Value: "k"},
		{Key: fieldRevision, Value: int64(7)},
	}
	if len(filter) != len(wantFilter) {
		t.Fatalf("filter = %#v, want %#v", filter, wantFilter)
	}

	for i := range wantFilter {
		if filter[i] != wantFilter[i] {
			t.Errorf("filter[%d] = %#v, want %#v", i, filter[i], wantFilter[i])
		}
	}

	if len(update) != 1 || update[0].Key != "$setOnInsert" {
		t.Fatalf("update = %#v, want one $setOnInsert", update)
	}

	set, ok := update[0].Value.(bson.D)
	if !ok || len(set) != 2 {
		t.Fatalf("$setOnInsert = %#v, want deleted_at and deleted_by", update[0].Value)
	}

	if set[0].Key != fieldDeletedAt || set[0].Value != at {
		t.Errorf("$setOnInsert[0] = %#v, want %s = %v", set[0], fieldDeletedAt, at)
	}

	if set[1].Key != fieldDeletedBy || set[1].Value != "$value" {
		t.Errorf("$setOnInsert[1] = %#v, want %s = the actor as written", set[1], fieldDeletedBy)
	}
}
