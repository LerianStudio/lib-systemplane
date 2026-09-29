//go:build unit

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.mongodb.org/mongo-driver/v2/mongo"
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

// TestDeletionRecord_TakenFromTheTombstone pins the record a recording Delete
// inserts: the tombstone's identity, revision and provenance, with a
// "$"-leading actor stored as written — a plain insert evaluates nothing.
func TestDeletionRecord_TakenFromTheTombstone(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	tomb := entryDoc{Namespace: "$ns", Key: "k", Revision: 7, UpdatedAt: at, UpdatedBy: "$value", Deleted: true}

	want := deletionDoc{Namespace: "$ns", Key: "k", Revision: 7, DeletedAt: at, DeletedBy: "$value"}
	if got := deletionRecord(tomb); got != want {
		t.Fatalf("deletionRecord = %+v, want %+v", got, want)
	}
}

// TestIsTransactionsUnsupported pins which error names the replica set
// requirement: only the standalone server's refusal of a transaction.
func TestIsTransactionsUnsupported(t *testing.T) {
	t.Parallel()

	standalone := mongo.CommandError{Code: 20, Message: "Transaction numbers are only allowed on a replica set member or mongos"}
	if !isTransactionsUnsupported(fmt.Errorf("tombstone: %w", standalone)) {
		t.Error("the standalone refusal was not recognised")
	}

	for _, err := range []error{
		nil,
		errors.New("network"),
		mongo.CommandError{Code: 20, Message: "some other illegal operation"},
		mongo.CommandError{Code: 112, Message: "WriteConflict"},
	} {
		if isTransactionsUnsupported(err) {
			t.Errorf("isTransactionsUnsupported(%v) = true, want false", err)
		}
	}
}
