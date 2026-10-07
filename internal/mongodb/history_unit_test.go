//go:build unit

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

var _ store.HistoryLister = (*Store)(nil)

func TestListHistory_PreIOPaths(t *testing.T) {
	t.Parallel()

	var nilStore *Store
	if _, err := nilStore.ListHistory(context.Background(), store.Scope{}, "ns", "k", 1, 0); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("nil ListHistory error = %v, want ErrClosed", err)
	}

	closed := newSubscribeStore()
	_ = closed.Close()

	if _, err := closed.ListHistory(context.Background(), store.Scope{}, "ns", "k", 1, 0); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("closed ListHistory error = %v, want ErrClosed", err)
	}

	s, err := New(Config{MultiTenantEnabled: true, RecordChanges: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := s.ListHistory(context.Background(), store.Scope{}, "", "k", 1, 0); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("empty namespace error = %v, want ErrValidation", err)
	}

	if _, err := s.ListHistory(context.Background(), store.Scope{}, "ns", "k", 0, 0); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("zero limit error = %v, want ErrValidation", err)
	}

	if _, err := s.ListHistory(context.Background(), store.Scope{}, "ns", "k", 1, -1); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("negative before error = %v, want ErrValidation", err)
	}

	if _, err := s.ListHistory(context.Background(), store.Scope{}, "ns", "k", 1, 0); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("ListHistory error = %v, want ErrTenantConnectionMissing", err)
	}

	if _, err := s.Set(context.Background(), store.Scope{}, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`)}); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("recording Set error = %v, want ErrTenantConnectionMissing", err)
	}
}

// TestChangeRecord_FromTheBeforeAndAfterImages pins the record a recording
// write inserts: the operation and previous value come from the document the
// transaction read before writing, the revision and provenance from the one it
// wrote, and a "$"-leading string is stored as written — a plain insert
// evaluates nothing.
func TestChangeRecord_FromTheBeforeAndAfterImages(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	strp := func(s string) *string { return &s }

	live := &entryDoc{Namespace: "$ns", Key: "k", Value: `"old"`, Revision: 4}
	tombstone := &entryDoc{Namespace: "$ns", Key: "k", Revision: 5, Deleted: true}
	written := entryDoc{Namespace: "$ns", Key: "k", Value: `"new"`, Revision: 7, UpdatedAt: at, UpdatedBy: "$value"}
	deleted := entryDoc{Namespace: "$ns", Key: "k", Revision: 8, UpdatedAt: at, UpdatedBy: "$value", Deleted: true}

	for _, tt := range []struct {
		name   string
		before *entryDoc
		after  entryDoc
		want   historyDoc
	}{
		{"create over nothing", nil, written, historyDoc{
			Namespace: "$ns", Key: "k", Seq: 3, Operation: store.ChangeCreate, Revision: 7,
			Value: strp(`"new"`), ChangedAt: at, ChangedBy: "$value",
		}},
		{"create over a tombstone", tombstone, written, historyDoc{
			Namespace: "$ns", Key: "k", Seq: 3, Operation: store.ChangeCreate, Revision: 7,
			Value: strp(`"new"`), ChangedAt: at, ChangedBy: "$value",
		}},
		{"update", live, written, historyDoc{
			Namespace: "$ns", Key: "k", Seq: 3, Operation: store.ChangeUpdate, Revision: 7,
			PreviousValue: strp(`"old"`), Value: strp(`"new"`), ChangedAt: at, ChangedBy: "$value",
		}},
		{"delete", live, deleted, historyDoc{
			Namespace: "$ns", Key: "k", Seq: 3, Operation: store.ChangeDelete, Revision: 8,
			PreviousValue: strp(`"old"`), ChangedAt: at, ChangedBy: "$value",
		}},
	} {
		if got := changeRecord(tt.before, tt.after, 3); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s: changeRecord = %+v, want %+v", tt.name, got, tt.want)
		}
	}
}

// TestHistoryDoc_ToChangeRecord pins the read side: values come back as the
// JSON the entry held, and an absent one as nil.
func TestHistoryDoc_ToChangeRecord(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	prev := `{"a":1}`

	got := historyDoc{
		Namespace: "ns", Key: "k", Seq: 9, Operation: store.ChangeDelete, Revision: 8,
		PreviousValue: &prev, ChangedAt: at, ChangedBy: "bob",
	}.toChangeRecord()

	if got.Namespace != "ns" || got.Key != "k" || got.Operation != store.ChangeDelete || got.Revision != 8 ||
		string(got.PreviousValue) != prev || got.Value != nil || !got.ChangedAt.Equal(at) || got.ChangedBy != "bob" {
		t.Fatalf("toChangeRecord = %+v", got)
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
