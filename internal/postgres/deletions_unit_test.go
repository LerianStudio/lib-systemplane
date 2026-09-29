//go:build unit

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
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

	if _, err := s.ListDeletions(context.Background(), store.Scope{}, "ns", "", 1); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("empty key error = %v, want ErrValidation", err)
	}

	if _, err := s.ListDeletions(context.Background(), store.Scope{}, "ns", "k", 0); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("zero limit error = %v, want ErrValidation", err)
	}

	if _, err := s.ListDeletions(context.Background(), store.Scope{}, "ns", "k", 1); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("ListDeletions error = %v, want ErrTenantConnectionMissing", err)
	}

	if err := s.Delete(context.Background(), store.Scope{}, "ns", "k", "actor"); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("recording Delete error = %v, want ErrTenantConnectionMissing", err)
	}
}

// TestRecordingDeleteQuery_IsOneStatement pins the atomicity the fail-closed
// promise rests on: the row is removed and the record written by ONE statement,
// so a missing systemplane_deletions fails the whole delete and the row stays.
func TestRecordingDeleteQuery_IsOneStatement(t *testing.T) {
	t.Parallel()

	q := deleteQuery(true)

	for _, frag := range []string{
		"WITH gone AS (DELETE FROM systemplane_entries WHERE namespace = $1 AND key = $2 RETURNING namespace, key, revision)",
		"INSERT INTO systemplane_deletions (namespace, key, revision, deleted_at, deleted_by)",
		"SELECT namespace, key, revision, $3, $4 FROM gone",
	} {
		if !strings.Contains(q, frag) {
			t.Errorf("recording delete query missing %q:\n%s", frag, q)
		}
	}

	if strings.Count(q, ";") > 0 {
		t.Errorf("recording delete query holds more than one statement:\n%s", q)
	}

	if plain := deleteQuery(false); strings.Contains(plain, "systemplane_deletions") {
		t.Errorf("delete without RecordDeletions names systemplane_deletions:\n%s", plain)
	}
}
