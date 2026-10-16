//go:build unit

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
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

	if _, err := s.ListHistory(context.Background(), store.Scope{}, "ns", "", 1, 0); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("empty key error = %v, want ErrValidation", err)
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

	if err := s.Delete(context.Background(), store.Scope{}, "ns", "k", "actor"); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("recording Delete error = %v, want ErrTenantConnectionMissing", err)
	}
}

// TestRecordingDeleteQuery_IsOneStatement pins the atomicity the fail-closed
// promise rests on: the row is removed and its change recorded, with the value
// it held, by ONE statement, so a missing systemplane_history fails the whole
// delete and the row stays.
func TestRecordingDeleteQuery_IsOneStatement(t *testing.T) {
	t.Parallel()

	q := deleteQuery(true)

	for _, frag := range []string{
		"WITH gone AS (DELETE FROM systemplane_entries WHERE namespace = $1 AND key = $2 RETURNING namespace, key, revision, value)",
		`INSERT INTO systemplane_history (namespace, "key", operation, revision, previous_value, value, changed_at, changed_by)`,
		"SELECT namespace, key, 'delete', revision, value, NULL, $3, $4 FROM gone",
	} {
		if !strings.Contains(q, frag) {
			t.Errorf("recording delete query missing %q:\n%s", frag, q)
		}
	}

	if strings.Count(q, ";") > 0 {
		t.Errorf("recording delete query holds more than one statement:\n%s", q)
	}

	if plain := deleteQuery(false); strings.Contains(plain, "systemplane_history") {
		t.Errorf("delete without RecordChanges names systemplane_history:\n%s", plain)
	}
}

// executorWithoutTx is a dbExecutor that cannot open a transaction: neither a
// *sql.DB nor a dbresolver.DB.
type executorWithoutTx struct{}

func (executorWithoutTx) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, errors.New("unexpected exec")
}

func (executorWithoutTx) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("unexpected query")
}

func (executorWithoutTx) QueryRowContext(context.Context, string, ...any) *sql.Row {
	return nil
}

// TestBeginTx_RefusesAHandleWithoutTransactions pins that a recording Set over
// a handle that cannot open a transaction fails with an error, never a panic,
// and runs no statement.
func TestBeginTx_RefusesAHandleWithoutTransactions(t *testing.T) {
	t.Parallel()

	for name, db := range map[string]dbExecutor{
		"foreign handle": executorWithoutTx{},
		"nil handle":     nil,
		"typed nil":      (*sql.DB)(nil),
	} {
		if tx, err := beginTx(context.Background(), db); err == nil || tx != nil {
			t.Errorf("%s: beginTx = (%v, %v), want (nil, error)", name, tx, err)
		}
	}
}
