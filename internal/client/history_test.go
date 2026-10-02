//go:build unit

package client

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// historyStore is a memStore that also keeps a change history, standing in for
// a backend built with RecordChanges. It records the actor every Set and
// Delete was handed and the arguments ListHistory was called with.
type historyStore struct {
	*memStore

	mu        sync.Mutex
	actors    []string
	history   []store.ChangeRecord
	listErr   error
	lastScope store.Scope
	lastNS    string
	lastKey   string
	lastLimit int
}

var _ store.HistoryLister = (*historyStore)(nil)

func newHistoryStore() *historyStore {
	return &historyStore{memStore: newMemStore(false)}
}

func (h *historyStore) Set(ctx context.Context, scope store.Scope, e store.Entry) (int64, error) {
	h.mu.Lock()
	h.actors = append(h.actors, "set:"+e.UpdatedBy)
	h.mu.Unlock()

	return h.memStore.Set(ctx, scope, e)
}

func (h *historyStore) Delete(ctx context.Context, scope store.Scope, ns, key, actor string) error {
	h.mu.Lock()
	h.actors = append(h.actors, "delete:"+actor)
	h.mu.Unlock()

	return h.memStore.Delete(ctx, scope, ns, key, actor)
}

func (h *historyStore) ListHistory(_ context.Context, scope store.Scope, ns, key string, limit int) ([]store.ChangeRecord, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastScope, h.lastNS, h.lastKey, h.lastLimit = scope, ns, key, limit

	if h.listErr != nil {
		return nil, h.listErr
	}

	return append([]store.ChangeRecord(nil), h.history...), nil
}

func (h *historyStore) lastCall() (store.Scope, string, string, int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.lastScope, h.lastNS, h.lastKey, h.lastLimit
}

func (h *historyStore) writeActors() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]string(nil), h.actors...)
}

func historyClient(t *testing.T, s store.Store, opts ...Option) *Client {
	t.Helper()

	cfg := defaultClientConfig()
	cfg.debounce = 0
	applyClientOptions(&cfg, opts)

	c := newClient(s, cfg)

	return startedClient(t, c)
}

func TestChangeHistory_ReturnsWhatTheStoreRecorded(t *testing.T) {
	s := newHistoryStore()
	at := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	s.history = []store.ChangeRecord{
		{Namespace: "ns", Key: "k", Operation: ChangeOperationDelete, Revision: 9, PreviousValue: json.RawMessage(`"v2"`), ChangedAt: at, ChangedBy: "bob"},
		{Namespace: "ns", Key: "k", Operation: ChangeOperationUpdate, Revision: 8, PreviousValue: json.RawMessage(`"v1"`), Value: json.RawMessage(`"v2"`), ChangedAt: at.Add(-time.Minute), ChangedBy: "carol"},
		{Namespace: "ns", Key: "k", Operation: ChangeOperationCreate, Revision: 4, Value: json.RawMessage(`"v1"`), ChangedAt: at.Add(-time.Hour), ChangedBy: "alice"},
	}

	c := historyClient(t, s, WithChangeHistory())

	got, err := c.ChangeHistory(context.Background(), "ns", "k", 10)
	if err != nil {
		t.Fatalf("ChangeHistory: %v", err)
	}

	if !reflect.DeepEqual(got, s.history) {
		t.Fatalf("ChangeHistory = %+v, want %+v", got, s.history)
	}

	scope, ns, key, limit := s.lastCall()
	if scope != (store.Scope{}) || ns != "ns" || key != "k" || limit != 10 {
		t.Errorf("ListHistory called with (%+v, %q, %q, %d), want the zero scope, ns, k, 10", scope, ns, key, limit)
	}
}

func TestChangeHistory_OperationNames(t *testing.T) {
	if ChangeOperationCreate != "create" || ChangeOperationUpdate != "update" || ChangeOperationDelete != "delete" {
		t.Errorf("operations = (%q, %q, %q), want (create, update, delete)",
			ChangeOperationCreate, ChangeOperationUpdate, ChangeOperationDelete)
	}

	if store.ChangeCreate != ChangeOperationCreate || store.ChangeUpdate != ChangeOperationUpdate || store.ChangeDelete != ChangeOperationDelete {
		t.Error("the client's operation names differ from the store's")
	}
}

func TestChangeHistory_NeverReturnsNil(t *testing.T) {
	c := historyClient(t, newHistoryStore(), WithChangeHistory())

	got, err := c.ChangeHistory(context.Background(), "ns", "k", 0)
	if err != nil {
		t.Fatalf("ChangeHistory: %v", err)
	}

	if got == nil {
		t.Error("ChangeHistory of a key never written = nil, want an empty slice")
	}
}

func TestChangeHistory_LimitDefaultsAndCap(t *testing.T) {
	for _, tt := range []struct {
		in, want int
	}{
		{0, DefaultChangeHistoryLimit},
		{-3, DefaultChangeHistoryLimit},
		{1, 1},
		{MaxChangeHistoryLimit, MaxChangeHistoryLimit},
		{MaxChangeHistoryLimit + 1, MaxChangeHistoryLimit},
	} {
		s := newHistoryStore()
		c := historyClient(t, s, WithChangeHistory())

		if _, err := c.ChangeHistory(context.Background(), "ns", "k", tt.in); err != nil {
			t.Fatalf("ChangeHistory(limit %d): %v", tt.in, err)
		}

		if _, _, _, limit := s.lastCall(); limit != tt.want {
			t.Errorf("ChangeHistory(limit %d) asked the store for %d, want %d", tt.in, limit, tt.want)
		}
	}

	if DefaultChangeHistoryLimit != 50 || MaxChangeHistoryLimit != 500 {
		t.Errorf("limits = (%d, %d), want (50, 500)", DefaultChangeHistoryLimit, MaxChangeHistoryLimit)
	}
}

func TestChangeHistory_Refusals(t *testing.T) {
	t.Run("option off", func(t *testing.T) {
		c := historyClient(t, newHistoryStore())

		if _, err := c.ChangeHistory(context.Background(), "ns", "k", 0); !errors.Is(err, ErrChangeHistoryDisabled) {
			t.Fatalf("err = %v, want ErrChangeHistoryDisabled", err)
		}
	})

	t.Run("store without the capability", func(t *testing.T) {
		c := historyClient(t, newMemStore(false), WithChangeHistory())

		if _, err := c.ChangeHistory(context.Background(), "ns", "k", 0); !errors.Is(err, ErrChangeHistoryDisabled) {
			t.Fatalf("err = %v, want ErrChangeHistoryDisabled", err)
		}
	})

	t.Run("unknown key", func(t *testing.T) {
		c := historyClient(t, newHistoryStore(), WithChangeHistory())

		if _, err := c.ChangeHistory(context.Background(), "ns", "missing", 0); !errors.Is(err, ErrUnknownKey) {
			t.Fatalf("err = %v, want ErrUnknownKey", err)
		}
	})

	t.Run("nil context", func(t *testing.T) {
		c := historyClient(t, newHistoryStore(), WithChangeHistory())

		//nolint:staticcheck // SA1012: the nil context is the input under test.
		if _, err := c.ChangeHistory(nil, "ns", "k", 0); !errors.Is(err, ErrNilContext) {
			t.Fatalf("err = %v, want ErrNilContext", err)
		}
	})

	t.Run("not started", func(t *testing.T) {
		cfg := defaultClientConfig()
		applyClientOptions(&cfg, []Option{WithChangeHistory()})

		c := newClient(newHistoryStore(), cfg)
		t.Cleanup(func() { _ = c.Close() })

		if err := c.Register("ns", "k", "default"); err != nil {
			t.Fatalf("register: %v", err)
		}

		if _, err := c.ChangeHistory(context.Background(), "ns", "k", 0); !errors.Is(err, ErrNotStarted) {
			t.Fatalf("err = %v, want ErrNotStarted", err)
		}
	})

	t.Run("closed", func(t *testing.T) {
		c := historyClient(t, newHistoryStore(), WithChangeHistory())
		_ = c.Close()

		if _, err := c.ChangeHistory(context.Background(), "ns", "k", 0); !errors.Is(err, ErrClosed) {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	})

	t.Run("nil client", func(t *testing.T) {
		var c *Client

		if _, err := c.ChangeHistory(context.Background(), "ns", "k", 0); !errors.Is(err, ErrClosed) {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	})

	t.Run("store error", func(t *testing.T) {
		s := newHistoryStore()
		s.listErr = store.ErrTenantConnectionMissing
		c := historyClient(t, s, WithChangeHistory())

		if _, err := c.ChangeHistory(context.Background(), "ns", "k", 0); !errors.Is(err, ErrTenantConnectionMissing) {
			t.Fatalf("err = %v, want ErrTenantConnectionMissing", err)
		}
	})
}

// TestWrites_HandTheActorToTheStore pins the half of BRSFN-14 and BRSFN-82 the
// client owns: the actor a caller names on Set and on Delete reaches the
// backend unchanged, where the change history records it.
func TestWrites_HandTheActorToTheStore(t *testing.T) {
	s := newHistoryStore()
	c := historyClient(t, s, WithChangeHistory())

	if err := c.Set(context.Background(), "ns", "k", "v", "alice"); err != nil {
		t.Fatalf("set: %v", err)
	}

	if err := c.Delete(context.Background(), "ns", "k", "bob"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if got := s.writeActors(); !reflect.DeepEqual(got, []string{"set:alice", "delete:bob"}) {
		t.Errorf("store write actors = %q, want [set:alice delete:bob]", got)
	}
}

// TestWithChangeHistory_ReachesBothBackends pins the wiring: the option is what
// turns the backends' change record on, and without it they stay off.
func TestWithChangeHistory_ReachesBothBackends(t *testing.T) {
	off := defaultClientConfig()
	applyClientOptions(&off, nil)

	on := defaultClientConfig()
	applyClientOptions(&on, []Option{WithChangeHistory()})

	if postgresConfig(nil, "", off).RecordChanges || mongoConfig(nil, "", off).RecordChanges {
		t.Error("RecordChanges is on without WithChangeHistory")
	}

	if !postgresConfig(nil, "", on).RecordChanges || !mongoConfig(nil, "", on).RecordChanges {
		t.Error("WithChangeHistory did not reach both backends' RecordChanges")
	}
}

// TestWrites_RefuseABlankActorUnderChangeHistory pins BRSFN-82's "every write
// names its actor": a history-enabled Client refuses a Set or Delete whose
// actor is empty or blank before the store is touched, so the append-only
// history never gains an unattributed record. Without the option the actor
// stays optional.
func TestWrites_RefuseABlankActorUnderChangeHistory(t *testing.T) {
	for _, actor := range []string{"", "   ", "\t\n"} {
		s := newHistoryStore()
		c := historyClient(t, s, WithChangeHistory())

		if err := c.Set(context.Background(), "ns", "k", "v", actor); !errors.Is(err, ErrValidation) {
			t.Errorf("Set with actor %q = %v, want ErrValidation", actor, err)
		}

		if err := c.Delete(context.Background(), "ns", "k", actor); !errors.Is(err, ErrValidation) {
			t.Errorf("Delete with actor %q = %v, want ErrValidation", actor, err)
		}

		if got := s.writeActors(); len(got) != 0 {
			t.Errorf("store saw writes %q for actor %q, want none", got, actor)
		}

		if got, ok, err := c.Get(context.Background(), "ns", "k"); err != nil || !ok || got != "default" {
			t.Errorf("Get after refused writes = (%v, %v, %v), want the default", got, ok, err)
		}
	}

	off := newHistoryStore()
	c := historyClient(t, off)

	if err := c.Set(context.Background(), "ns", "k", "v", ""); err != nil {
		t.Fatalf("Set with an empty actor and no change history: %v", err)
	}

	if err := c.Delete(context.Background(), "ns", "k", ""); err != nil {
		t.Fatalf("Delete with an empty actor and no change history: %v", err)
	}
}
