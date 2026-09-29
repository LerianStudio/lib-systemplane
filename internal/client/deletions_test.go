//go:build unit

package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// historyStore is a memStore that also keeps a deletion history, standing in
// for a backend built with RecordDeletions. It records the actor every Delete
// was handed and the arguments ListDeletions was called with.
type historyStore struct {
	*memStore

	mu        sync.Mutex
	actors    []string
	history   []store.Deletion
	listErr   error
	lastScope store.Scope
	lastNS    string
	lastKey   string
	lastLimit int
}

var _ store.DeletionLister = (*historyStore)(nil)

func newHistoryStore() *historyStore {
	return &historyStore{memStore: newMemStore(false)}
}

func (h *historyStore) Delete(ctx context.Context, scope store.Scope, ns, key, actor string) error {
	h.mu.Lock()
	h.actors = append(h.actors, actor)
	h.mu.Unlock()

	return h.memStore.Delete(ctx, scope, ns, key, actor)
}

func (h *historyStore) ListDeletions(_ context.Context, scope store.Scope, ns, key string, limit int) ([]store.Deletion, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastScope, h.lastNS, h.lastKey, h.lastLimit = scope, ns, key, limit

	if h.listErr != nil {
		return nil, h.listErr
	}

	return append([]store.Deletion(nil), h.history...), nil
}

func (h *historyStore) lastCall() (store.Scope, string, string, int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.lastScope, h.lastNS, h.lastKey, h.lastLimit
}

func (h *historyStore) deleteActors() []string {
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

func TestDeletions_ReturnsWhatTheStoreRecorded(t *testing.T) {
	s := newHistoryStore()
	at := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	s.history = []store.Deletion{
		{Namespace: "ns", Key: "k", Revision: 9, DeletedAt: at, DeletedBy: "bob"},
		{Namespace: "ns", Key: "k", Revision: 4, DeletedAt: at.Add(-time.Hour), DeletedBy: "alice"},
	}

	c := historyClient(t, s, WithDeletionHistory())

	got, err := c.Deletions(context.Background(), "ns", "k", 10)
	if err != nil {
		t.Fatalf("Deletions: %v", err)
	}

	if len(got) != 2 || got[0] != s.history[0] || got[1] != s.history[1] {
		t.Fatalf("Deletions = %+v, want %+v", got, s.history)
	}

	scope, ns, key, limit := s.lastCall()
	if scope != (store.Scope{}) || ns != "ns" || key != "k" || limit != 10 {
		t.Errorf("ListDeletions called with (%+v, %q, %q, %d), want the zero scope, ns, k, 10", scope, ns, key, limit)
	}
}

func TestDeletions_NeverReturnsNil(t *testing.T) {
	c := historyClient(t, newHistoryStore(), WithDeletionHistory())

	got, err := c.Deletions(context.Background(), "ns", "k", 0)
	if err != nil {
		t.Fatalf("Deletions: %v", err)
	}

	if got == nil {
		t.Error("Deletions of a key never deleted = nil, want an empty slice")
	}
}

func TestDeletions_LimitDefaultsAndCap(t *testing.T) {
	for _, tt := range []struct {
		in, want int
	}{
		{0, DefaultDeletionsLimit},
		{-3, DefaultDeletionsLimit},
		{1, 1},
		{MaxDeletionsLimit, MaxDeletionsLimit},
		{MaxDeletionsLimit + 1, MaxDeletionsLimit},
	} {
		s := newHistoryStore()
		c := historyClient(t, s, WithDeletionHistory())

		if _, err := c.Deletions(context.Background(), "ns", "k", tt.in); err != nil {
			t.Fatalf("Deletions(limit %d): %v", tt.in, err)
		}

		if _, _, _, limit := s.lastCall(); limit != tt.want {
			t.Errorf("Deletions(limit %d) asked the store for %d, want %d", tt.in, limit, tt.want)
		}
	}

	if DefaultDeletionsLimit != 50 || MaxDeletionsLimit != 500 {
		t.Errorf("limits = (%d, %d), want (50, 500)", DefaultDeletionsLimit, MaxDeletionsLimit)
	}
}

func TestDeletions_Refusals(t *testing.T) {
	t.Run("option off", func(t *testing.T) {
		c := historyClient(t, newHistoryStore())

		if _, err := c.Deletions(context.Background(), "ns", "k", 0); !errors.Is(err, ErrDeletionHistoryDisabled) {
			t.Fatalf("err = %v, want ErrDeletionHistoryDisabled", err)
		}
	})

	t.Run("store without the capability", func(t *testing.T) {
		c := historyClient(t, newMemStore(false), WithDeletionHistory())

		if _, err := c.Deletions(context.Background(), "ns", "k", 0); !errors.Is(err, ErrDeletionHistoryDisabled) {
			t.Fatalf("err = %v, want ErrDeletionHistoryDisabled", err)
		}
	})

	t.Run("unknown key", func(t *testing.T) {
		c := historyClient(t, newHistoryStore(), WithDeletionHistory())

		if _, err := c.Deletions(context.Background(), "ns", "missing", 0); !errors.Is(err, ErrUnknownKey) {
			t.Fatalf("err = %v, want ErrUnknownKey", err)
		}
	})

	t.Run("nil context", func(t *testing.T) {
		c := historyClient(t, newHistoryStore(), WithDeletionHistory())

		//nolint:staticcheck // SA1012: the nil context is the input under test.
		if _, err := c.Deletions(nil, "ns", "k", 0); !errors.Is(err, ErrNilContext) {
			t.Fatalf("err = %v, want ErrNilContext", err)
		}
	})

	t.Run("not started", func(t *testing.T) {
		cfg := defaultClientConfig()
		applyClientOptions(&cfg, []Option{WithDeletionHistory()})

		c := newClient(newHistoryStore(), cfg)
		t.Cleanup(func() { _ = c.Close() })

		if err := c.Register("ns", "k", "default"); err != nil {
			t.Fatalf("register: %v", err)
		}

		if _, err := c.Deletions(context.Background(), "ns", "k", 0); !errors.Is(err, ErrNotStarted) {
			t.Fatalf("err = %v, want ErrNotStarted", err)
		}
	})

	t.Run("closed", func(t *testing.T) {
		c := historyClient(t, newHistoryStore(), WithDeletionHistory())
		_ = c.Close()

		if _, err := c.Deletions(context.Background(), "ns", "k", 0); !errors.Is(err, ErrClosed) {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	})

	t.Run("nil client", func(t *testing.T) {
		var c *Client

		if _, err := c.Deletions(context.Background(), "ns", "k", 0); !errors.Is(err, ErrClosed) {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	})

	t.Run("store error", func(t *testing.T) {
		s := newHistoryStore()
		s.listErr = store.ErrTenantConnectionMissing
		c := historyClient(t, s, WithDeletionHistory())

		if _, err := c.Deletions(context.Background(), "ns", "k", 0); !errors.Is(err, ErrTenantConnectionMissing) {
			t.Fatalf("err = %v, want ErrTenantConnectionMissing", err)
		}
	})
}

// TestDelete_HandsTheActorToTheStore pins the half of BRSFN-14 the client owns:
// the actor a caller names on Delete reaches the backend unchanged.
func TestDelete_HandsTheActorToTheStore(t *testing.T) {
	s := newHistoryStore()
	c := historyClient(t, s, WithDeletionHistory())

	if err := c.Set(context.Background(), "ns", "k", "v", "alice"); err != nil {
		t.Fatalf("set: %v", err)
	}

	if err := c.Delete(context.Background(), "ns", "k", "bob"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if got := s.deleteActors(); len(got) != 1 || got[0] != "bob" {
		t.Errorf("store Delete actors = %q, want [bob]", got)
	}
}

// TestWithDeletionHistory_ReachesBothBackends pins the wiring: the option is
// what turns the backends' deletion record on, and without it they stay off.
func TestWithDeletionHistory_ReachesBothBackends(t *testing.T) {
	off := defaultClientConfig()
	applyClientOptions(&off, nil)

	on := defaultClientConfig()
	applyClientOptions(&on, []Option{WithDeletionHistory()})

	if postgresConfig(nil, "", off).RecordDeletions || mongoConfig(nil, "", off).RecordDeletions {
		t.Error("RecordDeletions is on without WithDeletionHistory")
	}

	if !postgresConfig(nil, "", on).RecordDeletions || !mongoConfig(nil, "", on).RecordDeletions {
		t.Error("WithDeletionHistory did not reach both backends' RecordDeletions")
	}
}
