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

// facadeTestStore is read and written from the caller's goroutine and from the
// engine's own (a reconcile per scope, a debounced re-read), so every field
// lives under mu and every method releases it before invoking the subscriber.
type facadeTestStore struct {
	mu           sync.Mutex
	entries      []TestEntry
	revision     int64
	gotSet       TestEntry
	gotDeleteNS  string
	gotDeleteKey string
	gotActor     string
	subscribeFn  func(TestEvent)
}

func (s *facadeTestStore) Start(context.Context) error { return nil }
func (s *facadeTestStore) Close() error                { return nil }
func (s *facadeTestStore) Get(_ context.Context, _ TestScope, ns, key string) (TestEntry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range s.entries {
		if e.Namespace == ns && e.Key == key {
			return e, true, nil
		}
	}

	return TestEntry{}, false, nil
}

func (s *facadeTestStore) Set(_ context.Context, _ TestScope, e TestEntry) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.revision++
	e.Revision = s.revision
	s.gotSet = e

	return s.revision, nil
}

func (s *facadeTestStore) Delete(_ context.Context, _ TestScope, ns, key, actor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.gotDeleteNS = ns
	s.gotDeleteKey = key
	s.gotActor = actor

	return nil
}
func (s *facadeTestStore) List(context.Context, TestScope) ([]TestEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.entries, nil
}
func (s *facadeTestStore) Subscribe(_ context.Context, _ TestScope, fn func(TestEvent)) (func(), error) {
	s.mu.Lock()
	s.subscribeFn = fn
	s.mu.Unlock()

	// Announce a connected changefeed (FC-2), outside the lock: the engine
	// reads this store back on the calling goroutine.
	fn(TestEvent{Op: store.OpResync})

	return func() {
		s.mu.Lock()
		s.subscribeFn = nil
		s.mu.Unlock()
	}, nil
}

func (s *facadeTestStore) lastSet() TestEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.gotSet
}

func (s *facadeTestStore) lastDelete() (ns, key, actor string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.gotDeleteNS, s.gotDeleteKey, s.gotActor
}

func (s *facadeTestStore) subscriber() func(TestEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.subscribeFn
}

func TestNewForTestingAdapterAndOptions(t *testing.T) {
	backend := &facadeTestStore{entries: []TestEntry{{
		Namespace: "ns",
		Key:       "k",
		Value:     []byte(`"stored"`),
		UpdatedAt: time.Now().UTC(),
		UpdatedBy: "tester",
	}}}

	c, err := NewForTesting(backend,
		WithPollInterval(time.Second),
		WithDebounce(time.Millisecond),
		WithModule("custom_module"),
		WithCatalogService("custom_service"),
	)
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	if c.CatalogService() != "custom_service" {
		t.Fatalf("CatalogService = %q, want custom_service", c.CatalogService())
	}

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	defer c.Close()

	if got, ok, err := c.GetString(context.Background(), "ns", "k"); err != nil || !ok || got != "stored" {
		t.Fatalf("GetString = (%q, %v, %v), want stored/true/nil", got, ok, err)
	}
	if err := c.Set(context.Background(), "ns", "k", "new", "actor"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if gotSet := backend.lastSet(); gotSet.Namespace != "ns" || gotSet.Key != "k" || string(gotSet.Value) != `"new"` {
		t.Fatalf("backend Set = %#v", gotSet)
	}
	if err := c.Delete(context.Background(), "ns", "k", "actor"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if ns, key, actor := backend.lastDelete(); ns != "ns" || key != "k" || actor != "actor" {
		t.Fatalf("backend Delete = %q/%q by %q", ns, key, actor)
	}

	unsub, err := c.store.Subscribe(context.Background(), store.Scope{}, func(evt store.Event) {
		// Subscribe announces a connected changefeed before returning (FC-2);
		// this callback only asserts on the per-key event fired below it.
		if evt.Op == store.OpResync {
			return
		}

		if evt.Namespace != "ns" || evt.Key != "k" || evt.Op != store.OpUpsert {
			t.Fatalf("event = %#v", evt)
		}
	})
	if err != nil {
		t.Fatalf("adapter Subscribe: %v", err)
	}
	backend.subscriber()(TestEvent{Namespace: "ns", Key: "k", Op: store.OpUpsert})
	unsub()
	if backend.subscriber() != nil {
		t.Fatal("unsubscribe did not clear callback")
	}
}

func TestNewForTestingRejectsNilStores(t *testing.T) {
	if _, err := NewForTesting(nil); !errors.Is(err, store.ErrNilBackend) {
		t.Fatalf("nil interface store error = %v, want ErrNilBackend", err)
	}

	var typedNil *facadeTestStore
	if _, err := NewForTesting(typedNil); !errors.Is(err, store.ErrNilBackend) {
		t.Fatalf("typed nil store error = %v, want ErrNilBackend", err)
	}
}
