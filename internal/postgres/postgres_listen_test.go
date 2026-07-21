//go:build unit

// Targeted goroutine-lifecycle tests for the postgres Subscribe path. These
// exercise the unsubscribe func returned by Subscribe — they do NOT require a
// live PostgreSQL because Subscribe in this package only manipulates the
// subscriber map (the LISTEN connection is owned by startListener, which we
// never call in these tests).
package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v2/internal/store"
	"go.uber.org/goleak"
)

// newSubscribeStore builds a Store whose state is sufficient for Subscribe.
// We bypass New() because that constructor validates DB/DSN; Subscribe itself
// doesn't touch either.
func newSubscribeStore() *Store {
	return &Store{
		cfg:         Config{Channel: defaultChannel, Table: defaultTable, Module: defaultModule},
		subscribers: make(map[uint64]func(store.Event)),
	}
}

func waitForObserverExit(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)

	for {
		if err := goleak.Find(); err == nil {
			return
		}

		if time.Now().After(deadline) {
			if err := goleak.Find(); err != nil {
				t.Fatalf("postgres subscribe goroutine did not exit: %v", err)
			}

			return
		}

		time.Sleep(5 * time.Millisecond)
	}
}

// The postgres Subscribe path is simpler than the mongodb one — there is no
// ctx-observer goroutine because the changefeed lives in startListener, not in
// Subscribe. We still assert (a) basic unsubscribe behavior, (b) idempotency
// under concurrent unsubscribe calls.
func TestPostgresSubscribe_UnsubscribeIsIdempotent(t *testing.T) {
	s := newSubscribeStore()

	unsub, err := s.Subscribe(context.Background(), func(_ store.Event) {})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			unsub()
		}()
	}

	wg.Wait()

	// Subscriber map must be empty after teardown.
	s.listenerMu.Lock()
	n := len(s.subscribers)
	s.listenerMu.Unlock()

	if n != 0 {
		t.Errorf("subscriber map size = %d, want 0 after unsubscribe", n)
	}

	waitForObserverExit(t)
}

// Subscribe with a nil callback returns a harmless no-op closer and must not
// register anything in the subscriber map.
func TestPostgresSubscribe_NilCallback(t *testing.T) {
	s := newSubscribeStore()

	unsub, err := s.Subscribe(context.Background(), nil)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	s.listenerMu.Lock()
	n := len(s.subscribers)
	s.listenerMu.Unlock()

	if n != 0 {
		t.Errorf("nil-cb subscribe registered %d subscribers; want 0", n)
	}

	unsub()
	waitForObserverExit(t)
}
