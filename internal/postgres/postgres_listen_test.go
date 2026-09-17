//go:build unit

// Targeted goroutine-lifecycle tests for the postgres Subscribe path. These
// exercise the unsubscribe func returned by Subscribe — they do NOT require a
// live PostgreSQL because Subscribe in this package only manipulates the
// zero-scope feed's subscriber map (the LISTEN connection is owned by
// startListener, which we never call in these tests).
package postgres

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.uber.org/goleak"
)

// newSubscribeStore builds a Store whose state is sufficient for Subscribe:
// a zero-scope feed with no connection behind it. We bypass New() because that
// constructor validates DB/DSN; Subscribe itself doesn't touch either.
func newSubscribeStore() *Store {
	return &Store{
		cfg:   Config{Channel: defaultChannel, Table: defaultTable, Module: defaultModule},
		feeds: map[string]*feed{"": newFeed(store.Scope{}, "")},
	}
}

// subscriberCount reports how many callbacks the zero-scope feed will fan out to.
func subscriberCount(s *Store) int {
	f := s.zeroFeed()

	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.subs)
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
// ctx-observer goroutine because the changefeed lives in the feed's reader,
// not in Subscribe. We still assert (a) basic unsubscribe behavior, (b) idempotency
// under concurrent unsubscribe calls.
func TestPostgresSubscribe_UnsubscribeIsIdempotent(t *testing.T) {
	s := newSubscribeStore()

	unsub, err := s.Subscribe(context.Background(), store.Scope{}, func(_ store.Event) {})
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
	n := subscriberCount(s)

	if n != 0 {
		t.Errorf("subscriber map size = %d, want 0 after unsubscribe", n)
	}

	waitForObserverExit(t)
}

// Subscribe with a nil callback returns a harmless no-op closer and must not
// register anything in the subscriber map.
func TestPostgresSubscribe_NilCallback(t *testing.T) {
	s := newSubscribeStore()

	unsub, err := s.Subscribe(context.Background(), store.Scope{}, nil)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if n := subscriberCount(s); n != 0 {
		t.Errorf("nil-cb subscribe registered %d subscribers; want 0", n)
	}

	unsub()
	waitForObserverExit(t)
}

// beginDisconnect is the single atomic decision point for "does this
// connection loss get announced". It must fire exactly once per outage and
// never during a clean teardown.
func TestPostgresFeed_BeginDisconnectSuppressedWhenClosing(t *testing.T) {
	t.Parallel()

	f := newFeed(store.Scope{}, "")
	f.subs[1] = &subscription{fn: func(store.Event) {}}
	f.subs[2] = &subscription{fn: func(store.Event) {}}

	subs, ok := f.beginDisconnect()
	if !ok {
		t.Fatal("beginDisconnect on a live feed = ok false; want the connected->disconnected edge to announce")
	}

	if len(subs) != 2 {
		t.Fatalf("beginDisconnect returned %d subscribers, want 2", len(subs))
	}

	// Edge-triggered: a second failure inside the same outage announces nothing.
	if subs, ok := f.beginDisconnect(); ok || len(subs) != 0 {
		t.Fatalf("second beginDisconnect in one outage = (%d subs, ok %v), want (0, false)", len(subs), ok)
	}

	// A successful reconnect re-arms the edge.
	if n := len(f.beginResync()); n != 2 {
		t.Fatalf("beginResync returned %d subscribers, want 2", n)
	}

	if subs, ok := f.beginDisconnect(); !ok || len(subs) != 2 {
		t.Fatalf("beginDisconnect after resync = (%d subs, ok %v), want (2, true)", len(subs), ok)
	}

	f.beginResync()

	// Teardown wins: a clean shutdown must emit no OpDisconnect at all.
	f.mu.Lock()
	f.closing = true
	f.mu.Unlock()

	subs, ok = f.beginDisconnect()
	if ok {
		t.Fatal("beginDisconnect during teardown = ok true; a clean shutdown must announce no disconnect")
	}

	if len(subs) != 0 {
		t.Fatalf("beginDisconnect during teardown returned %d subscribers, want 0", len(subs))
	}
}

// The teardown-vs-loss decision is one atomic step under f.mu, not a probe of
// the stop channel: once teardown has released f.mu with closing set, no
// later beginDisconnect may announce anything.
func TestPostgresFeed_CloseRacingConnectionLoss_EmitsNoDisconnect(t *testing.T) {
	f := newFeed(store.Scope{}, "")
	f.subs[1] = &subscription{fn: func(store.Event) {}}

	// tornDown flips only AFTER teardown released f.mu. Any beginDisconnect
	// that starts once it reads true is guaranteed by the mutex to observe
	// f.closing, so it must announce nothing.
	var tornDown atomic.Bool

	start := make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		<-start

		f.mu.Lock()
		f.closing = true
		f.mu.Unlock()

		tornDown.Store(true)

		close(f.stop)
	}()

	close(start)

	for range 10_000 {
		alreadyTornDown := tornDown.Load()

		subs, ok := f.beginDisconnect()
		if ok {
			if alreadyTornDown {
				t.Fatalf("beginDisconnect announced a disconnect to %d subscribers after teardown completed; a clean shutdown must announce none", len(subs))
			}

			f.beginResync() // re-arm the edge so the next iteration races again
		}

		if alreadyTornDown {
			break
		}
	}

	wg.Wait()

	if subs, ok := f.beginDisconnect(); ok {
		t.Fatalf("beginDisconnect after teardown = (%d subs, ok true), want ok false", len(subs))
	}
}
