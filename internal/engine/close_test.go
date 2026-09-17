//go:build unit

package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// closeEngine returns an Engine ready to be closed by the test itself: no
// t.Cleanup cancels the lifecycle context here, because Close is the thing
// under test and a cleanup that canceled it would hide a Close that did not.
func closeEngine(t *testing.T, timeout time.Duration) *Engine {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		scopes:          map[store.Scope]*scopeState{},
		lifecycleCtx:    ctx,
		lifecycleCancel: cancel,
		closeTimeout:    timeout,
	}

	return e
}

// mustReceive waits for ch to fire, failing the test rather than hanging the
// package when a delivery never happens.
func mustReceive(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestCloseWaitsForCtxHonoringCallbacks(t *testing.T) {
	e := closeEngine(t, 5*time.Second)
	nk := NSKey{Namespace: "ns", Key: "honors-ctx"}

	entered := make(chan struct{})
	returned := make(chan struct{})

	e.OnChange(nk, func(ctx context.Context, _ Change) {
		close(entered)
		<-ctx.Done()
		// Winding down takes a moment. Without it the assertion below would
		// pass whether or not Close waited, because the callback would finish
		// in the same instant cancellation reached it.
		time.Sleep(50 * time.Millisecond)
		close(returned)
	})

	e.publish(pub(nk, 1, "v1"))
	mustReceive(t, entered, "the subscriber to start running")

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil for a callback that honors ctx", err)
	}

	// The callback must be finished BEFORE Close returned, not merely on its
	// way out: that is the whole difference between waiting and cancelling.
	select {
	case <-returned:
	default:
		t.Fatal("Close returned while a ctx-honoring callback was still running")
	}
}

func TestCloseReportsTimeoutNamingStuckKey(t *testing.T) {
	e := closeEngine(t, 100*time.Millisecond)
	nk := NSKey{Namespace: "ns", Key: "ignores-ctx"}

	entered := make(chan struct{})
	returned := make(chan struct{})
	release := make(chan struct{})

	e.OnChange(nk, func(context.Context, Change) {
		close(entered)
		<-release // deliberately ignores ctx: the subscriber's own leak
		close(returned)
	})

	e.publish(pub(nk, 1, "v1"))
	mustReceive(t, entered, "the subscriber to start running")

	err := e.Close()
	if !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("Close() = %v, want an error wrapping ErrCloseTimeout", err)
	}

	for _, want := range []string{"single-tenant", nk.Namespace, nk.Key} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Close() error %q does not name %q", err, want)
		}
	}

	// A timeout still leaves the engine fully closed.
	if e.publish(pub(nk, 2, "v2")) {
		t.Error("publish after a timed-out Close was accepted, want dropped")
	}

	// Release the stuck callback and wait for it: a test that leaks on purpose
	// fails the whole package under goleak.
	close(release)
	mustReceive(t, returned, "the released subscriber to finish")
	e.dispatchWG.Wait()
}

func TestCloseIsIdempotent(t *testing.T) {
	e := closeEngine(t, 5*time.Second)

	var unsubscribes atomic.Int64

	sc := newScopeState(store.Scope{})
	sc.unsubscribe = func() { unsubscribes.Add(1) }
	e.scopes[store.Scope{}] = sc

	if err := e.Close(); err != nil {
		t.Fatalf("first Close() = %v, want nil", err)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("second Close() = %v, want nil", err)
	}

	if got := unsubscribes.Load(); got != 1 {
		t.Errorf("scope unsubscribed %d times across two Close calls, want 1", got)
	}
}

func TestCloseOnNilEngineReturnsNil(t *testing.T) {
	var e *Engine

	if err := e.Close(); err != nil {
		t.Fatalf("(*Engine)(nil).Close() = %v, want nil", err)
	}
}

func TestPublishAfterCloseIsDropped(t *testing.T) {
	e := closeEngine(t, 5*time.Second)
	nk := NSKey{Namespace: "ns", Key: "after-close"}

	if err := e.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	if e.publish(pub(nk, 1, "v1")) {
		t.Error("publish after Close was accepted, want dropped")
	}

	if _, ok := e.Lookup(store.Scope{}, nk); ok {
		t.Error("publish after Close created a scope entry, want none")
	}
}

func TestPublishRacingCloseStartsNoWorker(t *testing.T) {
	// publish's own closed check is a check-then-act: a publication that
	// passes it can reach the dispatch WaitGroup microseconds later, while
	// Close is already inside Wait. Go answers that with an unrecovered
	// "WaitGroup misuse: Add called concurrently with Wait" — a process kill
	// during shutdown, which this test reproduces by racing the two.
	e := closeEngine(t, 5*time.Second)

	for i := range 64 {
		nk := NSKey{Namespace: "ns", Key: fmt.Sprintf("key-%d", i)}
		e.OnChange(nk, func(context.Context, Change) {})
	}

	var wg sync.WaitGroup

	start := make(chan struct{})

	for i := range 64 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start
			e.publish(pub(NSKey{Namespace: "ns", Key: fmt.Sprintf("key-%d", i)}, 1, "v1"))
		}()
	}

	wg.Add(1)

	go func() {
		defer wg.Done()

		<-start

		if err := e.Close(); err != nil {
			t.Errorf("Close() = %v, want nil", err)
		}
	}()

	close(start)
	wg.Wait()

	// Nothing may start a worker once Close has shut the door, whichever side
	// of the race a straggler landed on.
	if w := e.workerFor(workerKey{NSKey: NSKey{Namespace: "ns", Key: "after"}}); w != nil {
		t.Error("workerFor started a worker after Close")
	}
}
