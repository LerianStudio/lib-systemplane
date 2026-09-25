package debounce

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/panicmetric"
)

// recordingLogger captures the fields lib-observability's panic recovery
// emits, so a test can read the component name the debouncer handed it.
type recordingLogger struct {
	log.Logger

	mu     sync.Mutex
	fields []log.Field
}

func (r *recordingLogger) Log(_ context.Context, _ int, _ string, fields ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, arg := range fields {
		switch v := arg.(type) {
		case []log.Field:
			r.fields = append(r.fields, v...)
		case log.Field:
			r.fields = append(r.fields, v)
		}
	}
}

// snapshot copies the captured fields under the lock. A timer goroutine can
// still be inside Log while a test renders a failure, so formatting the slice
// itself is a data race the race detector fails the run on.
func (r *recordingLogger) snapshot() []log.Field {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]log.Field(nil), r.fields...)
}

func (r *recordingLogger) field(key string) (log.Field, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, f := range r.fields {
		if f.Key == key {
			return f, true
		}
	}

	return log.Field{}, false
}

const (
	testWindow = 50 * time.Millisecond
	// slack provides headroom for timer scheduling jitter.
	slack = 100 * time.Millisecond
)

func TestDebouncer_SingleSubmitFiresAfterWindow(t *testing.T) {
	t.Parallel()

	d := New[string](testWindow)
	t.Cleanup(d.Close)

	var fired atomic.Int32

	d.Submit("k", func() { fired.Add(1) })

	time.Sleep(testWindow + slack)

	if got := fired.Load(); got != 1 {
		t.Fatalf("expected fn to fire exactly once, got %d", got)
	}
}

func TestDebouncer_RapidSubmitsCoalesce(t *testing.T) {
	t.Parallel()

	d := New[string](testWindow)
	t.Cleanup(d.Close)

	var fired atomic.Int32

	for range 10 {
		d.Submit("k", func() { fired.Add(1) })
		time.Sleep(testWindow / 5) // each submit is well within the window
	}

	// Wait for the trailing edge to fire.
	time.Sleep(testWindow + slack)

	if got := fired.Load(); got != 1 {
		t.Fatalf("expected coalesced to a single fire, got %d", got)
	}
}

func TestDebouncer_DifferentKeysIndependent(t *testing.T) {
	t.Parallel()

	d := New[string](testWindow)
	t.Cleanup(d.Close)

	var firedA, firedB atomic.Int32

	d.Submit("a", func() { firedA.Add(1) })
	d.Submit("b", func() { firedB.Add(1) })

	time.Sleep(testWindow + slack)

	if firedA.Load() != 1 {
		t.Fatalf("expected key 'a' to fire once, got %d", firedA.Load())
	}

	if firedB.Load() != 1 {
		t.Fatalf("expected key 'b' to fire once, got %d", firedB.Load())
	}
}

func TestDebouncer_ClosePreventsFires(t *testing.T) {
	t.Parallel()

	d := New[string](testWindow)

	var fired atomic.Int32

	d.Submit("k", func() { fired.Add(1) })
	d.Close()

	// Wait well past the window.
	time.Sleep(2 * testWindow)

	if got := fired.Load(); got != 0 {
		t.Fatalf("expected no fire after Close, got %d", got)
	}
}

func TestDebouncer_NilReceiverSafe(t *testing.T) {
	t.Parallel()

	var d *Debouncer[string]
	// Must not panic.
	d.Submit("k", func() {})
	d.Close()
}

// Not parallel: see panicmetric.
func TestDebouncer_PanicInFnRecovered(t *testing.T) {
	rec := &recordingLogger{Logger: log.NewNop()}
	d := New[string](testWindow, WithLogger[string](rec))
	t.Cleanup(d.Close)

	counter := panicmetric.Install(t)

	var secondFired atomic.Int32

	// First submit: panics.
	d.Submit("k1", func() { panic("boom") })
	time.Sleep(testWindow + slack)

	// Second submit after the panic: debouncer must still work.
	d.Submit("k2", func() { secondFired.Add(1) })
	time.Sleep(testWindow + slack)

	if secondFired.Load() != 1 {
		t.Fatal("debouncer broke after panic; second submit did not fire")
	}

	// The recovery names a constant site, never the key: arguments to a
	// deferred call are evaluated at defer time, so rendering the key into
	// them would charge a Sprintf to every debounced invocation, panic or not.
	// What that costs in identity, and who pays it back, is the
	// recoveryComponent godoc in debounce.go. The report itself is whole: the
	// line names the site and the panic is counted under the package.
	source, ok := rec.field("source")
	if !ok {
		t.Fatalf("panic recovery logged no source field: %v", rec.snapshot())
	}

	if source.Value != "invoke" {
		t.Errorf("panic recovery source: got %v, want %q", source.Value, "invoke")
	}

	counter.RequireOnly(t, "systemplane.debounce", "invoke")
}

func TestDebouncer_ZeroWindowInvokesSync(t *testing.T) {
	t.Parallel()

	d := New[string](0)
	t.Cleanup(d.Close)

	fired := false

	d.Submit("k", func() { fired = true })

	// With zero window, fn is invoked synchronously — no sleep needed.
	if !fired {
		t.Fatal("expected synchronous invocation with zero window")
	}
}

func TestDebouncer_CloseIdempotent(t *testing.T) {
	t.Parallel()

	d := New[string](testWindow)
	// Multiple closes must not panic.
	d.Close()
	d.Close()
	d.Close()
}

func TestDebouncer_SubmitAfterCloseIsNoop(t *testing.T) {
	t.Parallel()

	d := New[string](testWindow)
	d.Close()

	var fired atomic.Int32

	d.Submit("k", func() { fired.Add(1) })
	time.Sleep(testWindow + slack)

	if got := fired.Load(); got != 0 {
		t.Fatalf("expected no fire after Close, got %d", got)
	}
}

func TestDebouncer_NilFnIgnored(t *testing.T) {
	t.Parallel()

	d := New[string](testWindow)
	t.Cleanup(d.Close)

	// Must not panic.
	d.Submit("k", nil)
}

// TestDebouncer_StructKey verifies a plain comparable struct type works as the
// debouncer key. This is the shape used by systemplane's changefeed events to
// avoid per-event string-concat allocations on the changefeed hot path.
func TestDebouncer_StructKey(t *testing.T) {
	t.Parallel()

	type key struct {
		A string
		B string
	}

	d := New[key](testWindow)
	t.Cleanup(d.Close)

	var firedAB, firedCD atomic.Int32

	d.Submit(key{"a", "b"}, func() { firedAB.Add(1) })
	d.Submit(key{"c", "d"}, func() { firedCD.Add(1) })

	// Duplicate submit for (a, b) within the window must coalesce.
	d.Submit(key{"a", "b"}, func() { firedAB.Add(1) })

	time.Sleep(testWindow + slack)

	if got := firedAB.Load(); got != 1 {
		t.Fatalf("expected key{a,b} to coalesce to one fire, got %d", got)
	}

	if got := firedCD.Load(); got != 1 {
		t.Fatalf("expected key{c,d} to fire once, got %d", got)
	}
}
