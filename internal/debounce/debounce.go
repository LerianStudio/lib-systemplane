// Package debounce provides a trailing-edge, per-key debouncer the engine uses
// to coalesce a key's rapid changefeed notifications into one re-read of the
// store.
//
// The Debouncer is generic on the key type (any Go comparable); the engine's
// changefeed keys it by a struct of scope, namespace and key, so an event costs
// no string concatenation.
package debounce

import (
	"context"
	"sync"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
)

// Debouncer coalesces rapid submissions for the same key, firing the
// most-recently submitted function after the configured window elapses
// with no new submissions for that key.
//
// The type parameter K is any Go comparable type: strings, plain structs
// of comparable fields, pointers, etc. Structs are preferred on hot paths
// to avoid per-Submit string allocations.
//
// All methods are nil-receiver safe.
type Debouncer[K comparable] struct {
	window time.Duration
	logger log.Logger
	mu     sync.Mutex
	timers map[K]timerEntry
	closed bool
}

type timerEntry struct {
	timer      *time.Timer
	generation uint64
}

// Option configures a Debouncer. The type parameter matches the Debouncer
// it will be applied to.
type Option[K comparable] func(*Debouncer[K])

// WithLogger sets a structured logger for panic-recovery diagnostics.
func WithLogger[K comparable](l log.Logger) Option[K] {
	return func(d *Debouncer[K]) {
		if l != nil {
			d.logger = l
		}
	}
}

// New creates a trailing-edge debouncer with the given quiet window.
// A zero or negative window disables debouncing: Submit invokes fn
// synchronously inline, with panic recovery via lib-observability/runtime.
func New[K comparable](window time.Duration, opts ...Option[K]) *Debouncer[K] {
	d := &Debouncer[K]{
		window: window,
		logger: log.NewNop(),
		timers: make(map[K]timerEntry),
	}

	for _, opt := range opts {
		opt(d)
	}

	return d
}

// Submit schedules fn for invocation after the debouncer's quiet window
// elapses without another Submit for the same key. Subsequent calls for
// the same key reset the timer, so only the last-submitted fn fires.
//
// When the window is zero or negative, fn is invoked synchronously with
// panic recovery.
//
// Nil-receiver safe: does nothing if d is nil.
func (d *Debouncer[K]) Submit(key K, fn func()) {
	if d == nil || fn == nil {
		return
	}

	// Zero/negative window: synchronous invocation with panic recovery.
	if d.window <= 0 {
		d.invokeWithRecover(fn)
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return
	}

	// Stop existing timer for this key (if any) so we can reset.
	if existing, ok := d.timers[key]; ok {
		existing.timer.Stop()
	}

	generation := d.timers[key].generation + 1
	d.timers[key] = timerEntry{
		timer:      time.AfterFunc(d.window, func() { d.fire(key, generation, fn) }),
		generation: generation,
	}
}

// Close cancels all pending timers; later Submits schedule nothing, but a
// zero or negative window still runs fn inline. Idempotent. Nil-receiver safe.
func (d *Debouncer[K]) Close() {
	if d == nil {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return
	}

	d.closed = true

	for key, entry := range d.timers {
		entry.timer.Stop()
		delete(d.timers, key)
	}
}

// fire removes the key from the timer map (under lock) and invokes fn
// with panic recovery. If the debouncer has been closed between the
// timer being scheduled and firing, the invocation is skipped.
func (d *Debouncer[K]) fire(key K, generation uint64, fn func()) {
	d.mu.Lock()

	if d.closed {
		d.mu.Unlock()
		return
	}

	entry, ok := d.timers[key]
	if !ok || entry.generation != generation {
		d.mu.Unlock()
		return
	}

	delete(d.timers, key)

	d.mu.Unlock()

	d.invokeWithRecover(fn)
}

// recoveryComponent and recoveryName label every panic this package recovers.
// They are constants, never the key: deferred arguments are evaluated on every
// call. A caller needing the key in the report recovers first and logs it.
const (
	recoveryComponent = "systemplane.debounce"
	recoveryName      = "invoke"
)

// invokeWithRecover calls fn inside a deferred RecoverAndLogWithContext so
// that a panicking callback cannot crash the process or break the debouncer.
func (d *Debouncer[K]) invokeWithRecover(fn func()) {
	defer runtime.RecoverAndLogWithContext(context.Background(), d.logger, recoveryComponent, recoveryName)

	fn()
}
