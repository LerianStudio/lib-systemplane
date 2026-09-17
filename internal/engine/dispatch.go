package engine

import (
	"context"
	"sync"

	"github.com/LerianStudio/lib-observability/v4/runtime"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// subscription is one registered callback. The id is what unsubscribe removes:
// two subscriptions of the same key may share a function value, so the
// function pointer cannot identify one.
type subscription struct {
	id uint64
	fn func(ctx context.Context, ch Change)
}

// workerKey names the one worker that serializes deliveries for a key inside
// one scope. The same key in two scopes gets two workers, so a blocked tenant
// never delays another.
type workerKey struct {
	Scope store.Scope
	NSKey
}

// dispatchWorker is the mailbox half of one worker: a single slot holding the
// newest Change waiting to be delivered, plus a 1-buffered channel that wakes
// the goroutine. The slot is the coalescing: a publication that lands while
// the worker is inside a subscriber overwrites whatever was pending, so a
// subscriber may skip intermediate revisions but always ends on the newest and
// never sees two revisions out of order.
type dispatchWorker struct {
	mu      sync.Mutex
	pending *Change
	signal  chan struct{}
}

// submit replaces the pending Change and wakes the worker. The signal send is
// non-blocking: a full buffer already means "there is work", and blocking here
// would push a slow subscriber's backpressure onto the changefeed goroutine —
// the exact coupling this queue exists to break.
func (w *dispatchWorker) submit(ch Change) {
	w.mu.Lock()
	w.pending = &ch
	w.mu.Unlock()

	select {
	case w.signal <- struct{}{}:
	default:
	}
}

// take empties the slot, reporting whether anything was in it. A wake with an
// empty slot is normal: two submits can coalesce into one buffered signal.
func (w *dispatchWorker) take() (Change, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.pending == nil {
		return Change{}, false
	}

	ch := *w.pending
	w.pending = nil

	return ch, true
}

// OnChange registers fn for changes of nk in EVERY scope the engine tracks;
// Change.Tenant tells the callback which one. The registry is keyed by key
// alone, never by scope, so a subscription made before a tenant was activated
// still covers it.
//
// The returned unsubscribe is idempotent: calling it twice removes one
// subscription, not the next subscriber that reused the freed slot.
//
// It does not reject an unregistered key — the Client owns the registry and
// answers FC-4's ErrUnknownKey before delegating here.
//
// A closed engine accepts no subscription: registering one would hand back a
// callback nothing can ever invoke.
func (e *Engine) OnChange(nk NSKey, fn func(ctx context.Context, ch Change)) (unsubscribe func()) {
	noop := func() {}
	if e == nil || fn == nil || e.closed.Load() {
		return noop
	}

	id := e.nextSubID.Add(1)

	e.subsMu.Lock()

	if e.subscribers == nil {
		e.subscribers = make(map[NSKey][]subscription)
	}

	e.subscribers[nk] = append(e.subscribers[nk], subscription{id: id, fn: fn})
	e.subsMu.Unlock()

	var once sync.Once

	return func() {
		once.Do(func() {
			e.subsMu.Lock()
			defer e.subsMu.Unlock()

			subs := e.subscribers[nk]
			for i, sub := range subs {
				if sub.id == id {
					e.subscribers[nk] = append(subs[:i], subs[i+1:]...)

					return
				}
			}
		})
	}
}

// dispatch hands an accepted publication to its (scope, key) worker.
//
// It is called by publish while the scope's write lock is held, which is what
// keeps deliveries in revision order: the fence decision and the mailbox write
// are one atomic step, so a publication that lost the fence can never overtake
// the winner on the way to the slot. Nothing here runs a callback, so holding
// the lock costs a mutex and a non-blocking channel send.
//
// A key nobody subscribes to starts no worker: the delivery would have nowhere
// to go, and a process registering hundreds of keys should not pay a goroutine
// for each one that happens to change.
func (e *Engine) dispatch(pub publication) {
	e.subsMu.RLock()
	subscribed := len(e.subscribers[pub.NSKey]) > 0
	e.subsMu.RUnlock()

	if !subscribed {
		return
	}

	w := e.workerFor(workerKey{Scope: pub.Scope, NSKey: pub.NSKey})
	if w == nil {
		return
	}

	w.submit(Change{
		Tenant:    pub.Scope.Tenant,
		Namespace: pub.Namespace,
		Key:       pub.Key,
		Revision:  pub.Revision,
		Value:     pub.Value,
	})
}

// workerFor returns wk's worker, starting it on first use. Workers live until
// the lifecycle context is canceled, so the goroutine count is bounded by the
// number of (scope, key) pairs that actually published a change to a
// subscribed key.
//
// It returns nil once Close has shut the door. That check and the
// dispatchWG.Add below are under the same lock Close takes before it waits, so
// every Add provably happens-before the Wait. Without it a publication that
// passed publish's closed check microseconds earlier could reach Add while
// Close is inside Wait, which Go answers with an unrecovered "WaitGroup misuse:
// Add called concurrently with Wait" — a process kill during shutdown.
func (e *Engine) workerFor(wk workerKey) *dispatchWorker {
	e.workersMu.Lock()
	defer e.workersMu.Unlock()

	if e.workersClosed {
		return nil
	}

	if w := e.workers[wk]; w != nil {
		return w
	}

	if e.workers == nil {
		e.workers = make(map[workerKey]*dispatchWorker)
	}

	w := &dispatchWorker{signal: make(chan struct{}, 1)}
	e.workers[wk] = w

	e.dispatchWG.Add(1)

	go e.runWorker(wk, w)

	return w
}

// runWorker is the one goroutine that invokes subscribers of wk. It exits when
// the engine's lifecycle context is canceled, dropping whatever is still
// pending: a Change nobody has started delivering is not worth holding
// shutdown for.
func (e *Engine) runWorker(wk workerKey, w *dispatchWorker) {
	defer e.dispatchWG.Done()

	ctx := e.dispatchContext()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.signal:
			if ch, ok := w.take(); ok {
				// Marked for the whole delivery so a Close that times out can
				// name this (scope, key) as one it is stuck on.
				e.running.Store(wk, struct{}{})
				e.deliver(ctx, wk.NSKey, ch)
				e.running.Delete(wk)
			}
		}
	}
}

// deliver invokes every subscriber of nk serially, each with its own deep copy
// of the value: two subscribers of one key must not be able to see each
// other's mutations, and neither may reach the cached object. Each invocation
// runs under RecoverAndLog, so a panicking callback kills neither the worker
// nor the process, and the subscriber list is copied before any callback runs,
// so a callback may unsubscribe itself without deadlocking.
func (e *Engine) deliver(ctx context.Context, nk NSKey, ch Change) {
	e.subsMu.RLock()
	subs := make([]subscription, len(e.subscribers[nk]))
	copy(subs, e.subscribers[nk])
	e.subsMu.RUnlock()

	for _, sub := range subs {
		func() {
			defer runtime.RecoverAndLog(e.logger, "systemplane.engine.onchange")

			delivered := ch
			delivered.Value = Clone(ch.Value)

			sub.fn(ctx, delivered)
		}()
	}
}

// dispatchContext is the context every callback receives: the engine's
// lifecycle context, so Close cancels in-flight deliveries. An engine built
// without one (only reachable in tests) falls back to Background rather than
// panicking on a nil context.
func (e *Engine) dispatchContext() context.Context {
	if e.lifecycleCtx == nil {
		return context.Background()
	}

	return e.lifecycleCtx
}
