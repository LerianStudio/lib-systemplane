package engine

import (
	"context"
	"slices"
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

// workerKey names one scope's worker for one key. Each scope owns its workers,
// so the pairing is only ever assembled for the outside world: the marker a
// timed-out Close reads to name what it is stuck on. The same key in two
// scopes gets two workers, so a blocked tenant never delays another.
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
//
// The slot is a value, not a pointer: taking the address of the submitted
// Change would escape it to the heap on every delivered publication, and the
// mailbox holds at most one of them at a time. hasPending is what distinguishes
// an empty slot from a zero Change.
//
// done is closed when this one worker is stopped ahead of the engine — a
// dropped scope. Stopping per worker is what keeps a suspended or deleted
// tenant from leaving one parked goroutine per key behind until Close.
type dispatchWorker struct {
	mu         sync.Mutex
	pending    Change
	hasPending bool
	signal     chan struct{}

	done     chan struct{}
	stopOnce sync.Once
}

// stop ends the worker's goroutine. It is idempotent: a scope dropped twice,
// or dropped and then closed, must not close the channel twice.
func (w *dispatchWorker) stop() {
	w.stopOnce.Do(func() { close(w.done) })
}

// submit replaces the pending Change and wakes the worker. The signal send is
// non-blocking: a full buffer already means "there is work", and blocking here
// would push a slow subscriber's backpressure onto the changefeed goroutine —
// the exact coupling this queue exists to break.
func (w *dispatchWorker) submit(ch Change) {
	w.mu.Lock()
	w.pending = ch
	w.hasPending = true
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

	if !w.hasPending {
		return Change{}, false
	}

	// Cleared as well as flagged: the slot holds the delivered value, and
	// leaving it there keeps that object graph reachable until the next
	// publication for this key — which for a knob nobody touches again is
	// forever.
	ch := w.pending
	w.pending = Change{}
	w.hasPending = false

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
					// slices.Delete, not a re-slicing append: append leaves the
					// removed subscription in the backing array's tail slot, so
					// the callback and everything the consumer captured in it
					// stay reachable for the life of the Engine. Delete zeroes
					// the vacated slots.
					e.subscribers[nk] = slices.Delete(subs, i, i+1)

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
// the winner on the way to the slot. Nothing here runs a callback, so the lock
// usually costs a mutex and a non-blocking channel send — plus, on a
// subscribed key's first published change, the launch of that key's worker
// goroutine in workerFor. Workers are bounded at one per subscribed key per
// scope, so that launch is a one-off per key, and FC-11 puts almost every one
// of them in the first reconcile at Start.
//
// A key nobody subscribes to starts no worker: the delivery would have nowhere
// to go, and a process registering hundreds of keys should not pay a goroutine
// for each one that happens to change.
//
// sc is the publisher's own scope state, not a lookup by pub.Scope: a
// publication that raced the scope's drop must be discarded rather than
// delivered, and only the state it was fenced against can say so.
func (e *Engine) dispatch(sc *scopeState, pub publication) {
	e.subsMu.RLock()
	subscribed := len(e.subscribers[pub.NSKey]) > 0
	e.subsMu.RUnlock()

	if !subscribed {
		return
	}

	w := e.workerFor(sc, pub.NSKey)
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

// workerFor returns sc's worker for nk, starting it on first use. Workers live
// until the lifecycle context is canceled or their scope is dropped, so the
// goroutine count is one parked worker per subscribed key per tracked scope:
// FC-11 publishes every registered key at a scope's first reconcile, so from
// that moment on every key with a subscriber has a worker, and a key nobody
// subscribed to never gets one.
//
// The worker belongs to the scope state the caller resolved, not to the scope
// VALUE: a state that has been swept never hands one out again, and the state
// that replaced it keeps its own.
//
// It returns nil once Close has shut the door. That check and the
// dispatchWG.Add below are under the same lock Close takes before it waits, so
// every Add provably happens-before the Wait. Without it a publication that
// passed publish's closed check microseconds earlier could reach Add while
// Close is inside Wait, which Go answers with an unrecovered "WaitGroup misuse:
// Add called concurrently with Wait" — a process kill during shutdown.
//
// It returns nil for a dropped scope too, under that same lock and for the
// same reason: the sweep that ended the scope's workers marked its state, so a
// publisher that slipped past publish's refusal cannot start a replacement the
// drop will never come back to stop. The caller discards the publication.
//
// A worker whose goroutine ends reaps itself out of the map on the way out, so
// what this hands back is never a worker nobody is draining.
func (e *Engine) workerFor(sc *scopeState, nk NSKey) *dispatchWorker {
	e.workersMu.Lock()
	defer e.workersMu.Unlock()

	if e.workersClosed || sc.workersDropped {
		return nil
	}

	if w := sc.workers[nk]; w != nil {
		return w
	}

	w := &dispatchWorker{signal: make(chan struct{}, 1), done: make(chan struct{})}
	sc.workers[nk] = w

	e.dispatchWG.Add(1)

	wk := workerKey{Scope: sc.scope, NSKey: nk}

	runtime.SafeGoWithContextAndComponent(e.dispatchContext(), e.logger,
		"systemplane.engine", "dispatch", runtime.KeepRunning,
		func(ctx context.Context) {
			defer e.dispatchWG.Done()
			defer e.reapWorker(sc, nk, w)

			e.runWorker(ctx, wk, w)
		})

	return w
}

// reapWorker forgets a worker whose goroutine has ended, so the next
// publication for its (scope, key) starts a replacement instead of filling a
// mailbox nobody drains.
//
// Every ordinary exit leaves nothing to repair — the scope's stop and the
// engine's both sweep the map and refuse replacements under this same lock —
// so this exists for the one exit neither can see: a panic that escaped
// runWorker, which the launcher's policy recovers and returns from without
// restarting.
//
// No production path reaches that today. A callback panic is recovered per
// subscriber inside deliver, and the report deliver then makes cannot unwind
// out either: reportConsumerPanic hands it to reportRecovered, which swallows
// a panic raised by the consumer's own recorder or error reporter, and the
// consumer's logger is wrapped at construction by log.Guard. It is kept
// as defence in depth, the way ingestDefault keeps its unregistered-key
// branch: a future delivery step that panics outside deliver's recovery is
// repaired here rather than left in the map. Left in place, that dead worker
// goes on being handed every later Change for its key, silently, until the
// scope is dropped or the engine closes, and its marker makes a timed-out
// Close name a subscriber that has not been running since.
//
// The map entry goes only when it is still THIS worker. A scope dropped and
// brought back up, or a worker already swept and replaced, owns the slot now,
// and reaping by key alone would take a live worker's mailbox with it.
//
// It is deferred INSIDE the launch closure, registered after the WaitGroup
// release so it runs before it: a Close waiting on that WaitGroup must not be
// released while this worker is still named in running, or it reports a
// delivery nobody is running.
func (e *Engine) reapWorker(sc *scopeState, nk NSKey, w *dispatchWorker) {
	e.running.Delete(w)

	e.workersMu.Lock()
	defer e.workersMu.Unlock()

	if sc.workers[nk] == w {
		delete(sc.workers, nk)
	}
}

// runWorker is the one goroutine that invokes subscribers of wk. It exits when
// the engine's lifecycle context is canceled or this worker's scope is
// dropped, dropping whatever is still pending: a Change nobody has started
// delivering is not worth holding shutdown for, and one addressed to a scope
// the engine no longer tracks has nowhere to go.
// Either stop wins over a wake that is ready at the same moment, so no
// callback starts once the scope's drop or Close has begun.
//
// The whole goroutine, not just the callback, runs under lib-observability's
// recovery — a panic anywhere in the loop, cloning a pathological value say,
// would otherwise kill the process — because its caller launches it through
// runtime.SafeGoWithContextAndComponent rather than a raw go statement. The
// WaitGroup Done is deferred inside that launch, so it runs BEFORE the
// recovery on the way out of a panic and Close is never left waiting on a
// goroutine that is already gone.
func (e *Engine) runWorker(ctx context.Context, wk workerKey, w *dispatchWorker) {
	// Boxed once per worker rather than once per delivery: converting the
	// struct to an interface allocates, and the mark is written on every wake.
	mark := any(wk)

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			return
		case <-w.signal:
			// Marked BEFORE the mailbox is emptied, and for the whole
			// delivery. It is what a Close that times out uses to name this
			// (scope, key) as one it is stuck on, and what tells an observer
			// that a delivery is under way. Marking it after take() left a
			// window where the Change was in neither the mailbox nor the
			// marker: an observer sampling both saw an idle worker for a
			// delivery that was about to start. A wake with an empty slot
			// therefore marks the worker busy for the length of one
			// mutex-guarded slot read, which no observer can be stuck behind.
			//
			// Keyed by this worker, not by wk: a dropped scope's straggler and
			// the re-activated scope's worker for the same key share wk, so
			// clearing by wk would erase the other one's mark.
			e.running.Store(w, mark)

			// Re-checked after the wake, because select picks at random among
			// ready cases: a worker coming back from a delivery can find its
			// signal ready together with a stop and take the signal. Without
			// this it would hand a dropped scope's Change to subscribers after
			// dropScope returned, or start a callback after Close began. The
			// mark stays up across the check so the window it closes above
			// does not reopen here, and the deferred reap clears it on the way
			// out — these two exits need no clear of their own.
			select {
			case <-ctx.Done():
				return
			case <-w.done:
				return
			default:
			}

			if ch, ok := w.take(); ok {
				e.deliver(ctx, wk.Scope, wk.NSKey, ch)
			}

			e.running.Delete(w)
		}
	}
}

// deliver invokes every subscriber of nk serially, each with its own deep copy
// of the value: two subscribers of one key must not be able to see each
// other's mutations, and neither may reach the cached object. Each invocation
// runs under lib-observability's context-ful recovery, so a panicking callback
// kills neither the worker nor the process and is counted and recorded on the
// span rather than merely logged; the subscriber list is copied before any
// callback runs, so a callback may unsubscribe itself without deadlocking.
//
// The key's redaction policy is read only when a callback actually panics,
// and then once per panicking callback. A callback is consumer code holding
// the decoded value and a panic naming it —
// panic(fmt.Sprintf("cannot apply %v", ch.Value)) — is reported by that same
// recovery, which prints the panic value unless production mode is on, so the
// bit is needed there and nowhere else. Reading it before the fan-out took the
// registry's lock on every delivery of every subscribed key, contending with
// Register and with every ingress, to answer a question almost no delivery
// asks. An unregistered key reports false, which is the honest answer: nothing
// registered it, so nothing declared it sensitive.
func (e *Engine) deliver(ctx context.Context, scope store.Scope, nk NSKey, ch Change) {
	e.subsMu.RLock()
	subs := make([]subscription, len(e.subscribers[nk]))
	copy(subs, e.subscribers[nk])
	e.subsMu.RUnlock()

	if len(subs) == 0 {
		return
	}

	for _, sub := range subs {
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					def, _ := e.lookup(nk.Namespace, nk.Key)

					e.reportConsumerPanic(ctx, scope, nk, recovered, def.Redacted,
						"onchange callback panicked", "onchange")
				}
			}()

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
