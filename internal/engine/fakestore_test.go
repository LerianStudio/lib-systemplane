//go:build unit

package engine

import (
	"context"
	"sync"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// fakeStore is the package's store double: an in-memory table plus the three
// levers every engine test needs — hooks that can block or fail a Get or a
// List, an injector that plays changefeed events into whatever the engine
// subscribed, and counters that prove how often the engine actually touched
// the store.
//
// Both hooks are invoked OUTSIDE the fake's lock, so a test may block one Get
// or one List indefinitely and keep driving the store from another goroutine —
// which is how a reconcile's List is held open while the feed publishes.
type fakeStore struct {
	mu   sync.Mutex
	rows map[scopeNSKey]store.Entry

	// getHook, listHook and subscribeHook run before the call is served.
	// Returning an error fails that call; blocking inside one holds the call
	// open.
	getHook       func(scope store.Scope, nk NSKey) error
	listHook      func(scope store.Scope) error
	subscribeHook func(scope store.Scope) error

	// autoResync makes Subscribe emit store.OpResync the way a real backend
	// does once its connection is up. It is off by default so a test can model
	// a backend that never resyncs simply by not turning it on.
	autoResync bool

	getCalls       int
	listCalls      int
	subscribeCalls int
	liveSubs       int

	nextSubID int
	feeds     map[int]feedSubscription
}

// feedSubscription is one live Subscribe: the scope it covers and the callback
// emit plays events into.
type feedSubscription struct {
	scope store.Scope
	fn    func(store.Event)
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		rows:  map[scopeNSKey]store.Entry{},
		feeds: map[int]feedSubscription{},
	}
}

func rowKey(scope store.Scope, ns, key string) scopeNSKey {
	return scopeNSKey{Tenant: scope.Tenant, Namespace: ns, Key: key}
}

// seed writes a row the way a foreign writer would: directly, with no event
// emitted. That is what opens a gap between the store and the engine's cache.
func (f *fakeStore) seed(scope store.Scope, e store.Entry) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.rows[rowKey(scope, e.Namespace, e.Key)] = e
}

// remove drops a row with no event, so a later re-read reports not found.
func (f *fakeStore) remove(scope store.Scope, nk NSKey) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.rows, rowKey(scope, nk.Namespace, nk.Key))
}

func (f *fakeStore) onGet(hook func(scope store.Scope, nk NSKey) error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.getHook = hook
}

func (f *fakeStore) onList(hook func(scope store.Scope) error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.listHook = hook
}

func (f *fakeStore) onSubscribe(hook func(scope store.Scope) error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.subscribeHook = hook
}

// resyncOnSubscribe makes every later Subscribe emit store.OpResync once the
// subscription is live, which is what FC-2 guarantees after a (re)connect and
// what drives the engine's one initial reconcile.
func (f *fakeStore) resyncOnSubscribe() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.autoResync = true
}

func (f *fakeStore) getCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.getCalls
}

func (f *fakeStore) listCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.listCalls
}

func (f *fakeStore) subscribeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.subscribeCalls
}

func (f *fakeStore) liveSubscriptions() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.liveSubs
}

// emit plays evt into every live subscription of its scope, the way a backend
// changefeed would. It carries any Op, so one injector covers store.OpUpsert,
// store.OpDelete, store.OpResync and store.OpDisconnect.
func (f *fakeStore) emit(evt store.Event) {
	f.mu.Lock()

	fns := make([]func(store.Event), 0, len(f.feeds))

	for _, sub := range f.feeds {
		if sub.scope == evt.Scope {
			fns = append(fns, sub.fn)
		}
	}

	f.mu.Unlock()

	for _, fn := range fns {
		fn(evt)
	}
}

func (f *fakeStore) Start(context.Context) error { return nil }

func (f *fakeStore) Close() error { return nil }

func (f *fakeStore) Get(ctx context.Context, scope store.Scope, ns, key string) (store.Entry, bool, error) {
	f.mu.Lock()
	f.getCalls++
	hook := f.getHook
	f.mu.Unlock()

	if hook != nil {
		if err := hook(scope, NSKey{Namespace: ns, Key: key}); err != nil {
			return store.Entry{}, false, err
		}
	}

	if err := ctx.Err(); err != nil {
		return store.Entry{}, false, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	e, ok := f.rows[rowKey(scope, ns, key)]

	return e, ok, nil
}

func (f *fakeStore) Set(_ context.Context, scope store.Scope, e store.Entry) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.rows[rowKey(scope, e.Namespace, e.Key)] = e

	return e.Revision, nil
}

func (f *fakeStore) Delete(_ context.Context, scope store.Scope, ns, key, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.rows, rowKey(scope, ns, key))

	return nil
}

func (f *fakeStore) List(ctx context.Context, scope store.Scope) ([]store.Entry, error) {
	f.mu.Lock()
	f.listCalls++
	hook := f.listHook
	f.mu.Unlock()

	if hook != nil {
		if err := hook(scope); err != nil {
			return nil, err
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	entries := make([]store.Entry, 0, len(f.rows))

	for k, e := range f.rows {
		if k.Tenant == scope.Tenant {
			entries = append(entries, e)
		}
	}

	return entries, nil
}

// Subscribe registers fn and hands back an unsubscribe that drops it. The fake
// starts no goroutine of its own: events arrive only through emit, or through
// the OpResync autoResync plays once the subscription is live, so a test
// controls exactly when the feed speaks.
//
// A failing subscribeHook registers nothing, which is how a test models a
// listener that could not be opened.
func (f *fakeStore) Subscribe(_ context.Context, scope store.Scope, fn func(store.Event)) (func(), error) {
	f.mu.Lock()
	f.subscribeCalls++
	hook := f.subscribeHook
	f.mu.Unlock()

	if hook != nil {
		if err := hook(scope); err != nil {
			return nil, err
		}
	}

	f.mu.Lock()
	f.liveSubs++
	f.nextSubID++

	id := f.nextSubID
	f.feeds[id] = feedSubscription{scope: scope, fn: fn}
	auto := f.autoResync
	f.mu.Unlock()

	var once sync.Once

	unsubscribe := func() {
		once.Do(func() {
			f.mu.Lock()
			defer f.mu.Unlock()

			delete(f.feeds, id)
			f.liveSubs--
		})
	}

	// Emitted after the subscription is live and outside the lock, so the
	// reconcile it triggers can see the subscription and read the store.
	if auto {
		f.emit(store.Event{Scope: scope, Op: store.OpResync})
	}

	return unsubscribe, nil
}
