// OnChange callback registry for the Manager.
//
// Slice 1 ships the type declarations and a no-op RegisterCallback that
// returns an unsubscribe stub. Slice 5 (`feat(manager): OnChange MT-aware`)
// wires the registry to LISTEN dispatch.
package manager

import (
	"context"
	"sync"
	"sync/atomic"
)

// Callback is the function signature OnChange registers.
type Callback func(ctx context.Context, namespace, key string, newValue any)

// Unsubscribe removes a previously registered Callback. Safe to call more
// than once.
type Unsubscribe func()

// callbackEntry is one registered Callback with its monotonic ID for
// stable removal.
type callbackEntry struct {
	id uint64
	fn Callback
}

// callbackList holds the registered callbacks for a single (namespace, key).
type callbackList struct {
	mu    sync.RWMutex
	items []callbackEntry
}

// nextCallbackID is the process-wide monotonic ID source for callback
// registrations. Each registration receives a fresh ID so Unsubscribe can
// remove the right entry even if multiple callbacks for the same (ns, key)
// were registered.
var nextCallbackID atomic.Uint64

// RegisterCallback adds fn to the dispatch list for (namespace, key) and
// returns an unsubscribe closure.
//
// Slice 1: stores the callback but does not dispatch — there is no LISTEN
// goroutine yet. The returned Unsubscribe removes the callback so tests can
// verify register / unregister round-tripping. Slice 5 attaches the
// dispatch to the LISTEN path.
func (m *Manager) RegisterCallback(namespace, key string, fn Callback) Unsubscribe {
	noop := func() {}

	if m == nil || m.IsClosed() || fn == nil {
		return noop
	}

	nk := nsKey{Namespace: namespace, Key: key}
	id := nextCallbackID.Add(1)

	listVal, _ := m.callbacks.LoadOrStore(nk, &callbackList{})

	list, _ := listVal.(*callbackList)
	if list == nil {
		return noop
	}

	list.mu.Lock()
	list.items = append(list.items, callbackEntry{id: id, fn: fn})
	list.mu.Unlock()

	var once sync.Once

	return func() {
		once.Do(func() {
			list.mu.Lock()
			defer list.mu.Unlock()

			for i, e := range list.items {
				if e.id == id {
					list.items = append(list.items[:i], list.items[i+1:]...)

					return
				}
			}
		})
	}
}

// snapshotCallbacks returns a copy of the current callbacks for (ns, key)
// suitable for dispatch from outside the registry lock.
func (m *Manager) snapshotCallbacks(namespace, key string) []Callback {
	if m == nil {
		return nil
	}

	listVal, ok := m.callbacks.Load(nsKey{Namespace: namespace, Key: key})
	if !ok {
		return nil
	}

	list, _ := listVal.(*callbackList)
	if list == nil {
		return nil
	}

	list.mu.RLock()
	defer list.mu.RUnlock()

	out := make([]Callback, len(list.items))
	for i, e := range list.items {
		out[i] = e.fn
	}

	return out
}
