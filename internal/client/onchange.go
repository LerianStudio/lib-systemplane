// Change notification dispatch for systemplane Client.
package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/LerianStudio/lib-systemplane/v4/internal/engine"
	"github.com/LerianStudio/lib-systemplane/v4/internal/manager"
)

// OnChange registers a callback for backend-observed value changes.
//
// OnChange returns ErrUnknownKey for a key that was not registered: such a
// subscription could never deliver anything, so refusing it surfaces the typo
// instead of hiding it behind a callback that never fires.
//
// In single-tenant mode the callback fires whenever the changefeed echo for
// (namespace, key) arrives.
//
// In multi-tenant mode without a bound Manager, OnChange returns
// ErrNotSupportedInMultiTenant — preserving the v1.4.0 contract for callers
// that have not opted into the v1.5.0 Manager.
//
// In multi-tenant mode with a bound Manager, the callback is registered on
// the Manager's per-tenant LISTEN dispatcher. It fires once per NOTIFY
// observed across any active tenant's LISTEN goroutine; Change.Tenant names
// the tenant whose row changed and a delete delivers the registered default.
//
// In this wave-1 shim the Manager path reports Change.Revision == 0 on every
// delivery, upsert or delete, because the NOTIFY payload carries no revision
// until the storage lane lands.
func (c *Client) OnChange(namespace, key string, fn func(ctx context.Context, ch Change)) (func(), error) {
	noop := func() {}

	if c == nil || c.closed.Load() {
		return noop, ErrClosed
	}

	nk := nskey{Namespace: namespace, Key: key}

	c.registryMu.RLock()
	_, registered := c.registry[nk]
	c.registryMu.RUnlock()

	if !registered {
		return noop, fmt.Errorf("%w: %s/%s", ErrUnknownKey, namespace, key)
	}

	if c.multiTenant {
		mgr := c.boundManager()
		if mgr == nil {
			return noop, ErrNotSupportedInMultiTenant
		}

		if fn == nil {
			return noop, nil
		}

		unsub := mgr.RegisterCallback(namespace, key, c.managerCallback(fn))
		if unsub == nil {
			return noop, nil
		}

		return func() { unsub() }, nil
	}

	if fn == nil {
		return noop, nil
	}

	id := c.nextSubID.Add(1)

	c.subsMu.Lock()
	c.subscribers[nk] = append(c.subscribers[nk], subscription{
		id: id,
		// ctx is the Client's lifecycle context (passed in by fireSubscribers);
		// callbacks receive cancellation when the Client shuts down. Falling
		// back to context.Background() here would defeat that propagation.
		fn: func(ctx context.Context, newValue any) {
			// fireSubscribers already handed us a private clone.
			fn(ctx, Change{Namespace: namespace, Key: key, Value: newValue})
		},
	})
	c.subsMu.Unlock()

	var once sync.Once

	return func() {
		once.Do(func() {
			c.subsMu.Lock()
			defer c.subsMu.Unlock()

			subs := c.subscribers[nk]
			for i, s := range subs {
				if s.id == id {
					c.subscribers[nk] = append(subs[:i], subs[i+1:]...)

					return
				}
			}
		})
	}, nil
}

// managerCallback adapts a subscriber to the Manager dispatch signature. The
// Manager flags a delete; FC-4 publishes the registered default in its place,
// and the value is cloned so each subscriber owns its copy. An upsert is
// delivered as it decoded, nil included, so a key stored as null stays
// distinguishable from a deleted one.
func (c *Client) managerCallback(fn func(ctx context.Context, ch Change)) manager.Callback {
	return func(ctx context.Context, tenantID, ns, k string, revision int64, isDelete bool, newValue any) {
		if isDelete {
			c.registryMu.RLock()
			def, registered := c.registry[nskey{Namespace: ns, Key: k}]
			c.registryMu.RUnlock()

			if registered {
				newValue = def.defaultValue
			}
		}

		fn(ctx, Change{
			Tenant:    tenantID,
			Namespace: ns,
			Key:       k,
			Revision:  revision,
			Value:     engine.Clone(newValue),
		})
	}
}
