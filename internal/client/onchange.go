// Change notification dispatch for systemplane Client.
package client

import (
	"context"
	"sync"

	"github.com/LerianStudio/lib-observability/log"
)

// OnChange registers a callback for backend-observed value changes.
//
// In single-tenant mode the callback fires whenever the changefeed echo for
// (namespace, key) arrives. In multi-tenant mode OnChange returns
// ErrNotSupportedInMultiTenant — every read resolves a fresh tenant database
// per call, so there is no shared process-wide changefeed to attach to.
func (c *Client) OnChange(namespace, key string, fn func(ctx context.Context, ns, key string, newValue any)) (func(), error) {
	noop := func() {}

	if c == nil || c.closed.Load() {
		return noop, ErrClosed
	}

	if c.multiTenant {
		return noop, ErrNotSupportedInMultiTenant
	}

	if fn == nil {
		return noop, nil
	}

	nk := nskey{Namespace: namespace, Key: key}

	c.registryMu.RLock()
	_, registered := c.registry[nk]
	c.registryMu.RUnlock()

	if !registered {
		c.logDebug(context.Background(), "OnChange called for unregistered key, returning no-op",
			log.String("namespace", namespace),
			log.String("key", key),
		)

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
			fn(ctx, namespace, key, newValue)
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
