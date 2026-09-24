// Change notification dispatch for systemplane Client.
package client

import (
	"context"
	"fmt"

	"github.com/LerianStudio/lib-systemplane/v4/internal/engine"
)

// OnChange registers a callback for backend-observed value changes.
//
// OnChange returns ErrUnknownKey for a key that was not registered: such a
// subscription could never deliver anything, so refusing it surfaces the typo
// instead of hiding it behind a callback that never fires.
//
// In single-tenant mode deliveries are COALESCED per key and serialized, off
// the changefeed goroutine: while fn runs, a newer revision of the same key
// replaces the pending one, so fn may skip intermediate revisions but always
// receives the newest and never sees revisions out of order. Different keys
// deliver independently.
//
// A subscriber registered before [Client.Start] is handed the value in force
// once, as the first reconcile publishes every registered key (FC-11). The
// publication happens while Start is still on the stack; the DELIVERY is
// queued there and runs on the key's own worker goroutine, so it may land
// either side of Start's return. That decides what a callback may do:
//
//   - it may call Get, GetEntry, List and OnChange re-entrantly — no Client or
//     engine lock is held while it runs, and Get already serves the value the
//     delivery carries;
//   - it may call Set and Delete: the Client counts itself started before the
//     first reconcile runs, so a write from that first delivery lands on
//     either side of Start's return rather than being refused;
//   - Register, Start and Close block on the Client's start lock for as long
//     as Start is still running, and during Close until the close timeout
//     expires.
//
// In multi-tenant mode OnChange returns ErrNotSupportedInMultiTenant for every
// registered key: no scope is tracked and no changefeed runs, so no callback
// could ever fire. The wave-3 engine-tenants lane makes multi-tenant OnChange
// work on both backends, delivering per-tenant changes with Change.Tenant set.
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
		return noop, ErrNotSupportedInMultiTenant
	}

	if fn == nil {
		return noop, nil
	}

	// Straight through: the engine builds the whole Change — tenant, revision,
	// value — and hands each subscriber its own clone, so wrapping fn here
	// would double-clone and drop the revision.
	return c.engine.OnChange(engine.NSKey{Namespace: namespace, Key: key}, fn), nil
}
