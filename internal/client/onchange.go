// Change notification dispatch for systemplane Client.
package client

import (
	"context"
	"fmt"

	"github.com/LerianStudio/lib-systemplane/v4/internal/engine"
	"github.com/LerianStudio/lib-systemplane/v4/internal/manager"
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
// once, during Start, as the first reconcile publishes every registered key
// (FC-11). That delivery runs while Start is still on the stack, which decides
// what a callback may do:
//
//   - it may call Get, GetEntry, List and OnChange re-entrantly — no Client or
//     engine lock is held while it runs;
//   - Set and Delete called from that first delivery return ErrNotStarted,
//     because Start has not returned yet;
//   - Register, Start and Close block on the Client's start lock until Start
//     returns, and during Close until the close timeout expires.
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

	// Straight through: the engine builds the whole Change — tenant, revision,
	// value — and hands each subscriber its own clone, so wrapping fn here
	// would double-clone and drop the revision.
	return c.engine.OnChange(engine.NSKey{Namespace: namespace, Key: key}, fn), nil
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
