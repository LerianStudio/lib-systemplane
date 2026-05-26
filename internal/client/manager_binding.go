// Manager binding for the Client.
//
// In multi-tenant mode the Client can be bound to a *manager.Manager that
// owns the per-tenant cache and the push hot-reload changefeed. Binding is
// strictly opt-in — callers that never call BindManager observe identical
// v1.4.0 behaviour.
package client

import (
	"context"

	"github.com/LerianStudio/lib-systemplane/internal/manager"
)

// BindManager attaches a Manager to the Client. After binding, MT-mode Get
// reads route through the Manager's per-tenant cache and OnChange registers
// callbacks on the Manager's LISTEN dispatcher.
//
// Idempotent and goroutine-safe: only the first non-nil Bind takes effect.
// Subsequent calls are silently ignored so a consumer that defensively
// re-runs construction does not observe surprises.
//
// Safe to call before or after Start. The Client does not require the
// Manager to be present for Start to succeed — the Manager's own lifecycle
// is event-driven (OnTenantActivated et al.) rather than tied to Start.
func (c *Client) BindManager(m *manager.Manager) {
	if c == nil || m == nil {
		return
	}

	c.managerMu.Lock()
	defer c.managerMu.Unlock()

	if c.manager != nil {
		return
	}

	c.manager = m
	m.Bind(newClientHook(c))
}

// boundManager returns the current Manager attached to the Client, if any.
func (c *Client) boundManager() *manager.Manager {
	if c == nil {
		return nil
	}

	c.managerMu.RLock()
	defer c.managerMu.RUnlock()

	return c.manager
}

// newClientHook builds the adapter implementing manager.ClientHooks for c.
func newClientHook(c *Client) manager.ClientHooks {
	return &clientHook{c: c}
}

// clientHook is the Client side of the manager binding.
type clientHook struct {
	c *Client
}

func (h *clientHook) RegisteredKeys() []manager.RegisteredKey {
	if h == nil || h.c == nil {
		return nil
	}

	h.c.registryMu.RLock()
	defer h.c.registryMu.RUnlock()

	out := make([]manager.RegisteredKey, 0, len(h.c.registry))
	for nk, def := range h.c.registry {
		out = append(out, manager.RegisteredKey{
			Namespace:    nk.Namespace,
			Key:          nk.Key,
			DefaultValue: cloneValue(def.defaultValue),
		})
	}

	return out
}

func (h *clientHook) LifecycleContext() context.Context {
	if h == nil || h.c == nil || h.c.lifecycleCtx == nil {
		return context.Background()
	}

	return h.c.lifecycleCtx
}
