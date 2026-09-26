package client

import (
	"context"
	"fmt"

	tmevent "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/event"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// HandleTenantLifecycle routes FC-6's four lifecycle events to the tenant's
// scope. The engine verbs act in the background, so the only errors are the
// ones decided here: a closed Client and an event naming no tenant.
func (c *Client) HandleTenantLifecycle(_ context.Context, event tmevent.TenantLifecycleEvent) error {
	if c == nil || !c.tenantManaged {
		return nil
	}

	if c.closed.Load() {
		return ErrClosed
	}

	if event.TenantID == "" {
		return fmt.Errorf("%w: lifecycle event %q carries no tenant id", ErrValidation, event.EventType)
	}

	scope := store.Scope{Tenant: event.TenantID}

	switch event.EventType {
	case tmevent.EventTenantActivated:
		c.engine.Unblock(scope) // a read activates it: no feed for a tenant this process never reads
	case tmevent.EventTenantSuspended, tmevent.EventTenantDeleted:
		c.engine.Block(scope)

		c.registryMu.RLock()
		hooks := c.dropHooks
		c.registryMu.RUnlock()

		for _, drop := range hooks {
			drop(event.TenantID)
		}
	case tmevent.EventTenantCredentialsRotated:
		c.engine.Reactivate(scope)
	}

	return nil
}

// OnTenantDrop adds fn to what HandleTenantLifecycle runs, with the tenant id,
// when that tenant is suspended or deleted. A nil fn is ignored.
func (c *Client) OnTenantDrop(fn func(tenant string)) {
	if c == nil || fn == nil {
		return
	}

	c.registryMu.Lock()
	defer c.registryMu.Unlock()

	c.dropHooks = append(c.dropHooks, fn)
}
