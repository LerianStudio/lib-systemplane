package client

import (
	"context"
	"fmt"

	tmevent "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/event"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// HandleTenantLifecycle routes the four tenant lifecycle events to the
// tenant's scope. The engine verbs act in the background, so the only errors
// are the ones decided here: a closed Client and an event naming no tenant.
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
	case tmevent.EventTenantCredentialsRotated:
		c.engine.Reactivate(scope)
	}

	return nil
}

// TenantBlocked reports whether a tenant lifecycle event left tenant blocked.
func (c *Client) TenantBlocked(tenant string) bool {
	return c != nil && c.engine.Blocked(store.Scope{Tenant: tenant})
}
