// Tenant-lifecycle event router for the Manager.
//
// HandleTenantLifecycle owns the EventType switch so consumer services no
// longer hand-roll a `switch event.EventType { case ... }` around the four
// On* handlers. Its signature is intentionally identical to
// tmevent.EventHandler so a bound Manager can be registered directly as an
// event handler with the lib-commons tenant-manager event dispatcher.
package manager

import (
	"context"

	tmevent "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/event"
	"github.com/LerianStudio/lib-observability/log"
)

// HandleTenantLifecycle routes a tenant lifecycle event to the matching On*
// handler. Non-tenant-lifecycle event types are ignored (no-op).
//
// Best-effort (Option A): an On* handler error is logged at WARN via the
// Manager's logger and SWALLOWED (returns nil) — a transient LISTEN reconnect
// failure must never wedge the consumer's lifecycle dispatch pipeline.
//
// Nil-receiver safe: calling on a nil Manager is a no-op returning nil.
func (m *Manager) HandleTenantLifecycle(ctx context.Context, event tmevent.TenantLifecycleEvent) error {
	if m == nil {
		return nil
	}

	var handler func(ctx context.Context, tenantID string) error

	switch event.EventType {
	case tmevent.EventTenantActivated:
		handler = m.onTenantActivated
	case tmevent.EventTenantSuspended:
		handler = m.onTenantSuspended
	case tmevent.EventTenantDeleted:
		handler = m.onTenantDeleted
	case tmevent.EventTenantCredentialsRotated:
		handler = m.onTenantCredentialsRotated
	default:
		// Non-tenant-lifecycle event type (e.g. tenant.created, service
		// association). Not this Manager's concern.
		return nil
	}

	if handler == nil {
		return nil
	}

	if err := handler(ctx, event.TenantID); err != nil {
		m.logWarn(ctx, "systemplane manager lifecycle handler failed (swallowed)",
			log.String("event_type", event.EventType),
			log.String("tenant_id", event.TenantID),
			log.Err(err),
		)
	}

	return nil
}
