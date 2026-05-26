// Lifecycle handlers for the Manager — slice 1 stubs.
//
// Each handler is gated on m.closed and m.hooks; slice 1 returns nil for the
// happy path so the public surface compiles and callers can wire the
// handlers into their event plumbing even before slice 3 lands.
package manager

import (
	"context"

	"github.com/LerianStudio/lib-observability/log"
)

// OnTenantActivated bootstraps systemplane state for tenantID.
//
// Slice 3+: ensures the systemplane schema exists, seeds defaults via
// INSERT ON CONFLICT DO NOTHING, opens the LISTEN goroutine, and warm-loads
// every registered key into the cache. Slice 1 records the tenant in
// perTenant so subsequent Get calls observe an empty cache for it (which
// still falls through to the DB).
func (m *Manager) OnTenantActivated(ctx context.Context, tenantID string) error {
	if m == nil || m.IsClosed() || tenantID == "" {
		return nil
	}

	if m.hooks == nil || m.pgMgr == nil {
		// Manager not yet bound or running without a tmpostgres.Manager
		// (test contexts): no-op, log at debug so test runs stay quiet.
		m.logDebug(ctx, "OnTenantActivated skipped: manager not fully wired",
			log.String("tenant_id", tenantID),
		)

		return nil
	}

	// Slice 1: ensure a tenantState exists. Cache stays empty; Get hits DB.
	_ = m.tenantStateFor(tenantID)

	return nil
}

// OnTenantSuspended pauses the LISTEN goroutine and marks the cache stale.
// Cache entries are kept (suspension is reversible — fast reactivation
// avoids re-warming) but reads after suspension fall through to the DB.
func (m *Manager) OnTenantSuspended(_ context.Context, tenantID string) error {
	if m == nil || m.IsClosed() || tenantID == "" {
		return nil
	}

	ts, ok := m.loadTenantState(tenantID)
	if !ok {
		return nil
	}

	ts.markStale()

	return nil
}

// OnTenantDeleted closes the LISTEN goroutine and evicts the cache for
// tenantID. Frees process resources tied to the tenant.
func (m *Manager) OnTenantDeleted(_ context.Context, tenantID string) error {
	if m == nil || m.IsClosed() || tenantID == "" {
		return nil
	}

	m.perTenant.Delete(tenantID)

	return nil
}

// OnTenantCredentialsRotated tears down the existing LISTEN goroutine and
// re-runs the OnTenantActivated sequence with refreshed credentials.
//
// Slice 4 implements LISTEN restart; slice 1 routes through delete + activate
// so out-of-order delivery does not panic.
func (m *Manager) OnTenantCredentialsRotated(ctx context.Context, tenantID string) error {
	if m == nil || m.IsClosed() || tenantID == "" {
		return nil
	}

	if err := m.OnTenantDeleted(ctx, tenantID); err != nil {
		return err
	}

	return m.OnTenantActivated(ctx, tenantID)
}

// Drain closes every active LISTEN goroutine for graceful shutdown. After
// Drain returns the Manager rejects subsequent lifecycle calls and Gets
// fall through to the DB-read path. Idempotent.
//
// Slice 11 wires real LISTEN cancellation; slice 1 just sets the closed flag
// so future calls become no-ops.
func (m *Manager) Drain(_ context.Context) error {
	if m == nil {
		return nil
	}

	m.markClosed()

	return nil
}

// tenantStateFor returns the per-tenant state for tenantID, creating it if
// absent. Returns the canonical *tenantState (LoadOrStore semantics).
func (m *Manager) tenantStateFor(tenantID string) *tenantState {
	if existing, ok := m.perTenant.Load(tenantID); ok {
		ts, _ := existing.(*tenantState)
		if ts != nil {
			return ts
		}
	}

	fresh := newTenantState(tenantID)
	actual, _ := m.perTenant.LoadOrStore(tenantID, fresh)
	ts, _ := actual.(*tenantState)

	if ts == nil {
		return fresh
	}

	return ts
}

// loadTenantState returns the existing per-tenant state, or false if the
// tenant has never been activated.
func (m *Manager) loadTenantState(tenantID string) (*tenantState, bool) {
	v, ok := m.perTenant.Load(tenantID)
	if !ok {
		return nil, false
	}

	ts, _ := v.(*tenantState)

	return ts, ts != nil
}
