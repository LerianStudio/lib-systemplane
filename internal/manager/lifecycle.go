// Lifecycle handlers for the Manager — slice 1 stubs.
//
// Each handler is gated on m.closed and m.hooks; slice 1 returns nil for the
// happy path so the public surface compiles and callers can wire the
// handlers into their event plumbing even before slice 3 lands.
package manager

import (
	"context"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
)

// OnTenantActivated bootstraps systemplane state for tenantID.
//
// It warm-loads every registered key into the per-tenant cache and opens the
// LISTEN goroutine. It does NOT create the schema or seed defaults — those are
// provisioned externally by the consumer's migration pipeline (see
// internal/manager/schema.go and the root package's SchemaSQL() /
// DefaultSeedSQL()). Warm-load tolerates a not-yet-provisioned table: it logs
// and proceeds with an empty cache so a provisioning race never wedges
// activation; LISTEN/poll refreshes the cache once the table exists.
func (m *Manager) OnTenantActivated(ctx context.Context, tenantID string) error {
	if m == nil || m.IsClosed() || tenantID == "" {
		return nil
	}

	if m.hooks == nil || m.connector == nil {
		// Manager not yet bound or running without a Connector (test
		// contexts): no-op, log at debug so test runs stay quiet.
		m.logDebug(ctx, "OnTenantActivated skipped: manager not fully wired",
			log.String("tenant_id", tenantID),
		)

		return nil
	}

	start := time.Now()

	db, err := m.resolveTenantDB(ctx, tenantID)
	if err != nil {
		m.logWarn(ctx, "OnTenantActivated: tenant DB unavailable",
			log.String("tenant_id", tenantID),
			log.Err(err),
		)

		return err
	}

	ts := m.tenantStateFor(tenantID)
	registered := m.hooks.RegisteredKeys()

	if err := m.warmLoad(ctx, db, ts, registered); err != nil {
		m.logWarn(ctx, "OnTenantActivated: warm-load failed",
			log.String("tenant_id", tenantID),
			log.Err(err),
		)

		return err
	}

	if err := m.startListen(ctx, tenantID, ts); err != nil {
		m.logWarn(ctx, "OnTenantActivated: start LISTEN failed",
			log.String("tenant_id", tenantID),
			log.Err(err),
		)

		return err
	}

	m.metrics.recordTenantActivated(ctx, tenantID)
	m.metrics.recordCacheEntries(ctx, tenantID, ts.entryCount())
	m.metrics.recordWarmloadLatency(ctx, tenantID, time.Since(start).Seconds())

	m.logInfo(ctx, "tenant activated for systemplane",
		log.String("tenant_id", tenantID),
		log.Int("cache_entries", ts.entryCount()),
	)

	return nil
}

// OnTenantSuspended pauses the LISTEN goroutine and marks the cache stale.
// Cache entries are kept (suspension is reversible — fast reactivation
// avoids re-warming) but reads after suspension fall through to the DB.
func (m *Manager) OnTenantSuspended(ctx context.Context, tenantID string) error {
	if m == nil || m.IsClosed() || tenantID == "" {
		return nil
	}

	ts, ok := m.loadTenantState(tenantID)
	if !ok {
		return nil
	}

	m.stopListen(ts)
	ts.markStale()
	m.logInfo(ctx, "tenant suspended for systemplane (cache marked stale)",
		log.String("tenant_id", tenantID),
	)

	return nil
}

// OnTenantDeleted closes the LISTEN goroutine and evicts the cache for
// tenantID. Frees process resources tied to the tenant.
func (m *Manager) OnTenantDeleted(ctx context.Context, tenantID string) error {
	if m == nil || m.IsClosed() || tenantID == "" {
		return nil
	}

	if existing, ok := m.perTenant.LoadAndDelete(tenantID); ok {
		if ts, _ := existing.(*tenantState); ts != nil {
			m.stopListen(ts)
		}

		m.metrics.recordTenantDeactivated(ctx, tenantID)
		m.logInfo(ctx, "tenant deactivated for systemplane",
			log.String("tenant_id", tenantID),
		)
	}

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
// Closure proceeds best-effort: every per-tenant state is removed and its
// LISTEN goroutine canceled. Drain blocks up to listenCloseTimeout per
// goroutine waiting for it to exit (so total drain time is bounded by
// staleAfter * tenant_count); callers with strict shutdown budgets should
// supply a ctx and rely on context cancellation to abandon stuck waits.
func (m *Manager) Drain(ctx context.Context) error {
	if m == nil {
		return nil
	}

	m.markClosed()

	m.perTenant.Range(func(key, value any) bool {
		tenantID, _ := key.(string)

		ts, _ := value.(*tenantState)
		if ts != nil {
			m.stopListenCtx(ctx, ts)
		}

		m.perTenant.Delete(tenantID)

		// Stop iterating early if the ctx has canceled — the remaining
		// goroutines will still observe lifecycleCancel (the per-listen
		// ctx is wired to the Client lifecycle ctx) and exit, but the
		// caller's shutdown deadline is honoured.
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	})

	m.logInfo(ctx, "manager drained")

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
