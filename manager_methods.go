// Proxy methods exposing the Manager's lifecycle and drain API at the
// public package boundary. The Manager type aliases the internal
// implementation; these wrappers exist so external callers see the
// methods directly on the public type and so godoc renders them.
package systemplane

import "context"

// OnTenantActivated bootstraps systemplane state for tenantID:
//
//  1. Resolves the tenant's database via the bound tenant-manager
//     Postgres Manager.
//  2. Runs idempotent schema DDL (CREATE TABLE IF NOT EXISTS plus the
//     NOTIFY trigger function/triggers). Byte-compatible with the
//     existing lazy bootstrap in MT mode.
//  3. Seeds every registered key's default value with
//     INSERT ON CONFLICT (namespace, key) DO NOTHING. Never overwrites
//     operator-set values.
//  4. Warm-loads every registered key into the per-tenant cache.
//  5. Opens the per-tenant LISTEN goroutine on systemplane_changes.
//
// Idempotent. Calling twice in a row is safe: the second call observes
// the existing state and no-ops the parts already done. Order-tolerant:
// calling OnTenantSuspended or OnTenantDeleted before OnTenantActivated
// is fine — both are no-ops for unknown tenants, and a follow-up
// OnTenantActivated rebuilds state.
func (m *Manager) OnTenantActivated(ctx context.Context, tenantID string) error {
	return asInternalManager(m).OnTenantActivated(ctx, tenantID)
}

// OnTenantSuspended cancels the LISTEN goroutine for tenantID and marks
// the cache stale. Cache entries are retained so a fast reactivation
// avoids re-warming, but reads after suspension fall through to the
// tenant DB to surface any operator changes made during the suspension
// window. Idempotent.
func (m *Manager) OnTenantSuspended(ctx context.Context, tenantID string) error {
	return asInternalManager(m).OnTenantSuspended(ctx, tenantID)
}

// OnTenantDeleted closes the LISTEN goroutine for tenantID and evicts
// the cache. Frees process resources tied to the tenant. Idempotent —
// calling for an unknown tenant is a no-op.
func (m *Manager) OnTenantDeleted(ctx context.Context, tenantID string) error {
	return asInternalManager(m).OnTenantDeleted(ctx, tenantID)
}

// OnTenantCredentialsRotated closes the existing LISTEN goroutine,
// evicts per-tenant state, and re-runs the OnTenantActivated sequence
// with refreshed credentials. The tenant-manager Postgres Manager is
// expected to surface the new DSN through its existing config-change
// detection; the systemplane Manager simply asks for the connection
// again and gets the fresh handle.
func (m *Manager) OnTenantCredentialsRotated(ctx context.Context, tenantID string) error {
	return asInternalManager(m).OnTenantCredentialsRotated(ctx, tenantID)
}

// Drain closes every active per-tenant LISTEN goroutine for graceful
// shutdown. After Drain returns the Manager rejects subsequent lifecycle
// calls and Gets fall through to the DB-read path. Idempotent.
//
// Drain blocks up to an internal close timeout per goroutine waiting for
// it to exit, but honours ctx cancellation: if the caller's shutdown
// budget expires (ctx.Done fires) before every goroutine has acknowledged
// its cancel signal, Drain returns immediately. The in-flight goroutines
// still observe the per-listen ctx cancellation and will exit on their
// own — Drain just stops waiting for them.
func (m *Manager) Drain(ctx context.Context) error {
	return asInternalManager(m).Drain(ctx)
}

// IsClosed reports whether Drain has been called. Useful as a probe in
// shutdown plumbing.
func (m *Manager) IsClosed() bool {
	return asInternalManager(m).IsClosed()
}
