// Per-tenant cache lookup paths for the Manager.
//
// Slice 1 ships always-miss stubs. The Client.Get path checks if the Manager
// returned a hit and, if not, falls through to the existing DB-read code.
//
// Slice 2 wires real cache hit/miss semantics and populates the cache on
// miss. Slice 4 invalidates the cache on NOTIFY arrival.
package manager

import (
	"context"

	tmcore "github.com/LerianStudio/lib-commons/v6/commons/tenant-manager/core"
)

// TenantIDFromContext extracts the tenant ID set by lib-commons tenant-manager
// middleware. Returns "" when the ctx carries no tenant — typically a boot
// context that pre-dates request dispatch. Exposed to the Client side so the
// Get fallback path can branch on the same source of truth.
func TenantIDFromContext(ctx context.Context) string {
	return tmcore.GetTenantIDContext(ctx)
}

// Lookup attempts to satisfy a Get from the per-tenant cache.
//
// Returns (value, hit, error). When hit is false the caller MUST fall through
// to a tenant DB read. Slice 1: hit is always false. Slice 2 fills it in.
func (m *Manager) Lookup(ctx context.Context, tenantID, namespace, key string) (any, bool, error) {
	if m == nil || m.IsClosed() {
		return nil, false, nil
	}

	if tenantID == "" {
		return nil, false, nil
	}

	ts, ok := m.loadTenantState(tenantID)
	if !ok {
		// Slice 1: tenant not yet activated — caller falls through to DB.
		return nil, false, nil
	}

	ts.mu.RLock()
	defer ts.mu.RUnlock()

	if ts.stale {
		return nil, false, nil
	}

	v, found := ts.entries[nsKey{Namespace: namespace, Key: key}]
	if !found {
		m.metrics.recordGetCacheOutcome(ctx, tenantID, "miss")

		return nil, false, nil
	}

	m.metrics.recordGetCacheOutcome(ctx, tenantID, "hit")

	return v, true, nil
}

// Populate records a freshly-resolved value in the cache. Called from the
// Client.Get fallback path when a tenant-DB read succeeds, so subsequent Gets
// for the same key bypass the DB. Bounded by MaxEntriesPerTenant; over-bound
// writes are dropped silently (with a metric in slice 6).
func (m *Manager) Populate(ctx context.Context, tenantID, namespace, key string, value any) {
	if m == nil || m.IsClosed() || tenantID == "" {
		return
	}

	ts, ok := m.loadTenantState(tenantID)
	if !ok {
		// Not activated — refuse to seed a cache for a tenant the Manager
		// has never seen. The lifecycle handlers own creation; the Get path
		// MUST NOT create state behind their back.
		return
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	if len(ts.entries) >= m.cfg.maxEntriesPerTenantOverride {
		return
	}

	ts.entries[nsKey{Namespace: namespace, Key: key}] = value

	m.metrics.recordCacheEntries(ctx, tenantID, len(ts.entries))
}

// Invalidate removes a key from the per-tenant cache. Called by the LISTEN
// goroutine on delete NOTIFYs (slice 4). Safe to call for unknown tenants
// or keys — no-op.
func (m *Manager) Invalidate(ctx context.Context, tenantID, namespace, key string) {
	if m == nil || tenantID == "" {
		return
	}

	ts, ok := m.loadTenantState(tenantID)
	if !ok {
		return
	}

	ts.mu.Lock()
	delete(ts.entries, nsKey{Namespace: namespace, Key: key})
	count := len(ts.entries)
	ts.mu.Unlock()

	m.metrics.recordCacheEntries(ctx, tenantID, count)
}
