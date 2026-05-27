// Tenant DB resolution and shared constants for the Manager.
//
// This package performs NO runtime schema provisioning. The
// systemplane_entries table, the systemplane_notify_v3() trigger function, and
// the NOTIFY triggers MUST be provisioned externally (e.g. via the consumer's
// migration pipeline) using the DDL published by the root package's
// SchemaSQL() / DefaultSeedSQL(). The Manager only warm-loads, reads, writes
// values, and runs LISTEN; the runtime database role only needs DML + LISTEN
// privileges, never CREATE on the schema.
package manager

import (
	"context"
	"errors"

	"github.com/bxcodec/dbresolver/v2"
)

// defaultTable is the systemplane entries table name. Held here as a private
// constant rather than threaded through config because callers don't need
// configurable table names — the Manager always operates on the same table the
// consumer provisions via SchemaSQL().
//
// defaultChannel is the NOTIFY channel the externally provisioned triggers
// emit on; the LISTEN goroutine subscribes to it.
const (
	defaultTable   = "systemplane_entries"
	defaultChannel = "systemplane_changes"
)

// resolveTenantDB acquires the tenant's primary dbresolver.DB via the
// configured Connector. Returns ErrPgMgrUnavailable when no Connector is
// wired (test contexts).
func (m *Manager) resolveTenantDB(ctx context.Context, tenantID string) (dbresolver.DB, error) {
	if m == nil || m.connector == nil {
		return nil, ErrPgMgrUnavailable
	}

	return m.connector.ResolveDB(ctx, tenantID)
}

// ErrPgMgrUnavailable is returned when a lifecycle handler runs without a
// bound tenant-manager Postgres Manager. Surfaces typically in tests that
// constructed the Manager with a nil pgMgr.
var ErrPgMgrUnavailable = errors.New("systemplane/manager: tenant-manager postgres manager is not configured")
