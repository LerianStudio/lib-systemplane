// Connector is the dependency the Manager uses to resolve per-tenant
// Postgres handles. In production it is satisfied by a thin adapter over
// lib-commons tenant-manager Postgres Manager; in tests it can be a fake
// that returns ad-hoc DSN/dbresolver.DB pairs without spinning up the
// tenant-config gRPC client.
package manager

import (
	"context"
	"fmt"

	tmpostgres "github.com/LerianStudio/lib-commons/v6/commons/tenant-manager/postgres"
	"github.com/bxcodec/dbresolver/v2"
)

// Connector abstracts how the Manager resolves a tenant's Postgres handle
// and LISTEN DSN. Production code wraps tmpostgres.Manager; tests may
// substitute a fake implementation.
type Connector interface {
	// ResolveDB returns the tenant's primary database handle. Used for
	// warm-load and NOTIFY refresh reads. The schema is provisioned
	// externally; the Manager never issues DDL through this handle.
	ResolveDB(ctx context.Context, tenantID string) (dbresolver.DB, error)

	// ResolveDSN returns the connection string used by pgx.Connect to open
	// the dedicated LISTEN connection for the tenant.
	ResolveDSN(ctx context.Context, tenantID string) (string, error)
}

// pgMgrConnector is the production adapter wrapping a *tmpostgres.Manager.
type pgMgrConnector struct {
	mgr *tmpostgres.Manager
}

func (c *pgMgrConnector) ResolveDB(ctx context.Context, tenantID string) (dbresolver.DB, error) {
	if c == nil || c.mgr == nil {
		return nil, ErrPgMgrUnavailable
	}

	conn, err := c.mgr.GetConnection(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("systemplane/manager: get tenant connection %s: %w", tenantID, err)
	}

	db, err := conn.GetDB()
	if err != nil {
		return nil, fmt.Errorf("systemplane/manager: get tenant DB %s: %w", tenantID, err)
	}

	return db, nil
}

func (c *pgMgrConnector) ResolveDSN(ctx context.Context, tenantID string) (string, error) {
	if c == nil || c.mgr == nil {
		return "", ErrPgMgrUnavailable
	}

	conn, err := c.mgr.GetConnection(ctx, tenantID)
	if err != nil {
		return "", fmt.Errorf("systemplane/manager: get tenant connection %s: %w", tenantID, err)
	}

	if conn.ConnectionStringPrimary == "" {
		return "", fmt.Errorf("systemplane/manager: tenant %s has empty primary DSN", tenantID)
	}

	return conn.ConnectionStringPrimary, nil
}
