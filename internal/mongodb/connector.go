package mongodb

import (
	"context"
	"errors"
	"fmt"

	tmmongo "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Connector resolves a tenant's MongoDB database.
type Connector interface {
	// ResolveDatabase returns the tenant's database handle. It is a ROUND
	// TRIP, not a map lookup: the tenant manager reaches the tenant database
	// on every call, cache hit included. Callers therefore reconcile a scope
	// with ONE List per scope and never a Get per key — a hundred keys
	// resolved one at a time is a hundred round trips before the first value
	// is read.
	//
	// The database, not the collection: the collection name is the store's
	// own configuration (Config.Collection) and a connector must never need
	// to know it.
	//
	// Unlike the Postgres connector, which REFUSES a DSN that pins a schema
	// (ErrSchemaIsolationUnsupported), there is deliberately no MongoDB
	// analogue of that refusal, and its absence is not an omission. Postgres
	// refuses because NOTIFY is database-wide: two tenants sharing one
	// database would each receive the other's notifications stamped with
	// their own scope. A MongoDB change stream is opened on ONE collection in
	// ONE database, so two tenants sharing a Mongo server never observe each
	// other's events.
	ResolveDatabase(ctx context.Context, tenantID string) (*mongo.Database, error)
}

// ErrMongoMgrUnavailable is returned when a connector resolves a tenant
// without a bound tenant-manager Mongo Manager. Surfaces typically in tests
// that constructed the connector with a nil manager.
var ErrMongoMgrUnavailable = errors.New("systemplane/mongodb: tenant-manager mongo manager is not configured")

// NewTenantManagerConnector wraps a lib-commons tenant-manager Mongo Manager.
func NewTenantManagerConnector(mgr *tmmongo.Manager) Connector {
	return &mbMgrConnector{mgr: mgr}
}

// mbMgrConnector is the production adapter wrapping a *tmmongo.Manager. The
// schema is provisioned lazily per resolved database by the store itself, so
// no DDL is issued through this handle. Tests may substitute a fake Connector
// instead of spinning up the tenant-config gRPC client.
type mbMgrConnector struct {
	mgr *tmmongo.Manager
}

func (c *mbMgrConnector) ResolveDatabase(ctx context.Context, tenantID string) (*mongo.Database, error) {
	if c == nil || c.mgr == nil {
		return nil, ErrMongoMgrUnavailable
	}

	db, err := c.mgr.GetDatabaseForTenant(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("systemplane/mongodb: get tenant database %s: %w", tenantID, err)
	}

	return db, nil
}
