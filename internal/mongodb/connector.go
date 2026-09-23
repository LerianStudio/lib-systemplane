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
	// The database MUST be one no other scope of the same Store resolves to.
	// The Postgres half refuses a feed whose DSN reaches a database another
	// LIVE feed of that Store already listens on — identity taken from the
	// open connection (inet_server_addr / port plus current_database, see
	// serverDatabaseKey in internal/postgres/connector.go), so a DSN that
	// merely pins a search_path is admitted; it is the shared DATABASE that is
	// refused, with that package's ErrSharedDatabaseUnsupported. MongoDB
	// applies the collection-level analogue: a feed whose (client, database,
	// collection) triple a live feed already watches is refused with this
	// package's ErrSharedDatabaseUnsupported.
	//
	// Two tenants on two databases of ONE client stay admitted, and so do two
	// tenants on two collections: the refusal is about a changefeed being
	// SHARED, not about sharing a server.
	ResolveDatabase(ctx context.Context, tenantID string) (*mongo.Database, error)
}

// ErrSharedDatabaseUnsupported is returned when a changefeed would watch a
// collection another live feed of the same Store already watches — the
// signature of a connector that hands two tenants one database.
//
// A change stream is opened on ONE collection, so both scopes would receive
// every write stamped with their OWN scope and the engine's revision fence
// would treat a foreign revision as authoritative. The polling fallback reads
// that same collection and bleeds the same way.
//
// It is raised ONLY when a changefeed opens or reopens — Start for the zero
// scope, the first Subscribe for a named tenant — because that is the moment a
// second watcher would start receiving the first's events. A Store that never
// opens a feed never evaluates the rule: its reads and writes resolve straight
// through the connector, so two scopes sharing one database go unnoticed there
// and unpunished, since documents are keyed per collection and never mix.
//
// The refusal stands for as long as the two scopes resolve to one collection:
// the engine discards a failed activation and retries from scratch on the next
// read, so such a scope pays a tenant-manager round trip on every read until
// its configuration is fixed.
var ErrSharedDatabaseUnsupported = errors.New("systemplane/mongodb: two scopes resolve to the same database and collection; a change stream on a shared collection would deliver every scope's writes to both")

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
