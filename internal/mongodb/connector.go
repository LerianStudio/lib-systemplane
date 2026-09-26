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
	// ResolveDatabase returns the tenant's database handle. It is not a free
	// map lookup: a miss connects, and a hit health-checks the cached client
	// at most once per the tenant manager's health-check interval. Callers
	// therefore reconcile a scope with ONE List per scope and never a Get per
	// key.
	//
	// The database, not the collection: the collection name is the store's
	// own constant (collectionName) and a connector must never need
	// to know it.
	//
	// The database MUST be one no other scope of the same Store resolves to.
	// The Postgres half refuses a feed whose DSN reaches a database another
	// LIVE feed of that Store already listens on — identity taken from the
	// open connection (server start time, address, port and database; see
	// serverDatabaseKey in internal/postgres/connector.go), so a DSN that
	// merely pins a search_path is admitted; it is the shared DATABASE that is
	// refused, with that package's ErrSharedDatabaseUnsupported. MongoDB
	// applies the collection-level analogue: a feed whose (server, database,
	// collection) triple a live feed already watches is refused with this
	// package's ErrSharedDatabaseUnsupported. The SERVER, not the client
	// handle: lib-commons' tenant manager opens one *mongo.Client per TENANT,
	// so two handles are what a shared database looks like from here and the
	// identity comes from what hello reports instead — see serverKey in
	// internal/mongodb/mongodb_changestream.go for what that can and cannot
	// tell apart (two mongos routers fronting one sharded cluster are NOT
	// caught, and neither is a server restarted between two claims).
	//
	// Two tenants on two databases of ONE server stay admitted, and so do two
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
// What counts as "the same server", and which shapes this cannot tell apart,
// is serverKey's definition in mongodb_changestream.go. A server that cannot
// be reached, or that answers without identifying itself, is admitted rather
// than refused on a guess — it fails on the stream open a moment later anyway.
//
// The refusal stands for as long as the two scopes resolve to one collection:
// the engine discards a failed activation and retries from scratch on the next
// read, so such a scope pays a hello round trip to the server on every read
// until its configuration is fixed.
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
