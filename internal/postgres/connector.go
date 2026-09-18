package postgres

import (
	"context"
	"errors"
	"fmt"

	tmpostgres "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/postgres"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/jackc/pgx/v5/pgconn"
)

// Connector resolves a tenant's Postgres handle and LISTEN DSN.
type Connector interface {
	// ResolveDB returns the tenant's database handle. It is a ROUND TRIP, not
	// a map lookup: the tenant manager pings the tenant database on every
	// call, cache hit included. Callers therefore reconcile a scope with ONE
	// List per scope and never a Get per key — a hundred keys resolved one at
	// a time is a hundred round trips before the first value is read.
	ResolveDB(ctx context.Context, tenantID string) (dbresolver.DB, error)

	// ResolveDSN returns the connection string for the tenant's LISTEN
	// connection. The DSN MUST name a database no other scope resolves to:
	// NOTIFY is database-scoped and every feed listens on the same channel
	// name, so two tenants in one database (schema-per-tenant through a
	// search_path) would each receive the other's notifications stamped with
	// their own scope, and the revision fence would act on them. Values never
	// cross; notifications and revisions would.
	//
	// That constraint is enforced where it is decidable: a feed whose DSN
	// names a database another live feed is already listening on is refused
	// with ErrSharedDatabaseUnsupported. Two processes sharing one database
	// cannot see each other, and neither can a search_path installed as a
	// role or database default, so one database per scope remains the
	// operator's responsibility beyond this one process.
	ResolveDSN(ctx context.Context, tenantID string) (string, error)
}

// ErrPgMgrUnavailable is returned when a connector resolves a tenant without
// a bound tenant-manager Postgres Manager. Surfaces typically in tests that
// constructed the connector with a nil manager.
var ErrPgMgrUnavailable = errors.New("systemplane/postgres: tenant-manager postgres manager is not configured")

// ErrSharedDatabaseUnsupported is returned when a feed would listen on a
// database another live feed of the same Store already listens on — the
// signature of schema-per-tenant isolation, and of a connector that hands two
// tenants the same connection string. See dsnDatabaseKey.
//
// NOTIFY is database-wide and every feed listens on the same channel, so both
// scopes would receive every notification stamped with their OWN scope and the
// engine's revision fence would treat a foreign revision as authoritative.
// Refusing costs the second scope its changefeed; accepting silently corrupts
// both.
//
// The refusal is PERMANENT for as long as the two scopes resolve to one
// database: the engine discards a failed activation and retries from scratch
// on the next read, so such a scope pays a tenant-manager round trip on every
// read until its configuration is fixed.
var ErrSharedDatabaseUnsupported = errors.New("systemplane/postgres: two scopes resolve to the same database; systemplane needs one database per scope because NOTIFY is database-wide")

// dsnDatabaseKey identifies the physical database a DSN resolves to, so two
// scopes pointing at the same one can be told apart from two scopes pointing
// at different ones.
//
// The database — not the schema — is the discriminator, because NOTIFY is
// database-wide. A DSN that pins a schema is NOT refused on that basis: a
// tenant with its own database may legitimately name a schema, and lib-commons
// writes "options=-csearch_path=<schema>" for any tenant whose config declares
// one, whatever its isolation mode. Two such DSNs only collide when they name
// the same database, which is exactly what this key compares.
//
// pgconn.ParseConfig merges the process environment (PGHOST, PGPORT,
// PGDATABASE) before the connection string's own settings. That is correct
// here rather than a hazard: the same environment applies to every DSN, so two
// DSNs that both inherit a value really do resolve to the same server.
func dsnDatabaseKey(dsn string) (string, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return "", fmt.Errorf("systemplane/postgres: parse DSN: %w", err)
	}

	return fmt.Sprintf("%s:%d/%s", cfg.Host, cfg.Port, cfg.Database), nil
}

// NewTenantManagerConnector wraps a lib-commons tenant-manager Postgres Manager.
func NewTenantManagerConnector(mgr *tmpostgres.Manager) Connector {
	return &pgMgrConnector{mgr: mgr}
}

// pgMgrConnector is the production adapter wrapping a *tmpostgres.Manager.
// ResolveDB serves warm-load and NOTIFY refresh reads: the schema is
// provisioned externally, so no DDL is ever issued through that handle.
// ResolveDSN yields the connection string pgx.Connect uses to open the
// tenant's dedicated LISTEN connection. Tests may substitute a fake Connector
// instead of spinning up the tenant-config gRPC client.
type pgMgrConnector struct {
	mgr *tmpostgres.Manager
}

func (c *pgMgrConnector) ResolveDB(ctx context.Context, tenantID string) (dbresolver.DB, error) {
	if c == nil || c.mgr == nil {
		return nil, ErrPgMgrUnavailable
	}

	conn, err := c.mgr.GetConnection(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("systemplane/postgres: get tenant connection %s: %w", tenantID, err)
	}

	db, err := conn.GetDB()
	if err != nil {
		return nil, fmt.Errorf("systemplane/postgres: get tenant DB %s: %w", tenantID, err)
	}

	return db, nil
}

func (c *pgMgrConnector) ResolveDSN(ctx context.Context, tenantID string) (string, error) {
	if c == nil || c.mgr == nil {
		return "", ErrPgMgrUnavailable
	}

	conn, err := c.mgr.GetConnection(ctx, tenantID)
	if err != nil {
		return "", fmt.Errorf("systemplane/postgres: get tenant connection %s: %w", tenantID, err)
	}

	if conn.ConnectionStringPrimary == "" {
		return "", fmt.Errorf("systemplane/postgres: tenant %s has empty primary DSN", tenantID)
	}

	return conn.ConnectionStringPrimary, nil
}
