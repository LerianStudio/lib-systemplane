package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
	// connection. The DSN MUST name a database no other tenant shares:
	// NOTIFY is database-scoped and every feed listens on the same channel
	// name, so two tenants in one database (schema-per-tenant through a
	// search_path) would each receive the other's notifications stamped with
	// their own scope, and the revision fence would act on them. Values never
	// cross; notifications and revisions would.
	//
	// That constraint is refused wherever the DSN reveals it: a DSN that pins
	// a schema is rejected with ErrSchemaIsolationUnsupported before anything
	// is dialed, so the tenant loses its changefeed instead of the deployment
	// losing revision integrity. A role- or database-level default search_path
	// cannot be detected — it is applied by the server, after connecting, and
	// appears nowhere in the DSN — and remains the operator's responsibility.
	ResolveDSN(ctx context.Context, tenantID string) (string, error)
}

// ErrPgMgrUnavailable is returned when a connector resolves a tenant without
// a bound tenant-manager Postgres Manager. Surfaces typically in tests that
// constructed the connector with a nil manager.
var ErrPgMgrUnavailable = errors.New("systemplane/postgres: tenant-manager postgres manager is not configured")

// ErrSchemaIsolationUnsupported is returned when a DSN pins a schema, the
// signature of schema-per-tenant isolation. It covers a tenant's DSN and the
// single-tenant ListenDSN alike. See refuseSchemaIsolatedDSN.
var ErrSchemaIsolationUnsupported = errors.New("systemplane/postgres: DSN sets search_path (schema-per-tenant isolation); systemplane needs one database per tenant because NOTIFY is database-wide")

// refuseSchemaIsolatedDSN fails closed on a DSN that pins a schema, either as
// a bare search_path runtime parameter or inside libpq's options string (the
// shape lib-commons builds: "options=-csearch_path=<schema>"). Both survive
// pgconn.ParseConfig as runtime parameters, which is what makes the intent
// detectable before any connection is opened. It guards every DSN this package
// dials: a tenant's, resolved through the connector, and the single-tenant
// ListenDSN alike.
//
// The comparison is case-INSENSITIVE on both the parameter name and the
// options string, because pgconn keeps the connection string verbatim while
// Postgres resolves GUC names case-insensitively: "SEARCH_PATH=tenant_a" and
// "options=-cSEARCH_PATH=tenant_a" pin exactly the same schema as their
// lowercase spellings.
//
// It sees only what the DSN carries. A search_path installed as a role or
// database default is applied by the server after the connection is made and
// is invisible here; keeping one database per install remains the operator's
// responsibility.
//
// A pinned schema means several tenants share ONE database. NOTIFY is
// database-wide, so each tenant's feed would receive every other tenant's
// events stamped with its OWN scope, and the engine's revision fence would
// treat a foreign revision as authoritative. Refusing costs that tenant its
// changefeed; accepting silently corrupts every tenant in the database.
func refuseSchemaIsolatedDSN(dsn string) error {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("systemplane/postgres: parse DSN: %w", err)
	}

	for name, value := range cfg.RuntimeParams {
		switch {
		case strings.EqualFold(name, "search_path") && value != "":
			return ErrSchemaIsolationUnsupported
		case strings.EqualFold(name, "options") && strings.Contains(strings.ToLower(value), "search_path"):
			return ErrSchemaIsolationUnsupported
		}
	}

	return nil
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
