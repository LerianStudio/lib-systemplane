package postgres

import (
	"context"
	"errors"
	"fmt"

	tmpostgres "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/postgres"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/jackc/pgx/v5"
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
	// That constraint is checked WHEN A CHANGEFEED OPENS, and only then: a
	// feed about to listen on a database another LIVE feed of the same Store
	// already listens on is refused with ErrSharedDatabaseUnsupported. Reads
	// and writes do not evaluate it — they resolve through ResolveDB and never
	// compare databases — so a Store that only does CRUD and never subscribes
	// (no Start, no Subscribe) never learns that two of its scopes share a
	// database, and is not defended against it. Nothing is corrupted there
	// either: values are keyed per database and never cross. What the check
	// defends is the notification stream, which is why it lives where the
	// stream is opened.
	//
	// The database is identified by what the SERVER reports on the open
	// connection — its cluster identity, address, port and current_database()
	// — not by the DSN text, so two spellings that reach one server over one route are refused
	// however differently they are written. Routes the server describes
	// differently are NOT caught, and serverDatabaseKey names them: a
	// Unix-socket route against a TCP one, and two distinct interface
	// addresses of one host. Two processes sharing one database cannot see
	// each other either, and neither can a search_path installed as a role or
	// database default, so one database per scope remains the operator's
	// responsibility beyond this one process.
	ResolveDSN(ctx context.Context, tenantID string) (string, error)
}

// ErrPgMgrUnavailable is returned when a connector resolves a tenant without
// a bound tenant-manager Postgres Manager. Surfaces typically in tests that
// constructed the connector with a nil manager.
var ErrPgMgrUnavailable = errors.New("systemplane/postgres: tenant-manager postgres manager is not configured")

// ErrSharedDatabaseUnsupported is returned when a feed would listen on a
// database another live feed of the same Store already listens on — the
// signature of schema-per-tenant isolation, and of a connector that hands two
// tenants one database. What counts as "the same database", and which routes
// to one database this cannot tell apart, is serverDatabaseKey's definition.
//
// NOTIFY is database-wide and every feed listens on the same channel, so both
// scopes would receive every notification stamped with their OWN scope and the
// engine's revision fence would treat a foreign revision as authoritative.
// Refusing costs the second scope its changefeed; accepting silently corrupts
// both.
//
// It is raised ONLY when a changefeed opens — Start for the zero scope, the
// first Subscribe for a named tenant — because that is the moment a second
// listener would start receiving the first's notifications. A Store that never
// opens a feed never evaluates the rule: its reads and writes resolve straight
// through the connector, so two scopes sharing one database go unnoticed there
// and unpunished, since values are keyed per database and never mix. Such a
// Store gets no warning, and starts failing the day it subscribes.
//
// The refusal is PERMANENT for as long as the two scopes resolve to one
// database: the engine discards a failed activation and retries from scratch
// on the next read, so such a scope pays a tenant-manager round trip on every
// read until its configuration is fixed.
var ErrSharedDatabaseUnsupported = errors.New("systemplane/postgres: two scopes resolve to the same database; systemplane needs one database per scope because NOTIFY is database-wide")

// serverDatabaseKey identifies the physical database an OPEN connection
// actually reached, so two scopes pointing at the same one can be told apart
// from two scopes pointing at different ones.
//
// The identity comes from the server, not from the connection string: the
// server's own identity plus (inet_server_addr(), inet_server_port(),
// current_database()), all evaluated inside the server. The address is only
// how the SERVER sees itself, and two distinct servers can report the same one
// — containers on separate hosts each on 172.17.0.2:5432, overlapping private
// ranges, every tenant cluster naming its database alike — so the address
// alone would refuse a valid tenant forever. The server identity is
// pg_postmaster_start_time(), as epoch seconds at full microsecond precision.
//
// Every component is readable by every role, so the key does not vary with
// who connects: the guard compares keys across connections that may
// authenticate as different roles, and a key that depended on a privilege
// would call one database two and admit both feeds. That rules out the
// system_identifier from pg_control_system(), whose EXECUTE can be revoked.
// Epoch seconds rather than the timestamp's text form, because that text
// follows the session's TimeZone and DateStyle, which a role can set.
//
// Two distinct servers that also share address, port and database name would
// have to start in the same microsecond to collide; that is treated as
// impossible. Cloned data directories (pg_basebackup plus promote, a snapshot
// restore, a container image shipping a pre-initialized PGDATA) are told apart
// the same way, by their start times. A restart changes the key: that only
// admits a pair it should have refused, the failure this key already accepts,
// never the reverse.
//
// DSN text is a description of how to get there and two descriptions that
// land on one NOTIFY namespace need not match — a host name and the literal
// address it resolves to, a CNAME and its target, a pgbouncer address and the
// backend behind it — and a key built from the text would call those different
// databases and admit the second feed.
//
// What this key CANNOT tell apart, by construction, is two routes the server
// itself describes differently:
//
//   - A Unix-domain socket against a TCP port on the same host. The socket
//     route reports no address at all (see below) and is therefore keyed in a
//     different format, so the pair is admitted as two databases.
//   - Two distinct interface addresses of one host — a literal 127.0.0.1
//     against a name that resolves to ::1, or two NICs.
//
// Both pairs reach one NOTIFY namespace and neither is refused. One database
// per scope stays the operator's responsibility; this key catches the spelling
// mistakes, not every route.
//
// The database — not the schema — is the discriminator, because NOTIFY is
// database-wide. A DSN that pins a schema is NOT refused on that basis: a
// tenant with its own database may legitimately name a schema, and lib-commons
// writes "options=-csearch_path=<schema>" for any tenant whose config declares
// one, whatever its isolation mode. Two such connections only collide when
// they reach the same database, which is exactly what this key compares.
//
// A Unix-domain socket makes the server report no address at all. The socket
// directory from the DSN plus the database name is then the best identity
// available, and it is a sound one: two DSNs naming one socket directory do
// reach one postmaster.
func serverDatabaseKey(ctx context.Context, conn *pgx.Conn, dsn string) (string, error) {
	var (
		database  string
		addr      string
		port      int32
		startedAt string
	)

	const q = `SELECT current_database(), COALESCE(host(inet_server_addr()), ''), COALESCE(inet_server_port(), 0),
		extract(epoch FROM pg_postmaster_start_time())::text`

	if err := conn.QueryRow(ctx, q).Scan(&database, &addr, &port, &startedAt); err != nil {
		return "", fmt.Errorf("server identity query: %w", err)
	}

	return formatServerDatabaseKey(database, addr, port, startedAt, dsn)
}

// formatServerDatabaseKey turns what the server answered into the key
// serverDatabaseKey compares, and holds every decision that comparison rests
// on — see that function's comment for what the key does and does not tell
// apart. Split out so those decisions are testable without a server: the
// postmaster start time names the server process and prefixes every key; an
// address the server reported keys as TCP; no address means a Unix socket,
// and the socket directory the DSN names stands in for the address it cannot
// report.
func formatServerDatabaseKey(database, addr string, port int32, startedAt, dsn string) (string, error) {
	server := "started:" + startedAt

	if addr != "" {
		return fmt.Sprintf("%s/tcp:%s:%d/%s", server, addr, port, database), nil
	}

	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return "", fmt.Errorf("parse DSN for socket identity: %w", err)
	}

	return fmt.Sprintf("%s/unix:%s/%s", server, cfg.Host, database), nil
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
