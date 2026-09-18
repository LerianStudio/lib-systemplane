// Package postgres implements the internal store.Store interface over
// PostgreSQL.
//
// Two operating modes share this file:
//
//   - Single-tenant. The constructor receives a *sql.DB plus a ListenDSN.
//     Reads/writes go through that handle. A dedicated pgx connection runs
//     LISTEN/NOTIFY and feeds the subscriber registry.
//
//   - Multi-tenant. The constructor receives no db; instead the caller wires
//     lib-commons tenant-manager middleware so each request context carries
//     the per-tenant database. resolveDB(ctx) extracts that database and
//     returns the handle to the CRUD helpers. The zero scope has no durable
//     DSN to LISTEN on there, so Subscribe returns
//     store.ErrNotSupportedInMultiTenant for it; a NAMED tenant scope resolves
//     its own database and LISTEN DSN through the tenant connector and gets its
//     own changefeed, in either mode.
//
// This package performs NO runtime schema provisioning. The
// systemplane_entries table, its revision column, the revision sequence, the
// systemplane_bump_revision_v4() and systemplane_notify_v4() trigger
// functions, and the three triggers that bind them (one BEFORE INSERT OR
// UPDATE bump, two NOTIFY) MUST be provisioned externally (e.g. via the
// consumer's migration pipeline) using the DDL published by the root
// package's SchemaSQL() / DefaultSeedSQL(). The store only reads, writes
// values, and — in single-tenant mode — runs LISTEN/NOTIFY. The runtime
// database role only needs DML + LISTEN privileges, never CREATE on the
// schema: no statement this package issues names the revision sequence, and
// the trigger that advances it is SECURITY DEFINER, so the runtime role needs
// no grant on it either.
//
// # Connection budget
//
// Every ACTIVE tenant costs one extra Postgres backend per replica: its
// changefeed holds a dedicated LISTEN connection that lives outside the
// tenant-manager pool and is not shared with reads or writes. The budget is
// therefore active tenants x replicas, on top of whatever the pools hold, and
// max_connections on each tenant database must be sized against it. A tenant
// releases its backend when its last subscriber leaves.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/tracing"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"github.com/bxcodec/dbresolver/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Compile-time interface satisfaction check.
var _ store.Store = (*Store)(nil)

// safeIdentifierRe validates a BARE SQL identifier — one interpolated UNQUOTED
// into a statement (the table name, e.g. "... FROM <table>"). SQL statements
// cannot parameterize identifiers, so a bare-interpolated name must pass this
// strict check first; hyphens/dots are illegal because they would break the
// unquoted SQL.
var safeIdentifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// safeChannelRe validates the LISTEN/NOTIFY channel name. Unlike the table, the
// channel is always DOUBLE-QUOTED at use (LISTEN "<channel>" via quoteIdentifier),
// so it may safely contain hyphens — the common case for an ApplicationName-prefixed
// channel such as "my-service_systemplane_changes". It still rejects quotes,
// whitespace and other breakout characters; quoteIdentifier additionally escapes any
// embedded double quote, so the quoted channel is injection-safe regardless.
// Length is enforced separately in normalizeConfig: Postgres truncates identifiers
// to 63 bytes (NAMEDATALEN-1), so over-length channels are rejected outright.
var safeChannelRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_-]*$`)

const (
	tracerName     = "systemplane.postgres"
	defaultChannel = "systemplane_changes"
	defaultTable   = "systemplane_entries"
	defaultModule  = "systemplane"
)

// dbExecutor is the minimal interface the CRUD helpers need. Both *sql.DB and
// the dbresolver.DB returned by tmcore.GetPGContext satisfy it.
type dbExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Compile-time assertions that the two concrete sources both satisfy dbExecutor.
var (
	_ dbExecutor = (*sql.DB)(nil)
	_ dbExecutor = (dbresolver.DB)(nil)
)

// Config holds the parameters needed to construct a Postgres-backed Store.
type Config struct {
	// DB is the database/sql handle for single-tenant mode. MUST be non-nil
	// unless MultiTenantEnabled is true.
	DB *sql.DB

	// ListenDSN is the connection string used by pgx.Connect to establish a
	// dedicated LISTEN connection. Required in single-tenant mode.
	ListenDSN string

	// Channel is the Postgres LISTEN/NOTIFY channel name.
	// Default: "systemplane_changes". Hyphens are allowed (validated by
	// safeChannelRe) — the channel is double-quoted at LISTEN time.
	//
	// COUPLING: this is only the LISTEN side. The matching NOTIFY side lives in
	// the trigger DDL the consumer provisions (SchemaSQL() binds the reference
	// trigger to the default "systemplane_changes" via TG_ARGV[0]). A consumer
	// that sets a NON-default Channel here MUST bind the SAME name in its trigger
	// DDL, otherwise the store LISTENs on one channel while the trigger NOTIFYs
	// on another and no events are delivered.
	Channel string

	// ChannelExplicit suppresses the default-channel collision warning when
	// the caller deliberately selected the channel name.
	ChannelExplicit bool

	// Table is the Postgres table name. Default: "systemplane_entries".
	Table string

	// MultiTenantEnabled selects the tmcore-driven dispatch path. When true,
	// DB and ListenDSN may be empty; every method resolves the tenant
	// database from ctx via tmcore.GetPGContext(ctx, Module).
	MultiTenantEnabled bool

	// Module is the tenant-manager module name used as the context key for
	// dispatch. Default: "systemplane".
	Module string

	Connector Connector // nil in single-tenant mode

	Logger    log.Logger
	Telemetry store.Telemetry
}

// Store implements [store.Store] over Postgres.
type Store struct {
	cfg Config

	// feedsMu guards feeds, the LISTEN/NOTIFY changefeeds keyed by
	// scope.Tenant ("" is the zero, single-tenant scope), and every feed's
	// reference count: a feed's lifetime decision and its map slot change
	// together, in one lock hold.
	feedsMu sync.Mutex
	feeds   map[string]*feed

	// closing is set by Close under feedsMu, and is NOT the per-feed
	// feed.closing (which only suppresses OpDisconnect on a clean teardown).
	// A feed is created outside the map lock, so Close cannot stop an
	// in-flight creator by walking the map alone: it raises this flag instead,
	// and the creator rechecks it before publishing anything.
	closing bool

	// startMu serializes Start. The zero-scope feed is shared, so the check
	// for an existing reader and the launch of a new one must be one decision:
	// without it two concurrent Starts each open a LISTEN backend on the same
	// feed and every notification is delivered twice.
	startMu sync.Mutex

	mu     sync.Mutex
	closed bool

	// closedCh is closed exactly once, by Close, in the same s.mu hold that
	// sets closed — the early return above it is what makes that single. It is
	// the store-wide shutdown signal every subscription's ctx observer selects
	// on, so a subscriber whose ctx outlives the store does not leave a
	// goroutine parked forever. Created by New; a Store is not usable without
	// it.
	closedCh chan struct{}
}

// Start opens the single-tenant LISTEN connection. The schema is NOT created
// here — it must be provisioned externally (see the package doc). In
// multi-tenant mode Start is a no-op: there is no shared changefeed and reads
// go through the per-request tenant database.
func (s *Store) Start(ctx context.Context) error {
	if s == nil || s.isClosed() {
		return store.ErrClosed
	}

	if s.cfg.MultiTenantEnabled {
		return nil
	}

	return s.startListener(ctx)
}

// Close releases backend resources. Idempotent.
//
// It signals every changefeed and then waits for their readers under ONE
// shared closeTimeout, so shutdown costs a single bound no matter how many
// tenants the store carries. Called from INSIDE a subscriber callback it costs
// exactly that bound: the reader it is waiting for is the goroutine running
// the caller, so the wait can only end at the deadline. Close still returns,
// and the feeds are still torn down.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()

		return nil
	}

	s.closed = true

	close(s.closedCh)
	s.mu.Unlock()

	s.stopFeeds()

	return nil
}

func (s *Store) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

// isClosing reports the shutdown flag Close raises under feedsMu — the one a
// feed creator rechecks before it publishes anything. It is deliberately the
// same flag, not the s.mu one: a creator that dialed on one flag and published
// on the other could still hand a live connection to a shut-down store.
func (s *Store) isClosing() bool {
	s.feedsMu.Lock()
	defer s.feedsMu.Unlock()

	return s.closing
}

// resolveDB returns the database handle for the current call.
//
// The zero scope keeps today's behavior: single-tenant mode returns the
// constructor-supplied *sql.DB unchanged, multi-tenant mode extracts the
// dbresolver.DB stored in ctx by tenant-manager middleware. A named tenant
// resolves through the connector regardless of MultiTenantEnabled and
// regardless of whatever tenant ctx carries (FC-2: an explicitly named scope
// and a request-scoped ctx tenant must never silently disagree), and is
// refused with store.ErrTenantConnectorMissing when no connector is
// configured. The schema is assumed to be provisioned externally; the store
// does not create it.
//
// Whatever route produced the handle, a resolver that carries read replicas is
// narrowed to its primaries here — see pinPrimary.
func (s *Store) resolveDB(ctx context.Context, scope store.Scope) (dbExecutor, error) {
	if scope.Tenant != "" {
		if s.cfg.Connector == nil {
			return nil, store.ErrTenantConnectorMissing
		}

		db, err := s.cfg.Connector.ResolveDB(ctx, scope.Tenant)
		if err != nil {
			return nil, fmt.Errorf("systemplane/postgres: resolve tenant %s: %w", scope.Tenant, err)
		}

		// A nil handle with a nil error is a connector bug; refuse it here
		// rather than hand back something that panics on the first query.
		if db == nil {
			return nil, fmt.Errorf("systemplane/postgres: resolve tenant %s: %w", scope.Tenant, store.ErrTenantConnectorMissing)
		}

		return pinPrimary(db), nil
	}

	if !s.cfg.MultiTenantEnabled {
		return s.cfg.DB, nil
	}

	db := tmcore.GetPGContext(ctx, s.cfg.Module)
	if db == nil {
		return nil, store.ErrTenantConnectionMissing
	}

	return pinPrimary(db), nil
}

// pinPrimary keeps the whole systemplane path on the primary, reads included.
//
// dbresolver sends a statement to a replica unless it looks like a write, and
// its default checker recognizes a write only by the string "RETURNING"
// (dbresolver/v2 query.go). Set ends in RETURNING revision and Delete goes
// through ExecContext, so both reach the primary — while the plain SELECTs in
// Get and List would be served by a standby, and lib-commons registers a
// replica for every tenant that declares one. A caller could then read back a
// revision older than the one Set just returned, and older than the NOTIFY the
// changefeed is reconciling against, since the feed LISTENs on the primary
// DSN. Read-your-write and revision coherence are worth more than offloading a
// five-column configuration table, so a resolver carrying replicas is pinned
// to ONE primary. A resolver with no replicas is handed back untouched: it
// already resolves everything to a primary, and keeping the wrapper costs
// nothing.
//
// The pin is the FIRST primary, always, and that is the whole claim: one
// deterministic node, every standby excluded, no failover. lib-commons builds
// every tenant resolver from exactly one primary and one replica
// (commons/postgres createResolverFn), so the common case has only one primary
// to pick. A connector of a consumer's own making may report several; picking
// deterministically among them is what keeps a value Set returned readable by
// the next Get, and it costs no allocation on a path every query crosses.
// Handing those back inside a fresh resolver instead would buy nothing:
// dbresolver retries only on a net.Error, and a dead pool reports
// "sql: database is closed", which is not one.
func pinPrimary(db dbresolver.DB) dbExecutor {
	if len(db.ReplicaDBs()) == 0 {
		return db
	}

	// A resolver with replicas but no primary is a connector bug; there is
	// nothing better to fall back to than the resolver itself.
	primaries := db.PrimaryDBs()
	if len(primaries) == 0 {
		return db
	}

	return primaries[0]
}

// List returns every entry in the resolved database, ordered by (namespace, key).
func (s *Store) List(ctx context.Context, scope store.Scope) ([]store.Entry, error) {
	if s == nil || s.isClosed() {
		return nil, store.ErrClosed
	}

	db, err := s.resolveDB(ctx, scope)
	if err != nil {
		return nil, err
	}

	ctx, span, finish := s.startSpan(ctx, "systemplane.postgres.list")
	defer finish()

	query := fmt.Sprintf( // #nosec G201 -- table validated as a Postgres identifier
		`SELECT namespace, key, value, revision, updated_at, updated_by FROM %s ORDER BY namespace, key`,
		s.cfg.Table,
	)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		tracing.HandleSpanError(span, "list query failed", err)

		return nil, fmt.Errorf("systemplane/postgres: list: %w", err)
	}
	defer rows.Close()

	entries := []store.Entry{}

	for rows.Next() {
		var e store.Entry

		if err := rows.Scan(&e.Namespace, &e.Key, &e.Value, &e.Revision, &e.UpdatedAt, &e.UpdatedBy); err != nil {
			tracing.HandleSpanError(span, "list scan failed", err)

			return nil, fmt.Errorf("systemplane/postgres: list scan: %w", err)
		}

		entries = append(entries, e)
	}

	if err := rows.Err(); err != nil {
		tracing.HandleSpanError(span, "list rows iteration failed", err)

		return nil, fmt.Errorf("systemplane/postgres: list rows: %w", err)
	}

	return entries, nil
}

// Get returns a single entry by (namespace, key).
func (s *Store) Get(ctx context.Context, scope store.Scope, namespace, key string) (store.Entry, bool, error) {
	if s == nil || s.isClosed() {
		return store.Entry{}, false, store.ErrClosed
	}

	db, err := s.resolveDB(ctx, scope)
	if err != nil {
		return store.Entry{}, false, err
	}

	ctx, span, finish := s.startSpan(ctx, "systemplane.postgres.get",
		attribute.String("namespace", namespace),
		attribute.String("key", key),
	)
	defer finish()

	query := fmt.Sprintf( // #nosec G201 -- table validated as a Postgres identifier
		`SELECT namespace, key, value, revision, updated_at, updated_by FROM %s WHERE namespace = $1 AND key = $2`,
		s.cfg.Table,
	)

	var e store.Entry

	row := db.QueryRowContext(ctx, query, namespace, key)
	if err := row.Scan(&e.Namespace, &e.Key, &e.Value, &e.Revision, &e.UpdatedAt, &e.UpdatedBy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return store.Entry{}, false, nil
		}

		tracing.HandleSpanError(span, "get query failed", err)

		return store.Entry{}, false, fmt.Errorf("systemplane/postgres: get: %w", err)
	}

	return e, true, nil
}

// Set persists an entry using INSERT ... ON CONFLICT (namespace, key) DO UPDATE
// and returns the revision now stored. The number comes from the table-level
// systemplane_revision_seq sequence, never from the row, and it is drawn
// exclusively by systemplane_bump_revision_trigger — this statement names
// neither the sequence nor the revision column, which is what keeps the
// runtime role on plain DML: an insert always draws a new revision, a write
// that changes the value draws one too, and a write of an identical value
// leaves the revision the row already carried. A key deleted and recreated
// therefore always exceeds every revision it previously had, and revisions
// may skip numbers.
func (s *Store) Set(ctx context.Context, scope store.Scope, e store.Entry) (int64, error) {
	if s == nil || s.isClosed() {
		return 0, store.ErrClosed
	}

	if e.Namespace == "" {
		return 0, fmt.Errorf("systemplane/postgres: %w: namespace must not be empty", store.ErrValidation)
	}

	if e.Key == "" {
		return 0, fmt.Errorf("systemplane/postgres: %w: key must not be empty", store.ErrValidation)
	}

	if e.UpdatedAt.IsZero() {
		e.UpdatedAt = time.Now().UTC()
	}

	db, err := s.resolveDB(ctx, scope)
	if err != nil {
		return 0, err
	}

	ctx, span, finish := s.startSpan(ctx, "systemplane.postgres.set",
		attribute.String("namespace", e.Namespace),
		attribute.String("key", e.Key),
	)
	defer finish()

	query := fmt.Sprintf( // #nosec G201 -- table validated as a Postgres identifier
		`INSERT INTO %s (namespace, key, value, updated_at, updated_by)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (namespace, key) DO UPDATE
SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by
RETURNING revision`,
		s.cfg.Table,
	)

	var revision int64

	// revision is deliberately absent from the DO UPDATE set-list: the trigger
	// owns that column, and on an identical value it puts the stored revision
	// back, so RETURNING reports the revision the row already had. sql.ErrNoRows is not special-cased — an upsert with RETURNING
	// always yields a row, so its appearance is a real error and must propagate.
	if err := db.QueryRowContext(ctx, query, e.Namespace, e.Key, e.Value, e.UpdatedAt, e.UpdatedBy).Scan(&revision); err != nil {
		tracing.HandleSpanError(span, "set upsert failed", err)

		return 0, fmt.Errorf("systemplane/postgres: set: %w", err)
	}

	return revision, nil
}

// Delete removes a single (namespace, key) row. Idempotent.
func (s *Store) Delete(ctx context.Context, scope store.Scope, namespace, key, actor string) error {
	if s == nil || s.isClosed() {
		return store.ErrClosed
	}

	if namespace == "" {
		return fmt.Errorf("systemplane/postgres: %w: namespace must not be empty", store.ErrValidation)
	}

	if key == "" {
		return fmt.Errorf("systemplane/postgres: %w: key must not be empty", store.ErrValidation)
	}

	db, err := s.resolveDB(ctx, scope)
	if err != nil {
		return err
	}

	// actor is intentionally NOT a span attribute: it is unbounded caller
	// identity and would create a high-cardinality / potentially PII tag.
	// Audit trails capture it via the updated_by column on writes.
	_ = actor

	ctx, span, finish := s.startSpan(ctx, "systemplane.postgres.delete",
		attribute.String("namespace", namespace),
		attribute.String("key", key),
	)
	defer finish()

	query := fmt.Sprintf( // #nosec G201 -- table validated as a Postgres identifier
		`DELETE FROM %s WHERE namespace = $1 AND key = $2`,
		s.cfg.Table,
	)

	if _, err := db.ExecContext(ctx, query, namespace, key); err != nil {
		tracing.HandleSpanError(span, "delete failed", err)

		return fmt.Errorf("systemplane/postgres: delete: %w", err)
	}

	return nil
}

// startSpan creates a child span if telemetry is configured.
func (s *Store) startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span, func()) {
	noop := func() {}

	if s.cfg.Telemetry == nil {
		return ctx, trace.SpanFromContext(ctx), noop
	}

	tracer, err := s.cfg.Telemetry.Tracer(tracerName)
	if err != nil || tracer == nil {
		return ctx, trace.SpanFromContext(ctx), noop
	}

	ctx, span := tracer.Start(ctx, name, trace.WithAttributes(attrs...))

	return ctx, span, func() { span.End() }
}

func (s *Store) logInfo(ctx context.Context, msg string, fields ...log.Field) {
	if s.cfg.Logger != nil {
		s.cfg.Logger.Log(ctx, log.LevelInfo, msg, fields)
	}
}

func (s *Store) logWarn(ctx context.Context, msg string, fields ...log.Field) {
	if s.cfg.Logger != nil {
		s.cfg.Logger.Log(ctx, log.LevelWarn, msg, fields)
	}
}

func (s *Store) logDebug(ctx context.Context, msg string, fields ...log.Field) {
	if s.cfg.Logger != nil {
		s.cfg.Logger.Log(ctx, log.LevelDebug, msg, fields)
	}
}
