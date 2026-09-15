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
//     returns the handle to the CRUD helpers. LISTEN/NOTIFY is disabled in
//     this mode — Subscribe returns store.ErrNotSupportedInMultiTenant.
//
// This package performs NO runtime schema provisioning. The
// systemplane_entries table, the systemplane_notify_v3() trigger function, and
// the NOTIFY triggers MUST be provisioned externally (e.g. via the consumer's
// migration pipeline) using the DDL published by the root package's
// SchemaSQL() / DefaultSeedSQL(). The store only reads, writes values, and —
// in single-tenant mode — runs LISTEN/NOTIFY. The runtime database role only
// needs DML + LISTEN privileges, never CREATE on the schema.
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
	"github.com/LerianStudio/lib-systemplane/v3/internal/store"
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

	Logger    log.Logger
	Telemetry store.Telemetry
}

// Store implements [store.Store] over Postgres.
type Store struct {
	cfg Config

	// listenerMu / subscribers serve the single-tenant LISTEN/NOTIFY path.
	listenerMu  sync.Mutex
	subscribers map[uint64]func(store.Event)
	nextSubID   uint64
	listenStop  chan struct{}
	listenDone  chan struct{}

	mu     sync.Mutex
	closed bool
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
	s.mu.Unlock()

	s.stopListener()

	return nil
}

func (s *Store) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

// resolveDB returns the database handle for the current call.
//
// Single-tenant mode returns the constructor-supplied *sql.DB unchanged.
// Multi-tenant mode extracts the dbresolver.DB stored in ctx by tenant-manager
// middleware. The schema is assumed to be provisioned externally; the store
// does not create it.
func (s *Store) resolveDB(ctx context.Context) (dbExecutor, error) {
	if !s.cfg.MultiTenantEnabled {
		return s.cfg.DB, nil
	}

	db := tmcore.GetPGContext(ctx, s.cfg.Module)
	if db == nil {
		return nil, store.ErrTenantConnectionMissing
	}

	return db, nil
}

// List returns every entry in the resolved database, ordered by (namespace, key).
func (s *Store) List(ctx context.Context) ([]store.Entry, error) {
	if s == nil || s.isClosed() {
		return nil, store.ErrClosed
	}

	db, err := s.resolveDB(ctx)
	if err != nil {
		return nil, err
	}

	ctx, span, finish := s.startSpan(ctx, "systemplane.postgres.list")
	defer finish()

	query := fmt.Sprintf( // #nosec G201 -- table validated as a Postgres identifier
		`SELECT namespace, key, value, updated_at, updated_by FROM %s ORDER BY namespace, key`,
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

		if err := rows.Scan(&e.Namespace, &e.Key, &e.Value, &e.UpdatedAt, &e.UpdatedBy); err != nil {
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
func (s *Store) Get(ctx context.Context, namespace, key string) (store.Entry, bool, error) {
	if s == nil || s.isClosed() {
		return store.Entry{}, false, store.ErrClosed
	}

	db, err := s.resolveDB(ctx)
	if err != nil {
		return store.Entry{}, false, err
	}

	ctx, span, finish := s.startSpan(ctx, "systemplane.postgres.get",
		attribute.String("namespace", namespace),
		attribute.String("key", key),
	)
	defer finish()

	query := fmt.Sprintf( // #nosec G201 -- table validated as a Postgres identifier
		`SELECT namespace, key, value, updated_at, updated_by FROM %s WHERE namespace = $1 AND key = $2`,
		s.cfg.Table,
	)

	var e store.Entry

	row := db.QueryRowContext(ctx, query, namespace, key)
	if err := row.Scan(&e.Namespace, &e.Key, &e.Value, &e.UpdatedAt, &e.UpdatedBy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return store.Entry{}, false, nil
		}

		tracing.HandleSpanError(span, "get query failed", err)

		return store.Entry{}, false, fmt.Errorf("systemplane/postgres: get: %w", err)
	}

	return e, true, nil
}

// Set persists an entry using INSERT ... ON CONFLICT (namespace, key) DO UPDATE.
func (s *Store) Set(ctx context.Context, e store.Entry) error {
	if s == nil || s.isClosed() {
		return store.ErrClosed
	}

	if e.Namespace == "" {
		return fmt.Errorf("systemplane/postgres: %w: namespace must not be empty", store.ErrValidation)
	}

	if e.Key == "" {
		return fmt.Errorf("systemplane/postgres: %w: key must not be empty", store.ErrValidation)
	}

	if e.UpdatedAt.IsZero() {
		e.UpdatedAt = time.Now().UTC()
	}

	db, err := s.resolveDB(ctx)
	if err != nil {
		return err
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
SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by`,
		s.cfg.Table,
	)

	if _, err := db.ExecContext(ctx, query, e.Namespace, e.Key, e.Value, e.UpdatedAt, e.UpdatedBy); err != nil {
		tracing.HandleSpanError(span, "set upsert failed", err)

		return fmt.Errorf("systemplane/postgres: set: %w", err)
	}

	return nil
}

// Delete removes a single (namespace, key) row. Idempotent.
func (s *Store) Delete(ctx context.Context, namespace, key, actor string) error {
	if s == nil || s.isClosed() {
		return store.ErrClosed
	}

	if namespace == "" {
		return fmt.Errorf("systemplane/postgres: %w: namespace must not be empty", store.ErrValidation)
	}

	if key == "" {
		return fmt.Errorf("systemplane/postgres: %w: key must not be empty", store.ErrValidation)
	}

	db, err := s.resolveDB(ctx)
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
