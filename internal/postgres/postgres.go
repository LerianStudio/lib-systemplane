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
//     the per-tenant database. resolveDB(ctx) extracts that database, lazily
//     bootstraps the schema on first use per database, and returns the handle
//     to the CRUD helpers. LISTEN/NOTIFY is disabled in this mode — Subscribe
//     returns store.ErrNotSupportedInMultiTenant.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/tracing"
	"github.com/LerianStudio/lib-systemplane/internal/store"
	"github.com/bxcodec/dbresolver/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Compile-time interface satisfaction check.
var _ store.Store = (*Store)(nil)

// safeIdentifierRe validates that a SQL identifier contains only safe characters.
// DDL paths cannot use parameterized queries for identifiers, so any name
// interpolated into a statement must pass this check first.
var safeIdentifierRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

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
	// Default: "systemplane_changes".
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
	Telemetry *tracing.Telemetry
}

// Store implements [store.Store] over Postgres.
type Store struct {
	cfg Config

	// schemaOnce tracks lazy schema bootstrap per database handle in
	// multi-tenant mode. Single-tenant mode populates the sole entry at
	// Start() time.
	schemaOnce sync.Map // map[dbExecutor]*sync.Once
	schemaErr  sync.Map // map[dbExecutor]error

	// listenerMu / subscribers serve the single-tenant LISTEN/NOTIFY path.
	listenerMu  sync.Mutex
	subscribers map[uint64]func(store.Event)
	nextSubID   uint64
	listenStop  chan struct{}
	listenDone  chan struct{}

	mu     sync.Mutex
	closed bool
}

// New creates a Postgres-backed Store. Validates the configuration but does
// not touch the database — schema bootstrap happens lazily on first access
// (multi-tenant) or eagerly at Start() (single-tenant).
func New(cfg Config) (*Store, error) {
	if err := normalizeConfig(&cfg); err != nil {
		return nil, err
	}

	return &Store{cfg: cfg, subscribers: make(map[uint64]func(store.Event))}, nil
}

func normalizeConfig(cfg *Config) error {
	if cfg.Channel == "" {
		cfg.Channel = defaultChannel
	}

	if cfg.Table == "" {
		cfg.Table = defaultTable
	}

	if cfg.Module == "" {
		cfg.Module = defaultModule
	}

	if !safeIdentifierRe.MatchString(cfg.Channel) {
		return fmt.Errorf("systemplane/postgres: unsafe channel name %q", cfg.Channel)
	}

	if !safeIdentifierRe.MatchString(cfg.Table) {
		return fmt.Errorf("systemplane/postgres: unsafe table name %q", cfg.Table)
	}

	if cfg.MultiTenantEnabled {
		// In multi-tenant mode DB/ListenDSN are resolved per-request from
		// ctx; the constructor handles may be nil.
		return nil
	}

	if cfg.DB == nil {
		return store.ErrNilBackend
	}

	if cfg.ListenDSN == "" {
		return errors.New("systemplane/postgres: ListenDSN is required in single-tenant mode")
	}

	return nil
}

// Start performs single-tenant schema bootstrap and opens the LISTEN
// connection. In multi-tenant mode it is a no-op — schema bootstrap is lazy
// per tenant database and there is no shared changefeed.
func (s *Store) Start(ctx context.Context) error {
	if s == nil || s.isClosed() {
		return store.ErrClosed
	}

	if s.cfg.MultiTenantEnabled {
		return nil
	}

	if err := s.ensureSchema(ctx, s.cfg.DB); err != nil {
		return err
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
// Multi-tenant mode extracts the dbresolver.DB stored in ctx by
// tenant-manager middleware and lazily bootstraps the schema on first use per
// resolved database.
func (s *Store) resolveDB(ctx context.Context) (dbExecutor, error) {
	if !s.cfg.MultiTenantEnabled {
		return s.cfg.DB, nil
	}

	db := tmcore.GetPGContext(ctx, s.cfg.Module)
	if db == nil {
		return nil, store.ErrTenantConnectionMissing
	}

	if err := s.ensureSchema(ctx, db); err != nil {
		return nil, err
	}

	return db, nil
}

// ensureSchema runs the idempotent DDL exactly once per database handle. The
// once/err pair are keyed on the dbExecutor interface value, which is stable
// per tmpostgres.Manager pool — every Manager returns the same dbresolver.DB
// reference for a given tenant, so subsequent lookups hit the cache.
func (s *Store) ensureSchema(ctx context.Context, db dbExecutor) error {
	onceVal, _ := s.schemaOnce.LoadOrStore(db, &sync.Once{})
	once, _ := onceVal.(*sync.Once)

	once.Do(func() {
		if err := s.runSchema(ctx, db); err != nil {
			s.schemaErr.Store(db, err)
		}
	})

	if errVal, ok := s.schemaErr.Load(db); ok {
		if err, _ := errVal.(error); err != nil {
			return err
		}
	}

	return nil
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
		s.cfg.Logger.Log(ctx, log.LevelInfo, msg, fields...)
	}
}

func (s *Store) logWarn(ctx context.Context, msg string, fields ...log.Field) {
	if s.cfg.Logger != nil {
		s.cfg.Logger.Log(ctx, log.LevelWarn, msg, fields...)
	}
}

func (s *Store) logDebug(ctx context.Context, msg string, fields ...log.Field) {
	if s.cfg.Logger != nil {
		s.cfg.Logger.Log(ctx, log.LevelDebug, msg, fields...)
	}
}

func quoteIdentifier(name string) string {
	return `"` + name + `"`
}
