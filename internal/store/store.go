// Package store defines the internal storage abstraction for systemplane
// backends.
//
// It is the only interface kept between the public Client and the concrete
// Postgres / MongoDB implementations. The interface is intentionally small so
// each backend stays focused on a single responsibility.
package store

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Telemetry is the OpenTelemetry provider contract the backends accept.
//
// It names only go.opentelemetry.io/otel types — a stable v1 module — so it
// carries no other library's major version. lib-observability's
// *tracing.Telemetry satisfies it directly, from any major. It mirrors the
// public systemplane.Telemetry; the compiler enforces that the two agree,
// because a value accepted at the public boundary is passed straight here.
//
// Implementations must be safe to call with a nil receiver: both methods
// return an error rather than panicking when no provider is configured.
type Telemetry interface {
	Tracer(name string) (trace.Tracer, error)
	Meter(name string) (metric.Meter, error)
}

// Scope identifies whose configuration a call refers to.
// The zero Scope is the single-tenant scope.
type Scope struct {
	Tenant string
}

const (
	OpUpsert = "upsert"
	OpDelete = "delete"
	// OpResync is emitted by Subscribe exactly once after every successful
	// (re)connect of the changefeed, before any per-key event from the new
	// connection. Namespace, Key and Revision are empty; the engine reloads
	// the whole scope in response.
	OpResync = "resync"
	// OpDisconnect is emitted by Subscribe exactly once when the changefeed
	// loses its connection, before the first reconnect attempt. Namespace,
	// Key and Revision are empty; the engine marks the scope Stale until the
	// OpResync that follows the reconnect has been reconciled.
	OpDisconnect = "disconnect"
)

// Sentinel errors returned by Store implementations.
var (
	// ErrNilBackend is returned when a Store constructor receives a nil
	// database handle while running in single-tenant mode.
	ErrNilBackend = errors.New("systemplane/store: nil backend handle")

	// ErrClosed is returned when a method is called on a nil or closed Store.
	ErrClosed = errors.New("systemplane/store: store is closed or nil")

	// ErrNotSupportedInMultiTenant is returned by Subscribe for the zero scope
	// of a multi-tenant Store: that scope resolves a fresh tenant database on
	// every call, so there is no process-wide changefeed to subscribe to.
	ErrNotSupportedInMultiTenant = errors.New("systemplane/store: operation not supported in multi-tenant mode")

	// ErrTenantConnectionMissing is returned when a zero-scope call runs in
	// multi-tenant mode and the caller's context carries no tenant database
	// for the configured module. The caller must wire TenantMiddleware (or
	// an equivalent that calls tmcore.ContextWithPG / ContextWithMB) before
	// invoking the Client.
	ErrTenantConnectionMissing = errors.New("systemplane/store: tenant database missing from context")

	// ErrTenantConnectorMissing is returned when a call names Scope.Tenant but
	// the backend was constructed without a tenant connector.
	ErrTenantConnectorMissing = errors.New("systemplane/store: tenant connector not configured")

	// ErrValidation is returned when a backend rejects input that fails a
	// structural precondition (e.g. empty namespace or key). The admin layer
	// maps this to HTTP 400 via the client's ErrValidation alias.
	ErrValidation = errors.New("systemplane/store: validation failed")
)

type Entry struct {
	Namespace string
	Key       string

	// Value is the JSON-encoded value, and it belongs to the RECEIVER from
	// the moment a Store hands it over: the engine retains the slice in its
	// snapshots and in the changes it publishes, and reads it long after the
	// call that produced it returned. A Store must therefore never reuse,
	// re-slice or mutate that memory afterwards, and must never hand out a
	// view into a driver buffer the driver reuses on its next read — pgx
	// RawValues and sql.RawBytes on Postgres, bson.Raw and bson.RawValue
	// views on MongoDB all alias such buffers. A backend that cannot return
	// memory of its own must copy at the boundary (bytes.Clone). The contract
	// suite pins this for every backend as ValueBytesBelongToTheCaller.
	Value []byte

	Revision  int64 // monotonic per (namespace, key); 0 = unknown
	UpdatedAt time.Time
	UpdatedBy string
}

type Event struct {
	Scope     Scope
	Namespace string
	Key       string
	Op        string
	Revision  int64 // 0 for OpDelete, OpResync, OpDisconnect, or unknown
}

// Store is the contract implemented by internal/postgres and internal/mongodb.
//
// Get, Set, Delete and List resolve the database from scope: the zero Scope
// uses the constructor handle (single-tenant) or the tenant database carried
// by ctx (multi-tenant request path, set by tenant-manager middleware); a
// non-empty Scope.Tenant resolves through the tenant connector regardless of
// ctx and returns ErrTenantConnectorMissing when none is configured.
type Store interface {
	Start(ctx context.Context) error
	Close() error
	Get(ctx context.Context, scope Scope, ns, key string) (Entry, bool, error)
	// Set upserts with last-write-wins and returns the revision now stored.
	Set(ctx context.Context, scope Scope, e Entry) (revision int64, err error)
	Delete(ctx context.Context, scope Scope, ns, key, actor string) error
	List(ctx context.Context, scope Scope) ([]Entry, error)
	// Subscribe opens a changefeed for scope for the lifetime of ctx, emits
	// OpDisconnect when the connection is lost and OpResync after every
	// (re)connect. Returns ErrNotSupportedInMultiTenant only for a backend
	// that has no changefeed for that scope.
	Subscribe(ctx context.Context, scope Scope, fn func(Event)) (unsubscribe func(), err error)
}
