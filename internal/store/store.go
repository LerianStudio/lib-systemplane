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

// Telemetry is the OpenTelemetry provider contract the backends and the
// Manager accept.
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
	// (Amendment of 2026-09-17, after the contracts lane landed store.go:
	// the storage lane adds this constant; signatures are unchanged.)
	OpDisconnect = "disconnect"
)

// Sentinel errors returned by Store implementations.
var (
	// ErrNilBackend is returned when a Store constructor receives a nil
	// database handle while running in single-tenant mode.
	ErrNilBackend = errors.New("systemplane/store: nil backend handle")

	// ErrClosed is returned when a method is called on a nil or closed Store.
	ErrClosed = errors.New("systemplane/store: store is closed or nil")

	// ErrNotSupportedInMultiTenant is returned by methods that have no
	// sensible per-tenant implementation — currently Subscribe and the
	// in-process cache primitives. Multi-tenant mode resolves a fresh
	// tenant database on every call, so there is no shared process-wide
	// changefeed to subscribe to.
	ErrNotSupportedInMultiTenant = errors.New("systemplane/store: operation not supported in multi-tenant mode")

	// ErrTenantConnectionMissing is returned when a method runs in
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
	Value     []byte // JSON-encoded
	Revision  int64  // monotonic per (namespace, key); 0 = unknown
	UpdatedAt time.Time
	UpdatedBy string
}

type Event struct {
	Scope     Scope
	Namespace string
	Key       string
	Op        string
	Revision  int64 // revision after the change; 0 for OpDelete, OpResync, or unknown
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
	// Subscribe opens a changefeed for scope for the lifetime of ctx and
	// emits OpResync after every (re)connect. Returns
	// ErrNotSupportedInMultiTenant when the backend has no changefeed for
	// that scope (MongoDB with a non-empty tenant).
	Subscribe(ctx context.Context, scope Scope, fn func(Event)) (unsubscribe func(), err error)
}
