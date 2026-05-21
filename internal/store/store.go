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
)

// Op classifies a change event delivered by a backend changefeed.
const (
	OpUpsert = "upsert"
	OpDelete = "delete"
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

	// ErrValidation is returned when a backend rejects input that fails a
	// structural precondition (e.g. empty namespace or key). The admin layer
	// maps this to HTTP 400 via the client's ErrValidation alias.
	ErrValidation = errors.New("systemplane/store: validation failed")
)

// Entry is the persisted shape of a single configuration key.
type Entry struct {
	Namespace string
	Key       string
	Value     []byte // JSON-encoded
	UpdatedAt time.Time
	UpdatedBy string
}

// Event is what a changefeed delivers when a key is modified.
//
// Op is either OpUpsert (insert/update) or OpDelete (row removed).
type Event struct {
	Namespace string
	Key       string
	Op        string
}

// Store is the contract implemented by internal/postgres and internal/mongodb.
//
// All methods take a context; in multi-tenant mode the context MUST carry the
// tenant database for the configured module (set by lib-commons
// tenant-manager's TenantMiddleware via tmcore.ContextWithPG / ContextWithMB).
// In single-tenant mode the context is used only for cancellation and
// telemetry; the constructor-supplied handle is the database.
type Store interface {
	// Start performs any one-time bootstrap work that needs the live ctx
	// (e.g. opening the LISTEN connection in single-tenant Postgres). It is
	// idempotent and safe to call multiple times.
	Start(ctx context.Context) error

	// Close releases backend resources. Idempotent. Does NOT close any
	// externally-supplied database handle.
	Close() error

	// Get returns a single entry by namespace and key.
	Get(ctx context.Context, ns, key string) (Entry, bool, error)

	// Set persists an entry using last-write-wins semantics.
	Set(ctx context.Context, e Entry) error

	// Delete removes a single (ns, key) row. actor is recorded for audit
	// purposes via spans/logs; it is not persisted. Idempotent — deleting a
	// row that does not exist returns nil.
	Delete(ctx context.Context, ns, key, actor string) error

	// List returns every entry in the underlying database/collection,
	// ordered by (namespace, key).
	List(ctx context.Context) ([]Entry, error)

	// Subscribe registers a changefeed listener for the lifetime of ctx. The
	// returned unsubscribe func can be called to remove the listener early.
	// Returns ErrNotSupportedInMultiTenant when the backend was constructed
	// with WithMultiTenantEnabled().
	Subscribe(ctx context.Context, fn func(ev Event)) (unsubscribe func(), err error)
}
