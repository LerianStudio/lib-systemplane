package systemplane

import (
	"context"
	"time"

	tmevent "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/event"
	"github.com/LerianStudio/lib-observability/v4/log"
	internalclient "github.com/LerianStudio/lib-systemplane/v4/internal/client"
)

func asInternalClient(c *Client) *internalclient.Client {
	return (*internalclient.Client)(c)
}

// Register declares a configuration key with its default value and options.
// Must be called before [Client.Start].
//
// defaultValue is stored, served and announced in its CANONICAL shape — the
// shape a stored row comes back in: numbers as float64, objects as
// map[string]any, arrays as []any. One key therefore answers with one Go type
// whether a row exists or not, which is what lets a subscriber type-assert the
// shape its validator grades without the boot announcement and every delete
// handing it something else. A default that does not survive a JSON round trip
// is refused with [ErrValidation].
func (c *Client) Register(namespace, key string, defaultValue any, opts ...KeyOption) error {
	return asInternalClient(c).Register(namespace, key, defaultValue, opts...)
}

// Start subscribes to the backend changefeed, reconciles every registered key
// against the store and only then returns, so a read taken after Start reports
// what is actually stored rather than the registered default.
//
// A stored value a key's validator refuses never comes into force: the
// registered default stays in force and a WARN naming the key and the
// validator's error is logged. The refused value itself is never logged.
//
// Every subscriber registered before Start is handed the value in force once,
// including the keys the store had no row for and the keys whose row was
// refused — those are announced as the registered default. A consumer can
// therefore put its reload in [Client.OnChange] alone and be correct from
// boot. The announcement is queued while Start runs and delivered on the key's
// own goroutine, so a callback may run just after Start returns; what Start
// itself guarantees is that every read taken after it already serves the value
// that announcement carries.
//
// A Start that fails is retryable: the Client stays usable and subscriptions
// registered before it survive. After a failed reconcile the next Start
// reconciles from nothing; after a ctx expiry it waits for the reconcile
// already pending, and [Client.Register] stays refused.
//
// The Client counts as started from the moment that reconcile begins, so a
// [Client.Set] racing Start writes its row and then reports [ErrNotStarted]
// rather than being refused before the store is touched.
//
// In multi-tenant mode it only marks the Client started; with a tenant
// manager, a tenant's scope comes up on that tenant's first read.
func (c *Client) Start(ctx context.Context) error {
	return asInternalClient(c).Start(ctx)
}

// Close cancels the context handed to running callbacks, waits for them up to
// [WithCloseTimeout] and stops every changefeed; the database handle passed to
// the constructor stays open. Later calls return the first call's result.
func (c *Client) Close() error {
	return asInternalClient(c).Close()
}

// Get returns the current value for namespace/key.
//
// In single-tenant mode reads are served in process from the cache the
// changefeed keeps current, without touching the database. In multi-tenant mode
// the call resolves the per-tenant database from ctx (set by tenant-manager
// middleware) and reads through, graded by the validator; with
// [WithPostgresTenantManager] or [WithMongoTenantManager] that read activates
// the tenant's scope and later reads are served in process like single-tenant
// ones.
func (c *Client) Get(ctx context.Context, namespace, key string) (any, bool, error) {
	return asInternalClient(c).Get(ctx, namespace, key)
}

// GetEntry resolves the caller's scope like Get. ok is false for an
// unregistered key. Revision, UpdatedAt and UpdatedBy describe the persisted
// row behind the value, and are zero when the registered default is in force
// because no row exists or the stored one was refused. Stale is true while
// nothing is confirming THIS key: before a single-tenant [Client.Start], while
// the changefeed is disconnected or has not been reconciled since it connected,
// and while this key could not be re-read after its last change. A sibling key
// that could not be re-read does not make this one stale, and a multi-tenant
// read served per request is never stale.
func (c *Client) GetEntry(ctx context.Context, namespace, key string) (e Entry, ok bool, err error) {
	return asInternalClient(c).GetEntry(ctx, namespace, key)
}

// GetString returns the value as a string.
func (c *Client) GetString(ctx context.Context, namespace, key string) (string, bool, error) {
	return asInternalClient(c).GetString(ctx, namespace, key)
}

// GetInt returns the value as an int64.
func (c *Client) GetInt(ctx context.Context, namespace, key string) (int64, bool, error) {
	return asInternalClient(c).GetInt(ctx, namespace, key)
}

// GetBool returns the value as a bool.
func (c *Client) GetBool(ctx context.Context, namespace, key string) (bool, bool, error) {
	return asInternalClient(c).GetBool(ctx, namespace, key)
}

// GetFloat64 returns the value as a float64.
func (c *Client) GetFloat64(ctx context.Context, namespace, key string) (float64, bool, error) {
	return asInternalClient(c).GetFloat64(ctx, namespace, key)
}

// GetDuration returns the value as a time.Duration.
func (c *Client) GetDuration(ctx context.Context, namespace, key string) (time.Duration, bool, error) {
	return asInternalClient(c).GetDuration(ctx, namespace, key)
}

// Set persists a new value for namespace/key.
//
// A Set racing [Client.Start] can persist its row and still report
// [ErrNotStarted], because the Client counts as started before its first
// reconcile has brought the scope up.
func (c *Client) Set(ctx context.Context, namespace, key string, value any, actor string) error {
	return asInternalClient(c).Set(ctx, namespace, key, value, actor)
}

// Delete removes the row for namespace/key.
func (c *Client) Delete(ctx context.Context, namespace, key, actor string) error {
	return asInternalClient(c).Delete(ctx, namespace, key, actor)
}

// List returns all registered entries in namespace.
func (c *Client) List(ctx context.Context, namespace string) ([]ListEntry, error) {
	entries, err := asInternalClient(c).List(ctx, namespace)
	if err != nil {
		return nil, err
	}

	out := make([]ListEntry, len(entries))
	for i, entry := range entries {
		out[i] = ListEntry{
			Key:         entry.Key,
			Value:       entry.Value,
			Description: entry.Description,
		}
	}

	return out, nil
}

// Catalog returns a registry-only snapshot of all registered keys.
func (c *Client) Catalog() Catalog {
	return asInternalClient(c).Catalog()
}

// CatalogKey returns registry-only detail metadata for namespace/key.
func (c *Client) CatalogKey(namespace, key string) (CatalogKeyDetail, bool) {
	return asInternalClient(c).CatalogKey(namespace, key)
}

// CatalogService returns the service name emitted by catalog responses.
func (c *Client) CatalogService() string {
	return asInternalClient(c).CatalogService()
}

// OnChange registers a callback for backend-observed value changes of
// (namespace, key). Change.Tenant names the tenant whose row changed ("" in
// single-tenant mode) and a delete delivers the registered default with
// Revision 0.
//
// In single-tenant mode a subscriber registered before [Client.Start] is
// handed the value in force once: Start queues that announcement and the key's
// own goroutine delivers it, so it may land either side of Start's return.
// Deliveries for one key are serialized and coalesced off the caller's
// goroutine: while a callback runs, a newer revision of that key replaces the
// pending one, so a callback may skip intermediate revisions but always
// receives the newest and never sees revisions out of order. Different keys
// deliver independently. A callback may read the Client re-entrantly, and may
// write: [Client.Set] and [Client.Delete] called from that first delivery land
// whether or not it wins the race with Start's return. The ctx a callback
// receives is the engine's own lifecycle context — no request values, no
// tenant — so a callback that needs the tenant reads Change.Tenant, not ctx.
//
// With [WithPostgresTenantManager] or [WithMongoTenantManager] one
// subscription covers every tenant: a tenant's scope announces every
// registered key each time it comes up (on the read that activates it, and on
// a rebuild after a credentials rotation), then delivers its own changes,
// serialized and coalesced per (tenant, key).
//
// OnChange returns ErrUnknownKey for a key that was not registered, in both
// modes. A multi-tenant Client with no tenant manager then returns
// ErrNotSupportedInMultiTenant for every registered key: no scope is tracked,
// so no callback could ever fire. On a closed Client it returns ErrClosed.
func (c *Client) OnChange(namespace, key string, fn func(ctx context.Context, ch Change)) (unsubscribe func(), err error) {
	return asInternalClient(c).OnChange(namespace, key, fn)
}

// HandleTenantLifecycle applies a tenant-manager lifecycle event to that
// tenant's scope. It has the tmevent.EventHandler signature, so it registers
// with the tenant-manager event listener as is:
//
//   - tenant.activated clears the tenant's blocked marker and activates
//     nothing: the tenant's next read does, so a process opens no feed for a
//     tenant it never reads.
//   - tenant.suspended and tenant.deleted drop the tenant's scope and block
//     it: its reads resolve the tenant database per request, and none brings
//     the scope back until the next tenant.activated.
//   - tenant.credentials.rotated rebuilds an active tenant's scope on a fresh
//     feed and leaves a blocked tenant blocked.
//
// Every other event type, a nil Client and a Client built without
// [WithPostgresTenantManager] or [WithMongoTenantManager] return nil. The
// scope work runs in the background, so the only errors are [ErrClosed] after
// [Client.Close] and [ErrValidation] for an event with no TenantID; a failed
// activation is logged and retried by a later read. The tenant-manager
// listener logs a returned error and moves on; a dispatcher of your own that
// stops on an error must handle these two.
//
// Chain it after the tenant-manager dispatcher's own HandleEvent: on a
// rotation the dispatcher closes and reloads the tenant's pools, and a rebuild
// that ran first could resolve a pool about to be closed.
func (c *Client) HandleTenantLifecycle(ctx context.Context, event tmevent.TenantLifecycleEvent) error {
	return asInternalClient(c).HandleTenantLifecycle(ctx, event)
}

// KeyDescription returns the registered human-readable description for a key.
func (c *Client) KeyDescription(namespace, key string) string {
	return asInternalClient(c).KeyDescription(namespace, key)
}

// IsRegistered reports whether (namespace, key) was registered.
func (c *Client) IsRegistered(namespace, key string) bool {
	return asInternalClient(c).IsRegistered(namespace, key)
}

// Logger returns the logger attached to the Client.
func (c *Client) Logger() log.Logger {
	return asInternalClient(c).Logger()
}
