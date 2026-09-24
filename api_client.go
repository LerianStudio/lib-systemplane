package systemplane

import (
	"context"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	internalclient "github.com/LerianStudio/lib-systemplane/v4/internal/client"
)

func asInternalClient(c *Client) *internalclient.Client {
	return (*internalclient.Client)(c)
}

// Register declares a configuration key with its default value and options.
// Must be called before [Client.Start].
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
// A Start that fails is retryable: the Client stays usable, subscriptions
// registered before it survive, and the next Start reconciles from nothing.
//
// In multi-tenant mode it is a no-op beyond marking the Client started —
// every read resolves a fresh tenant database.
func (c *Client) Start(ctx context.Context) error {
	return asInternalClient(c).Start(ctx)
}

// Close stops change notification processing and releases backend resources.
func (c *Client) Close() error {
	return asInternalClient(c).Close()
}

// Get returns the current value for namespace/key.
//
// In single-tenant mode reads are served in process from the value last
// reconciled or written, without touching the database. In multi-tenant mode
// the call resolves the per-tenant database from ctx (set by tenant-manager
// middleware) and reads through.
func (c *Client) Get(ctx context.Context, namespace, key string) (any, bool, error) {
	return asInternalClient(c).Get(ctx, namespace, key)
}

// GetEntry resolves the caller's scope like Get. ok is false for an
// unregistered key. Revision, UpdatedAt and UpdatedBy describe the persisted
// row behind the value, and are zero when the registered default is in force
// because no row exists or the stored one was refused. Stale is true while the
// value has not been reconciled with the store — before [Client.Start], and
// while the changefeed is disconnected.
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
// deliver independently. A callback may read the Client re-entrantly;
// [Client.Set] and [Client.Delete] called from that first delivery return
// ErrNotStarted when it wins the race with Start's return.
//
// OnChange returns ErrUnknownKey for a key that was not registered, in both
// modes. In multi-tenant mode it then returns ErrNotSupportedInMultiTenant for
// every registered key: no scope is tracked and no changefeed runs, so no
// callback could ever fire. The wave-3 engine-tenants lane makes multi-tenant
// OnChange work on both backends. On a closed Client it returns ErrClosed.
func (c *Client) OnChange(namespace, key string, fn func(ctx context.Context, ch Change)) (unsubscribe func(), err error) {
	return asInternalClient(c).OnChange(namespace, key, fn)
}

// KeyDescription returns the registered human-readable description for a key.
func (c *Client) KeyDescription(namespace, key string) string {
	return asInternalClient(c).KeyDescription(namespace, key)
}

// KeyRedaction returns the registered redaction policy for a key.
func (c *Client) KeyRedaction(namespace, key string) RedactPolicy {
	return RedactPolicy(asInternalClient(c).KeyRedaction(namespace, key))
}

// IsRegistered reports whether (namespace, key) was registered.
func (c *Client) IsRegistered(namespace, key string) bool {
	return asInternalClient(c).IsRegistered(namespace, key)
}

// Logger returns the logger attached to the Client.
func (c *Client) Logger() log.Logger {
	return asInternalClient(c).Logger()
}
