package systemplane

import (
	"context"
	"time"

	"github.com/LerianStudio/lib-observability/v2/log"
	internalclient "github.com/LerianStudio/lib-systemplane/v2/internal/client"
)

func asInternalClient(c *Client) *internalclient.Client {
	return (*internalclient.Client)(c)
}

// Register declares a configuration key with its default value and options.
// Must be called before [Client.Start].
func (c *Client) Register(namespace, key string, defaultValue any, opts ...KeyOption) error {
	return asInternalClient(c).Register(namespace, key, defaultValue, opts...)
}

// Start hydrates registered keys from the backing store and begins consuming
// backend change notifications. In multi-tenant mode it is a no-op beyond
// marking the Client started — every read resolves a fresh tenant database.
func (c *Client) Start(ctx context.Context) error {
	return asInternalClient(c).Start(ctx)
}

// Close stops change notification processing and releases backend resources.
func (c *Client) Close() error {
	return asInternalClient(c).Close()
}

// Get returns the current value for namespace/key.
//
// In single-tenant mode reads come from the in-process cache. In multi-tenant
// mode the call resolves the per-tenant database from ctx (set by
// tenant-manager middleware) and reads through.
func (c *Client) Get(ctx context.Context, namespace, key string) (any, bool, error) {
	return asInternalClient(c).Get(ctx, namespace, key)
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

// OnChange registers a callback for backend-observed value changes.
// Returns ErrNotSupportedInMultiTenant in multi-tenant mode.
func (c *Client) OnChange(namespace, key string, fn func(ctx context.Context, ns, key string, newValue any)) (func(), error) {
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
