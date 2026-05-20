package systemplane

import (
	"context"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	internalclient "github.com/LerianStudio/lib-systemplane/internal/client"
)

func asInternalClient(c *Client) *internalclient.Client {
	return (*internalclient.Client)(c)
}

// Register declares a global configuration key with its default value and
// optional key options. It must be called before [Client.Start].
func (c *Client) Register(namespace, key string, defaultValue any, opts ...KeyOption) error {
	return asInternalClient(c).Register(namespace, key, defaultValue, opts...)
}

// RegisterTenantScoped declares a key eligible for per-tenant overrides while
// keeping the legacy global read/list/subscription behavior intact.
func (c *Client) RegisterTenantScoped(namespace, key string, defaultValue any, opts ...KeyOption) error {
	return asInternalClient(c).RegisterTenantScoped(namespace, key, defaultValue, opts...)
}

// Start hydrates registered keys from the backing store and begins consuming
// backend change notifications.
func (c *Client) Start(ctx context.Context) error {
	return asInternalClient(c).Start(ctx)
}

// Close stops change notification processing and releases backend resources.
func (c *Client) Close() error {
	return asInternalClient(c).Close()
}

// Get returns the current value for namespace/key and whether the key is known.
func (c *Client) Get(namespace, key string) (any, bool) {
	return asInternalClient(c).Get(namespace, key)
}

// GetString returns the current value as a string, or "" on miss or type mismatch.
func (c *Client) GetString(namespace, key string) string {
	return asInternalClient(c).GetString(namespace, key)
}

// GetInt returns the current value as an int, or 0 on miss or type mismatch.
func (c *Client) GetInt(namespace, key string) int {
	return asInternalClient(c).GetInt(namespace, key)
}

// GetBool returns the current value as a bool, or false on miss or type mismatch.
func (c *Client) GetBool(namespace, key string) bool {
	return asInternalClient(c).GetBool(namespace, key)
}

// GetFloat64 returns the current value as a float64, or 0 on miss or type mismatch.
func (c *Client) GetFloat64(namespace, key string) float64 {
	return asInternalClient(c).GetFloat64(namespace, key)
}

// GetDuration returns the current value as a duration, or 0 on miss or parse/type mismatch.
func (c *Client) GetDuration(namespace, key string) time.Duration {
	return asInternalClient(c).GetDuration(namespace, key)
}

// List returns all registered entries in namespace sorted by key.
func (c *Client) List(namespace string) []ListEntry {
	entries := asInternalClient(c).List(namespace)
	if entries == nil {
		return nil
	}

	out := make([]ListEntry, len(entries))
	for i, entry := range entries {
		out[i] = ListEntry{
			Key:         entry.Key,
			Value:       entry.Value,
			Description: entry.Description,
		}
	}

	return out
}

// KeyDescription returns the registered human-readable description for a key.
func (c *Client) KeyDescription(namespace, key string) string {
	return asInternalClient(c).KeyDescription(namespace, key)
}

// KeyRedaction returns the registered redaction policy for a key.
func (c *Client) KeyRedaction(namespace, key string) RedactPolicy {
	return RedactPolicy(asInternalClient(c).KeyRedaction(namespace, key))
}

// KeyStatus reports whether a key is registered and whether it is tenant-scoped.
func (c *Client) KeyStatus(namespace, key string) (registered, tenantScoped bool) {
	return asInternalClient(c).KeyStatus(namespace, key)
}

// Logger returns the logger attached to the Client, or a nop logger when unset.
func (c *Client) Logger() log.Logger {
	return asInternalClient(c).Logger()
}

// Set validates and persists a new global value for namespace/key.
func (c *Client) Set(ctx context.Context, namespace, key string, value any, actor string) error {
	return asInternalClient(c).Set(ctx, namespace, key, value, actor)
}

// OnChange registers a callback for backend-observed global value changes.
func (c *Client) OnChange(namespace, key string, fn func(newValue any)) (unsubscribe func()) {
	return asInternalClient(c).OnChange(namespace, key, fn)
}

// GetForTenant returns the tenant-effective value for a tenant-scoped key.
func (c *Client) GetForTenant(ctx context.Context, namespace, key string) (any, bool, error) {
	return asInternalClient(c).GetForTenant(ctx, namespace, key)
}

// SetForTenant persists a tenant-specific override for namespace/key.
func (c *Client) SetForTenant(ctx context.Context, namespace, key string, value any, actor string) error {
	return asInternalClient(c).SetForTenant(ctx, namespace, key, value, actor)
}

// DeleteForTenant removes a tenant-specific override for namespace/key.
func (c *Client) DeleteForTenant(ctx context.Context, namespace, key, actor string) error {
	return asInternalClient(c).DeleteForTenant(ctx, namespace, key, actor)
}

// ListTenantsForKey returns tenant IDs with overrides for namespace/key.
func (c *Client) ListTenantsForKey(namespace, key string) []string {
	return asInternalClient(c).ListTenantsForKey(namespace, key)
}

// ListTenantsForKeyContext returns tenant IDs with overrides for namespace/key,
// surfacing lifecycle, context, registration, and backend errors to the caller.
func (c *Client) ListTenantsForKeyContext(ctx context.Context, namespace, key string) ([]string, error) {
	return asInternalClient(c).ListTenantsForKeyContext(ctx, namespace, key)
}

// OnTenantChange registers a callback for backend-observed tenant override changes.
func (c *Client) OnTenantChange(namespace, key string, fn func(ctx context.Context, namespace, key, tenantID string, newValue any)) (unsubscribe func()) {
	return asInternalClient(c).OnTenantChange(namespace, key, fn)
}

// GetStringForTenant returns the tenant-effective value as a string.
func (c *Client) GetStringForTenant(ctx context.Context, namespace, key string) (string, error) {
	return asInternalClient(c).GetStringForTenant(ctx, namespace, key)
}

// GetIntForTenant returns the tenant-effective value as an int.
func (c *Client) GetIntForTenant(ctx context.Context, namespace, key string) (int, error) {
	return asInternalClient(c).GetIntForTenant(ctx, namespace, key)
}

// GetBoolForTenant returns the tenant-effective value as a bool.
func (c *Client) GetBoolForTenant(ctx context.Context, namespace, key string) (bool, error) {
	return asInternalClient(c).GetBoolForTenant(ctx, namespace, key)
}

// GetFloat64ForTenant returns the tenant-effective value as a float64.
func (c *Client) GetFloat64ForTenant(ctx context.Context, namespace, key string) (float64, error) {
	return asInternalClient(c).GetFloat64ForTenant(ctx, namespace, key)
}

// GetDurationForTenant returns the tenant-effective value as a duration.
func (c *Client) GetDurationForTenant(ctx context.Context, namespace, key string) (time.Duration, error) {
	return asInternalClient(c).GetDurationForTenant(ctx, namespace, key)
}
