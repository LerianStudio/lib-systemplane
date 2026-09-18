// Write path for systemplane Client.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/manager"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// Set writes a new value for (namespace, key). The value is validated against
// the key's registered validator (if any), JSON-marshaled, and persisted to
// the resolved backing store. In single-tenant mode the in-process cache is
// updated synchronously for same-process read consistency.
func (c *Client) Set(ctx context.Context, namespace, key string, value any, actor string) error {
	if c == nil || c.closed.Load() {
		return ErrClosed
	}

	if ctx == nil {
		return ErrNilContext
	}

	if !c.started.Load() {
		return ErrNotStarted
	}

	nk := nskey{Namespace: namespace, Key: key}

	c.registryMu.RLock()
	def, registered := c.registry[nk]
	c.registryMu.RUnlock()

	if !registered {
		return fmt.Errorf("%w: %s/%s", ErrUnknownKey, namespace, key)
	}

	if def.validator != nil {
		if err := def.validator(value); err != nil {
			return fmt.Errorf("%w: %w", ErrValidation, err)
		}
	}

	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w: value is not JSON-serializable: %w", ErrValidation, err)
	}

	entry := store.Entry{
		Namespace: namespace,
		Key:       key,
		Value:     jsonBytes,
		UpdatedAt: time.Now().UTC(),
		UpdatedBy: actor,
	}

	// The revision the store reports is discarded here: publishing it is
	// the engine's job, not the facade's.
	if _, err := c.store.Set(ctx, store.Scope{}, entry); err != nil {
		return err
	}

	// Write through the in-process cache. Without this a reader between the
	// commit and the changefeed round trip falls back to the store, and a read
	// that does not yet see the fresh row reports the registered default for a
	// key that already had a value.
	var canonical any
	if err := json.Unmarshal(jsonBytes, &canonical); err != nil {
		canonical = value
	}

	if !c.multiTenant {
		c.cacheMu.Lock()
		c.cache[nk] = canonical
		c.cacheMu.Unlock()

		return nil
	}

	if mgr, tenantID := c.boundManager(), manager.TenantIDFromContext(ctx); mgr != nil && tenantID != "" {
		mgr.Populate(ctx, tenantID, namespace, key, canonical)
	}

	return nil
}

// Delete removes a single (namespace, key) row.
func (c *Client) Delete(ctx context.Context, namespace, key, actor string) error {
	if c == nil || c.closed.Load() {
		return ErrClosed
	}

	if ctx == nil {
		return ErrNilContext
	}

	if !c.started.Load() {
		return ErrNotStarted
	}

	nk := nskey{Namespace: namespace, Key: key}

	c.registryMu.RLock()
	_, registered := c.registry[nk]
	c.registryMu.RUnlock()

	if !registered {
		return fmt.Errorf("%w: %s/%s", ErrUnknownKey, namespace, key)
	}

	if err := c.store.Delete(ctx, store.Scope{}, namespace, key, actor); err != nil {
		return err
	}

	if !c.multiTenant {
		c.cacheMu.Lock()
		delete(c.cache, nk)
		c.cacheMu.Unlock()

		return nil
	}

	if mgr, tenantID := c.boundManager(), manager.TenantIDFromContext(ctx); mgr != nil && tenantID != "" {
		mgr.Invalidate(ctx, tenantID, namespace, key)
	}

	return nil
}
