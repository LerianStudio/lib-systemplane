// Read paths and listing for systemplane Client.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/LerianStudio/lib-observability/log"
)

// ListEntry is a single entry returned by [Client.List].
type ListEntry struct {
	Key         string
	Value       any
	Description string
}

// Get returns the current value for (namespace, key).
//
// In single-tenant mode it returns the cached value (or the registered
// default when the cache is empty). In multi-tenant mode it resolves the
// tenant database from ctx and reads through, returning the registered
// default when the row is absent.
func (c *Client) Get(ctx context.Context, namespace, key string) (any, bool, error) {
	if c == nil || c.closed.Load() {
		return nil, false, ErrClosed
	}

	if ctx == nil {
		return nil, false, ErrNilContext
	}

	nk := nskey{Namespace: namespace, Key: key}

	c.registryMu.RLock()
	def, registered := c.registry[nk]
	c.registryMu.RUnlock()

	if !registered {
		return nil, false, nil
	}

	if !c.multiTenant {
		c.cacheMu.RLock()
		v, inCache := c.cache[nk]
		c.cacheMu.RUnlock()

		if inCache {
			return cloneValue(v), true, nil
		}

		return cloneValue(def.defaultValue), true, nil
	}

	// Multi-tenant: read through to the resolved tenant DB.
	entry, found, err := c.store.Get(ctx, namespace, key)
	if err != nil {
		return nil, false, fmt.Errorf("systemplane: Get: %w", err)
	}

	if !found {
		return cloneValue(def.defaultValue), true, nil
	}

	var decoded any
	if err := json.Unmarshal(entry.Value, &decoded); err != nil {
		c.logWarn(ctx, "failed to unmarshal stored value, returning default",
			log.String("namespace", namespace),
			log.String("key", key),
			log.Err(err),
		)

		return cloneValue(def.defaultValue), true, nil
	}

	return decoded, true, nil
}

// GetString returns the value as a string.
//
// When the stored value is not a string, returns (zero, false, ErrValidation)
// so callers can distinguish a missing/typed-incompatible value from a
// legitimate empty string. Callers that only care about success can check the
// second return.
func (c *Client) GetString(ctx context.Context, namespace, key string) (string, bool, error) {
	v, ok, err := c.Get(ctx, namespace, key)
	if err != nil || !ok {
		return "", ok, err
	}

	s, isString := v.(string)
	if !isString {
		return "", false, fmt.Errorf("%w: %s/%s: stored value is %T, want string", ErrValidation, namespace, key, v)
	}

	return s, true, nil
}

// GetInt returns the value as an int64.
//
// Accepts int, int64, and integer-valued float64 (JSON-decoded numbers).
// Fractional float64 values, strings, and other types fail conversion and
// return (0, false, ErrValidation). This avoids silently truncating
// fractional input or returning 0 for a malformed value.
func (c *Client) GetInt(ctx context.Context, namespace, key string) (int64, bool, error) {
	v, ok, err := c.Get(ctx, namespace, key)
	if err != nil || !ok {
		return 0, ok, err
	}

	switch n := v.(type) {
	case int:
		return int64(n), true, nil
	case int64:
		return n, true, nil
	case float64:
		// JSON decodes all numbers as float64. Reject any value that would
		// lose precision when truncated to int64 (NaN, Inf, fractional).
		if n != float64(int64(n)) {
			return 0, false, fmt.Errorf("%w: %s/%s: stored value %v is not an integer", ErrValidation, namespace, key, n)
		}

		return int64(n), true, nil
	default:
		return 0, false, fmt.Errorf("%w: %s/%s: stored value is %T, want int", ErrValidation, namespace, key, v)
	}
}

// GetBool returns the value as a bool.
//
// Returns (false, false, ErrValidation) when the stored value is not a bool.
func (c *Client) GetBool(ctx context.Context, namespace, key string) (bool, bool, error) {
	v, ok, err := c.Get(ctx, namespace, key)
	if err != nil || !ok {
		return false, ok, err
	}

	b, isBool := v.(bool)
	if !isBool {
		return false, false, fmt.Errorf("%w: %s/%s: stored value is %T, want bool", ErrValidation, namespace, key, v)
	}

	return b, true, nil
}

// GetFloat64 returns the value as a float64.
//
// Returns (0, false, ErrValidation) when the stored value is not a number.
func (c *Client) GetFloat64(ctx context.Context, namespace, key string) (float64, bool, error) {
	v, ok, err := c.Get(ctx, namespace, key)
	if err != nil || !ok {
		return 0, ok, err
	}

	switch n := v.(type) {
	case float64:
		return n, true, nil
	case int:
		return float64(n), true, nil
	case int64:
		return float64(n), true, nil
	default:
		return 0, false, fmt.Errorf("%w: %s/%s: stored value is %T, want float64", ErrValidation, namespace, key, v)
	}
}

// GetDuration returns the value as a time.Duration.
//
// Accepts time.Duration, parseable duration string (e.g. "30s"), and integer
// float64 nanoseconds. All other shapes — including unparseable strings —
// return (0, false, ErrValidation).
func (c *Client) GetDuration(ctx context.Context, namespace, key string) (time.Duration, bool, error) {
	v, ok, err := c.Get(ctx, namespace, key)
	if err != nil || !ok {
		return 0, ok, err
	}

	switch d := v.(type) {
	case time.Duration:
		return d, true, nil
	case string:
		parsed, parseErr := time.ParseDuration(d)
		if parseErr != nil {
			return 0, false, fmt.Errorf("%w: %s/%s: cannot parse %q as duration: %w",
				ErrValidation, namespace, key, d, parseErr)
		}

		return parsed, true, nil
	case float64:
		return time.Duration(int64(d)), true, nil
	default:
		return 0, false, fmt.Errorf("%w: %s/%s: stored value is %T, want time.Duration", ErrValidation, namespace, key, v)
	}
}

// List returns all registered entries in namespace sorted by key.
//
// In multi-tenant mode List resolves the tenant database from ctx; in
// single-tenant mode it serves from the in-process cache and registered
// defaults.
func (c *Client) List(ctx context.Context, namespace string) ([]ListEntry, error) {
	if c == nil || c.closed.Load() {
		return nil, ErrClosed
	}

	if ctx == nil {
		return nil, ErrNilContext
	}

	c.registryMu.RLock()

	keys := make([]nskey, 0)

	for nk := range c.registry {
		if nk.Namespace == namespace {
			keys = append(keys, nk)
		}
	}

	c.registryMu.RUnlock()

	if len(keys) == 0 {
		return []ListEntry{}, nil
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i].Key < keys[j].Key
	})

	if c.multiTenant {
		return c.listFromStore(ctx, namespace, keys)
	}

	return c.listFromCache(keys), nil
}

func (c *Client) listFromCache(keys []nskey) []ListEntry {
	entries := make([]ListEntry, 0, len(keys))

	c.registryMu.RLock()
	c.cacheMu.RLock()

	for _, nk := range keys {
		val, inCache := c.cache[nk]

		def, registered := c.registry[nk]
		if !inCache && registered {
			val = def.defaultValue
		}

		var desc string
		if registered {
			desc = def.description
		}

		entries = append(entries, ListEntry{
			Key:         nk.Key,
			Value:       cloneValue(val),
			Description: desc,
		})
	}

	c.cacheMu.RUnlock()
	c.registryMu.RUnlock()

	return entries
}

func (c *Client) listFromStore(ctx context.Context, namespace string, keys []nskey) ([]ListEntry, error) {
	stored, err := c.store.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("systemplane: List: %w", err)
	}

	storedByKey := make(map[string][]byte, len(stored))

	for _, entry := range stored {
		if entry.Namespace != namespace {
			continue
		}

		storedByKey[entry.Key] = entry.Value
	}

	entries := make([]ListEntry, 0, len(keys))

	c.registryMu.RLock()
	defer c.registryMu.RUnlock()

	for _, nk := range keys {
		def := c.registry[nk]
		val := cloneValue(def.defaultValue)

		if raw, ok := storedByKey[nk.Key]; ok {
			var decoded any
			if err := json.Unmarshal(raw, &decoded); err != nil {
				c.logWarn(ctx, "failed to unmarshal stored value, using default",
					log.String("namespace", namespace),
					log.String("key", nk.Key),
					log.Err(err),
				)
			} else {
				val = decoded
			}
		}

		entries = append(entries, ListEntry{
			Key:         nk.Key,
			Value:       val,
			Description: def.description,
		})
	}

	return entries, nil
}

// KeyDescription returns the human-readable description for a registered key.
func (c *Client) KeyDescription(namespace, key string) string {
	if c == nil || c.closed.Load() {
		return ""
	}

	nk := nskey{Namespace: namespace, Key: key}

	c.registryMu.RLock()
	def, registered := c.registry[nk]
	c.registryMu.RUnlock()

	if !registered {
		return ""
	}

	return def.description
}

// KeyRedaction returns the redaction policy for a registered key.
func (c *Client) KeyRedaction(namespace, key string) RedactPolicy {
	if c == nil || c.closed.Load() {
		return RedactNone
	}

	nk := nskey{Namespace: namespace, Key: key}

	c.registryMu.RLock()
	def, registered := c.registry[nk]
	c.registryMu.RUnlock()

	if !registered {
		return RedactNone
	}

	return def.redaction
}

// Logger returns the logger attached to this Client.
func (c *Client) Logger() log.Logger {
	if c == nil || c.logger == nil {
		return log.NewNop()
	}

	return c.logger
}
