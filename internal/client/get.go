package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/engine"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// ListEntry is a single entry returned by [Client.List].
type ListEntry struct {
	Key         string
	Value       any
	Description string
}

// Get returns the current value for (namespace, key).
//
// In single-tenant mode it returns the value the engine has published (or the
// registered default when the engine has published nothing for the key yet).
// In multi-tenant mode it reads through the tenant database resolved from ctx,
// returning the registered default when the row is absent; on a tenant-managed
// Client that read also activates the tenant's scope, and later reads are
// served from it.
func (c *Client) Get(ctx context.Context, namespace, key string) (any, bool, error) {
	e, ok, err := c.getEntry(ctx, namespace, key)

	return e.Value, ok, err
}

// GetEntry resolves the caller's scope like Get. ok is false for an
// unregistered key. Revision, UpdatedAt and UpdatedBy describe the persisted
// row behind the value, zero when the registered default is in force. Stale is
// true while nothing confirms THIS key: before a single-tenant Start, while the
// changefeed is down or unreconciled, or while this key failed its re-read. A
// read served per request is never stale.
func (c *Client) GetEntry(ctx context.Context, namespace, key string) (e Entry, ok bool, err error) {
	e, ok, err = c.getEntry(ctx, namespace, key)

	return e, ok, err
}

// singleTenantEntry serves a registered key from the engine's published state.
func (c *Client) singleTenantEntry(namespace, key string, def keyDef) Entry {
	published, ok := c.engine.Lookup(store.Scope{}, engine.NSKey{Namespace: namespace, Key: key})
	if ok {
		// Returned verbatim: Entry is an alias of the engine's, the value is
		// already a private clone, and the revision and provenance are the
		// row's.
		return published
	}

	// A miss means the engine has published nothing for this key: before
	// Start, while Start is still bringing the scope up, or after a Start
	// whose first reconcile confirmed nothing. The registered default is what
	// reads serve, and it is Stale in every one of those cases — nobody has
	// confirmed it. A completed first reconcile publishes EVERY registered
	// key, so on a started, reconciled Client a registered key is never a miss
	// and this branch is never the answer.
	return Entry{
		Value: engine.Clone(def.defaultValue),
		Stale: true,
	}
}

// getEntry is the single read path behind Get and GetEntry.
func (c *Client) getEntry(ctx context.Context, namespace, key string) (Entry, bool, error) {
	if c == nil || c.closed.Load() {
		return Entry{}, false, ErrClosed
	}

	if ctx == nil {
		return Entry{}, false, ErrNilContext
	}

	nk := nskey{Namespace: namespace, Key: key}

	c.registryMu.RLock()
	def, registered := c.registry[nk]
	c.registryMu.RUnlock()

	if !registered {
		return Entry{}, false, nil
	}

	if !c.multiTenant {
		return c.singleTenantEntry(namespace, key, def), true, nil
	}

	return c.tenantEntry(ctx, nk, def)
}

// tenantScope is the scope a multi-tenant read may be cached in, and whether
// there is one: only a tenant-managed Client caches, and only a named tenant.
func (c *Client) tenantScope(ctx context.Context) (store.Scope, bool) {
	scope := c.scopeFor(ctx)

	return scope, c.tenantManaged && scope.Tenant != ""
}

// tenantEntry serves a multi-tenant read from the tenant's cached scope, or
// per request while it is not cached, starting its activation on the way.
func (c *Client) tenantEntry(ctx context.Context, nk nskey, def keyDef) (Entry, bool, error) {
	scope, cacheable := c.tenantScope(ctx)
	if cacheable {
		if e, ok := c.engine.Lookup(scope, engine.NSKey(nk)); ok {
			return e, true, nil
		}

		c.engine.Activate(scope)
	}

	// The zero scope, never the tenant's: the middleware-resolved database the
	// request was authorized (or refused, for a suspended tenant) against, which
	// the connector behind a named scope would bypass.
	entry, found, err := c.store.Get(ctx, store.Scope{}, nk.Namespace, nk.Key)
	if err != nil {
		return Entry{}, false, fmt.Errorf("systemplane: Get: %w", err)
	}

	if !found {
		return Entry{Value: engine.Clone(def.defaultValue)}, true, nil
	}

	value, accepted, err := c.readThrough(ctx, scope, nk, def, entry.Value)
	if err != nil {
		return Entry{}, false, err
	}

	if !accepted {
		return Entry{Value: value}, true, nil
	}

	// Stale stays false: this is the live row, read just now.
	return Entry{
		Value:     value,
		Revision:  entry.Revision,
		UpdatedAt: entry.UpdatedAt,
		UpdatedBy: entry.UpdatedBy,
	}, true, nil
}

// readThrough decodes a row read per request and grades it as every ingress
// does, so a read never serves a value Set refuses and a key never flips value
// as a tenant's cache warms. A refusal serves the registered default, accepted false.
func (c *Client) readThrough(ctx context.Context, scope store.Scope, nk nskey, def keyDef, raw []byte) (value any, accepted bool, err error) {
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		c.logRead(ctx, log.LevelError, "failed to unmarshal stored value",
			log.String("namespace", nk.Namespace),
			log.String("keyname", nk.Key),
			log.Err(err),
		)

		return nil, false, decodeErr(ctx, nk.Namespace, nk.Key, err)
	}

	if err := c.engine.RunValidator(ctx, scope, engine.NSKey(nk), def.validator, decoded); err != nil {
		c.logRead(ctx, log.LevelWarn, "stored value rejected by validator, serving the registered default",
			log.String("namespace", nk.Namespace),
			log.String("keyname", nk.Key),
			log.Err(err),
		)

		return engine.Clone(def.defaultValue), false, nil
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
// Accepts integer-valued float64, the only shape a number ever reaches a
// reader in: every value in force has been through JSON, whether it came from
// a store row or from the registered default Register canonicalises.
// Fractional float64 values, strings and other types return
// (0, false, ErrValidation), so a malformed value neither truncates silently
// nor reads as 0.
func (c *Client) GetInt(ctx context.Context, namespace, key string) (int64, bool, error) {
	e, ok, err := c.getEntry(ctx, namespace, key)
	if err != nil || !ok {
		return 0, ok, err
	}

	v := e.Value

	switch n := v.(type) {
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
	default:
		return 0, false, fmt.Errorf("%w: %s/%s: stored value is %T, want float64", ErrValidation, namespace, key, v)
	}
}

// GetDuration returns the value as a time.Duration.
//
// Accepts a parseable duration string (e.g. "30s") and float64 nanoseconds,
// which are the only two shapes a value ever reaches a reader in: every value
// in force has been through JSON, whether it came from a store row or from the
// registered default Register canonicalises. All other shapes — including
// unparseable strings — return (0, false, ErrValidation).
func (c *Client) GetDuration(ctx context.Context, namespace, key string) (time.Duration, bool, error) {
	e, ok, err := c.getEntry(ctx, namespace, key)
	if err != nil || !ok {
		return 0, ok, err
	}

	v := e.Value

	switch d := v.(type) {
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
// It serves what the engine has published for the caller's scope, falling back
// to the registered defaults; a multi-tenant scope that is not cached is read
// through the tenant database resolved from ctx, exactly as Get reads it.
func (c *Client) List(ctx context.Context, namespace string) ([]ListEntry, error) {
	if c == nil || c.closed.Load() {
		return nil, ErrClosed
	}

	if ctx == nil {
		return nil, ErrNilContext
	}

	// One hold of registryMu for the whole call: the definition travels with
	// the key, so neither list path takes a Client lock per key, and the
	// snapshot both paths work from is internally consistent.
	c.registryMu.RLock()

	keys := make([]registeredKey, 0, len(c.registry))

	for nk, def := range c.registry {
		if nk.Namespace == namespace {
			keys = append(keys, registeredKey{nskey: nk, def: def})
		}
	}

	c.registryMu.RUnlock()

	if len(keys) == 0 {
		return []ListEntry{}, nil
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i].Key < keys[j].Key
	})

	if !c.multiTenant {
		entries, _ := c.listFromEngine(store.Scope{}, keys)

		return entries, nil
	}

	scope, cacheable := c.tenantScope(ctx)
	if cacheable {
		if entries, cached := c.listFromEngine(scope, keys); cached {
			return entries, nil
		}

		c.engine.Activate(scope)
	}

	return c.listFromStore(ctx, scope, namespace, keys)
}

// registeredKey is a registered key travelling with its definition, captured
// in List's single walk of the registry.
type registeredKey struct {
	nskey

	def keyDef
}

// listFromEngine reads every key of scope through the engine, falling back to
// the registered default for one the engine has published nothing for, and
// reports whether every key was cached. ListEntry carries no revision, so the
// provenance the engine holds is dropped here on purpose.
//
// The engine is read outside registryMu — it takes locks of its own and must
// never be called under the Client's — which List already guarantees by
// releasing the lock before it calls here.
func (c *Client) listFromEngine(scope store.Scope, keys []registeredKey) (entries []ListEntry, cached bool) {
	entries = make([]ListEntry, 0, len(keys))
	cached = true

	for _, rk := range keys {
		published, ok := c.engine.Lookup(scope, engine.NSKey(rk.nskey))

		val := published.Value
		if !ok {
			val = engine.Clone(rk.def.defaultValue)
			cached = false
		}

		entries = append(entries, ListEntry{
			Key:         rk.Key,
			Value:       val,
			Description: rk.def.description,
		})
	}

	return entries, cached
}

// listFromStore reads namespace per request, through the zero scope for the
// reason tenantEntry states, and grades each row like tenantEntry does.
func (c *Client) listFromStore(ctx context.Context, scope store.Scope, namespace string, keys []registeredKey) ([]ListEntry, error) {
	stored, err := c.store.List(ctx, store.Scope{})
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

	for _, rk := range keys {
		val := engine.Clone(rk.def.defaultValue)

		if raw, ok := storedByKey[rk.Key]; ok {
			var err error
			if val, _, err = c.readThrough(ctx, scope, rk.nskey, rk.def, raw); err != nil {
				return nil, err
			}
		}

		entries = append(entries, ListEntry{
			Key:         rk.Key,
			Value:       val,
			Description: rk.def.description,
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

// Logger returns the logger attached to this Client.
func (c *Client) Logger() log.Logger {
	if c == nil || c.logger == nil {
		return log.NewNop()
	}

	return c.logger
}
