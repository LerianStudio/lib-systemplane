// Read paths and listing for systemplane Client.
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
// In multi-tenant mode it resolves the tenant database from ctx and reads
// through, returning the registered default when the row is absent.
func (c *Client) Get(ctx context.Context, namespace, key string) (any, bool, error) {
	e, ok, err := c.getEntry(ctx, namespace, key)

	return e.Value, ok, err
}

// GetEntry resolves the caller's scope like Get. ok is false for an
// unregistered key. Revision, UpdatedAt and UpdatedBy describe the persisted
// row backing the value in force, and Stale reports whether anything is
// currently confirming THIS key: true while the changefeed is disconnected or
// has not been reconciled since it connected, and true while this key could not
// be re-read after its last change. A sibling key nobody could re-read leaves
// this one confirmed (FC-5).
func (c *Client) GetEntry(ctx context.Context, namespace, key string) (e Entry, ok bool, err error) {
	return c.getEntry(ctx, namespace, key)
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
	// confirmed it (FC-5). A completed first reconcile publishes EVERY
	// registered key (FC-11), so on a started, reconciled Client a registered
	// key is never a miss and this branch is never the answer.
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

	// Multi-tenant: resolve the tenant database from ctx and read through.
	// There is no in-process cache on this path, so a read always reflects
	// what the tenant row holds right now, including this caller's own write.
	entry, found, err := c.store.Get(ctx, store.Scope{}, namespace, key)
	if err != nil {
		return Entry{}, false, fmt.Errorf("systemplane: Get: %w", err)
	}

	if !found {
		return Entry{Value: engine.Clone(def.defaultValue)}, true, nil
	}

	var decoded any
	if err := json.Unmarshal(entry.Value, &decoded); err != nil {
		c.logError(ctx, "failed to unmarshal stored value",
			log.String("namespace", namespace),
			log.String("keyname", key),
			engine.ErrorDetail(def.redaction != RedactNone, "decode failed", err),
		)

		return Entry{}, false, decodeErr(ctx, namespace, key, def.redaction != RedactNone, err)
	}

	return Entry{
		Value:     decoded,
		Revision:  entry.Revision,
		UpdatedAt: entry.UpdatedAt,
		UpdatedBy: entry.UpdatedBy,
	}, true, nil
}

// withheldValueErr is a typed getter's rejection for a key registered redacted:
// what the value failed to be and its dynamic type, never the value itself.
//
// GetInt and GetDuration are the two getters whose message needs the value to
// be useful — which number is not whole, which string is not a duration — and
// they are the two that leaked it. An error travels further than a log line,
// into response bodies, error trackers and retry logs, so the same policy that
// keeps a value off a log line keeps it out of here, exactly as [decodeErr]
// does for an undecodable row. time.ParseDuration's own error quotes its input,
// so it is not wrapped either. The shape-mismatch branches never had the
// problem: they print %T and stop.
func withheldValueErr(namespace, key, want string, v any) error {
	return fmt.Errorf("%w: %s/%s: stored value is not %s (%T, value withheld: key registered redacted)",
		ErrValidation, namespace, key, want, v)
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
	v, ok, err := c.Get(ctx, namespace, key)
	if err != nil || !ok {
		return 0, ok, err
	}

	switch n := v.(type) {
	case float64:
		// JSON decodes all numbers as float64. Reject any value that would
		// lose precision when truncated to int64 (NaN, Inf, fractional).
		if n != float64(int64(n)) {
			if c.KeyRedaction(namespace, key) != RedactNone {
				return 0, false, withheldValueErr(namespace, key, "an integer", v)
			}

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
	v, ok, err := c.Get(ctx, namespace, key)
	if err != nil || !ok {
		return 0, ok, err
	}

	switch d := v.(type) {
	case string:
		parsed, parseErr := time.ParseDuration(d)
		if parseErr != nil {
			if c.KeyRedaction(namespace, key) != RedactNone {
				return 0, false, withheldValueErr(namespace, key, "a parseable duration", v)
			}

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
// single-tenant mode it serves what the engine has published, falling back to
// the registered defaults.
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

	if c.multiTenant {
		return c.listFromStore(ctx, namespace, keys)
	}

	return c.listFromEngine(keys), nil
}

// registeredKey is a registered key travelling with its definition, captured
// in List's single walk of the registry.
type registeredKey struct {
	nskey

	def keyDef
}

// listFromEngine reads every key through the engine, falling back to the
// registered default for one the engine has published nothing for. ListEntry
// carries no revision (FC-10), so the provenance the engine holds is dropped
// here on purpose.
//
// The engine is read outside registryMu — it takes locks of its own and must
// never be called under the Client's — which List already guarantees by
// releasing the lock before it calls here.
func (c *Client) listFromEngine(keys []registeredKey) []ListEntry {
	entries := make([]ListEntry, 0, len(keys))

	for _, rk := range keys {
		published, ok := c.engine.Lookup(store.Scope{}, engine.NSKey{Namespace: rk.Namespace, Key: rk.Key})

		val := published.Value
		if !ok {
			val = engine.Clone(rk.def.defaultValue)
		}

		entries = append(entries, ListEntry{
			Key:         rk.Key,
			Value:       val,
			Description: rk.def.description,
		})
	}

	return entries
}

func (c *Client) listFromStore(ctx context.Context, namespace string, keys []registeredKey) ([]ListEntry, error) {
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
			var decoded any
			if err := json.Unmarshal(raw, &decoded); err != nil {
				c.logError(ctx, "failed to unmarshal stored value",
					log.String("namespace", namespace),
					log.String("keyname", rk.Key),
					engine.ErrorDetail(rk.def.redaction != RedactNone, "decode failed", err),
				)

				return nil, decodeErr(ctx, namespace, rk.Key, rk.def.redaction != RedactNone, err)
			}

			val = decoded
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
