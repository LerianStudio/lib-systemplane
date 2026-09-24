// The Client's key registry, seen from the engine.
package client

import "github.com/LerianStudio/lib-systemplane/v4/internal/engine"

// The Client is the engine's registry: it owns Register and the map that
// Register fills, and the engine never imports internal/client.
//
// Neither method reaches the public surface. systemplane.Client is a defined
// type over this one, not an alias, so it inherits no methods.
var _ engine.Registry = (*Client)(nil)

// Lookup returns the registered definition of (namespace, key).
//
// The default is returned as it is stored, not copied: the port's contract
// says the engine never mutates what it receives and clones before caching or
// delivering, so a copy here would buy nothing and cost one per read. Redacted
// collapses RedactMask and RedactFull alike to true — the engine needs the
// fact that a value must never reach a log line, not the policy for rendering
// it, which stays here.
func (c *Client) Lookup(namespace, key string) (engine.KeyDef, bool) {
	if c == nil {
		return engine.KeyDef{}, false
	}

	c.registryMu.RLock()
	defer c.registryMu.RUnlock()

	def, ok := c.registry[nskey{Namespace: namespace, Key: key}]
	if !ok {
		return engine.KeyDef{}, false
	}

	return engine.KeyDef{
		Default:  def.defaultValue,
		Validate: def.validator,
		Redacted: def.redaction != RedactNone,
	}, true
}

// AnyRedacted reports whether any registered key carries a redaction policy.
//
// Scanned rather than counted: it is read only when the engine recovers a
// panic under a whole-scope snapshot, which is rare, and a counter maintained
// beside the map would have to be kept correct by every future writer of it
// for a saving nothing measures.
func (c *Client) AnyRedacted() bool {
	if c == nil {
		return false
	}

	c.registryMu.RLock()
	defer c.registryMu.RUnlock()

	for _, def := range c.registry {
		if def.redaction != RedactNone {
			return true
		}
	}

	return false
}

// Keys returns every registered key, in no particular order.
func (c *Client) Keys() []engine.NSKey {
	if c == nil {
		return nil
	}

	c.registryMu.RLock()
	defer c.registryMu.RUnlock()

	keys := make([]engine.NSKey, 0, len(c.registry))
	for nk := range c.registry {
		keys = append(keys, engine.NSKey{Namespace: nk.Namespace, Key: nk.Key})
	}

	return keys
}
