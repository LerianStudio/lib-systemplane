package engine

import "context"

// Registry is the engine's read-only view of the Client's key registry.
// internal/client implements it; the engine never imports internal/client.
type Registry interface {
	// Lookup returns the registered definition for (namespace, key).
	// ok is false for an unregistered key, which the engine skips.
	Lookup(namespace, key string) (KeyDef, bool)
	// Keys returns every registered key. Reconcile uses it to decide which
	// keys are absent from a List snapshot and must fall back to default.
	Keys() []NSKey
}

// KeyDef is the subset of a registered key the engine needs.
type KeyDef struct {
	// Default is the registered default value. The engine never mutates what
	// it receives and clones before caching or delivering; the Registry may
	// return its stored default directly.
	Default any
	// Validate rejects a decoded value at ingress. nil accepts anything.
	//
	// The context is the one the ingress itself runs under, and which context
	// that is differs per ingress by design. A value arriving through Publish
	// is validated with the WRITER's context — the one the consumer handed to
	// Set — so a validator may resolve a tenant, a locale or a policy from the
	// request that is writing. A value arriving from the changefeed re-read or
	// from a reconcile snapshot is validated with the engine's dispatch
	// context, which carries no tenant and no request: nothing of whatever
	// goroutine called Start survives into it. A validator that refuses
	// whenever the context lacks a tenant therefore refuses every stored row,
	// and the ingress contract decides what follows — the last value that
	// passed stays in force, or the registered default at Revision 0 when no
	// row was ever accepted, and the rejection is logged once per ingestion
	// attempt — so a flapping changefeed repeats that WARN once per
	// registered key per resync, rather than once for the life of the key.
	Validate func(context.Context, any) error
	// Redacted reports that the key was registered with a redaction policy
	// other than "none": its value is sensitive and must never reach a log
	// line. The engine needs the fact, not the policy — masking and hiding
	// are the same decision to a log stream, and rendering a value for an
	// admin response belongs to the Client, which owns the policy itself.
	Redacted bool
}

// NSKey identifies one registered key inside a scope.
type NSKey struct {
	Namespace string
	Key       string
}

// lookup is the registry read every ingress path goes through.
//
// A nil registry reports nothing registered rather than dereferencing nil.
// Config documents Registry as required and Start refuses an engine without
// one, but Publish runs on the CONSUMER's own goroutine — a Client that
// publishes before Start, or an engine assembled by hand, must degrade to "no
// key is known" instead of taking that goroutine down mid-Set.
func (e *Engine) lookup(namespace, key string) (KeyDef, bool) {
	if e.registry == nil {
		return KeyDef{}, false
	}

	return e.registry.Lookup(namespace, key)
}

// registeredKeys is the reconcile's view of the registry: with a nil one
// nothing is registered, so a snapshot's absent-key pass has nothing to
// announce and nothing to fall back to a default.
func (e *Engine) registeredKeys() []NSKey {
	if e.registry == nil {
		return nil
	}

	return e.registry.Keys()
}
