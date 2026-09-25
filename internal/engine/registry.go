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
	// The engine runs it on the READ-BACK paths only: the changefeed re-read
	// and the reconcile snapshot, both under the engine's dispatch context,
	// which carries no request — nothing of whatever goroutine called Start
	// survives into it — and a tenant only as Config.ValidatorContext adds it
	// for a tenant scope. A validator that refuses whenever the context lacks
	// a tenant therefore refuses every stored row that reaches it without one,
	// and the ingress contract decides what follows — the last value that
	// passed stays in force, or the registered default at Revision 0 when no
	// row was ever accepted, and the rejection is logged once per ingestion
	// attempt — so a flapping changefeed repeats that WARN once per registered
	// key per resync, rather than once for the life of the key.
	//
	// A LOCAL write is graded by the Registry's owner instead, once, before
	// the store is written: Client.Set runs this same function against the
	// same canonical value under the WRITER's context — the one the consumer
	// handed to Set — so a validator may still resolve a tenant, a locale or a
	// policy from the request that is writing. Publish then does not run it
	// again; see its own documentation for why grading one write twice was a
	// correctness bug rather than a redundancy.
	Validate func(context.Context, any) error
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
