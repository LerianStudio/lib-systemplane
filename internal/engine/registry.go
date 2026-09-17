package engine

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
	// Default is the registered default value. The Registry returns a copy
	// the engine may keep.
	Default any
	// Validate rejects a decoded value at ingress. nil accepts anything.
	Validate func(any) error
}

// NSKey identifies one registered key inside a scope.
type NSKey struct {
	Namespace string
	Key       string
}
