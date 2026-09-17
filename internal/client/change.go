// Published-state types for systemplane Client subscribers.
package client

// Change is one published revision of a registered key in one scope.
type Change struct {
	Tenant    string // "" in single-tenant mode
	Namespace string
	Key       string
	Revision  int64 // 0 when no row exists: Value is the registered default
	Value     any   // decoded and validated; the receiver owns this copy
}
