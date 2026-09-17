package systemplane

import (
	"fmt"

	"github.com/LerianStudio/lib-systemplane/v4/internal/group"
)

// Group binds a typed configuration document to one (namespace, key).
//
// A group is exactly one registered key whose stored value is the JSON
// document of T, so the atomicity of a group is the atomicity of one row.
type Group[T any] struct {
	client    *Client
	namespace string
	key       string
}

// Snapshot is the state of a group's document in one scope.
type Snapshot[T any] struct {
	Value    T
	Revision int64  // 0 when the row is absent
	Tenant   string // "" in single-tenant mode
	Stale    bool
}

// Bind registers (namespace, key) with defaults as the value in force when no
// row exists. validate (may be nil) runs on every ingress: defaults at Bind,
// Set, hydration, refresh, reconcile. Must be called before c.Start.
//
// The value registered is not defaults itself but its canonical JSON document:
// defaults marshaled and unmarshaled back into an any. A stored row, a Set
// write and the registered default therefore all have the same shape, and the
// document is always safely cloneable. The consequence to know is that
// validate sees the round-tripped document, so a field excluded with json:"-"
// is absent when validate runs — which is the honest semantics, because it
// validates what will actually be in force.
//
// Bind on a nil Client returns ErrClosed. Defaults that validate rejects
// surface as the ErrValidation that Register returns.
func Bind[T any](c *Client, namespace, key string, defaults T, validate func(T) error, opts ...KeyOption) (*Group[T], error) {
	if c == nil {
		return nil, ErrClosed
	}

	canonical, err := group.Canonical(defaults)
	if err != nil {
		return nil, fmt.Errorf("%w: defaults for %s/%s are not JSON-serializable: %w", ErrValidation, namespace, key, err)
	}

	ingress := func(value any) error {
		decoded, decodeErr := group.Decode[T](value)
		if decodeErr != nil {
			return decodeErr
		}

		if validate == nil {
			return nil
		}

		return validate(decoded)
	}

	// The type check goes LAST: applyKeyOptions applies options in order and
	// the last writer wins, so a caller's own WithValidator would otherwise
	// silently disable it for their own group.
	keyOpts := make([]KeyOption, 0, len(opts)+1)
	keyOpts = append(keyOpts, opts...)
	keyOpts = append(keyOpts, WithValidator(ingress))

	// Register runs the registered validator against the registered default,
	// so the defaults are validated here exactly once.
	if err := c.Register(namespace, key, canonical, keyOpts...); err != nil {
		return nil, err
	}

	return &Group[T]{client: c, namespace: namespace, key: key}, nil
}
