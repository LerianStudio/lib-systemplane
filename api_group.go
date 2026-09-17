package systemplane

import (
	"context"
	"fmt"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
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

// Snapshot returns the group's document in the caller's scope, decoded into T,
// with the revision and freshness of the value in force. Before any write it
// returns the registered defaults at Revision 0.
//
// Snapshot does NOT run the consumer's validate: whatever is in force already
// passed it on ingress, so a second call would be a callback per read that can
// never fail. A document that cannot decode into T returns an error wrapping
// [ErrValidation] and a zero Value — never a half-filled T. Through an
// engine-backed Client that path is unreachable, because a document that fails
// to decode cannot pass the registered validator either.
//
// Tenant is the tenant id carried by ctx, "" in single-tenant mode. Snapshot on
// a nil *Group returns ErrClosed; errors from the Client ([ErrClosed],
// [ErrNilContext], store errors) are returned unchanged.
func (g *Group[T]) Snapshot(ctx context.Context) (Snapshot[T], error) {
	if g == nil {
		return Snapshot[T]{}, ErrClosed
	}

	entry, ok, err := g.client.GetEntry(ctx, g.namespace, g.key)
	if err != nil {
		return Snapshot[T]{}, err
	}

	// A group's key is registered by construction, so !ok means the Client was
	// torn down underneath the group.
	if !ok {
		return Snapshot[T]{}, fmt.Errorf("%w: %s/%s", ErrUnknownKey, g.namespace, g.key)
	}

	value, err := group.Decode[T](entry.Value)
	if err != nil {
		return Snapshot[T]{}, fmt.Errorf("%w: %s/%s is not a %T: %w", ErrValidation, g.namespace, g.key, value, err)
	}

	return Snapshot[T]{
		Value:    value,
		Revision: entry.Revision,
		Tenant:   tmcore.GetTenantIDContext(ctx),
		Stale:    entry.Stale,
	}, nil
}

// Set writes value as the group's whole document in the caller's scope,
// attributing the change to actor. The write is last-write-wins across every
// field of the document — that single row is what makes a group atomic — so a
// caller changing one field writes the rest back unchanged.
//
// value is validated by the group's registered validator before it reaches the
// store, so a document validate rejects returns an error wrapping
// [ErrValidation] and persists nothing. Set before [Client.Start] returns
// ErrNotStarted; Set on a nil *Group returns ErrClosed. Errors from the Client
// are returned unchanged.
func (g *Group[T]) Set(ctx context.Context, value T, actor string) error {
	if g == nil {
		return ErrClosed
	}

	// value itself, not its canonical form: the facade marshals it and the
	// registered validator accepts a typed T directly.
	return g.client.Set(ctx, g.namespace, g.key, value, actor)
}
