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

	// nullIsDocument records whether a JSON null is a legitimate document for
	// T, which is true exactly when the zero T is itself nil. Ingress refuses a
	// null for every other T; Snapshot refuses to decode one, so a row that
	// predates the guard cannot be read back as a wholly blank configuration.
	nullIsDocument bool

	// coordinator holds the per-scope publication cache and the registered
	// apply functions. It is built at Bind, so it is never nil on a group the
	// caller holds.
	coordinator *group.Coordinator[T]

	// unsubscribe releases the single OnChange subscription Bind took, so the
	// handle owns what it created. Nothing calls it today: a group has no
	// Close, and the Client's own teardown drops its subscribers.
	unsubscribe func()

	// subscribeErr is the refusal OnChange returned at Bind, reported by
	// OnApply. A multi-tenant Client refuses the subscription and must still
	// get a working Snapshot and Set, so Bind records the error instead of
	// failing.
	subscribeErr error
}

// Snapshot is the state of a group's document in one scope.
type Snapshot[T any] struct {
	Value    T
	Revision int64  // 0 when the row is absent
	Tenant   string // "" in single-tenant mode
	Stale    bool
}

// Bind registers (namespace, key) with defaults as the value in force when no
// row exists. validate (may be nil) becomes the key's registered validator: the
// client runs it on the default at Bind, on every Set, and on every other
// ingress the client validates. A row already in the store is decoded on read
// and never re-validated by the group, so a row that entered the store without
// passing the registered validator surfaces from [Group.Snapshot] as a decode
// error at worst, not as a validated document. Must be called before c.Start.
//
// The value registered is not defaults itself but its canonical JSON document:
// defaults marshaled and unmarshaled back into an any. A stored row, a Set
// write and the registered default therefore all have the same shape, and the
// document is always safely cloneable. The consequence to know is that
// validate sees the round-tripped document, so a field excluded with json:"-"
// is absent when validate runs — which is the honest semantics, because it
// validates what will actually be in force.
//
// A JSON null is refused on ingress unless the zero T is itself nil, because
// a null would otherwise decode to the zero T and blank every field of the
// group at once; the document in force stays in force instead. What decides is
// the canonical document, not the caller's Go value, so a typed nil pointer, a
// nil map and a nil slice are all refused for a struct-shaped group.
//
// When the zero T is nil — a pointer-, map-, slice- or interface-shaped group —
// a null IS a document, and both validate and [Group.Snapshot] can therefore
// see that nil. A validator for such a group must guard its argument rather
// than dereference it.
//
// The validate parameter is the group's validator. A [WithValidator] passed in
// opts is ignored: Bind's own validator is registered last and replaces it, so
// a caller cannot disable the type check on their own group. Every other key
// option in opts is forwarded to [Client.Register] unchanged.
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

	// A JSON null decodes to the zero T, so accepting one on ingress would
	// silently replace the whole document with zero values — through Set,
	// through the admin PUT, or from a row already holding null. It is a
	// legitimate document only when the zero T is itself nil (a pointer, map,
	// slice or interface shaped group), which is exactly when the zero
	// canonicalizes to nil. A type that cannot be canonicalized at all never
	// gets the exemption.
	var zero T

	zeroDocument, zeroErr := group.Canonical(zero)
	nullIsDocument := zeroErr == nil && zeroDocument == nil

	ingress := func(value any) error {
		// Canonicalize FIRST, so every ingress validates the document that
		// will actually be persisted rather than the caller's Go value. Two
		// things ride on it: a field excluded with json:"-" is absent when
		// validate runs (D-G1's stated semantics), and a typed nil — a nil
		// pointer, a nil map, a raw JSON null — is unmasked as the null
		// document it marshals to, which a check against the incoming any
		// would let straight through.
		document, canonicalErr := group.Canonical(value)
		if canonicalErr != nil {
			return canonicalErr
		}

		if document == nil && !nullIsDocument {
			return fmt.Errorf("null is not a %T document", zero)
		}

		decoded, decodeErr := group.Decode[T](document)
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

	g := &Group[T]{client: c, namespace: namespace, key: key, nullIsDocument: nullIsDocument}

	g.coordinator = group.NewCoordinator[T](c.Logger(), g.decodePublished, g.seedCurrentEntry)

	// The group's one subscription, taken here — before Start, and therefore
	// before any publication can exist. That is the structural half of FC-7's
	// "no revision can fall between the initial delivery and the
	// subscription"; the coordinator's seed watermark is the other half.
	unsubscribe, subscribeErr := c.OnChange(namespace, key, func(ctx context.Context, ch Change) {
		g.coordinator.Publish(ctx, group.Publication{Tenant: ch.Tenant, Revision: ch.Revision, Value: ch.Value})
	})
	if subscribeErr != nil {
		g.subscribeErr = subscribeErr
	} else {
		g.unsubscribe = unsubscribe
	}

	return g, nil
}

// decodePublished is the codec the coordinator decodes every publication with.
// It is not bare group.Decode: it repeats Snapshot's null rule, so a published
// null for a group whose zero T is not nil is recorded as a rejection instead
// of blanking the whole document through an applier.
func (g *Group[T]) decodePublished(value any) (T, error) {
	if value == nil && !g.nullIsDocument {
		var zero T

		return zero, fmt.Errorf("%w: %s/%s published a null document, which is not a %T", ErrValidation, g.namespace, g.key, zero)
	}

	return group.Decode[T](value)
}

// seedCurrentEntry reads the scope's current entry for a registration that
// finds no publication cached yet — the window after Start returns and before
// the engine's dispatch has delivered the scope's first publication. Entry.Stale
// is how the group asks whether the Client already tracks the scope: a stale
// entry means the scope has not reconciled, which is the pre-Start case, and
// nothing is seeded.
//
// The read uses context.Background() because OnApply takes no context, so a
// seeded snapshot carries the single-tenant scope.
func (g *Group[T]) seedCurrentEntry() (group.Publication, bool) {
	entry, ok, err := g.client.GetEntry(context.Background(), g.namespace, g.key)
	if err != nil || !ok || entry.Stale {
		return group.Publication{}, false
	}

	return group.Publication{Revision: entry.Revision, Value: entry.Value}, true
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
// A row holding a JSON null is refused the same way, unless the zero T is
// itself nil — in which case the null IS the document and Snapshot returns that
// nil Value with no error, so a caller of a pointer-, map- or slice-shaped
// group must guard Value rather than dereference it.
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

	// Defence in depth behind the ingress guard: a row holding a null predates
	// it (an older binary, another writer, a hand-edited row), and Decode turns
	// a null into the zero T by design (D-G2). Returning that would report a
	// wholly blank configuration as the one in force.
	if entry.Value == nil && !g.nullIsDocument {
		var zero T

		return Snapshot[T]{}, fmt.Errorf("%w: %s/%s holds a null document, which is not a %T", ErrValidation, g.namespace, g.key, zero)
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

// Applied is delivered to an OnApply function for the newest published
// revision per scope, serially; revisions published while fn runs are
// coalesced into the next delivery (FC-4). Previous is the snapshot fn last
// accepted for that scope, nil on the first delivery.
//
// A delivered Applied always carries Stale false. Staleness describes a read,
// while a publication is a value the engine has just observed; a subscriber
// that needs to know whether its scope is currently converged calls
// [Group.Snapshot].
//
// Tenant names the scope that published, which is not the same rule
// [Snapshot] follows on a read.
//
// Value is the one decoded document every applier of the group receives, and
// is what Previous carries on the next delivery, so an applier must treat it
// as read-only. A caller that wants an owned copy calls [Group.Snapshot],
// which decodes per call.
type Applied[T any] struct {
	Snapshot[T]
	Previous *Snapshot[T]
}

// ApplyStatus reports one scope's desired and applied revisions.
//
// Desired is the newest revision published for the scope, advancing even when
// coalescing meant no applier saw the intermediate ones. Applied is the newest
// revision every registered function has accepted, so it means the document is
// in force everywhere; with no function registered nothing can lag and the
// scope reads as converged. LastErr is nil whenever Applied equals Desired;
// the converse does not hold, because a delivery in flight leaves Desired
// ahead of Applied with no error.
type ApplyStatus struct {
	Tenant  string
	Desired int64 // latest published revision
	Applied int64 // latest revision fn accepted
	LastErr error // nil when Desired == Applied
}

// OnApply subscribes first and then delivers the current snapshot of every
// scope the Client already tracks, so no revision can fall between the
// initial delivery and the subscription; the same non-zero revision with the
// same value bytes is never delivered twice (Revision 0 is never deduplicated). Later revisions arrive
// serialized and coalesced per scope of this group (a group is one key, so
// this is the per-(scope, key) rule of FC-4); Status.Desired always names the
// newest published revision even when fn has not seen intermediate ones. fn returning an error records that revision as rejected for the
// scope (visible in Status) and keeps the previously applied revision as
// current; the engine does not retry. Before Start, OnApply registers and the
// initial delivery happens during Start.
//
// fn runs with no lock held and may call [Group.Snapshot], [Group.Status],
// [Group.Set] or OnApply for its own group: the resulting delivery runs after
// the current one returns, on the same goroutine. An applier that writes on
// every delivery therefore loops forever.
//
// OnApply blocks while a delivery for the same scope is in flight. The replay
// and the seeded delivery run on the calling goroutine with a background
// context; a delivery driven by a publication carries the context the Client
// hands its subscribers, which [Client.Close] cancels. OnApply after Close
// registers and replays the last observed snapshot, and no further delivery
// can arrive.
//
// A nil fn registers nothing and returns no error, matching [Client.OnChange].
// unsubscribe is idempotent, is safe to call from inside fn itself, and
// releases that function's hold on the scope's applied revision. In
// multi-tenant mode OnApply returns ErrNotSupportedInMultiTenant, while
// [Group.Snapshot] and [Group.Set] keep working; on a nil *Group it returns
// ErrClosed. unsubscribe is never nil, so a caller may defer it before
// checking err.
func (g *Group[T]) OnApply(fn func(ctx context.Context, a Applied[T]) error) (unsubscribe func(), err error) {
	noop := func() {}

	if g == nil {
		return noop, ErrClosed
	}

	if g.subscribeErr != nil {
		return noop, g.subscribeErr
	}

	if fn == nil {
		return noop, nil
	}

	return g.coordinator.Register(func(ctx context.Context, current group.Decoded[T], previous *group.Decoded[T]) error {
		a := Applied[T]{Snapshot: appliedSnapshot(current)}

		if previous != nil {
			earlier := appliedSnapshot(*previous)
			a.Previous = &earlier
		}

		return fn(ctx, a)
	}), nil
}

// Status reports the desired and applied revisions of every scope the group
// has observed a publication for, sorted by tenant. Status on a nil *Group
// returns nil.
func (g *Group[T]) Status() []ApplyStatus {
	if g == nil {
		return nil
	}

	observed := g.coordinator.Status()

	out := make([]ApplyStatus, len(observed))
	for i, st := range observed {
		// A struct conversion, not a field-by-field copy: it stops compiling
		// the moment the two shapes drift, which is the cheapest lock between
		// the contract here and the aggregation in internal/group.
		out[i] = ApplyStatus(st)
	}

	return out
}

// appliedSnapshot maps a delivered publication onto the snapshot shape. Stale
// is false by construction: a publication is a value the engine has just
// observed, never a read that could be lagging.
func appliedSnapshot[T any](d group.Decoded[T]) Snapshot[T] {
	return Snapshot[T]{Value: d.Value, Revision: d.Revision, Tenant: d.Tenant}
}
