package systemplane

import (
	"context"
	"fmt"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-systemplane/v4/internal/group"
)

var (
	// ErrApplyPanicked is what an OnApply function's panic becomes in
	// ApplyStatus.LastErr; the recovered value and the stack stay in the log.
	ErrApplyPanicked = group.ErrApplyPanicked
)

// Group binds a typed configuration document to one (namespace, key).
//
// A group is exactly one registered key whose stored value is the JSON
// document of T, so the atomicity of a group is the atomicity of one row.
type Group[T any] struct {
	client    *Client
	namespace string
	key       string

	// coordinator holds the per-scope publication cache and the registered
	// apply functions. It is built at Bind, so it is never nil on a group the
	// caller holds.
	coordinator *group.Coordinator[T]

	// subscribeErr is the refusal OnChange returned at Bind, reported by
	// OnApply. A multi-tenant Client without a tenant manager refuses the
	// subscription and must still get a working Snapshot and Set, so Bind
	// records the error instead of failing.
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
// ingress the client validates. In single-tenant mode that includes the row
// already in the store: the engine grades it on the reconcile that follows
// every changefeed (re)connect, the first of which runs at Start, so a row that
// entered the store without passing the registered validator never becomes the
// group's document — the registered defaults stay in force and
// [Group.Snapshot] returns them with no error. A multi-tenant per-request read
// grades the tenant row the same way and serves the registered defaults for a
// refused one.
// Must be called before c.Start.
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
// The validate parameter is the group's validator. A [WithValidator] or a
// [WithContextValidator] passed in opts is ignored: both set the same single
// validator, and Bind appends its own [WithValidator] after opts, so whichever
// of the two a caller passes is replaced and cannot disable the type check on
// their own group. Every other key option in opts is forwarded to
// [Client.Register] unchanged.
//
// Bind also takes the group's one subscription to (namespace, key). It is taken
// here, before c.Start and therefore before any publication can exist, which is
// what lets [Group.OnApply] promise that no revision falls between its initial
// delivery and its subscription. A Client that refuses the subscription — a
// multi-tenant one without a tenant manager — still yields a working handle: the refusal is recorded and
// returned by OnApply, while [Group.Snapshot] and [Group.Set] keep working.
//
// That subscription is never released. A group has no Close, so nothing
// could ever call the unsubscribe, and the subscription therefore lives as long
// as the Client does; [Client.Close] does not drop subscribers either, it
// cancels the context they are delivered under.
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
		// validate runs, and a typed nil — a nil pointer, a nil map, a raw
		// JSON null — is unmasked as the null document it marshals to, which
		// a check against the incoming any would let straight through.
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
	// the last writer wins, so a caller's own WithValidator — or
	// WithContextValidator, which sets the same single validator — would
	// otherwise silently disable it for their own group.
	keyOpts := make([]KeyOption, 0, len(opts)+1)
	keyOpts = append(keyOpts, opts...)
	keyOpts = append(keyOpts, WithValidator(ingress))

	// Register runs the registered validator against the registered default,
	// so the defaults are validated here exactly once.
	if err := c.Register(namespace, key, canonical, keyOpts...); err != nil {
		return nil, err
	}

	g := &Group[T]{client: c, namespace: namespace, key: key}

	// The Client's mode decides the tenant stamp on the coordinator's reports.
	// Register above succeeded, so CatalogKey knows the key and TenantScoped
	// reports the mode verbatim.
	detail, _ := c.CatalogKey(namespace, key)

	g.coordinator = group.NewCoordinator[T](c.Logger(), g.namespace, g.key, detail.TenantScoped,
		group.Decode[T], g.seedCurrentEntry)

	// The group's one subscription, taken here — before Start, and therefore
	// before any publication can exist. That is the structural half of
	// OnApply's "no revision can fall between the initial delivery and the
	// subscription"; the coordinator's seed watermark is the other half.
	//
	// The unsubscribe is discarded: a group has no Close, so the subscription
	// outlives every caller of it.
	_, subscribeErr := c.OnChange(namespace, key, g.publish)
	g.subscribeErr = subscribeErr

	return g, nil
}

// publish is the group's OnChange handler: it carries one published change of
// the group's key into the coordinator, which decodes it and fans it out to the
// registered appliers. The tenant travels Change.Tenant -> Publication.Tenant
// -> Decoded.Tenant -> Applied.Tenant, and this is the only hop the root
// package owns, so it is a method rather than a closure to keep that hop
// testable in-package without a live multi-tenant feed.
func (g *Group[T]) publish(ctx context.Context, ch Change) {
	g.coordinator.Publish(ctx, group.Publication{Tenant: ch.Tenant, Revision: ch.Revision, Value: ch.Value})
}

// seedCurrentEntry reads the scope's current entry for a registration that
// finds no publication cached yet — the window after Start returns and before
// the engine's dispatch has delivered the scope's first publication. Entry.Stale
// is how the group asks whether the Client already tracks the scope: a stale
// entry means the scope has not reconciled, which is the pre-Start case, and
// nothing is seeded.
//
// A read that FAILS is not "nothing to seed": it is a registration that will
// receive no initial delivery and no later one either, on a key nobody may
// write again, so the error travels back to the registrant instead of being
// collapsed into silence.
//
// The read uses context.Background() because OnApply takes no context, so a
// seeded snapshot carries the single-tenant scope.
//
// A tenant-scoped key is the one case with nothing to seed by construction:
// its document belongs to a tenant, and every tenant's own arrives as a
// publication of its scope, so there is no single document in force for a
// context carrying no tenant to read. Reading anyway would either refuse for
// want of a tenant database or, worse, deliver the zero scope's document as
// though it were every tenant's. The registration takes the live deliveries
// only. CatalogKey is how the facade asks; on a closed Client it reports no
// key at all, and the read below is then the right answer either way, because
// it fails with ErrClosed and that travels back to the registrant.
//
// The question the gate actually asks is "is this Client multi-tenant".
// CatalogKey answers it only because TenantScoped is a catalog PRESENTATION
// field that reports the Client's mode verbatim, and it pays a deep clone of
// the registered default to return one bool. The gate is still safe on every
// known=false answer. Bind calls Register before it hands this closure to
// NewCoordinator, so the key is registered by the time the coordinator can
// invoke it and an unregistered key never reaches here. And a multi-tenant
// Client that falls through does not serve the zero scope: its GetEntry fails
// closed with ErrTenantConnectionMissing, because context.Background()
// carries no tenant database.
func (g *Group[T]) seedCurrentEntry() (group.Publication, bool, error) {
	if detail, known := g.client.CatalogKey(g.namespace, g.key); known && detail.TenantScoped {
		return group.Publication{}, false, nil
	}

	entry, ok, err := g.client.GetEntry(context.Background(), g.namespace, g.key)
	if err != nil {
		return group.Publication{}, false, err
	}

	if !ok || entry.Stale {
		return group.Publication{}, false, nil
	}

	return group.Publication{Revision: entry.Revision, Value: entry.Value}, true, nil
}

// Snapshot returns the group's document in the caller's scope, decoded into T,
// with the revision and freshness of the value in force. Before any write it
// returns the registered defaults at Revision 0.
//
// Snapshot does NOT run the consumer's validate: every document it returns
// already passed the group's ingress, in force or graded on a per-request read,
// so a second call would be a callback per read that can never fail. A row the
// ingress refuses, a stored null included, leaves the last valid document in
// force, or the registered defaults when there is none; a per-request read
// serves the defaults.
//
// A JSON null is the document only when the zero T is itself nil. Snapshot then
// returns that nil Value with no error, so a caller of a pointer-, map- or
// slice-shaped group must guard Value rather than dereference it.
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

	// Defensive guard: GetEntry reports !ok only for an unregistered key, and a
	// group's own key is registered by construction, so this cannot fire.
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

// Applied is delivered to an OnApply function for the newest published
// revision per scope, serially; revisions published while fn runs are
// coalesced into the next delivery. Previous is the snapshot fn last accepted
// for that scope, nil until it has accepted one.
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
//
// Value can be nil on a pointer-, map- or slice-shaped T. A JSON null is a
// legitimate document for such a group, and decoding turns it into the zero T,
// so an applier for one of these types must guard Value rather than
// dereference it: a nil dereference panics, and the panic is recovered and
// recorded as that revision's rejection in [Group.Status]'s LastErr, which
// leaves the group unconverged rather than crashing the process.
type Applied[T any] struct {
	Snapshot[T]
	Previous *Snapshot[T]
}

// ApplyStatus reports one scope's desired and applied revisions.
//
// Desired is the newest revision published for the scope, advancing even when
// coalescing meant no applier saw the intermediate ones. Applied is the revision
// accepted by the function furthest behind, so it means the document is in force
// everywhere; with no function registered nothing can lag and the scope reads as
// converged. LastErr holds the last rejection until every function still
// registered has accepted the newest publication, whether by accepting it or
// by the one holding the scope back unsubscribing. Unregistering every
// function never clears it, including the last one unsubscribing from inside
// its own delivery, so a group nobody applies keeps reporting its last
// rejection rather than reading healthy while it is being torn down.
//
// Desired equal to Applied is not convergence on its own: a delete and a key
// with no row both publish Revision 0, which is also the Applied of a function
// that has accepted nothing, so a function that refused everything can report
// the very revision the scope desires. Read LastErr for that rejection; with no
// function registered, nothing applies the document at all. A delivery in
// flight also leaves Applied behind with no error, because Status is a
// point-in-time read rather than a transaction.
type ApplyStatus struct {
	Tenant  string
	Desired int64 // latest published revision
	Applied int64 // latest revision fn accepted
	LastErr error // nil once every registered fn has accepted the newest published revision
}

// OnApply subscribes first and then delivers the current snapshot of every
// scope this group has observed, so no revision can fall between the initial
// delivery and the subscription; the same non-zero revision with the same
// value bytes is never delivered twice while its scope stays up (Revision 0
// may repeat). Later revisions arrive serialized and coalesced per
// scope; Status.Desired always names the newest published revision even when
// fn has not seen intermediate ones. fn returning an error records that
// revision as rejected for the scope (visible in Status) and keeps the
// previously applied revision as current; the engine does not retry. Before a
// single-tenant Start, OnApply registers and its initial delivery is Start's
// announcement, which may land either side of Start's return.
//
// fn runs with no lock held and may call [Group.Snapshot], [Group.Status],
// [Group.Set] or OnApply for its own group. A re-entrant OnApply appends its
// function; in the scope being delivered, that function's initial delivery is
// deferred to the running fan-out's next iteration, and an unsubscribe called
// before that iteration cancels it. A re-entrant Set that changes the document
// reaches the applier as a later delivery, after the current one returns, so
// an applier that changes it on every delivery keeps the group reloading
// forever.
// An error fn returns is logged at error level and published in
// [Group.Status]'s LastErr, held there as [ApplyStatus] describes, so it must
// name what was refused and must not embed the decoded document.
//
// The initial delivery — the replay, or the seeded one — runs on the calling
// goroutine, before OnApply returns, whenever no fan-out for that scope is in
// flight. When another goroutine is already fanning that scope out, OnApply
// returns without waiting and THAT fan-out delivers to the newly registered fn
// before it completes, because it re-reads the registered functions on every
// iteration. Which goroutine delivers never decides the context fn receives: a
// published snapshot carries the context the Client handed its subscribers when
// it published, unless that context is already cancelled — a replay long after
// its publisher moved on, [Client.Close] being the usual cause — in which case
// it carries the delivering goroutine's own, because a delivery is never
// retried and a hook that honours cancellation would otherwise reject the
// document for good. A seeded snapshot carries a background context.
// OnApply after Close registers and replays the last observed snapshot, and no
// further delivery can arrive.
//
// When nothing has been published for any scope yet, OnApply reads the document
// in force to deliver it, and a read that fails is returned: fn is registered,
// nothing is delivered, and the caller learns that hot reload is not live
// instead of running its compiled-in defaults until a write that may never come.
// Reading after [Client.Close] fails this way. A key with nothing stored is not
// a failure — the registered defaults are delivered once the Client tracks it.
// An open multi-tenant Client takes no such read.
//
// A nil fn registers nothing and returns no error, matching [Client.OnChange].
// unsubscribe is idempotent, is safe to call from inside fn itself, and
// releases that function's hold on the scope's applied revision. Once it has
// returned fn is not started again, including by a fan-out already under way
// that had not reached it; an invocation already running completes. A
// multi-tenant Client without a tenant manager returns
// ErrNotSupportedInMultiTenant, while [Group.Snapshot] and [Group.Set] keep
// working. On a tenant-managed Client nothing is seeded, because every
// document belongs to a tenant: fn gets the replay of every tenant this group
// has observed, one the Client has since dropped included, then each tenant's
// publications, the first on the read that activates it, with the tenant in
// Applied.Tenant, the ONLY tenant identity fn receives. The delivered ctx
// carries no tenant, so a re-read or a write-back runs under a tenant-scoped
// context the consumer owns, the one tenant-manager middleware builds. On a nil
// *Group it returns ErrClosed.
// unsubscribe is never nil, so a caller may defer it before checking err.
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

	// Register's unsubscribe is never nil, including on the error return, which
	// is what lets a caller defer it before checking err.
	return g.coordinator.Register(func(ctx context.Context, current group.Decoded[T], previous *group.Decoded[T]) error {
		a := Applied[T]{Snapshot: appliedSnapshot(current)}

		if previous != nil {
			earlier := appliedSnapshot(*previous)
			a.Previous = &earlier
		}

		return fn(ctx, a)
	})
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
