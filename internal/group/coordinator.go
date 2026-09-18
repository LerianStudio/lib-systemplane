// Publication coordinator: the per-scope delivery core behind typed groups.
package group

import (
	"context"
	"slices"
	"strings"
	"sync"

	"github.com/LerianStudio/lib-observability/v4/log"
)

// Publication is one published revision of a group's document in one scope.
type Publication struct {
	Tenant   string
	Revision int64
	Value    any // the raw published document; the coordinator decodes it
}

// Decoded is one publication after decoding, as an applier receives it.
type Decoded[T any] struct {
	Tenant   string
	Revision int64
	Value    T
}

// ApplyFunc is what Register takes. previous is nil on the first delivery for
// the scope and is the last value THIS function accepted otherwise.
type ApplyFunc[T any] func(ctx context.Context, current Decoded[T], previous *Decoded[T]) error

// Status reports one scope's desired and applied revisions.
type Status struct {
	Tenant  string
	Desired int64
	Applied int64
	LastErr error
}

// observation is one decoded publication plus the coordinator-wide sequence
// number that orders it. The sequence, never the revision, drives delivery and
// coalescing: Revision 0 repeats legitimately (a delete, or a row that carries
// no revision) and would otherwise look like "nothing new".
type observation[T any] struct {
	seq   uint64
	value Decoded[T]
}

// scope is the per-tenant publication cache. delivering says a fan-out is
// running for this scope on some goroutine; it is read and written only under
// the coordinator's state mutex, which is what makes a publication recorded
// mid-fan-out impossible to miss.
type scope[T any] struct {
	current    observation[T]
	observed   bool
	delivering bool
	desired    int64
	lastErr    error
}

// applierScope is one applier's bookkeeping for one scope: the newest
// observation it has been offered, and the newest snapshot it accepted.
type applierScope[T any] struct {
	deliveredSeq uint64
	accepted     *Decoded[T]
}

type applier[T any] struct {
	id    uint64
	fn    ApplyFunc[T]
	state map[string]*applierScope[T]
}

// delivery is one applier's pending invocation for one observation, carried
// out of the state mutex so the applier runs without any lock held.
type delivery[T any] struct {
	ap       *applier[T]
	current  Decoded[T]
	previous *Decoded[T]
	err      error
}

// Coordinator holds the per-scope publication cache and the registered
// appliers of one group. It spawns no goroutines: a fan-out runs on the
// goroutine that published, or on the one that registered.
//
// Deliveries are serialized per scope and coalesced trailing-edge: a
// publication that arrives while a fan-out for the same scope is running
// replaces the pending one rather than queueing behind it, so an applier
// sees the newest document and may never see the intermediate ones. Scopes
// are independent of each other.
//
// An applier runs with no lock held, so it may call back into the coordinator:
// a Publish or a Register issued from inside an applier is recorded at once and
// delivered on the running fan-out's next iteration, on the same goroutine,
// after the current delivery returns. The one consumer bug this leaves is
// unbounded rather than deadlocked: an applier that publishes on EVERY delivery
// keeps the fan-out looping forever.
//
// A nil *Coordinator is safe: Publish and Register are no-ops and Status
// returns nil.
type Coordinator[T any] struct {
	logger log.Logger
	decode func(any) (T, error)
	seed   func() (Publication, bool)

	mu       sync.Mutex
	seq      uint64
	nextID   uint64
	scopes   map[string]*scope[T]
	appliers []*applier[T]
}

// NewCoordinator builds a coordinator. logger (may be nil) receives decode
// failures and applier panics; decode converts a published document into T;
// seed reads the group's current entry through the Client and reports ok=false
// when the Client does not yet track the scope. seed is consulted only by a
// Register that finds no observed publication at all.
func NewCoordinator[T any](
	logger log.Logger,
	decode func(any) (T, error),
	seed func() (Publication, bool),
) *Coordinator[T] {
	return &Coordinator[T]{
		logger: logger,
		decode: decode,
		seed:   seed,
		scopes: map[string]*scope[T]{},
	}
}

// Publish records pub as the newest state of its scope and delivers it to
// every registered applier, serialized per scope. A publication that arrives
// while a fan-out for the same scope is running replaces the pending one
// rather than queueing behind it.
//
// A document decode cannot fail into an applier: the failure is recorded on
// the scope and the last good publication stays replayable.
func (c *Coordinator[T]) Publish(ctx context.Context, pub Publication) {
	if c == nil || c.decode == nil {
		return
	}

	value, err := c.decode(pub.Value)

	c.mu.Lock()

	sc := c.scopeLocked(pub.Tenant)
	// Assignment, not max: a delete publishes Revision 0 and that IS the newest
	// state of the scope, so max would report a converged group as lagging.
	sc.desired = pub.Revision

	if err != nil {
		sc.lastErr = err
		c.mu.Unlock()

		return
	}

	c.seq++
	sc.current = observation[T]{
		seq:   c.seq,
		value: Decoded[T]{Tenant: pub.Tenant, Revision: pub.Revision, Value: value},
	}
	sc.observed = true

	c.mu.Unlock()

	c.drain(ctx, sc)
}

// Register adds fn and synchronously delivers the cached publication of every
// scope already observed. Returns a function that removes fn; calling it more
// than once, or from inside fn itself, is safe.
func (c *Coordinator[T]) Register(fn ApplyFunc[T]) func() {
	if c == nil || fn == nil {
		return func() {}
	}

	c.mu.Lock()

	c.nextID++
	id := c.nextID
	c.appliers = append(c.appliers, &applier[T]{id: id, fn: fn, state: map[string]*applierScope[T]{}})

	observed := make([]*scope[T], 0, len(c.scopes))

	for _, sc := range c.scopes {
		if sc.observed {
			observed = append(observed, sc)
		}
	}

	c.mu.Unlock()

	// The replay is the same code path as a publication, which is why a
	// concurrent fan-out can neither double-deliver nor invert the order.
	ctx := context.Background()
	for _, sc := range observed {
		c.drain(ctx, sc)
	}

	var once sync.Once

	return func() {
		once.Do(func() {
			c.remove(id)
		})
	}
}

// Status reports desired and applied revisions per observed scope, sorted by
// tenant.
func (c *Coordinator[T]) Status() []Status {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]Status, 0, len(c.scopes))

	for tenant, sc := range c.scopes {
		if !sc.observed {
			continue
		}

		out = append(out, Status{Tenant: tenant, Desired: sc.desired, LastErr: sc.lastErr})
	}

	slices.SortFunc(out, func(a, b Status) int { return strings.Compare(a.Tenant, b.Tenant) })

	return out
}

func (c *Coordinator[T]) remove(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, ap := range c.appliers {
		if ap.id == id {
			c.appliers = append(c.appliers[:i], c.appliers[i+1:]...)

			return
		}
	}
}

// scopeLocked resolves or creates a scope. The caller holds the state mutex.
func (c *Coordinator[T]) scopeLocked(tenant string) *scope[T] {
	sc, ok := c.scopes[tenant]
	if !ok {
		sc = &scope[T]{}
		c.scopes[tenant] = sc
	}

	return sc
}

// drain is the serialization, ordering and coalescing mechanism. One goroutine
// at a time fans a scope out; every other caller records its observation and
// returns, leaving the running fan-out to pick it up on its next iteration.
func (c *Coordinator[T]) drain(ctx context.Context, sc *scope[T]) {
	c.mu.Lock()

	if sc.delivering {
		c.mu.Unlock()

		return
	}

	sc.delivering = true

	for {
		observed := sc.current
		pending := c.pendingLocked(sc, observed)

		if len(pending) == 0 {
			sc.delivering = false
			c.mu.Unlock()

			return
		}

		c.mu.Unlock()

		for i := range pending {
			pending[i].err = pending[i].ap.fn(ctx, pending[i].current, pending[i].previous)
		}

		c.mu.Lock()
		recordLocked(sc, pending)
	}
}

// pendingLocked collects the appliers that have not been offered obs yet and
// marks them as offered BEFORE the invocation, because a rejection is never
// retried. The caller holds the state mutex.
func (c *Coordinator[T]) pendingLocked(sc *scope[T], observed observation[T]) []delivery[T] {
	pending := make([]delivery[T], 0, len(c.appliers))

	for _, ap := range c.appliers {
		st := ap.scopeState(observed.value.Tenant)
		if st.deliveredSeq >= observed.seq {
			continue
		}

		st.deliveredSeq = observed.seq

		pending = append(pending, delivery[T]{ap: ap, current: observed.value, previous: st.previous()})
	}

	return pending
}

// recordLocked writes back what each applier did with its delivery. The caller
// holds the state mutex.
func recordLocked[T any](sc *scope[T], pending []delivery[T]) {
	for i := range pending {
		d := &pending[i]
		st := d.ap.scopeState(d.current.Tenant)

		if d.err != nil {
			sc.lastErr = d.err

			continue
		}

		accepted := d.current
		st.accepted = &accepted
	}
}

func (a *applier[T]) scopeState(tenant string) *applierScope[T] {
	st, ok := a.state[tenant]
	if !ok {
		st = &applierScope[T]{}
		a.state[tenant] = st
	}

	return st
}

// previous copies the last accepted snapshot, so an applier that mutates what
// it receives cannot corrupt the coordinator's bookkeeping.
func (a *applierScope[T]) previous() *Decoded[T] {
	if a.accepted == nil {
		return nil
	}

	out := *a.accepted

	return &out
}
