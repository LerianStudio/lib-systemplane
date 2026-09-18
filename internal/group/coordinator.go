// Publication coordinator: the per-scope delivery core behind typed groups.
package group

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"runtime/debug"
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
	tenant     string
	current    observation[T]
	observed   bool
	delivering bool
	desired    int64
	lastErr    error

	// Seed watermark. A seed and the publication it anticipates are one
	// observation, not two: a recorded seed arms the watermark, and the next
	// publication for the scope disarms it, dropped only when it is proven to
	// be that same document (revision no newer AND equal marshalled bytes).
	// Bytes as well as revision because the wave-1 facade publishes every
	// revision as 0, where a revision-only rule would swallow a genuinely
	// different document.
	seedArmed bool
	seedRev   int64
	seedBytes []byte
}

// applierScope is one applier's bookkeeping for one scope: the newest
// observation it has been offered, and the revision and snapshot it last
// accepted. A rejection moves neither of the latter two.
type applierScope[T any] struct {
	deliveredSeq uint64
	appliedRev   int64
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

	mu        sync.Mutex
	seq       uint64
	nextID    uint64
	seedTaken bool
	scopes    map[string]*scope[T]
	appliers  []*applier[T]
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

	// The one publication a seed anticipates is the same observation as the
	// seed, so it is spent here rather than delivered a second time.
	if dropsAfterSeedLocked(sc, pub) {
		c.mu.Unlock()

		return
	}

	// Assignment, not max: a delete publishes Revision 0 and that IS the newest
	// state of the scope, so max would report a converged group as lagging.
	sc.desired = pub.Revision

	if err != nil {
		sc.lastErr = err
		c.mu.Unlock()

		c.logError(ctx, "systemplane.group: published document failed to decode",
			log.Err(err), log.String("tenant", pub.Tenant), log.Any("revision", pub.Revision))

		return
	}

	c.observeLocked(sc, pub, value)

	c.mu.Unlock()

	c.drain(ctx, sc)
}

// observeLocked records value as the scope's newest observation. The sequence
// number, never the revision, is what every dedupe and coalescing decision
// downstream reads. The caller holds the state mutex.
func (c *Coordinator[T]) observeLocked(sc *scope[T], pub Publication, value T) {
	c.seq++
	sc.current = observation[T]{
		seq:   c.seq,
		value: Decoded[T]{Tenant: pub.Tenant, Revision: pub.Revision, Value: value},
	}
	sc.observed = true
}

// dropsAfterSeedLocked spends the seed watermark on the first publication that
// follows a seed and reports whether that publication is the seed's own
// document arriving late. Anything else — a newer revision, or the same
// revision carrying different bytes — is delivered, and the ordinary rules
// resume from there, so a later Revision 0 delete still reaches its appliers.
// A marshal failure on either side means "not proven identical": deliver. The
// caller holds the state mutex.
func dropsAfterSeedLocked[T any](sc *scope[T], pub Publication) bool {
	if !sc.seedArmed {
		return false
	}

	sc.seedArmed = false

	if pub.Revision > sc.seedRev || sc.seedBytes == nil {
		return false
	}

	data, err := json.Marshal(pub.Value)
	if err != nil {
		return false
	}

	return bytes.Equal(data, sc.seedBytes)
}

// Register adds fn and synchronously delivers the cached publication of every
// scope already observed. When nothing has been observed at all it takes one
// seed and delivers that instead, which is what covers a publication still in
// flight on its way out of the engine. Returns a function that removes fn;
// calling it more than once, or from inside fn itself, is safe.
func (c *Coordinator[T]) Register(fn ApplyFunc[T]) func() {
	if c == nil || fn == nil {
		return func() {}
	}

	ctx := context.Background()

	c.mu.Lock()

	c.nextID++
	id := c.nextID
	c.appliers = append(c.appliers, &applier[T]{id: id, fn: fn, state: map[string]*applierScope[T]{}})

	seeded, seedErr := c.seedLocked()

	observed := make([]*scope[T], 0, len(c.scopes))

	for _, sc := range c.scopes {
		if sc.observed {
			observed = append(observed, sc)
		}
	}

	c.mu.Unlock()

	if seedErr != nil {
		c.logError(ctx, "systemplane.group: seeded document failed to decode",
			log.Err(seedErr), log.String("tenant", seeded.Tenant), log.Any("revision", seeded.Revision))
	}

	// The replay is the same code path as a publication, which is why a
	// concurrent fan-out can neither double-deliver nor invert the order.
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

// Status reports desired and applied revisions per scope the coordinator has
// seen a publication for, sorted by tenant.
//
// Desired is the newest revision observed, advancing even when coalescing meant
// no applier saw the intermediate ones and even when the publication was
// rejected at decode. Applied is the newest revision EVERY registered applier
// has accepted, so it means the document is in force everywhere; with no
// applier registered nothing can lag and the scope reads as converged. LastErr
// holds the last rejection and is nil whenever Applied equals Desired; the
// converse does not hold, because a delivery in flight leaves Desired ahead of
// Applied with no error and Status is a point-in-time read.
func (c *Coordinator[T]) Status() []Status {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]Status, 0, len(c.scopes))

	for tenant, sc := range c.scopes {
		applied := c.appliedLocked(sc)

		lastErr := sc.lastErr
		if applied == sc.desired {
			lastErr = nil
		}

		out = append(out, Status{Tenant: tenant, Desired: sc.desired, Applied: applied, LastErr: lastErr})
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

// seedLocked takes one seed when the coordinator has observed no publication at
// all — the window between the Client reconciling a scope and the first
// dispatch delivery landing, where a registration would otherwise replay
// nothing. ok=false means the Client does not track the scope yet (the
// pre-Start case): nothing is delivered, nothing is recorded, and the next
// registration tries again. A taken seed is an observation like any other, so
// an applier that accepts it sets the scope's desired and applied revisions to
// the seeded one, and it arms the watermark that spends the publication it
// anticipates.
//
// It returns the publication it took and the decode error, for the caller to
// log outside the mutex: a seed that fails to decode is recorded exactly like a
// published document that does, reaches no applier, and is never retried. The
// caller holds the state mutex.
func (c *Coordinator[T]) seedLocked() (Publication, error) {
	if c.seed == nil || c.seedTaken || c.decode == nil || c.anyObservedLocked() {
		return Publication{}, nil
	}

	pub, ok := c.seed()
	if !ok {
		return Publication{}, nil
	}

	c.seedTaken = true

	sc := c.scopeLocked(pub.Tenant)
	sc.desired = pub.Revision

	value, err := c.decode(pub.Value)
	if err != nil {
		sc.lastErr = err

		return pub, err
	}

	c.observeLocked(sc, pub, value)

	sc.seedArmed = true
	sc.seedRev = pub.Revision

	// Bytes that fail to marshal stay nil, which disarms the drop: the next
	// publication can then never be proven identical, so it is delivered.
	if data, marshalErr := json.Marshal(pub.Value); marshalErr == nil {
		sc.seedBytes = data
	}

	return pub, nil
}

// anyObservedLocked reports whether any scope has a publication behind it. The
// caller holds the state mutex.
func (c *Coordinator[T]) anyObservedLocked() bool {
	for _, sc := range c.scopes {
		if sc.observed {
			return true
		}
	}

	return false
}

// scopeLocked resolves or creates a scope. The caller holds the state mutex.
func (c *Coordinator[T]) scopeLocked(tenant string) *scope[T] {
	sc, ok := c.scopes[tenant]
	if !ok {
		sc = &scope[T]{tenant: tenant}
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
			pending[i].err = c.invoke(ctx, pending[i].ap.fn, pending[i].current, pending[i].previous)
		}

		c.mu.Lock()
		c.recordLocked(sc, pending)
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

// invoke runs one applier and turns every failure mode into an error: a
// returned error passes through, and a panic is recovered into one. The recover
// is the coordinator's own rather than a lib-observability helper because those
// swallow the recovered value, and FC-7 needs it as the scope's LastErr.
func (c *Coordinator[T]) invoke(
	ctx context.Context,
	fn ApplyFunc[T],
	current Decoded[T],
	previous *Decoded[T],
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("systemplane/group: apply function panicked: %v", recovered)

			c.logError(ctx, "systemplane.group: apply function panicked",
				log.Any("panic", recovered), log.String("stack", string(debug.Stack())))
		}
	}()

	return fn(ctx, current, previous)
}

func (c *Coordinator[T]) logError(ctx context.Context, msg string, fields ...log.Field) {
	if c.logger == nil {
		return
	}

	c.logger.Log(ctx, log.LevelError, msg, fields)
}

// recordLocked writes back what each applier did with its delivery: an
// acceptance moves that applier's applied revision and becomes its next
// previous, while a rejection leaves both untouched and is recorded on the
// scope. Nothing is ever retried — the drain marked the observation as offered
// before invoking. The scope's error clears as soon as every applier has caught
// up with the newest revision. The caller holds the state mutex.
func (c *Coordinator[T]) recordLocked(sc *scope[T], pending []delivery[T]) {
	for i := range pending {
		d := &pending[i]
		st := d.ap.scopeState(d.current.Tenant)

		if d.err != nil {
			sc.lastErr = d.err

			continue
		}

		accepted := d.current
		st.appliedRev = accepted.Revision
		st.accepted = &accepted
	}

	if c.appliedLocked(sc) == sc.desired {
		sc.lastErr = nil
	}
}

// appliedLocked is the minimum applied revision across the CURRENTLY registered
// appliers, so an unsubscribed applier stops holding the scope down. With no
// applier registered there is nothing that could lag and the scope is converged
// by definition. The caller holds the state mutex.
func (c *Coordinator[T]) appliedLocked(sc *scope[T]) int64 {
	if len(c.appliers) == 0 {
		return sc.desired
	}

	applied := int64(math.MaxInt64)

	for _, ap := range c.appliers {
		var revision int64

		if st, ok := ap.state[sc.tenant]; ok {
			revision = st.appliedRev
		}

		applied = min(applied, revision)
	}

	return applied
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
