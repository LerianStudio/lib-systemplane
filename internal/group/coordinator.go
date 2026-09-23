// Publication coordinator: the per-scope delivery core behind typed groups.
//
// Ordering is arrival order. Every publication is stamped with a sequence
// number as it enters Publish, and that sequence — never the revision — drives
// dedupe, coalescing and convergence. The coordinator can therefore keep what
// it is handed in order, but it cannot repair a caller that hands it one key's
// callbacks out of order, which the current Client's debounce dispatch can do
// from its timer goroutines. The engine-backed Client closes that gap by
// serializing per scope and key in revision order before it publishes.
//
// Nothing is pruned. A scope entry and every registered function's bookkeeping
// for it live as long as the coordinator, so a tenant the Client has stopped
// serving keeps its row in Status. Releasing a scope needs a DropScope hook
// called from the Client's tenant teardown, which the engine-tenants lane owns.
package group

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"
	"sync"

	"github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
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
	seq uint64

	// ctx is the context the publisher supplied. An observation is routinely
	// delivered by a goroutine other than the one that published it — a fan-out
	// already running for the scope picks it up, and a later registration
	// replays it — so the context travels with the observation instead of being
	// taken from whoever happens to deliver it. A seed has no publisher and
	// carries the background context of the goroutine that registered.
	ctx context.Context

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

	// latestSeq is the sequence of the newest observation of this scope,
	// INCLUDING one that failed to decode and therefore never became current.
	// It is what convergence is measured against, so a document nobody could
	// apply keeps the scope unconverged instead of being forgotten the moment
	// a later registration replays the last good one.
	latestSeq uint64

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
// observation it has been offered, the observation it last accepted, and the
// revision and snapshot that observation carried. A rejection moves none of the
// last three.
//
// acceptedSeq, not appliedRev, is what says how far this applier has got.
// Revisions cannot: a delete publishes Revision 0, and every publication the
// wave-1 facade makes carries Revision 0, so an applier that has accepted
// nothing and one that has accepted everything report the same number.
type applierScope[T any] struct {
	deliveredSeq uint64
	acceptedSeq  uint64
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
	seed   func() (Publication, bool, error)

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
// when the Client does not yet track the scope. A seed that cannot read at all
// returns its error instead, and Register hands that error to the registrant.
// seed is consulted only by a Register that finds no observed publication at
// all.
func NewCoordinator[T any](
	logger log.Logger,
	decode func(any) (T, error),
	seed func() (Publication, bool, error),
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

	if ctx == nil {
		ctx = context.Background()
	}

	// The observation sequence is stamped here, at ingress, so that arrival
	// order is what orders a scope's observations. Decoding runs outside the
	// mutex because it is the consumer's codec, which means an older
	// publication can finish decoding after a newer one has already committed;
	// commit discards it there rather than letting the scope move backwards.
	seq := c.stamp()

	value, err := c.decode(pub.Value)

	sc := c.commit(ctx, pub, seq, value, err)

	// Logged before the nil check, not after: a malformed document that is also
	// superseded, or spent by the seed watermark, records nothing on any scope,
	// so the log line is the only place it is ever named. Every document that
	// cannot be parsed is named exactly once.
	if err != nil {
		c.logError(ctx, "systemplane.group: published document failed to decode",
			log.Err(err), log.String(constants.AttrKeyTenantID, pub.Tenant), log.Any("revision", pub.Revision))

		return
	}

	if sc == nil {
		return
	}

	c.drain(ctx, sc)
}

// stamp draws the next observation sequence at ingress, before the decode the
// caller is about to run outside the lock.
func (c *Coordinator[T]) stamp() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.nextSeqLocked()
}

// commit stores pub as the scope's newest state and returns the scope to drain,
// or nil when the publication is not worth delivering: superseded by a newer
// one that committed first, or spent by the seed watermark. The unlock is
// deferred rather than manual because this critical section runs code the
// consumer owns — json.Marshal on its own document, reached through the
// watermark — and a panic in it must not leave the group's mutex held: every
// later Publish and every Status would then block forever, silently, with hot
// reload stopped and no signal anywhere. The decode failure is logged by the
// caller, outside the lock, for the same reason: the logger is the consumer's
// too.
func (c *Coordinator[T]) commit(
	ctx context.Context,
	pub Publication,
	seq uint64,
	value T,
	decodeErr error,
) *scope[T] {
	c.mu.Lock()
	defer c.mu.Unlock()

	sc := c.scopeLocked(pub.Tenant)

	// A publication stamped before one that has already committed is an
	// intermediate revision that arrived late: coalescing permits skipping it,
	// and skipping is what keeps Desired from moving backwards and keeps
	// observations committing in the order they arrived. Checked before the
	// watermark so a publication nobody will ever see cannot spend it.
	if seq < sc.latestSeq {
		return nil
	}

	// The one publication a seed anticipates is the same observation as the
	// seed, so it is spent here rather than delivered a second time.
	if dropsAfterSeedLocked(sc, pub) {
		return nil
	}

	// Assignment, not max: a delete publishes Revision 0 and that IS the newest
	// state of the scope, so max would report a converged group as lagging.
	sc.desired = pub.Revision

	if decodeErr != nil {
		// The failure counts as an observation even though it never becomes
		// current: no applier can have accepted it, so the scope stays
		// unconverged and the error stays readable until a document that does
		// decode is accepted by everyone. Observed with it, so a later Register
		// spends no read seeding a scope that has already been heard from and
		// decodes no unparseable document twice; the replay it triggers finds
		// nothing pending, because current is still whatever last decoded.
		sc.latestSeq = seq
		sc.lastErr = decodeErr
		sc.observed = true

		return sc
	}

	c.observeLocked(ctx, sc, pub, value, seq)

	return sc
}

// observeLocked records value as the scope's newest observation under seq and
// the context it will be delivered with, wherever it is finally picked up. The
// sequence number, never the revision, is what every dedupe and coalescing
// decision downstream reads. The caller holds the state mutex.
func (c *Coordinator[T]) observeLocked(ctx context.Context, sc *scope[T], pub Publication, value T, seq uint64) {
	sc.current = observation[T]{
		seq:   seq,
		ctx:   ctx,
		value: Decoded[T]{Tenant: pub.Tenant, Revision: pub.Revision, Value: value},
	}
	sc.observed = true
	sc.latestSeq = seq
}

// nextSeqLocked hands out the next observation sequence. The caller holds the
// state mutex.
func (c *Coordinator[T]) nextSeqLocked() uint64 {
	c.seq++

	return c.seq
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
// flight on its way out of the engine. The returned function removes fn, is
// never nil, and is safe to call more than once or from inside fn itself.
//
// The error is the seed READ's own. A registration whose initial read failed is
// registered and told so, rather than left running the consumer's compiled-in
// defaults believing hot reload is live while no delivery ever arrives. A seed
// that reports no entry is not an error — nothing is tracked yet — and a seed
// that fails to DECODE is recorded on its scope like any other document that
// cannot be applied, where Status reports it.
func (c *Coordinator[T]) Register(fn ApplyFunc[T]) (func(), error) {
	if c == nil || fn == nil {
		return func() {}, nil
	}

	ctx := context.Background()

	id, observed, seeded := c.add(fn)
	if seeded.decodeErr != nil {
		c.logError(ctx, "systemplane.group: seeded document failed to decode",
			log.Err(seeded.decodeErr),
			log.String(constants.AttrKeyTenantID, seeded.pub.Tenant),
			log.Any("revision", seeded.pub.Revision))
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
	}, seeded.readErr
}

// add appends fn, takes the seed when no publication has been observed yet, and
// reports the scopes worth replaying. The unlock is deferred rather than manual
// because this critical section runs code the consumer owns — the seed read,
// the group's decoder and json.Marshal on its own document — and a panic in it
// must not leave the group's mutex held: every later Publish and every Status
// would then block forever, silently, with hot reload stopped and no signal
// anywhere.
func (c *Coordinator[T]) add(fn ApplyFunc[T]) (uint64, []*scope[T], seedOutcome) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nextID++
	id := c.nextID
	c.appliers = append(c.appliers, &applier[T]{id: id, fn: fn, state: map[string]*applierScope[T]{}})

	seeded := c.seedLocked()

	observed := make([]*scope[T], 0, len(c.scopes))

	for _, sc := range c.scopes {
		if sc.observed {
			observed = append(observed, sc)
		}
	}

	return id, observed, seeded
}

// Status reports desired and applied revisions per scope the coordinator has
// seen a publication for, sorted by tenant.
//
// Desired is the newest revision observed, advancing even when coalescing meant
// no applier saw the intermediate ones and even when the publication was
// rejected at decode. Applied is the revision the applier furthest behind has
// accepted, so it means the document is in force everywhere; with no applier
// registered nothing can lag and the scope reads as converged. LastErr holds
// the last rejection and survives until every registered applier has accepted
// the scope's newest observation.
//
// Desired equal to Applied is therefore not convergence on its own: a delete
// publishes Revision 0 and every publication the wave-1 facade makes carries
// Revision 0, so an applier that refused everything reports the very revision
// the scope desires. Read LastErr. A delivery in flight also leaves Applied
// behind with no error at all, because Status is a point-in-time read rather
// than a transaction.
func (c *Coordinator[T]) Status() []Status {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]Status, 0, len(c.scopes))

	for tenant, sc := range c.scopes {
		out = append(out, Status{
			Tenant:  tenant,
			Desired: sc.desired,
			Applied: c.appliedLocked(sc),
			LastErr: sc.lastErr,
		})
	}

	slices.SortFunc(out, func(a, b Status) int { return strings.Compare(a.Tenant, b.Tenant) })

	return out
}

func (c *Coordinator[T]) remove(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, ap := range c.appliers {
		if ap.id != id {
			continue
		}

		c.appliers = append(c.appliers[:i], c.appliers[i+1:]...)

		// The applier that left may have been the one holding the scope back,
		// and its rejection no longer describes anybody still registered.
		for _, sc := range c.scopes {
			c.clearErrIfConvergedLocked(sc)
		}

		return
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
// It returns what the attempt produced, for the caller to act on outside the
// mutex: a read that failed goes back to the registrant, while a seed that
// fails to decode is recorded exactly like a published document that does,
// reaches no applier, and is never retried. A failed read takes no seed, so the
// next registration reads again. The caller holds the state mutex.
//
// Holding it across the seed is deliberate, and it is what lets commit's
// seq < latestSeq guard drop a publication stamped after the seed read, so the
// invariant it imposes on the Client is load-bearing: the seed closure reaches
// Client.GetEntry from inside this mutex, therefore the Client must never hold
// a lock across an OnChange dispatch, and must tolerate a subscriber calling
// Get or GetEntry re-entrantly. Verified 2026-09-23: refreshFromStore releases
// registryMu and cacheMu before fireSubscribers (internal/client/client.go).
func (c *Coordinator[T]) seedLocked() seedOutcome {
	if c.seed == nil || c.seedTaken || c.decode == nil || c.anyObservedLocked() {
		return seedOutcome{}
	}

	pub, ok, readErr := c.seed()
	if readErr != nil {
		return seedOutcome{readErr: readErr}
	}

	if !ok {
		return seedOutcome{}
	}

	c.seedTaken = true

	sc := c.scopeLocked(pub.Tenant)
	sc.desired = pub.Revision

	seq := c.nextSeqLocked()

	value, decodeErr := c.decode(pub.Value)
	if decodeErr != nil {
		// Observed, exactly as commit marks a published document that failed to
		// decode (A6): the rejection is what the coordinator heard from this
		// scope, so anyObservedLocked is the single truth about whether anything
		// has been heard at all and cannot disagree with the spent-seed flag.
		// current stays as it was, so the replay this arms finds nothing to
		// deliver and no applier is ever handed a document nobody could parse.
		sc.latestSeq = seq
		sc.lastErr = decodeErr
		sc.observed = true

		return seedOutcome{pub: pub, decodeErr: decodeErr}
	}

	// A seed has no publisher of its own: it is read and delivered by the
	// goroutine that registered, whose context is the background one.
	c.observeLocked(context.Background(), sc, pub, value, seq)

	sc.seedArmed = true
	sc.seedRev = pub.Revision

	// Bytes that fail to marshal stay nil, which disarms the drop: the next
	// publication can then never be proven identical, so it is delivered.
	if data, marshalErr := json.Marshal(pub.Value); marshalErr == nil {
		sc.seedBytes = data
	}

	return seedOutcome{pub: pub}
}

// seedOutcome is what one seed attempt produced: the publication it read, the
// error the READ itself returned, and the error decoding what it read. The two
// errors are mutually exclusive — a read that failed produced nothing to
// decode — and they are handled differently: the read error goes back to the
// registrant, the decode error is recorded on the scope and logged.
type seedOutcome struct {
	pub       Publication
	readErr   error
	decodeErr error
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
//
// ctx is only the fallback: every publication is delivered under the context
// its own publisher supplied, even when the goroutine that picks it up is
// another one's fan-out.
//
// The delivering flag is released on EVERY exit path, a panic included. The
// invocation below runs code the consumer owns — the applier, and the panic
// reporting that serializes through the consumer's logger — and a panic
// escaping it with the flag still set would stop hot reload for that scope
// forever, silently, while Desired kept advancing. The flag is cleared while
// the state mutex is still held, because a publisher records its observation
// under that same mutex and then drains: releasing the flag any earlier would
// leave a window where a publication is recorded and the fan-out has already
// decided it has nothing left to do, and a burst would silently drop its last
// item.
func (c *Coordinator[T]) drain(ctx context.Context, sc *scope[T]) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if sc.delivering {
		return
	}

	sc.delivering = true

	defer func() { sc.delivering = false }()

	for {
		observed := sc.current

		pending := c.pendingLocked(sc, observed)
		if len(pending) == 0 {
			return
		}

		c.deliver(deliveryCtx(ctx, observed), pending)
		c.recordLocked(sc, pending)
	}
}

// deliveryCtx is the context an observation is delivered under: the one its
// publisher supplied, or the draining goroutine's own when the observation has
// no publisher (a seed), when no observation has landed yet, or when the
// publisher's context is already cancelled.
//
// That last case is what keeps a replay usable. A registration replays an
// observation whose publisher is long gone — the Client hands its subscribers a
// context that Close cancels — and no delivery is ever retried, so handing a
// ctx-aware applier a dead context would reject the document permanently and
// leave the scope unconverged for good.
func deliveryCtx[T any](fallback context.Context, observed observation[T]) context.Context {
	if observed.ctx != nil && observed.ctx.Err() == nil {
		return observed.ctx
	}

	return fallback
}

// deliver runs each pending applier with no lock held and re-takes the state
// mutex before returning — on the panic path too, so the caller's deferred
// bookkeeping still runs under the lock and still releases it however an
// applier fails.
func (c *Coordinator[T]) deliver(ctx context.Context, pending []delivery[T]) {
	c.mu.Unlock()
	defer c.mu.Lock()

	for i := range pending {
		pending[i].err = c.invoke(ctx, pending[i].ap.fn, pending[i].current, pending[i].previous)
	}
}

// pendingLocked collects the appliers that have not been offered obs yet and
// marks them as offered BEFORE the invocation, because a rejection is never
// retried. The caller holds the state mutex.
//
// Bookkeeping is keyed by the SCOPE, never by the observation's own tenant:
// a scope observed only through a document that failed to decode still has the
// zero observation as its current one, and that carries no tenant at all, so
// keying on the payload would file this scope's state under the empty tenant.
func (c *Coordinator[T]) pendingLocked(sc *scope[T], observed observation[T]) []delivery[T] {
	pending := make([]delivery[T], 0, len(c.appliers))

	for _, ap := range c.appliers {
		st := ap.scopeState(sc.tenant)
		if st.deliveredSeq >= observed.seq {
			continue
		}

		st.deliveredSeq = observed.seq

		pending = append(pending, delivery[T]{ap: ap, current: observed.value, previous: st.previous()})
	}

	return pending
}

// ErrApplyPanicked is the error an apply function's panic becomes. It carries
// no part of the recovered value: a panic value is whatever the panicking hook
// was holding, routinely the decoded document with its endpoints and its
// credentials, and this error is a field operators read and log. The value and
// the stack are in the log line the panic handler writes, redacted in
// production mode.
var ErrApplyPanicked = errors.New("systemplane/group: apply function panicked; the value and stack are in the log line under group.apply")

// invoke runs one applier and turns every failure mode into an error: a
// returned error passes through, and a panic is recovered into one. The recover
// is the coordinator's own because the RecoverAndLog family swallows the
// recovered value and FC-7 needs a rejection in the scope's LastErr; the value
// itself never goes into that error — a panic value is whatever the panicking
// hook was holding, routinely the decoded document with its endpoints and
// credentials, and LastErr is a field operators read and log. The value is
// handed to runtime.HandlePanicValue, which is built for a panic recovered
// elsewhere and does not recover itself. That is what keeps a panicking
// hot-reload hook on the fleet's panic counter, on the publication's span and
// at the error reporter, and what redacts the value and the stack in
// production mode.
//
// Both failure modes are logged here, at error level, naming the scope and the
// revision: Status is a surface somebody has to think to read, while a
// configuration that stopped being applied is something an operator needs told.
func (c *Coordinator[T]) invoke(
	ctx context.Context,
	fn ApplyFunc[T],
	current Decoded[T],
	previous *Decoded[T],
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = ErrApplyPanicked

			// Two lines, because HandlePanicValue carries the recovered value
			// and the stack but neither the tenant nor the revision, and
			// redacts both in production mode: without this one the log says
			// something panicked and never says what stopped being applied.
			// The recovered value stays out of it — redaction lives with the
			// handler.
			c.logError(ctx, "systemplane.group: apply function panicked",
				log.String(constants.AttrKeyTenantID, current.Tenant),
				log.Any("revision", current.Revision))

			c.reportPanic(ctx, recovered)
		}
	}()

	err = fn(ctx, current, previous)
	if err != nil {
		c.logError(ctx, "systemplane.group: apply function rejected the published document",
			log.Err(err),
			log.String(constants.AttrKeyTenantID, current.Tenant),
			log.Any("revision", current.Revision))
	}

	return err
}

// reportPanic hands a recovered applier panic to lib-observability and refuses
// to let the reporting escape: HandlePanicValue serializes the recovered value
// through the consumer's logger, so a logger that panics would otherwise unwind
// out of invoke — past the error return that LastErr is built on, and out
// through the fan-out that recorded the scope as delivering.
//
// The logger goes in wrapped because HandlePanicValue LOGS the panic before it
// records the counter, the span event and the error report: a logger that
// panics on that line would take all three down with it, and the recover above
// would then swallow the unwind, leaving a panicking hot-reload hook visible
// nowhere but Status. Wrapping keeps the canonical handler and its production
// redaction while making the record step independent of the consumer's logger.
func (c *Coordinator[T]) reportPanic(ctx context.Context, recovered any) {
	defer swallowPanic()

	runtime.HandlePanicValue(ctx, safeLogger{inner: c.logger}, recovered, "systemplane", "group.apply")
}

// safeLogger is a logger whose Log cannot unwind into its caller. It exists for
// the panic handler above, which runs the consumer's logger ahead of everything
// else it does.
type safeLogger struct{ inner log.Logger }

func (l safeLogger) Log(ctx context.Context, level int, msg string, fields ...any) {
	if l.inner == nil {
		return
	}

	defer swallowPanic()

	l.inner.Log(ctx, level, msg, fields...)
}

func (c *Coordinator[T]) logError(ctx context.Context, msg string, fields ...log.Field) {
	if c.logger == nil {
		return
	}

	defer swallowPanic()

	c.logger.Log(ctx, log.LevelError, msg, fields)
}

// swallowPanic discards a panic raised by the consumer's own observability
// code. There is nowhere left to report it — the logger is what panicked — and
// the alternative is unwinding a publication, or a recovered applier panic,
// over a log line.
func swallowPanic() {
	_ = recover()
}

// recordLocked writes back what each applier did with its delivery: an
// acceptance moves that applier's accepted observation and applied revision and
// becomes its next previous, while a rejection leaves all three untouched and is
// recorded on the scope. Nothing is ever retried — the drain marked the
// observation as offered before invoking. The caller holds the state mutex.
func (c *Coordinator[T]) recordLocked(sc *scope[T], pending []delivery[T]) {
	for i := range pending {
		d := &pending[i]
		st := d.ap.scopeState(d.current.Tenant)

		if d.err != nil {
			sc.lastErr = d.err

			continue
		}

		accepted := d.current
		st.acceptedSeq = st.deliveredSeq
		st.appliedRev = accepted.Revision
		st.accepted = &accepted
	}

	c.clearErrIfConvergedLocked(sc)
}

// clearErrIfConvergedLocked drops the scope's error once every applier still
// registered has accepted its newest observation. What decides is the
// observation, never the revisions matching: through the wave-1 facade every
// publication carries Revision 0, so a revision test would erase each rejection
// the instant it was recorded and leave a consumer with no error surface at all.
// The caller holds the state mutex.
func (c *Coordinator[T]) clearErrIfConvergedLocked(sc *scope[T]) {
	// With nobody registered the loop below is vacuously true, and clearing on
	// it would erase a rejection nothing ever applied (A12: LastErr clears when
	// an applier ACCEPTS). The error waits for the next acceptance instead.
	// Both entry points reach this — an unsubscribe, and a delivery whose
	// applier unsubscribed itself before rejecting — so the guard lives here.
	if len(c.appliers) == 0 {
		return
	}

	for _, ap := range c.appliers {
		if st, ok := ap.state[sc.tenant]; !ok || st.acceptedSeq < sc.latestSeq {
			return
		}
	}

	sc.lastErr = nil
}

// appliedLocked is the revision accepted by the applier furthest behind among
// the CURRENTLY registered ones, so an unsubscribed applier stops holding the
// scope down and "applied" keeps meaning in force everywhere. Furthest behind is
// decided by observation and not by revision: a delete publishes Revision 0, so
// the lowest revision can be the newest thing the scope published, and reporting
// it while another applier still has the pre-delete document in force would call
// a half-applied delete convergence. With no applier registered there is nothing
// that could lag and the scope is converged by definition. The caller holds the
// state mutex.
func (c *Coordinator[T]) appliedLocked(sc *scope[T]) int64 {
	if len(c.appliers) == 0 {
		return sc.desired
	}

	oldest := uint64(math.MaxUint64)

	var applied int64

	for _, ap := range c.appliers {
		st, ok := ap.state[sc.tenant]
		if !ok {
			// An applier that has never been offered this scope is as far
			// behind as it gets.
			return 0
		}

		if st.acceptedSeq < oldest {
			oldest = st.acceptedSeq
			applied = st.appliedRev
		}
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
