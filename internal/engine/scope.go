package engine

import (
	"sync"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// entry is the whole published state of one key: the value plus the
// provenance of the row backing it. Revision 0 means no row — the registered
// default is in force.
type entry struct {
	Value any
	// Raw is the row's JSON exactly as the store handed it over, kept beside
	// the decoded value so the fence can recognise the echo of a write by
	// comparing bytes instead of walking the decoded document. It is nil for a
	// publication that has no row behind it — a delete, or a reconcile-absent
	// publishing the registered default.
	//
	// The engine RETAINS the store's own slice here for the life of the cache
	// entry and compares it against every later publication of the key, so a
	// Store must return a slice it will never mutate afterwards. database/sql
	// decodes into a fresh *[]byte per row today; a pooled buffer or a
	// sql.RawBytes handed back instead would leave the fence comparing bytes
	// that changed underneath it, and a real configuration change whose new
	// text happened to land in the same buffer would be swallowed as an echo.
	//
	// Retaining it costs one copy of every key's row JSON, so the engine's
	// cache holds the registered keys times the tracked scopes in raw bytes on
	// top of the decoded values. Negligible single-tenant, where there is one
	// scope; tenant-managed, it is multiplied by the number of tenants the
	// process has activated.
	Raw       []byte
	Revision  int64
	UpdatedAt time.Time
	UpdatedBy string
}

// keyFence counts the two things a changefeed re-read must be able to notice
// across its store call: the deletes the key has taken in this scope, and the
// publications its cache entry has accepted.
//
// Both live BESIDE the entries map rather than inside an entry. A delete can
// arrive for a key nothing has published yet — before the first reconcile has
// run, or after a re-read of it failed — and inventing a cache entry to carry
// the count would put a nil value in force behind every read of that key.
//
// deletes is what makes a delete refusable at all. A delete publishes the
// registered default at Revision 0, as the public contract requires, so the
// revision alone cannot tell a re-read that the row it is holding has since
// been removed: every revision beats 0. It is bumped the instant the
// changefeed reports the row gone — at event ARRIVAL, before the key's quiet
// window, by recordFeedDelete — and by a Client Delete, which publishes on the
// caller's own goroutine for read-your-writes. The feed bumps it exactly once
// per event: the re-read that event schedules publishes through the ordinary
// no-row ingress and counts no second delete.
//
// A re-read reads both counters BEFORE its store call and hands them back
// afterwards. That is causal rather than numeric — it asks "did a delete land
// while I was reading?", not "is this revision high enough" — so it refuses a
// row read under a snapshot that predates the DELETE (READ COMMITTED gives a
// reader exactly that) while still accepting a recreate at any revision,
// including one below the deleted row's. The store brings a recreate through
// the library back strictly above every earlier revision, save a residual it
// documents, and the engine does not need to care.
//
// publications counts every publication the revision fence accepted, and is
// what the OTHER outcome of a delete's re-read is fenced on: a read that comes
// back EMPTY publishes the registered default at Revision 0, which wins
// unconditionally, so it must not land on top of a value published while it
// was reading — the echo of a Set the caller made right after the delete.
//
// A reconcile that finds a key absent bumps deletes for neither reason:
// absence from a photograph is a conclusion about a snapshot, and a re-read in
// flight may hold the fresher fact.
type keyFence struct {
	deletes      uint64
	publications uint64
}

// feedFence is what a changefeed re-read carries across its store call: the
// key's counters as they stood when the read began. The zero value is unarmed,
// and every ingress that is not a re-read passes it — a write's own value is
// never superseded by a delete it did not see.
type feedFence struct {
	keyFence

	armed bool
}

// fenceFor arms a fence for nk as it stands now. A key nothing has touched yet
// reads zero, which is what makes the FIRST delete of a key — the one that has
// no cached revision to fence against either — still refuse a re-read that
// started before it.
func (sc *scopeState) fenceFor(nk NSKey) feedFence {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return feedFence{keyFence: sc.fences[nk], armed: true}
}

// supersededByDelete reports whether a delete landed on nk since fence was
// armed, which makes whatever row the re-read is holding older than the cache.
//
// The caller holds sc.reconcileMu, which is what makes the answer and the
// publication it gates one step: PublishDelete holds the same lock across its own
// publish-and-record pair, so a delete can no longer land between this check
// and the publication it permitted.
func (sc *scopeState) supersededByDelete(nk NSKey, fence feedFence) bool {
	if !fence.armed {
		return false
	}

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return sc.fences[nk].deletes != fence.deletes
}

// supersededByPublication reports whether ANYTHING landed on nk since fence
// was armed — a publication, or another delete.
//
// It is the wider of the two questions, and only the empty re-read of a delete
// asks it. What that re-read is about to publish is the registered default at
// Revision 0, which no revision can refuse, so the ordinary fence cannot
// protect a value that arrived while the read was in flight. Anything at all
// having landed means the cache already holds a fact this read did not see,
// and the read's conclusion — "there is no row" — is the older one.
//
// The caller holds sc.reconcileMu, for the reason supersededByDelete states.
func (sc *scopeState) supersededByPublication(nk NSKey, fence feedFence) bool {
	if !fence.armed {
		return false
	}

	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return sc.fences[nk] != fence.keyFence
}

// bumpDeletes records that the changefeed reported nk's row removed, so every
// re-read already inside its store call is refused from this instant. It is
// deliberately not a publication: what goes in force for the key is decided by
// the re-read the event schedules, once the key's quiet window closes.
func (sc *scopeState) bumpDeletes(nk NSKey) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	fence := sc.fences[nk]
	fence.deletes++
	sc.fences[nk] = fence
}

// scopeState is one tracked scope.
type scopeState struct {
	scope store.Scope
	reads readOptions // cache_reads_total's options, built as it enters e.scopes

	mu      sync.RWMutex
	entries map[NSKey]entry
	// fences holds the per-key counters a changefeed re-read is graded
	// against. It is keyed like entries and guarded by the same mutex, but is
	// a map of its OWN because a key can be fenced before it is ever cached —
	// see keyFence. Entries are never removed: the count is bounded by the
	// registered keys of this scope.
	fences map[NSKey]keyFence
	// stale is the scope-wide flag: the changefeed is disconnected, or the
	// scope has not been reconciled since it (re)connected. It is set by a
	// disconnect and by every arming, and cleared only by a reconcile that
	// applied a snapshot under unmoved generations.
	//
	// It is no longer the whole of what a caller sees as Stale. A re-read that
	// failed twice belongs to ONE key, and raising a scope-wide flag for it
	// put the repair in the hands of whatever happened to clear the flag next:
	// a reconcile already in flight cleared it without ever having decided
	// that key, and on a connection that never drops nothing cleared it at
	// all, so the scope reported every converged value as unconfirmed for the
	// life of the process. unconfirmed carries that half instead.
	stale bool
	// unconfirmed holds the keys whose last change the engine could not read
	// back: a changefeed re-read that failed twice, and a retry that found no
	// row for an upsert. Entries are added by the terminal branch of each,
	// and removed by any later ingress that DECIDED the key — a re-read, a
	// Set echo, a delete publication — which is every path through publish,
	// plus the one reconcile outcome that decides a key without publishing: a
	// snapshot that finds it absent and agreeing with the cache
	// (markConfirmed).
	//
	// A reconcile's snapshot ROW is the one ingress that never decides it.
	// The photograph was taken at a moment nothing here knows and may predate
	// the very change the failed re-read was sent for — advancing the cache
	// does not prove otherwise — so applySnapshotRow puts the record straight
	// back for every key its window marked unusable.
	//
	// Lookup reports Stale for a key held in this set, and for that key only:
	// what was lost is one row nobody could re-read, and converging that key
	// is what takes it back. Reading the set's size instead made one
	// unreadable row report every sibling stale on a hold nothing but that key
	// releases. clearStale never touches it: a reconcile that skipped the key
	// decided nothing about it.
	//
	// Guarded by mu, alongside stale. Created lazily: most scopes never have
	// one.
	unconfirmed map[NSKey]struct{}
	// retrying holds the repairs already pending, so a failed re-read arms at
	// most ONE per key and op. Without it every failed read of a key the feed
	// is hot on scheduled a second store call a quarter of a second later,
	// with nothing deduplicating them: the pool checkouts for that key doubled
	// exactly while the pool was scarce, which is what made the reads fail to
	// begin with. With it the in-flight reads for one key stay capped at
	// THREE — a first attempt, the one repair an upsert asks for, and the one
	// a delete asks for.
	//
	// The op is half the key because it is half the answer: an empty read
	// means "not visible yet, keep the value" after an upsert and "the row is
	// gone" after a delete. Keyed by NSKey alone, an upsert's pending repair
	// refused the delete that followed it one of its own, and then answered
	// for it as an upsert — leaving the deleted row in force. Two upserts
	// still share one repair, which is the bound this field exists for.
	//
	// An entry is made before the retry goroutine is launched and removed by
	// that goroutine's own defer, so a launch that loses the race to Close
	// takes the entry back too and the next failure may still retry.
	//
	// Guarded by mu, alongside unconfirmed, and created lazily for the same
	// reason.
	retrying map[retryKey]struct{}
	// inflight holds the keys with a tracked re-read running; again, the keys
	// notified meanwhile, each owed one trailing read that is a delete if any
	// notification it stands for was. Guarded by mu.
	inflight map[NSKey]struct{}
	again    map[NSKey]bool
	// getSem holds one token per Store.Get this scope's re-reads have in flight.
	getSem chan struct{}
	// disconnectGen is bumped on every OpDisconnect. A reconcile records it
	// when it starts and clears stale only if it is unchanged at completion,
	// so a reconcile that spans a new disconnect cannot clear the flag that
	// disconnect just set. Guarded by mu, alongside stale.
	disconnectGen uint64

	// firstReconcileDone is closed exactly once, when this scope's first
	// reconcile finishes — successfully or not. Start waits on it instead of
	// running a reconcile of its own. firstReconcileErr carries that
	// reconcile's outcome to Start; it is written under mu BEFORE the channel
	// is closed, so a reader that observed the close is guaranteed to see it.
	// Both belong to the FIRST reconcile alone: no later reconcile writes
	// either one.
	firstReconcileDone chan struct{}
	firstReconcileOnce sync.Once
	firstReconcileErr  error // guarded by mu

	// resyncMu guards the single-slot reconcile mailbox below. One goroutine
	// per scope drains that slot, which is what serializes reconciles — two
	// OpResync events can never apply two snapshots at once — and what makes a
	// burst of reconnects coalesce into ONE pending reconcile instead of one
	// goroutine each. armReconcile still runs on the changefeed goroutine
	// and never waits on a List that is in flight.
	//
	// reconcileStop is closed when the scope is dropped, so the goroutine ends
	// with the scope instead of lingering until Close. workers, workerStarted
	// and workersDropped are guarded by Engine.workersMu, alongside the
	// WaitGroup the goroutines are registered in.
	//
	// workers holds this scope's delivery goroutines, one per key that has
	// actually published a change to a subscribed key. It lives on the state
	// rather than in one engine-wide map keyed by scope, for the same reason
	// workersDropped does: a scope dropped and brought back up is a NEW state,
	// so the drop's sweep reaches only the workers of the state it is ending,
	// and a re-activation racing that sweep keeps its own. The sweep also
	// touches only this scope's keys instead of walking every tenant's under
	// the lock each publication takes.
	//
	// workersDropped is set when this scope's delivery workers are swept, and
	// refuses every later one. A publisher still holding the old state is
	// refused forever while the new one starts workers freely.
	resyncMu       sync.Mutex
	resyncPending  *reconcileArming
	resyncSignal   chan struct{}
	reconcileStop  chan struct{}
	stopOnce       sync.Once
	workers        map[NSKey]*dispatchWorker
	workerStarted  bool
	workersDropped bool

	// reconcileMu guards reconcileGen and windows, and is held across every
	// check-and-publish pair on both sides of the fence: a reconcile deciding
	// one key, and the changefeed publishing one. That is what makes the two
	// atomic against each other — without it a feed delete lands between a
	// reconcile reading the fence and applying its snapshot row, and the
	// photograph resurrects the deleted key.
	//
	// windows holds one reconcileWindow per reconcile in flight, keyed by the
	// generation that opened it. Each reconcile reads only its own fences and
	// the feed writes into all of them, so two overlapping reconciles never
	// inherit each other's: a window that did would skip keys ITS OWN List
	// answered freshly.
	//
	// reconcileGen names the newest window. Every armReconcile bumps it, so
	// a reconcile whose generation no longer matches knows a newer OpResync
	// took the scope and abandons its snapshot instead of publishing a
	// photograph of a connection that has already dropped.
	reconcileMu  sync.Mutex
	reconcileGen uint64
	windows      map[uint64]*reconcileWindow

	unsubscribe func()
}

// refreshGetLimit caps the Store.Get calls one scope's re-reads hold at once, so
// a bulk delete of K keys queues K reads instead of checking out K connections.
// ponytail: fixed per-scope cap; an option if a consumer's pool sizing needs it
const refreshGetLimit = 8

// newScopeState begins tracking scope.
//
// The scope starts stale: until its first reconcile completes, nothing has
// confirmed the cache against the store. Its entries map starts EMPTY and is
// deliberately NOT pre-seeded with registered defaults — the first reconcile
// must publish every registered key as a FIRST publication, which is what
// makes the fence accept it and notify subscribers registered before Start.
// A Lookup miss is the signal the Client uses to fall back to the registered
// default, so reads before the first reconcile keep working.
func newScopeState(scope store.Scope) *scopeState {
	return &scopeState{
		scope:              scope,
		entries:            make(map[NSKey]entry),
		fences:             make(map[NSKey]keyFence),
		inflight:           make(map[NSKey]struct{}),
		again:              make(map[NSKey]bool),
		getSem:             make(chan struct{}, refreshGetLimit),
		workers:            make(map[NSKey]*dispatchWorker),
		stale:              true,
		firstReconcileDone: make(chan struct{}),
		resyncSignal:       make(chan struct{}, 1),
		reconcileStop:      make(chan struct{}),
	}
}

// markUnconfirmed records that nk's last change could not be read back, so
// reads of nk report Stale until some later ingress decides the key.
//
// It is per key rather than scope-wide because that is the size of what was
// actually lost: one row nobody could re-read. See the unconfirmed field.
func (sc *scopeState) markUnconfirmed(nk NSKey) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.unconfirmed == nil {
		sc.unconfirmed = make(map[NSKey]struct{}, 1)
	}

	sc.unconfirmed[nk] = struct{}{}
}

// markConfirmed takes back what markUnconfirmed recorded, for an ingress that
// decided nk without publishing anything: a reconcile whose snapshot found the
// key absent and agreeing with the cache. Every ingress that DOES publish
// clears the record inside publish itself, and a reconcile snapshot row whose
// publication did not advance the cache puts it straight back.
func (sc *scopeState) markConfirmed(nk NSKey) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	delete(sc.unconfirmed, nk)
}

// recordUnconfirmed records nk as unconfirmed unless a later ingress already
// decided it, which is the terminal verdict of a repair that ran out of
// attempts: a retry whose store call failed again, and a retry that found no
// row for an upsert.
//
// fence is the one the FIRST attempt armed, carried down into the retry. A
// re-read, a reconcile row, a Set echo or a delete that landed while the retry
// was waiting out its delay has already confirmed the key, and the retry's own
// arming happens after it and cannot see it. Recording the key unconfirmed on
// top of that convergence puts the scope back on a Stale nothing will ever
// clear — on a connection that never drops, for the life of the process, over
// a value that is correct.
//
// It takes reconcileMu itself, so the question and the record it gates are one
// step against the feed — the lock every other reader of a fence holds for the
// same reason.
func (sc *scopeState) recordUnconfirmed(nk NSKey, fence feedFence) {
	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	if sc.supersededByPublication(nk, fence) {
		return
	}

	sc.markUnconfirmed(nk)
}

// retryKey is one repair slot: a key and the op whose answer the repair is
// carrying. See the retrying field for why the op belongs in the key.
type retryKey struct {
	nk      NSKey
	deleted bool
}

// beginRetry claims the single retry slot rk has, reporting false when one is
// already pending. See the retrying field.
func (sc *scopeState) beginRetry(rk retryKey) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if _, pending := sc.retrying[rk]; pending {
		return false
	}

	if sc.retrying == nil {
		sc.retrying = make(map[retryKey]struct{}, 1)
	}

	sc.retrying[rk] = struct{}{}

	return true
}

// endRetry releases rk's retry slot, so the next failed re-read of that key
// and op may arm one again.
func (sc *scopeState) endRetry(rk retryKey) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	delete(sc.retrying, rk)
}

// beginRefresh claims nk's tracked re-read, reporting false when one is already
// running: that one then owes a trailing read, a delete if deleted.
func (sc *scopeState) beginRefresh(nk NSKey, deleted bool) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if _, running := sc.inflight[nk]; running {
		sc.again[nk] = sc.again[nk] || deleted

		return false
	}

	sc.inflight[nk] = struct{}{}

	return true
}

// endRefresh reports whether nk owes a trailing read and whether it is a
// delete, releasing the claim beginRefresh took when it owes none.
func (sc *scopeState) endRefresh(nk NSKey) (again, deleted bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if deleted, again = sc.again[nk]; again {
		delete(sc.again, nk)

		return true, deleted
	}

	delete(sc.inflight, nk)

	return false, false
}

// armReconcile opens a reconcile window and puts it in the scope's single-slot
// mailbox as ONE indivisible step, waking the reconcile goroutine. It runs on
// the changefeed goroutine, before the List. The send is non-blocking: a full
// buffer already means "there is work", and blocking here would push a
// reconcile's List latency onto the changefeed goroutine.
//
// The arming names the generations the reconcile must still see to be allowed
// to apply its snapshot and to clear stale.
//
// It returns the arming it displaced, if any. A displaced reconcile will never
// take a photograph, so the caller releases its window: fences nobody closes
// go on collecting every feed event for the life of the scope.
//
// Opening the window and queueing it are ONE function body, with no separately
// callable step between them, because two OpResync events that opened under
// one lock and queued under another could reach the mailbox in the opposite
// order to the one they opened in. The mailbox then held the OLDER arming —
// which the reconcile goroutine drops as superseded — while the newer window
// had already been released as the one it displaced. Nothing reconciled, and
// the scope stayed stale until another resync happened to arrive. With one
// body there is no seam to split: mailbox order is arming order.
//
// Every call gets its OWN empty fences, even while another reconcile is still
// applying a snapshot under a window of its own. Sharing them was the defect:
// the feed publishes revision 3 during the first window, the store moves to
// revision 9 during the second outage, and a second reconcile that inherited
// the first window's touched set skips the very row its own List went and
// fetched — leaving the cache six revisions behind and reporting it as fresh.
// Both windows stay open and the feed fills both, so neither photograph can
// overwrite a publication the feed made while it was being taken.
func (sc *scopeState) armReconcile() (displaced *reconcileArming) {
	sc.resyncMu.Lock()
	defer sc.resyncMu.Unlock()

	// One acquisition covers marking the scope stale AND bumping the
	// generation, so clearStale — which holds the same lock across its own
	// check-and-write — can never land between the two and clear a flag this
	// arming has just set.
	sc.reconcileMu.Lock()
	defer sc.reconcileMu.Unlock()

	sc.mu.Lock()
	sc.stale = true
	disconnect := sc.disconnectGen
	sc.mu.Unlock()

	sc.reconcileGen++

	if sc.windows == nil {
		sc.windows = make(map[uint64]*reconcileWindow, 1)
	}

	window := &reconcileWindow{
		touched:  make(map[NSKey]struct{}),
		unusable: make(map[NSKey]struct{}),
	}
	sc.windows[sc.reconcileGen] = window

	arm := reconcileArming{reconcile: sc.reconcileGen, disconnect: disconnect, window: window}
	displaced, sc.resyncPending = sc.resyncPending, &arm

	select {
	case sc.resyncSignal <- struct{}{}:
	default:
	}

	return displaced
}

// takeReconcile empties the mailbox, reporting whether anything was in it. A
// wake with an empty slot is normal: two submits can coalesce into one
// buffered signal.
func (sc *scopeState) takeReconcile() (reconcileArming, bool) {
	sc.resyncMu.Lock()
	defer sc.resyncMu.Unlock()

	if sc.resyncPending == nil {
		return reconcileArming{}, false
	}

	arm := *sc.resyncPending
	sc.resyncPending = nil

	return arm, true
}

// stopReconcileWorker ends this scope's reconcile goroutine. It is what a
// dropped scope uses: the scope is gone, so a goroutine still waiting for its
// next OpResync has nothing left to reconcile.
func (sc *scopeState) stopReconcileWorker() {
	sc.stopOnce.Do(func() { close(sc.reconcileStop) })
}
