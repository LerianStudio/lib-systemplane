// Change-stream and polling changefeeds for the MongoDB backend.
//
// One feed per scope: the zero-scope feed is opened by Start and lives until
// Close, and a named tenant's feed is opened by its first Subscribe and closed
// by its last unsubscribe. Multi-tenant deployments resolve a fresh database on
// every call, so the zero scope has no durable collection to watch there and
// Subscribe returns store.ErrNotSupportedInMultiTenant.
//
// Start opens the change stream SYNCHRONOUSLY, on the caller's goroutine,
// before it returns. That handshake is not a style choice: a change stream
// opened without a resume token attaches at the CURRENT oplog position, so a
// write that lands before the attach is never delivered — not late, never. The
// gap left by a LATER outage is covered instead by store.OpResync, which every
// (re)open announces so the engine reconciles the whole scope.
//
// The polling fallback runs the same handshake for the same reason: its FIRST
// round trip runs on the caller's goroutine and anchors the watermark every
// later query filters on, so a watermark anchored on a background goroutine
// nobody waits for would swallow every write that landed before it.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/LerianStudio/lib-commons/v7/commons/backoff"
	obsconstants "github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const operationTypeDelete = "delete"

// Bounds on one change-stream open and on shutdown. EVERY open is bounded by
// the same pair — the first one as much as a reopen — so an unreachable
// MongoDB fails Start within watchTimeout instead of parking it for the life
// of the caller's ctx and pinning the reserved feed slot with it. They are
// vars, not consts, only so the tests can shrink them; nothing in production
// writes them.
var (
	watchTimeout = 10 * time.Second
	closeTimeout = 5 * time.Second
)

// pollRoundTimeout bounds ONE poll round trip, the synchronous first one as
// much as a tick, so an unreachable MongoDB fails Start or Subscribe instead of
// parking it for the life of the caller's ctx.
const pollRoundTimeout = 5 * time.Second

// errFeedStopped ends a reopen loop that lost its race with teardown. It never
// reaches a caller.
var errFeedStopped = errors.New("systemplane/mongodb: changefeed stopped")

// errIdentityProbeFailed ends a reopen that resolved a tenant database whose
// server would not say who it is. Retryable: the next cycle probes again.
var errIdentityProbeFailed = errors.New("systemplane/mongodb: server identity probe failed")

// changeEvent is the subset of a MongoDB change stream event we decode.
type changeEvent struct {
	OperationType string `bson:"operationType"`
	DocumentKey   struct {
		ID compoundID `bson:"_id"`
	} `bson:"documentKey"`
	// FullDocument is the after-image, present for insert, update and replace
	// because the stream is opened with fullDocument: updateLookup. A raw
	// delete never carries one, which is why documentKey._id stays the
	// identity source and is never replaced by this.
	//
	// It is kept RAW on purpose. Decoding it into entryDoc here would make the
	// whole event — identity included — fail on one badly typed foreign field:
	// an operator who stored the value as a sub-document, or updated_at as a
	// string, would silently unsubscribe the engine from that key. FC-9 and D3
	// require the opposite, so classification reads the two fields it needs
	// out of this, field by field, and tolerates everything else.
	FullDocument bson.Raw `bson:"fullDocument"`
}

// feed is one changefeed for one scope. The zero-scope feed is created by
// Start and lives until Close.
type feed struct {
	scope store.Scope
	coll  *mongo.Collection

	// ready is closed exactly once, by the creator, when creation finishes:
	// with err nil the feed is live, with err non-nil creation failed and the
	// slot has already been retracted from the feeds map. err is written BEFORE
	// the close, so the close is the happens-before edge that publishes it.
	// Only a named tenant's feed has one. On the zero-scope feed ready is always
	// nil: Start alone brings that feed up, serialized by startMu, so nothing
	// waits on it, and stopFeeds relies on the nil to signal it rather than
	// fail it.
	// readyClosed guards the single close of ready. Both the creator and Close
	// can reach a reserved slot, so the flag lives under Store.feedsMu — the
	// lock both of them already take — not under f.mu.
	ready       chan struct{}
	err         error
	readyClosed bool

	// collID names the collection this feed watches, guarded by Store.feedsMu:
	// it is claimed there before the stream opens and re-claimed on every
	// reopen, so no two live feeds of this Store can watch one collection.
	// Separate from coll, which the reader goroutine owns and rewrites without
	// a lock.
	collID collIdentity

	// refs counts the callers holding this feed, guarded by Store.feedsMu (NOT
	// f.mu): the count decides the feed's lifetime and must be read and written
	// in the same lock hold that publishes or removes the map slot. Meaningful
	// only while the feed is in that map.
	refs int

	mu           sync.Mutex
	subs         map[uint64]*subscription
	nextID       uint64
	connected    bool // true between a successful open and the loss of that stream
	disconnected bool // true once OpDisconnect has been emitted for the CURRENT outage
	closing      bool // set by teardown under mu, BEFORE stop is closed

	// dispatching counts the deliveries the reader goroutine is currently
	// inside. A callback can reach teardown from there — unsubscribing itself
	// as the last subscriber of a tenant feed, or closing the store — and the
	// goroutine it would then wait on is the one running it, so the wait can
	// only ever end at the closeTimeout with the feed frozen meanwhile. While
	// this is non-zero a teardown signals and returns instead of waiting; the
	// reader still closes done on its way out.
	dispatching int

	stop chan struct{}
	done chan struct{}

	// pollStart is the state the feed's first poll round trip established,
	// handed to the reader goroutine that owns it from then on. Written by the
	// creator BEFORE the reader is launched — the goroutine start is the
	// happens-before edge — and never written again, so it needs no lock.
	// Unused on a change-stream feed.
	pollStart pollState
}

// subscription serializes delivery to one callback. sub.mu is held for the
// whole of fn, which is what lets Subscribe emit a joining subscriber's marker
// without racing the reader goroutine.
type subscription struct {
	mu sync.Mutex
	fn func(store.Event)
}

// newFeed builds an unconnected feed. done stays nil until its reader
// goroutine launches, which is what marks the feed as running.
func newFeed(scope store.Scope, coll *mongo.Collection) *feed {
	return &feed{
		scope: scope,
		coll:  coll,
		subs:  make(map[uint64]*subscription),
		stop:  make(chan struct{}),
	}
}

// label names the feed's scope in an error or log message: empty for the zero,
// single-tenant scope, " tenant <id>" for a named one.
func (f *feed) label() string {
	if f.scope.Tenant == "" {
		return ""
	}

	return " tenant " + f.scope.Tenant
}

// beginDisconnect decides, atomically with any concurrent teardown, whether
// this stream loss must emit OpDisconnect to the returned subscribers.
// It is EDGE-TRIGGERED: ok is true only on the connected→disconnected
// transition. ok is false when f.closing is already set (clean shutdown, see
// below) or when f.disconnected is already set (a reopen attempt failed while
// the feed was already known to be down — one outage, one disconnect).
// Teardown sets f.closing under f.mu before closing f.stop, so a teardown
// that wins the race is always visible here.
func (f *feed) beginDisconnect() (subs []*subscription, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closing || f.disconnected {
		return nil, false
	}

	f.connected = false
	f.disconnected = true

	return f.snapshotLocked(), true
}

// beginResync marks the feed connected again, clears f.disconnected so the
// next real loss can emit once more, and returns the subscribers to receive
// OpResync. Called after every successful (re)open of the change stream.
//
// Like beginDisconnect it is atomic with teardown: ok is false once f.closing
// is set, so a (re)open that completes just as Close lands announces nothing.
// A resync emitted then would tell the engine to reconcile a scope whose feed
// is already gone.
func (f *feed) beginResync() (subs []*subscription, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closing {
		return nil, false
	}

	return f.markConnectedLocked(), true
}

// beginResyncAfterOutage is the polling loop's variant: it announces ONLY on
// the disconnected→connected edge. A change stream hangs its announcement on a
// reopen, which happens once per outage; polling has no reopen — every tick is
// a fresh round trip — so announcing on each success would tell the engine to
// reload the scope forever.
func (f *feed) beginResyncAfterOutage() (subs []*subscription, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closing || !f.disconnected {
		return nil, false
	}

	return f.markConnectedLocked(), true
}

// markConnectedLocked records the connection and clears the outage flag so the
// next real loss can announce once more. The caller MUST hold f.mu.
func (f *feed) markConnectedLocked() []*subscription {
	f.connected = true
	f.disconnected = false

	return f.snapshotLocked()
}

// zeroFeedSlotLocked returns the zero-scope slot, creating it when Subscribe
// runs before Start, and refuses to hand it out — or resurrect it — once Close
// has begun. The caller MUST hold Store.feedsMu, which is what makes the check
// atomic with the map walk in stopFeeds: a Start or a Subscribe that already
// passed the s.closed check would otherwise re-insert a slot into a shut-down
// store, and nothing would ever tear it down again.
func (s *Store) zeroFeedSlotLocked() (*feed, error) {
	if s.closing {
		return nil, store.ErrClosed
	}

	if f, ok := s.feeds[""]; ok {
		return f, nil
	}

	f := newFeed(store.Scope{}, s.coll)
	s.feeds[""] = f

	return f, nil
}

// zeroFeedLocked is the SUBSCRIBE side of that slot: it reports the refusal a
// Start recorded instead of handing back a feed nothing is bringing up. The
// zero-scope slot is never retracted — its subscribers hold it and a retried
// Start must reopen the one they hold — so the recorded cause is the only thing
// that can tell a later Subscribe the changefeed never came up.
func (s *Store) zeroFeedLocked() (*feed, error) {
	f, err := s.zeroFeedSlotLocked()
	if err != nil {
		return nil, err
	}

	if f.err != nil {
		return nil, f.err
	}

	return f, nil
}

// collIdentity names the collection a feed watches: the SERVER that answers for
// it, plus the database and collection names. Comparable, so two feeds are told
// apart — or found to be the same — with ==.
//
// The server is identified by what it REPORTS, never by the *mongo.Client
// handle it is reached through. lib-commons' tenant manager opens one client
// per tenant id — commons/tenant-manager/mongo caches connections[tenantID]
// and calls mongo.Connect once per entry — so two tenants misconfigured onto
// one database arrive here as two distinct handles, and a pointer comparison
// would admit both: the very misconfiguration this refusal exists to catch,
// invisible on the only connector that ships. Mirrors serverDatabaseKey on the
// Postgres side, which asks the server rather than trusting the DSN text.
type collIdentity struct {
	server string
	db     string
	coll   string
}

// serverKey turns a hello reply into the identity two feeds are compared on.
//
// A replica set is keyed by its name and its member list, both of which every
// member reports identically, so two clients that landed on two members of one
// set still compare equal. Anything else — a standalone, a mongos — reports no
// set, and the per-process id in topologyVersion stands in: it is unique to a
// running mongod/mongos process, so two standalone servers are told apart and
// two clients of one are not.
//
// What this key CANNOT tell apart, stated rather than discovered:
//
//   - Two mongos routers fronting ONE sharded cluster. Each reports its own
//     process id, so two tenants routed through different routers to the same
//     database are admitted.
//   - A server restarted between two claims. The new process reports a new id,
//     so a feed that claimed before the restart no longer matches one claiming
//     after it. Named feeds re-claim on every reopen, which closes the window
//     they can actually reach.
//   - A replica set reconfigured between two claims. The key is the set name
//     plus the sorted member hosts, so adding or replacing a member changes
//     it, and a feed that claimed before the reconfiguration no longer matches
//     one claiming after it. Same mitigation as the restart above: named feeds
//     re-claim on every reopen.
//
// An empty key means the server said nothing that identifies it; the caller
// admits the feed rather than refuse on a guess.
func serverKey(setName string, hosts []string, processID bson.ObjectID) string {
	if setName != "" {
		members := slices.Clone(hosts)
		slices.Sort(members)

		return "rs:" + setName + "/" + strings.Join(members, ",")
	}

	if processID != bson.NilObjectID {
		return "proc:" + processID.Hex()
	}

	return ""
}

// collIdentityOf asks the collection's server who it is and returns the
// identity a claim is decided on.
//
// A server that cannot be reached, or that answers without identifying itself,
// yields the ZERO identity, which claims nothing and refuses nothing. That is
// deliberate: a server we cannot reach cannot be shown to be shared, and the
// very next thing the caller does on that handle — materialize the collection,
// open the change stream, run the first poll — fails against it anyway, so no
// unidentified feed is ever admitted into service. Refusing here instead would
// turn one unreachable moment into a cross-tenant misconfiguration report.
func collIdentityOf(ctx context.Context, coll *mongo.Collection) collIdentity {
	if coll == nil {
		return collIdentity{}
	}

	db := coll.Database()

	ctx, cancel := context.WithTimeout(ctx, watchTimeout)
	defer cancel()

	var reply struct {
		SetName         string   `bson:"setName"`
		Hosts           []string `bson:"hosts"`
		TopologyVersion struct {
			ProcessID bson.ObjectID `bson:"processId"`
		} `bson:"topologyVersion"`
	}

	// hello is the driver's own handshake command: runnable on any database
	// and answerable before authentication, so this costs one round trip and
	// no privilege.
	if err := db.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&reply); err != nil {
		return collIdentity{}
	}

	key := serverKey(reply.SetName, reply.Hosts, reply.TopologyVersion.ProcessID)
	if key == "" {
		return collIdentity{}
	}

	return collIdentity{server: key, db: db.Name(), coll: coll.Name()}
}

// claimFeedColl records the collection f is about to watch and REFUSES it when
// a live feed of this Store already watches that very collection.
//
// A change stream is opened on one collection, so two scopes sharing one would
// each receive the other's writes stamped with their OWN scope, and the
// engine's revision fence would treat a foreign revision as authoritative: the
// configuration a tenant reads becomes whichever tenant wrote last. Values are
// keyed per collection and never mix; the event stream is what crosses, which
// is why the check lives where a feed claims its collection.
//
// It is evaluated ONLY when a changefeed opens or reopens — reads and writes
// resolve straight through the connector and never compare databases — so a
// Store that never subscribes never learns that two of its scopes share a
// collection, exactly as on the Postgres side.
//
// The claim is released by the feed leaving the feeds map: a creation that
// fails retracts its slot, and the last subscriber to leave a tenant removes
// it. The zero-scope slot never leaves, which is correct — it is the store's
// own collection for as long as the store lives.
func (s *Store) claimFeedColl(ctx context.Context, f *feed, coll *mongo.Collection) error {
	return s.claimFeedIdentity(f, s.collIdentityFor(ctx, coll))
}

// collIdentityFor is collIdentityOf behind the identityProbe test seam.
func (s *Store) collIdentityFor(ctx context.Context, coll *mongo.Collection) collIdentity {
	if s.identityProbe != nil {
		// Test seam — see the identityProbe field.
		return s.identityProbe(ctx, coll)
	}

	return collIdentityOf(ctx, coll)
}

// claimFeedIdentity is claimFeedColl's decision, with the round trip already
// paid. Split out so the comparison — which pair of scopes is refused, which is
// admitted — is exercised without a server.
func (s *Store) claimFeedIdentity(f *feed, id collIdentity) error {
	s.feedsMu.Lock()
	defer s.feedsMu.Unlock()

	// A probe that could not identify the server refuses nothing — and changes
	// nothing either. The zero identity means "the server did not answer", not
	// "this feed has moved", so a feed that arrives here unidentified keeps
	// what it holds: clearing it opened a window in which a scope misconfigured
	// onto this collection was admitted while the holder's hello was timing
	// out, streamed the holder's rows as its own, and then owned the claim the
	// holder could never win back. A claim is released by the feed leaving the
	// feeds map or by a SUCCESSFUL re-claim that replaces it; refreshFeedColl
	// keeps f.coll and f.collID consistent by refusing to move either until a
	// probe answers.
	if id == (collIdentity{}) {
		return nil
	}

	for _, other := range s.feeds {
		// A feed that has not claimed a collection yet carries the zero
		// identity and can never match a real one.
		if other == f || other.collID != id {
			continue
		}

		return fmt.Errorf(
			"systemplane/mongodb: scope %q and scope %q both resolve to %s.%s: %w",
			f.scope.Tenant, other.scope.Tenant, id.db, id.coll, ErrSharedDatabaseUnsupported,
		)
	}

	f.collID = id

	return nil
}

// acquireFeed returns the live feed for scope — creating it when this caller is
// the first to ask for that tenant — and takes one reference on it, released by
// releaseFeed. Feeds are SHARED: a scope has exactly one change stream no
// matter how many subscribers it has.
//
// The feeds-map lock is never held across the connector call or coll.Watch, or
// one unreachable tenant would freeze every other tenant's Subscribe. The
// creator instead reserves the map slot with an unconnected placeholder
// carrying a ready channel, resolves and opens outside the lock, and then
// publishes or retracts it.
func (s *Store) acquireFeed(ctx context.Context, scope store.Scope) (*feed, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	s.feedsMu.Lock()

	// One shutdown fence for both branches, hoisted above them: a closing store
	// must not reserve a slot and resolve a tenant, which is exactly what the
	// named branch would do if only the zero scope were fenced.
	if s.closing {
		s.feedsMu.Unlock()

		return nil, store.ErrClosed
	}

	if scope.Tenant == "" {
		f, err := s.zeroFeedLocked()
		if err != nil {
			s.feedsMu.Unlock()

			return nil, err
		}

		f.refs++
		s.feedsMu.Unlock()

		return f, nil
	}

	if f, ok := s.feeds[scope.Tenant]; ok {
		f.refs++
		s.feedsMu.Unlock()

		return s.awaitFeed(ctx, f)
	}

	f := newFeed(scope, nil)
	f.ready = make(chan struct{})
	f.refs = 1
	s.feeds[scope.Tenant] = f
	s.feedsMu.Unlock()

	if err := s.createFeed(ctx, f); err != nil {
		return nil, err
	}

	return f, nil
}

// awaitFeed blocks until the creator publishes or retracts the feed.
//
// A creation failure reaches EVERY waiter carrying the creator's own cause: a
// waiter that blocked until its own ctx died instead would strand the caller.
// It is never retried here — the slot is already gone, so the next Subscribe
// for that tenant builds a fresh placeholder. On ctx cancellation the waiter
// leaves the creator alone; the creator finishes or retracts on its own.
func (s *Store) awaitFeed(ctx context.Context, f *feed) (*feed, error) {
	select {
	case <-f.ready:
		if f.err != nil {
			s.releaseFeed(f)

			return nil, f.err
		}

		return f, nil
	case <-ctx.Done():
		s.releaseFeed(f)

		return nil, ctx.Err()
	}
}

// createFeed resolves the tenant's collection through the connector and opens
// its first change stream synchronously, so an unreachable tenant fails the
// Subscribe call instead of looping in the background, then publishes the feed
// to every waiter.
//
// The connector is consulted again on every reopen, through refreshFeedColl:
// the tenant manager owns the client behind this handle and disconnects it on
// LRU eviction or a credentials swap, so a feed that kept the handle it was
// built with would reopen forever against a dead client.
func (s *Store) createFeed(ctx context.Context, f *feed) error {
	tenant := f.scope.Tenant

	db, err := s.cfg.Connector.ResolveDatabase(ctx, tenant)
	if err != nil {
		return s.retractFeed(f, fmt.Errorf("systemplane/mongodb: resolve tenant %s: %w", tenant, err))
	}

	// A nil handle reported with a nil error is a connector bug; refuse it here
	// rather than attach a stream to something that panics on the first command.
	if db == nil {
		return s.retractFeed(f, fmt.Errorf("systemplane/mongodb: resolve tenant %s: %w", tenant, store.ErrTenantConnectorMissing))
	}

	coll := db.Collection(collectionName)

	// Refused before anything is created or opened: a collection another live
	// scope already watches would deliver that scope's writes to this one too.
	if err := s.claimFeedColl(ctx, f, coll); err != nil {
		return s.retractFeed(f, err)
	}

	// A change stream attaches to a collection, so a tenant database that has
	// never been written to needs its collection materialized first.
	if err := s.ensureSchema(ctx, tenant, coll, true); err != nil {
		return s.retractFeed(f, err)
	}

	f.coll = coll

	// Polling serves a named tenant exactly as it serves the zero scope: the
	// first round trip replaces the first Watch, and its failure retracts the
	// slot the same way.
	if s.cfg.PollInterval > 0 {
		if err := s.startPolling(ctx, f); err != nil {
			return s.retractFeed(f, err)
		}

		return s.publishFeed(ctx, f, nil)
	}

	stream, err := s.openWatch(ctx, f)
	if err != nil {
		return s.retractFeed(f, err)
	}

	return s.publishFeed(ctx, f, stream)
}

// retractFeed publishes a creation failure to every waiter and removes the dead
// slot.
func (s *Store) retractFeed(f *feed, err error) error {
	s.feedsMu.Lock()
	defer s.feedsMu.Unlock()

	cause := s.failLocked(f, err)

	f.closeReadyLocked()

	return cause
}

// releaseFeed drops one reference. When the last one goes, a NAMED feed leaves
// the map and its reader is stopped. The zero-scope feed is exempt: Start owns
// it and it must survive an empty subscriber map. Deciding under feedsMu is
// what stops a concurrent Subscribe from attaching to a feed that is being torn
// down.
func (s *Store) releaseFeed(f *feed) {
	s.feedsMu.Lock()

	f.refs--

	if f.scope.Tenant == "" || f.refs > 0 || s.feeds[f.scope.Tenant] != f {
		s.feedsMu.Unlock()

		return
	}

	delete(s.feeds, f.scope.Tenant)
	s.feedsMu.Unlock()

	s.stopFeed(f)
}

// Subscribe registers fn to be invoked for every change event in scope. The
// returned unsubscribe func removes fn from the dispatch list, and the last
// subscriber to leave a tenant scope closes that tenant's change stream.
//
// A subscriber joining a feed that has already announced its state is told that
// state before anything else: store.OpResync on a connected feed,
// store.OpDisconnect on one in an announced outage. Without it a subscriber
// joining a quiet scope after Start — which is exactly what the engine does —
// would hear nothing and never reconcile.
//
// A feed that has announced nothing yet — created, not yet through its reader's
// first resync — emits nothing here: that resync is imminent and this
// subscriber is already in the map, so it receives that one. Announcing here
// too would double it.
//
// The zero scope in multi-tenant mode returns
// store.ErrNotSupportedInMultiTenant: every method there resolves a per-call
// tenant database, so there is no shared process-wide changefeed to attach to.
// A named tenant scope is served regardless of MultiTenantEnabled — it resolves
// its own database through the connector — and is refused with
// store.ErrTenantConnectorMissing when none is configured.
//
// The subscription lives for the lifetime of ctx: when ctx is cancelled the
// callback is removed and the feed released, exactly as if unsubscribe had been
// called.
func (s *Store) Subscribe(ctx context.Context, scope store.Scope, fn func(store.Event)) (func(), error) {
	if s == nil || s.isClosed() {
		return nil, store.ErrClosed
	}

	if scope.Tenant == "" {
		if s.cfg.MultiTenantEnabled {
			return nil, store.ErrNotSupportedInMultiTenant
		}
	} else if s.cfg.Connector == nil {
		return nil, store.ErrTenantConnectorMissing
	}

	// Checked before any feed work: a nil callback must never open a stream.
	if fn == nil {
		return func() {}, nil
	}

	f, err := s.acquireFeed(ctx, scope)
	if err != nil {
		return nil, err
	}

	sub := &subscription{fn: fn}

	// sub.mu is taken BEFORE the subscription becomes reachable and released
	// only when Subscribe returns. The reader goroutine can reach this
	// subscriber only after seeing it in f.subs, and any such delivery then
	// blocks here until the joining marker below has returned — so the joining
	// callback can never observe a key event before its own marker. The defer
	// is load-bearing on the panicking path: a manual Unlock skipped by an
	// unwinding callback would leave sub.mu held forever.
	sub.mu.Lock()
	defer sub.mu.Unlock()

	// The subscriber is added and the feed's announced state read in ONE hold,
	// which is what keeps the announcement exactly-once: whichever of this and
	// the reader's own beginResync/beginDisconnect runs second sees the other's
	// work, so the joiner is either announced to here or included in the
	// reader's broadcast, never both and never neither.
	f.mu.Lock()
	f.nextID++
	id := f.nextID
	f.subs[id] = sub
	joining := f.joiningOpLocked()
	f.mu.Unlock()

	// deliverLocked, never sub.fn directly: it puts the joining emission under
	// the same recovery guard as every reader-goroutine delivery, so a
	// panicking callback cannot escape through Subscribe to the caller. A
	// connection lost between the read above and this emission yields one
	// extra marker, which is harmless — both markers are idempotent.
	if joining != "" {
		sub.deliverLocked(s.cfg.Logger, store.Event{Scope: f.scope, Op: joining})
	}

	// cancelCh stops the ctx observer below. Every teardown action — removing
	// the callback, releasing the feed, stopping the observer — runs inside one
	// sync.Once, so a caller-driven unsubscribe racing ctx cancellation can
	// never double-release the feed or double-close the channel.
	cancelCh := make(chan struct{})

	var once sync.Once

	teardown := func() {
		once.Do(func() {
			f.mu.Lock()
			delete(f.subs, id)
			f.mu.Unlock()

			close(cancelCh)

			s.releaseFeed(f)
		})
	}

	// The observer exists only to watch a ctx that CAN end: a nil ctx would
	// panic on ctx.Done(), and context.Background() has no Done channel at all,
	// so both get no goroutine and the same unsubscribe func.
	//
	// s.closedCh is the third arm, and it is what bounds the observer's life by
	// the STORE's: a subscription whose ctx outlives the store (an engine root
	// ctx) would otherwise keep this goroutine — and the feed graph it closes
	// over — parked forever after Close. Teardown runs on that arm too, so the
	// subscriber slot goes with it.
	if ctx != nil && ctx.Done() != nil {
		go func() {
			// Same guard the reader goroutine carries: teardown releases the
			// feed, and a panic here would take the process down from a
			// goroutine no caller can recover for.
			defer runtime.RecoverAndLog(s.cfg.Logger, "systemplane.mongodb.subscriber")

			select {
			case <-ctx.Done():
				teardown()
			case <-s.closedCh:
				teardown()
			case <-cancelCh:
			}
		}()
	}

	return teardown, nil
}

// startListener opens the zero-scope feed's change stream and launches its
// reader. The stream is established BEFORE this returns, so a caller that
// writes immediately after Start observes its own write, and an unreachable
// MongoDB surfaces as a Start error instead of looping in the background.
//
// It publishes through publishFeed for the same reason a tenant creator would:
// the stream is opened outside the feeds-map lock, so a Close that lands
// meanwhile must be able to throw it away instead of inheriting a live cursor
// and a reader goroutine no later Close will ever stop.
//
// Polling mode has no stream to open: its first round trip runs here instead,
// on this goroutine, and its reader is launched with a nil stream. A first
// round trip that fails returns the error and launches no ticker; the
// zero-scope slot stays in the feeds map with the cause recorded, so its
// subscribers keep the feed a retried Start reopens.
func (s *Store) startListener(ctx context.Context) error {
	// ONE Start at a time, end to end. The zero-scope feed is SHARED, so the
	// "already running" check below and the reader launch inside publishFeed
	// have to be one decision: two concurrent Starts that both saw no reader
	// would each open a change stream on the same feed, the second
	// startFeedReader would overwrite f.done, and from then on every event and
	// every marker would be delivered twice by two readers while Close waited
	// on only one of them. Each Start also reports the outcome of the open IT
	// performed, never another attempt's record, which a later retry is free to
	// clear. The open itself is bounded by watchTimeout (or pollRoundTimeout),
	// so a second caller waits at most that long.
	s.startMu.Lock()
	defer s.startMu.Unlock()

	f, err := s.zeroFeedForStart()
	if err != nil {
		return err
	}

	f.mu.Lock()
	running := f.done != nil
	f.mu.Unlock()

	// Already running: Start is idempotent.
	if running {
		return nil
	}

	// The zero scope is held to the same one-collection rule as a tenant's: a
	// Store that also serves named tenants must not watch a collection one of
	// them watches, or every event would reach both feeds.
	if err := s.claimFeedColl(ctx, f, s.coll); err != nil {
		return s.retractFeed(f, err)
	}

	if s.cfg.PollInterval > 0 {
		if err := s.startPolling(ctx, f); err != nil {
			return s.retractFeed(f, err)
		}

		return s.publishFeed(ctx, f, nil)
	}

	stream, err := s.openWatch(ctx, f)
	if err != nil {
		return s.retractFeed(f, err)
	}

	return s.publishFeed(ctx, f, stream)
}

// zeroFeedForStart hands Start the zero-scope feed and clears the refusal a
// PREVIOUS attempt recorded. Start retries the feed its subscribers are already
// holding, so that record belongs to the attempt that produced it, not to the
// feed forever. Callers must NOT hold feedsMu, and MUST hold startMu: clearing
// the record is only safe while no other Start can be reading it.
func (s *Store) zeroFeedForStart() (*feed, error) {
	s.feedsMu.Lock()
	defer s.feedsMu.Unlock()

	f, err := s.zeroFeedSlotLocked()
	if err != nil {
		return nil, err
	}

	f.err = nil

	return f, nil
}

// openWatch opens one change stream on the feed's collection, bounded by
// watchTimeout. Inheriting the caller's ctx unbounded is what would let an
// unreachable MongoDB park Start for the life of that ctx, and with it the
// reserved feed slot.
//
// fullDocument: updateLookup is set here, once, so insert / update / replace
// all carry the after-image. No resume token is used: OpResync after every
// (re)open is what covers the gap instead.
//
// The ctx bounds the OPEN only — iteration runs on the reader's own ctx, which
// the driver takes per call to Next.
func (s *Store) openWatch(ctx context.Context, f *feed) (*mongo.ChangeStream, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{
				{Key: "$in", Value: bson.A{"insert", "update", "replace", operationTypeDelete}},
			}},
		}}},
	}

	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	watchCtx, cancel := context.WithTimeout(ctx, watchTimeout)
	defer cancel()

	stream, err := f.coll.Watch(watchCtx, pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("systemplane/mongodb: watch%s: %w", f.label(), err)
	}

	s.logInfo(ctx, "change stream established",
		log.String("collection", collectionName),
		log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
	)

	return stream, nil
}

// publishFeed hands the opened feed to its waiters — or throws the stream away
// when Close ran while this creator was still opening it. The recheck is the
// only thing standing between a shut-down store and a live cursor, because the
// slot Close found in the map carried nothing it could stop.
//
// Publishing the feed and launching its reader happen in ONE feedsMu hold:
// Close decides what to tear down by walking that map, so a feed must never be
// visible there without its goroutine already running, or Close would wait the
// full closeTimeout on a done channel nothing will ever close.
func (s *Store) publishFeed(ctx context.Context, f *feed, stream *mongo.ChangeStream) error {
	s.feedsMu.Lock()

	if s.closing {
		cause := s.failLocked(f, store.ErrClosed)

		f.closeReadyLocked()
		s.feedsMu.Unlock()

		closeStream(ctx, stream)

		return cause
	}

	s.startFeedReader(f, stream)
	f.closeReadyLocked()
	s.feedsMu.Unlock()

	return nil
}

// closeReadyLocked wakes every waiter, exactly once. The caller MUST hold
// Store.feedsMu. Only a NAMED tenant's feed has a ready channel: the zero-scope
// feed is brought up by Start alone, under startMu, so nothing ever waits on it
// and this is a no-op there.
func (f *feed) closeReadyLocked() {
	if f.ready == nil || f.readyClosed {
		return
	}

	f.readyClosed = true

	close(f.ready)
}

// failLocked records the cause a waiter will see and retracts the dead slot.
// The caller MUST hold Store.feedsMu, and MUST close ready afterwards in the
// same hold: err is written BEFORE the close, so the close is the
// happens-before edge that publishes it, and the slot is gone BEFORE the
// waiters wake, so the next Subscribe for that tenant builds a fresh
// placeholder instead of finding a corpse. The FIRST cause wins, and a named
// feed's slot is gone with it, so nothing writes that record again.
//
// The zero-scope record is the one that IS rewritten, by the next Start
// (zeroFeedForStart clears it for its own attempt). That is safe only because
// startMu makes Start the sole writer of it and no Start reads another
// attempt's record: each one reports the outcome of the open it performed.
func (s *Store) failLocked(f *feed, err error) error {
	if f.err == nil {
		f.err = err
	}

	// The zero-scope slot is the exception: Start owns it, Subscribe can attach
	// to it BEFORE Start, and a retried Start must bring up the very feed those
	// subscribers hold. Retracting it would strand them on a feed nothing
	// reopens and nothing tears down, while handing the next Subscribe a fresh
	// feed that has announced nothing and looks healthy. The recorded cause
	// stays on the slot instead, and zeroFeedLocked reports it.
	if f.scope.Tenant == "" {
		return f.err
	}

	if s.feeds[f.scope.Tenant] == f {
		delete(s.feeds, f.scope.Tenant)
	}

	return f.err
}

// startFeedReader records the reader's done channel and launches it. done is
// what stopFeed waits on, so it must be recorded before the goroutine starts.
// A nil stream means polling mode.
func (s *Store) startFeedReader(f *feed, stream *mongo.ChangeStream) {
	done := make(chan struct{})

	f.mu.Lock()
	f.done = done
	f.mu.Unlock()

	go func() {
		defer close(done)
		defer runtime.RecoverAndLog(s.cfg.Logger, "systemplane.mongodb.listener")

		if stream == nil {
			s.pollForever(f, f.pollStart)

			return
		}

		s.runFeed(f, stream)
	}()
}

// runFeed is THE change-stream loop, for every scope. stream is the
// already-open first cursor. Per stream it emits, in order:
// store.Event{Scope: f.scope, Op: store.OpResync}, then the decoded events from
// that cursor, then — on loss, before the first reopen attempt — exactly one
// store.Event{Scope: f.scope, Op: store.OpDisconnect}. Then it repeats through
// the existing backoff.
func (s *Store) runFeed(f *feed, stream *mongo.ChangeStream) {
	attempt := 0

	for {
		if subs, ok := f.beginResync(); ok {
			s.broadcast(f, subs, store.Event{Scope: f.scope, Op: store.OpResync})
		}

		openedAt := time.Now()

		consumed := s.consumeUntilFailure(f, stream)

		// Only a cursor that did some work clears the backoff. A replica set
		// that accepts a change stream and drops it at once would otherwise
		// reset the sequence on every cycle and the feed would reopen at the
		// first delay forever.
		if streamWasUseful(consumed, time.Since(openedAt)) {
			attempt = 0
		}

		if subs, ok := f.beginDisconnect(); ok {
			s.broadcast(f, subs, store.Event{Scope: f.scope, Op: store.OpDisconnect})
		}

		closeStream(context.Background(), stream)

		select {
		case <-f.stop:
			return
		default:
		}

		var err error

		stream, err = s.reopenWatch(f, &attempt)
		if err != nil {
			return
		}
	}
}

// consumeUntilFailure drains one cursor until it dies or teardown closes
// f.stop. It never reports the difference between the two: the f.closing check
// inside beginDisconnect is what keeps a clean shutdown from announcing an
// outage, and the ctx below is what keeps the shutdown from being logged as a
// failure.
//
// It DOES report whether the cursor carried at least one event, which is what
// tells runFeed the stream was worth keeping and its backoff can start over.
//
// Every document-level warning in this file, here and in the polling loop,
// names the feed's tenant. Without it an operator reading the logs of a
// process carrying dozens of tenant feeds cannot tell which database is
// emitting garbage, and the warning degrades into noise nobody can act on.
func (s *Store) consumeUntilFailure(f *feed, stream *mongo.ChangeStream) (consumed bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		defer runtime.RecoverAndLog(s.cfg.Logger, "systemplane.mongodb.observer")

		select {
		case <-f.stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	for stream.Next(ctx) {
		// Counted before classification: a cursor that delivered an event this
		// process could not use still proves the connection carried traffic.
		consumed = true

		var event changeEvent
		if err := stream.Decode(&event); err != nil {
			s.logWarn(ctx, "change stream decode error, skipping event",
				log.Err(err),
				log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
			)

			continue
		}

		evt, ok := eventFromChange(event)
		if !ok {
			s.droppedEvents.Add(1)

			s.logWarn(ctx, "change stream event dropped — missing identifiers",
				log.String("operationType", event.OperationType),
				log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
			)

			continue
		}

		f.dispatch(s.cfg.Logger, evt)
	}

	if err := stream.Err(); err != nil && ctx.Err() == nil {
		s.logDebug(ctx, "change stream read failed",
			log.Err(err),
			log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
		)
	}

	return consumed
}

// streamWasUseful reports whether the cursor that just ended earns a fresh
// backoff sequence. A cursor that carried at least one event did work; so did
// one that merely stayed open past the cap, since a feed that survives that
// long is not the flapping case the escalation exists for. Everything else
// keeps the sequence climbing, so a MongoDB that accepts a change stream and
// drops it immediately — a replica-set election, a proxy killing idle cursors,
// a mongos mid-failover — escalates to the cap instead of reopening four times
// a second, each cycle costing a tenant re-resolve, a watch aggregate and an
// OpResync that makes the engine reload the whole scope.
func streamWasUseful(consumed bool, lifetime time.Duration) bool {
	return consumed || lifetime >= reconnectMaxDelay
}

// reconnectDelay is how long a feed waits after its attempt-th consecutive
// failure: the exponential is capped FIRST and the result jittered, so the
// delay is always drawn from [0, ceiling). Both loops use it — the change
// stream between reopen attempts and the poller between failed round trips —
// so an outage costs the same on either path.
//
// Jittering before the cap would collapse the draw onto the cap exactly once
// the outage is long enough to matter: past the cap almost every draw from the
// (much wider) exponential window clips to the same value, so every feed in the
// process reopens on the same tick, each cycle costing a tenant re-resolve, a
// watch aggregate and an OpResync that makes the engine reload the whole scope.
func reconnectDelay(attempt int) time.Duration {
	return backoff.FullJitter(min(backoff.Exponential(reconnectBaseDelay, attempt), reconnectMaxDelay))
}

// reopenWatch retries the open until it succeeds or teardown stops the feed.
// It never clears attempt: an open that succeeds proves nothing about the
// connection behind it, and runFeed resets the sequence once the cursor it
// returns has actually done some work (see streamWasUseful).
func (s *Store) reopenWatch(f *feed, attempt *int) (*mongo.ChangeStream, error) {
	// One warning per loss: reopenWatch is entered once per loss and retries
	// internally. attempt is how many attempts this backoff sequence has
	// already spent, so a flapping backend reports a RISING number instead of
	// going silent after the first cycle — which is what gating this on
	// attempt == 0 would now do, since a bare reopen no longer clears it.
	s.logWarn(context.Background(), "change stream disconnected, reconnecting",
		log.Int("attempt", *attempt),
		log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
	)

	// The LOG streak, one bool per distinct cause, scoped to this entry into
	// reopenWatch — that is, to one stream loss. Kept apart from attempt, whose
	// counter only a useful cursor clears: after one unproductive cycle it
	// never returns to zero, and a loud line gated on it would go silent for
	// the life of the feed. Per cause, so a streak that opens on a tenant that
	// will not resolve is still loud when it becomes a stream that will not
	// open — a different outage, told once.
	var warnedResolveFailed, warnedReopenFailed bool

	for {
		select {
		case <-f.stop:
			return nil, errFeedStopped
		default:
		}

		delay := reconnectDelay(*attempt)
		*attempt++

		select {
		case <-f.stop:
			return nil, errFeedStopped
		case <-time.After(delay):
		}

		if err := s.refreshFeedColl(context.Background(), f); err != nil {
			s.logStreakFailure(&warnedResolveFailed, "tenant re-resolve before reopen failed",
				log.Err(err),
				log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
			)

			continue
		}

		stream, err := s.openWatch(context.Background(), f)
		if err != nil {
			s.logStreakFailure(&warnedReopenFailed, "change stream reopen failed",
				log.Err(err),
				log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
			)

			continue
		}

		return stream, nil
	}
}

// refreshFeedColl re-resolves a NAMED scope's collection through the connector,
// before a reopen or after a failed poll round trip. It is a no-op for the zero
// scope and for a store with no connector.
//
// The tenant manager owns the client behind that handle and Disconnect()s it on
// LRU eviction or a credentials swap — and it ranks eviction candidates by the
// last GetConnection, which changefeed I/O never touches, so a tenant this store
// serves entirely from its feed looks perfectly idle and is evicted FIRST. A
// feed that pinned its handle for life would then fail every reopen with
// mongo.ErrClientDisconnected, forever, leaving the scope permanently stale
// behind a reader spinning the backoff for the life of the process. Re-resolving
// also picks up rotated credentials without waiting for the last subscriber to
// leave.
//
// The freshly resolved handle is adopted only once its server has said who it
// is: an unanswered hello returns errIdentityProbeFailed and leaves BOTH f.coll
// and f.collID as they were, so the pair the shared-collection refusal is
// decided on never goes half-updated and the claim this feed already holds
// stands. Every caller treats that as retryable — reopenWatch logs it through
// logStreakFailure and comes back after the backoff.
//
// Called only from the reader goroutine, which owns f.coll once the feed is
// published, so the write needs no lock.
func (s *Store) refreshFeedColl(ctx context.Context, f *feed) error {
	if f.scope.Tenant == "" || s.cfg.Connector == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, watchTimeout)
	defer cancel()

	db, err := s.cfg.Connector.ResolveDatabase(ctx, f.scope.Tenant)
	if err != nil {
		return fmt.Errorf("systemplane/mongodb: resolve tenant %s: %w", f.scope.Tenant, err)
	}

	// A nil handle with a nil error is a connector bug; refuse it rather than
	// reopen against something that panics on the first command.
	if db == nil {
		return fmt.Errorf("systemplane/mongodb: resolve tenant %s: %w", f.scope.Tenant, store.ErrTenantConnectorMissing)
	}

	coll := db.Collection(collectionName)

	// The tenant may have been moved onto a collection another live scope
	// watches; re-claiming keeps the identity the refusal is decided on honest
	// through every reopen. The probe is run here rather than through
	// claimFeedColl so an unanswered one aborts the whole refresh: admitting it
	// would leave f.coll on the new collection and f.collID on the old one, and
	// the refusal would then be decided on a pair that never existed.
	id := s.collIdentityFor(ctx, coll)
	if id == (collIdentity{}) {
		return fmt.Errorf("systemplane/mongodb: reopen tenant %s: %w", f.scope.Tenant, errIdentityProbeFailed)
	}

	if err := s.claimFeedIdentity(f, id); err != nil {
		return err
	}

	f.coll = coll

	return nil
}

// closeStream closes a dead or abandoned cursor on a ctx of its own: the one
// that just died would abandon the server-side cursor instead of closing it.
func closeStream(ctx context.Context, stream *mongo.ChangeStream) {
	if stream == nil {
		return
	}

	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()

	_ = stream.Close(closeCtx)
}

// stopFeeds tears down every feed the store owns. Idempotent.
//
// The store-wide closing flag is raised in the SAME hold that walks the map, so
// no creator can publish a feed into a shut-down store afterwards. A slot whose
// creator is still opening its stream has no reader to stop and no cursor to
// close yet: it is failed with store.ErrClosed here — so every caller parked on
// it learns the store is gone instead of waiting for a feed that will never
// arrive — and its creator closes the stream it ends up with on its own.
func (s *Store) stopFeeds() {
	s.feedsMu.Lock()
	s.closing = true
	feeds := make([]*feed, 0, len(s.feeds))

	for _, f := range s.feeds {
		if f.ready != nil && !f.readyClosed {
			_ = s.failLocked(f, store.ErrClosed)

			f.closeReadyLocked()

			continue
		}

		feeds = append(feeds, f)
	}

	clear(s.feeds)
	s.feedsMu.Unlock()

	// Signal every feed FIRST, then wait on all of them against one shared
	// deadline. Stopping them one at a time costs tenants x closeTimeout, so
	// six unresponsive tenants alone overrun the engine's 30s Close budget and,
	// behind that, the pod's termination grace period — the process is killed
	// mid-shutdown instead of closing its cursors.
	waits := make([]<-chan struct{}, 0, len(feeds))

	for _, f := range feeds {
		if done := s.signalFeed(f, false); done != nil {
			waits = append(waits, done)
		}
	}

	if len(waits) == 0 {
		return
	}

	deadline := time.NewTimer(closeTimeout)
	defer deadline.Stop()

	for _, done := range waits {
		select {
		case <-done:
		case <-deadline.C:
			return
		}
	}
}

// signalFeed marks the feed closing under f.mu BEFORE closing f.stop, so the
// reader's beginDisconnect can never announce a disconnect for a shutdown. It
// returns the channel to wait on, or nil when there is nothing to wait for:
// the feed was already stopped, or it never had a reader.
//
// skipSelfWait is the self-teardown escape, and ONLY the last-unsubscribe path
// sets it. A callback can drop its scope's last subscription from inside the
// reader's own delivery, and waiting for the reader there is waiting on the
// goroutine doing the waiting — it can only end at closeTimeout, with the feed
// frozen meanwhile. f.dispatching cannot tell "the caller IS the reader" from
// "some other goroutine is mid-callback", so Close never skips on it: a Close
// that did would return with a live cursor and a live reader behind it whenever
// a subscriber happened to be running.
func (s *Store) signalFeed(f *feed, skipSelfWait bool) <-chan struct{} {
	f.mu.Lock()

	if f.closing {
		f.mu.Unlock()

		return nil
	}

	f.closing = true
	done := f.done
	selfTeardown := skipSelfWait && f.dispatching > 0

	f.mu.Unlock()

	close(f.stop)

	if selfTeardown {
		s.logDebug(context.Background(), "changefeed torn down from inside a callback; not waiting for its reader",
			log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
		)

		return nil
	}

	return done
}

// stopFeed signals one feed and waits up to closeTimeout for its reader to
// exit. Used by the last unsubscribe of a tenant feed; Close signals all of its
// feeds before waiting on any of them.
func (s *Store) stopFeed(f *feed) {
	done := s.signalFeed(f, true)
	if done == nil {
		return
	}

	select {
	case <-done:
	case <-time.After(closeTimeout):
	}
}

// nsKey is the polling-mode set element used to detect deletes. We can't
// rely on _id (compound subdocument) as a map key directly, so we tuple it.
type nsKey struct {
	Namespace string
	Key       string
}

// pollState is everything one poll round trip carries into the next. The FIRST
// round trip establishes it on the caller's goroutine; from the moment the feed
// is published it belongs to the reader goroutine alone, so it needs no lock.
type pollState struct {
	// watermark is the updated_at floor of the incremental query. Only a round
	// trip that COMPLETED advances it: advancing on a partial read would
	// silently skip every row that read never saw.
	watermark time.Time
	// known is the live (namespace, key) set the last completed round trip
	// saw. A key in it that is absent now is the one delete that leaves
	// nothing to announce — a foreign deleteOne that removed the document
	// outright instead of tombstoning it.
	known map[nsKey]struct{}
	// seenAtWatermark holds the keys whose updated_at equals the watermark,
	// each with a content hash of the value emitted for it. Two consecutive
	// round trips observing the same key at that millisecond skip re-emission
	// only when the hash matches too — see seenEntry / boundaryDedupHit.
	seenAtWatermark map[nsKey]seenEntry
	// first suppresses delete synthesis on the very first round trip, whose
	// known set is empty by construction.
	first bool
}

// newPollState anchors the watermark at now, truncated to the BSON
// millisecond so a row written earlier inside the same millisecond is still
// caught by the $gte.
func newPollState() pollState {
	return pollState{
		watermark:       time.Now().UTC().Truncate(time.Millisecond),
		known:           make(map[nsKey]struct{}),
		seenAtWatermark: make(map[nsKey]seenEntry),
		first:           true,
	}
}

// startPolling runs the feed's FIRST poll round trip synchronously and records
// the state it established for the reader goroutine.
//
// Synchronous for the same reason openWatch is: the watermark this round trip
// anchors is the floor of every later query, so anchoring it on a goroutine the
// caller never waits for swallows the writes that land in between — not late,
// never. On failure it returns the wrapped error and leaves the caller to
// retract the slot: no ticker, no placeholder, no partial subscription.
//
// It dispatches nothing (nil emit): the feed has announced nothing yet, and a
// subscriber registered before Start — which is what the engine does — would
// otherwise receive key events ahead of its first OpResync, breaking FC-2's
// order. Nothing is lost by the silence: the OpResync pollForever broadcasts
// immediately after has the engine reload the whole scope, and this round trip
// still anchors the watermark so the next one is incremental.
func (s *Store) startPolling(ctx context.Context, f *feed) error {
	st, err := s.pollOnce(ctx, f, newPollState(), nil)
	if err != nil {
		return err
	}

	f.pollStart = st

	return nil
}

// pollForever is THE polling loop, for every scope. st is the state the
// synchronous first round trip established, so the loop never re-anchors the
// watermark.
//
// Connectivity narration, the SAME edge-triggered pair the change stream uses:
// the first failure of a streak announces one store.OpDisconnect and every
// later failure announces nothing, so a long outage costs one disconnect rather
// than one per tick; the round trip that ends the streak announces one
// store.OpResync BEFORE the first key event it read (pollEmitter), or right
// after it returns when it read none.
//
// A failure streak backs off exactly as the change stream's reopen loop does,
// so a MongoDB that is down costs one round trip per backoff step instead of
// one per tick — and, for a named tenant, one tenant-manager resolution per
// tick on top of it. The counter resets on the first success.
func (s *Store) pollForever(f *feed, st pollState) {
	// The first round trip already succeeded — the caller would not have
	// published this feed otherwise — so the connection is announced here, on
	// the reader goroutine, exactly where runFeed announces its own open.
	if subs, ok := f.beginResync(); ok {
		s.broadcast(f, subs, store.Event{Scope: f.scope, Op: store.OpResync})
	}

	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	attempt := 0

	// The LOG streak, one bool per distinct cause, cleared by the round trip
	// that ends the streak — the same edge that clears attempt, kept as its own
	// state so the two never have to mean the same thing.
	var warnedPollFailed, warnedResolveFailed bool

	for {
		select {
		case <-f.stop:
			return
		case <-ticker.C:
			next, err := s.pollOnce(context.Background(), f, st, s.pollEmitter(f))
			if err != nil {
				s.logStreakFailure(&warnedPollFailed, "poll round trip failed",
					log.Err(err),
					log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
				)

				if subs, ok := f.beginDisconnect(); ok {
					s.broadcast(f, subs, store.Event{Scope: f.scope, Op: store.OpDisconnect})
				}

				if !s.pollBackoff(f, &attempt) {
					return
				}

				// The tenant manager may have Disconnect()ed the client behind
				// f.coll, in which case every later round trip fails on the
				// dead handle forever. Re-resolve before the next tick.
				if err := s.refreshFeedColl(context.Background(), f); err != nil {
					s.logStreakFailure(&warnedResolveFailed, "tenant re-resolve after a failed poll failed",
						log.Err(err),
						log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
					)
				}

				// st is deliberately left untouched: the failed round trip
				// established nothing.
				continue
			}

			attempt = 0
			warnedPollFailed, warnedResolveFailed = false, false

			// The recovery announcement for a round trip that read no key
			// event: pollEmitter never ran, so there was nothing to precede.
			// Announcing twice is free — beginResyncAfterOutage is
			// edge-triggered and the emitter already cleared the edge.
			s.announceRecovery(f)

			st = next
		}
	}
}

// pollEmitter returns the dispatch callback for ONE poll round trip. It hangs
// the recovery OpResync on the round's FIRST key event, so a subscriber is
// never told about a key by a feed that has not yet told it the feed is back:
// the engine answers OpResync by reloading the scope, and a key event applied
// before that marker is applied into a scope the engine still believes stale
// (FC-2).
func (s *Store) pollEmitter(f *feed) func(store.Event) {
	announced := false

	return func(evt store.Event) {
		if !announced {
			announced = true

			s.announceRecovery(f)
		}

		f.dispatch(s.cfg.Logger, evt)
	}
}

// announceRecovery broadcasts the one OpResync that ends a failure streak.
// Edge-triggered through beginResyncAfterOutage: outside a streak, and on every
// call after the first of one round trip, it announces nothing.
func (s *Store) announceRecovery(f *feed) {
	if subs, ok := f.beginResyncAfterOutage(); ok {
		s.broadcast(f, subs, store.Event{Scope: f.scope, Op: store.OpResync})
	}
}

// pollBackoff waits out one failed round trip and advances the streak counter.
// It reports false when teardown stopped the feed meanwhile, so a Close never
// waits out a backoff that can reach reconnectMaxDelay.
//
// The ticker keeps running underneath: its channel buffers one tick, so the
// round trip that follows fires immediately after the wait rather than a whole
// interval later.
func (s *Store) pollBackoff(f *feed, attempt *int) bool {
	delay := reconnectDelay(*attempt)
	*attempt++

	select {
	case <-f.stop:
		return false
	case <-time.After(delay):
		return true
	}
}

// pollOnce performs one poll round trip on the feed's own collection:
//
//   - Emits store.OpUpsert carrying the document's stored revision for every
//     live document with updated_at >= the watermark, unless it is an
//     idempotent rewrite at the boundary millisecond (same key, same
//     millisecond, and the same value hash as the one emitted last round).
//     Same key at the same millisecond with a DIFFERENT value is a real new
//     write and IS emitted — otherwise peer caches stay stale until a later,
//     strictly newer write advances the watermark.
//   - Emits store.OpDelete at Revision 0 for every TOMBSTONE it reads: the
//     document Delete rewrote in place (FC-9). The incremental query is
//     deliberately NOT filtered on "deleted" — the poller has to see the
//     tombstone in order to announce it.
//   - Diffs the live key set against the previous round's to catch the one
//     delete that leaves nothing behind: a foreign deleteOne. A key already
//     announced from its tombstone this round is excluded from that diff, or
//     the same delete would fire twice.
//
// Every one of those events goes to emit, never to f.dispatch directly, which
// is what lets the caller decide what precedes them: pollForever hands in an
// emitter that announces the recovery OpResync before the first of them, and
// the synchronous first round trip hands in nil and dispatches nothing at all.
// A nil emit still reads every document and advances the state normally.
//
// It returns the state the next round trip carries. On failure it returns the
// state it was GIVEN, unchanged, so nothing advances over rows it never read.
func (s *Store) pollOnce(ctx context.Context, f *feed, st pollState, emit func(store.Event)) (pollState, error) {
	if emit == nil {
		emit = func(store.Event) {}
	}

	ctx, cancel := context.WithTimeout(ctx, pollRoundTimeout)
	defer cancel()

	// $gte, not $gt, so two writes landing in the previous boundary
	// millisecond are not silently skipped; the duplicate that follows is
	// filtered by boundaryDedupHit's content discriminator.
	filter := bson.D{{Key: fieldUpdatedAt, Value: bson.D{{Key: "$gte", Value: st.watermark}}}}

	findOpts := options.Find().SetSort(bson.D{
		{Key: fieldUpdatedAt, Value: 1},
		{Key: fieldNamespace, Value: 1},
		{Key: fieldKey, Value: 1},
	})

	cur, err := f.coll.Find(ctx, filter, findOpts)
	if err != nil {
		return st, fmt.Errorf("systemplane/mongodb: poll find%s: %w", f.label(), err)
	}
	defer cur.Close(ctx)

	next := pollState{
		watermark:       st.watermark,
		known:           st.known,
		seenAtWatermark: make(map[nsKey]seenEntry),
	}

	// The keys this round announced from their tombstone. They are already
	// absent from the live key set (snapshotKeys filters tombstones out), so
	// the diff below would fire a second delete for each of them.
	tombstoned := make(map[nsKey]struct{})

	for cur.Next(ctx) {
		var doc entryDoc

		if err := cur.Decode(&doc); err != nil {
			s.logWarn(ctx, "poll decode error, skipping document",
				log.Err(err),
				log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
			)

			continue
		}

		nk := nsKey{Namespace: doc.Namespace, Key: doc.Key}
		valueHash := hashValue(doc.Value)
		atBoundary := doc.UpdatedAt.Equal(st.watermark)

		if doc.Deleted {
			tombstoned[nk] = struct{}{}
		}

		// A tombstone runs through the same discriminator: its value is unset
		// and hashes differently from the value it replaced, so the delete
		// transition always emits, while two observations of the SAME
		// tombstone at the boundary collapse into one.
		if boundaryDedupHit(st.seenAtWatermark, nk, atBoundary, valueHash) {
			// Preserved so the next round still dedupes against it: the $gte
			// re-reads this row until the watermark advances past it.
			next.seenAtWatermark[nk] = seenEntry{valueHash: valueHash}

			continue
		}

		evt := store.Event{Namespace: doc.Namespace, Key: doc.Key, Op: store.OpUpsert, Revision: doc.Revision}
		if doc.Deleted {
			evt.Op, evt.Revision = store.OpDelete, 0
		}

		emit(evt)

		switch {
		case doc.UpdatedAt.After(next.watermark):
			next.watermark = doc.UpdatedAt
			next.seenAtWatermark = map[nsKey]seenEntry{nk: {valueHash: valueHash}}
		case doc.UpdatedAt.Equal(next.watermark):
			next.seenAtWatermark[nk] = seenEntry{valueHash: valueHash}
		}
	}

	if err := cur.Err(); err != nil {
		return st, fmt.Errorf("systemplane/mongodb: poll cursor%s: %w", f.label(), err)
	}

	currentKnown, err := s.snapshotKeys(ctx, f)
	if err != nil {
		// A partial scan would synthesize deletes for rows it merely failed to
		// read, so the previous key set is kept and the diff skipped: a late
		// delete beats a phantom one.
		s.logWarn(ctx, "poll snapshot failed, skipping delete diff",
			log.Err(err),
			log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
		)

		return next, nil
	}

	if !st.first {
		for nk := range st.known {
			if _, stillThere := currentKnown[nk]; stillThere {
				continue
			}

			if _, announced := tombstoned[nk]; announced {
				continue
			}

			emit(store.Event{Namespace: nk.Namespace, Key: nk.Key, Op: store.OpDelete})
		}
	}

	next.known = currentKnown

	return next, nil
}

// snapshotKeys returns the LIVE (namespace, key) set of a collection —
// tombstones excluded through the same notDeleted guard the reads use, so a key
// this lib deleted is already gone from here and is announced from its
// tombstone instead. What the resulting diff still covers is the delete that
// leaves nothing to read: a foreign deleteOne.
func (s *Store) snapshotKeys(ctx context.Context, f *feed) (map[nsKey]struct{}, error) {
	coll := f.coll

	projection := bson.D{
		{Key: fieldNamespace, Value: 1},
		{Key: fieldKey, Value: 1},
		{Key: fieldID, Value: 0},
	}
	findOpts := options.Find().SetProjection(projection)

	cur, err := coll.Find(ctx, bson.D{notDeleted()}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("snapshot find: %w", err)
	}
	defer cur.Close(ctx)

	out := make(map[nsKey]struct{})

	for cur.Next(ctx) {
		var doc struct {
			Namespace string `bson:"namespace"`
			Key       string `bson:"key"`
		}

		if err := cur.Decode(&doc); err != nil {
			s.logWarn(ctx, "snapshot decode error, skipping document",
				log.Err(err),
				log.String(obsconstants.AttrKeyTenantID, f.scope.Tenant),
			)

			continue
		}

		if doc.Namespace == "" || doc.Key == "" {
			continue
		}

		out[nsKey{Namespace: doc.Namespace, Key: doc.Key}] = struct{}{}
	}

	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("snapshot cursor: %w", err)
	}

	return out, nil
}
