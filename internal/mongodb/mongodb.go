// Package mongodb implements the internal store.Store interface over MongoDB.
//
// Two operating modes share this file:
//
//   - Single-tenant. The constructor receives a *mongo.Client plus a database
//     name. Reads/writes route through the resulting *mongo.Collection. A
//     change stream listens for upsert/delete events on that collection and
//     fans them out to subscribers.
//
//   - Multi-tenant. The constructor receives no client; the caller wires
//     lib-commons tenant-manager middleware so each request context carries
//     the per-tenant *mongo.Database. resolveCollection(ctx) extracts the
//     database, lazily ensures the collection on first use, and returns it to
//     the CRUD helpers. The ZERO scope has no durable collection to watch
//     there — every call resolves a fresh database from ctx — so subscribing
//     to it is refused with store.ErrNotSupportedInMultiTenant.
//
// Changefeeds are per scope, and a named Scope.Tenant is served in BOTH modes:
// it resolves its database through Config.Connector regardless of ctx and of
// MultiTenantEnabled, and opens its own change stream (or polling loop) on that
// database's collection.
//
// The document _id is the compound sub-document {namespace, key} — no
// tenant_id field. Storing the tuple in _id is what makes change-stream
// delete events self-describing: a delete event has no fullDocument, only
// documentKey._id, so the (namespace, key) pair must live there.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	obsconstants "github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/tracing"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	collectionName     = "systemplane_entries"
	defaultModule      = "systemplane"
	reconnectBaseDelay = 500 * time.Millisecond
	reconnectMaxDelay  = 30 * time.Second
	tracerName         = "systemplane.mongodb"
)

// Compile-time interface check.
var _ store.Store = (*Store)(nil)

// Config holds the parameters needed to construct a MongoDB-backed Store.
type Config struct {
	// Client is the mongo client for single-tenant mode. MAY be nil when
	// MultiTenantEnabled is true.
	Client *mongo.Client

	// Database is the database name for single-tenant mode. MAY be empty
	// when MultiTenantEnabled is true.
	Database string

	// PollInterval enables polling mode when positive. A zero value uses
	// change streams (which require a replica set). Polling serves every
	// scope a change stream serves — the zero scope and a named tenant
	// alike — with the same store.OpResync, store.OpDisconnect and revision
	// guarantees.
	PollInterval time.Duration

	// MultiTenantEnabled selects the tmcore-driven dispatch path. When true,
	// Client/Database may be empty; the zero scope resolves the per-tenant
	// database from ctx via tmcore.GetMBContext(ctx, Module).
	MultiTenantEnabled bool

	// Module is the tenant-manager module name used as the context key for
	// dispatch. Default: "systemplane".
	Module string

	Connector Connector // nil in single-tenant mode

	Logger    log.Logger
	Telemetry store.Telemetry
}

// compoundID is the shape of the document _id. The tuple lives in _id so that
// change-stream delete events — which surface only documentKey — still carry
// enough information to reconstruct the event.
type compoundID struct {
	Namespace string `bson:"namespace"`
	Key       string `bson:"key"`
}

// entryDoc is the BSON document shape persisted in the collection.
type entryDoc struct {
	ID        compoundID `bson:"_id"`
	Namespace string     `bson:"namespace"`
	Key       string     `bson:"key"`
	Value     string     `bson:"value"`
	Revision  int64      `bson:"revision"`
	UpdatedAt time.Time  `bson:"updated_at"`
	UpdatedBy string     `bson:"updated_by"`
	// Deleted is present and true only on a tombstone: a document Delete
	// rewrote in place so the revision the key reached survives (FC-9, D11).
	// Get and List filter these out, so the store surface never shows one.
	Deleted bool `bson:"deleted"`
}

// Store implements [store.Store] over MongoDB.
type Store struct {
	cfg    Config
	coll   *mongo.Collection
	tracer trace.Tracer

	// schemaOnce caches the lazy ensure-collection step per resolved tenant
	// database. Keyed by the stable string schemaCacheKey composes,
	// "<tenant>/<db.Name()>/<collection>", rather than by the
	// *mongo.Collection pointer: the mongo-driver/v2 Database.Collection
	// method MAY return a fresh handle per call, so a pointer-keyed map would
	// miss every time and rerun the schema probe. See schemaCacheKey for why
	// the tenant leads the key and why the client handle is absent.
	schemaOnce sync.Map // map[string]*sync.Once
	schemaErr  sync.Map // map[string]error

	// schemaRunner allows unit tests to inject a runSchema replacement that
	// fails on demand without standing up a live MongoDB. Production callers
	// leave this nil; ensureSchema falls back to s.runSchema in that case.
	//
	// The signature uses an interface argument so tests don't need to
	// construct a *mongo.Collection; we pass the cacheKey string instead.
	schemaRunner func(ctx context.Context, cacheKey string) error

	// identityProbe allows unit tests to answer the hello collIdentityOf asks,
	// which no offline handle can. Production callers leave this nil;
	// collIdentityFor falls back to collIdentityOf in that case.
	identityProbe func(ctx context.Context, coll *mongo.Collection) collIdentity

	// noTenantIDWarn bounds warnSchemaWithoutTenantID to one line per Store.
	// The path it narrates is on every read and write, so a per-call line
	// would be a line per request; the condition is a wiring mistake that is
	// either there for the life of the store or not there at all. Per store
	// and not per process on purpose: two Stores are two consumers or two
	// configurations, and each is entitled to say it once.
	noTenantIDWarn sync.Once

	// feedsMu guards feeds, the changefeeds keyed by scope.Tenant ("" is the
	// zero, single-tenant scope), and every feed's reference count: a feed's
	// lifetime decision and its map slot change together, in one lock hold.
	feedsMu sync.Mutex
	feeds   map[string]*feed

	// closing is set by Close under feedsMu, and is NOT the per-feed
	// feed.closing (which only suppresses OpDisconnect on a clean teardown).
	// A feed is opened outside the map lock, so Close cannot stop an in-flight
	// creator by walking the map alone: it raises this flag instead, and the
	// creator rechecks it before publishing anything.
	closing bool

	// startMu serializes Start end to end. The zero-scope feed is shared, so
	// the check for an existing reader, the open and the publish must be one
	// decision: without it two concurrent Starts each open a change stream on
	// the same feed and every event is delivered twice, and a Start that
	// merely waited on another attempt's outcome could read that attempt's
	// recorded cause after a THIRD Start had already cleared it for its own
	// retry — returning nil for a changefeed that never opened.
	startMu sync.Mutex

	mu     sync.Mutex
	closed bool

	// closedCh is closed exactly once, by Close, in the same s.mu hold that
	// sets closed — the early return above it is what makes that single. It is
	// the store-wide shutdown signal every subscription's ctx observer selects
	// on, so a subscriber whose ctx outlives the store does not leave a
	// goroutine parked forever. Created by New; a Store is not usable without
	// it.
	closedCh chan struct{}

	droppedEvents atomic.Int64
}

// Start performs single-tenant collection bootstrap and opens the change
// stream, synchronously: it does not return until the stream is established or
// has failed, so a write that lands right after it is observed rather than
// lost. In polling mode the same applies to the first poll round trip, which
// anchors the watermark every later query filters on. In multi-tenant mode it
// is a no-op.
func (s *Store) Start(ctx context.Context) error {
	if s == nil || s.isClosed() {
		return store.ErrClosed
	}

	// A nil ctx is normalized rather than dereferenced: the public API refuses
	// one before the store is reached, but the store is its own unit and the
	// first thing this path does is derive a timeout from ctx.
	if ctx == nil {
		ctx = context.Background()
	}

	if s.cfg.MultiTenantEnabled {
		return nil
	}

	if err := s.ensureSchema(ctx, "", s.coll, false); err != nil {
		return err
	}

	return s.startListener(ctx)
}

// Close releases backend resources. Idempotent. Does NOT close the externally
// supplied mongo client.
//
// It signals every changefeed and then waits for their readers under ONE
// shared closeTimeout, so shutdown costs a single bound no matter how many
// scopes the store carries. Called from INSIDE a subscriber callback it costs
// exactly that bound: the reader it is waiting for is the goroutine running
// the caller, so the wait can only end at the deadline. Close still returns,
// and the feeds are still torn down.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()

		return nil
	}

	s.closed = true

	close(s.closedCh)
	s.mu.Unlock()

	s.stopFeeds()

	return nil
}

func (s *Store) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

// DroppedEvents returns the total number of change-stream events dropped due
// to incomplete identifiers since Store construction.
func (s *Store) DroppedEvents() int64 {
	if s == nil {
		return 0
	}

	return s.droppedEvents.Load()
}

// resolveCollection returns the collection handle for the current call.
//
// The zero scope keeps today's behavior: single-tenant mode returns the
// constructor-supplied collection, multi-tenant mode extracts the
// *mongo.Database stored in ctx by tenant-manager middleware. A named tenant
// resolves through the connector regardless of MultiTenantEnabled and
// regardless of whatever tenant ctx carries (FC-2: an explicitly named scope
// and a request-scoped ctx tenant must never silently disagree), and is
// refused with store.ErrTenantConnectorMissing when no connector is
// configured.
//
// Every tenant database — ctx-carried or connector-resolved — goes through
// the lazy per-database bootstrap, which materializes the collection so a
// read before the first write cannot mistake a permissions problem for an
// empty scope.
func (s *Store) resolveCollection(ctx context.Context, scope store.Scope) (*mongo.Collection, error) {
	if scope.Tenant != "" {
		if s.cfg.Connector == nil {
			return nil, store.ErrTenantConnectorMissing
		}

		db, err := s.cfg.Connector.ResolveDatabase(ctx, scope.Tenant)
		if err != nil {
			return nil, fmt.Errorf("systemplane/mongodb: resolve tenant %s: %w", scope.Tenant, err)
		}

		// A nil handle with a nil error is a connector bug; refuse it here
		// rather than hand back something that panics on the first command.
		if db == nil {
			return nil, fmt.Errorf("systemplane/mongodb: resolve tenant %s: %w", scope.Tenant, store.ErrTenantConnectorMissing)
		}

		coll := db.Collection(collectionName)
		if err := s.ensureSchema(ctx, scope.Tenant, coll, true); err != nil {
			return nil, err
		}

		return coll, nil
	}

	if !s.cfg.MultiTenantEnabled {
		return s.coll, nil
	}

	db := tmcore.GetMBContext(ctx, s.cfg.Module)
	if db == nil {
		return nil, store.ErrTenantConnectionMissing
	}

	// The tenant the middleware resolved this database for names the memo, so
	// two tenants on two clusters that both call their database "systemplane"
	// each get their own bootstrap. A ctx that carries the database but no id
	// is possible — the two are independent context keys — and ensureSchema
	// answers it by skipping the memo rather than sharing one entry between
	// those two tenants.
	coll := db.Collection(collectionName)
	if err := s.ensureSchema(ctx, tmcore.GetTenantIDContext(ctx), coll, true); err != nil {
		return nil, err
	}

	return coll, nil
}

// schemaCacheKey returns the stable key used to memoize the per-database
// schema bootstrap: the tenant the collection was resolved for, plus the
// database and collection names ("<tenant>/<db.Name()>/<collection>"). The
// tenant is load-bearing — two tenants on two clusters may both call their
// database "systemplane", and a name-only key would report the second one as
// already bootstrapped and never materialize its collection. The zero scope
// has no tenant id and needs none: its database is the store's own.
//
// The client HANDLE is deliberately absent. It is not stable identity: the
// tenant manager disconnects a client when it evicts it, and the next client
// allocated can land on the freed address — so a pointer-keyed memo could hand
// a brand-new client the previous one's completed bootstrap and never create
// its collection or indexes.
//
// What this key CANNOT tell apart, stated rather than discovered: the key
// carries no SERVER identity, so a tenant whose database is relocated to
// another cluster under the same database name keeps the entry the old cluster
// filled, and its collection and indexes are never created on the new one. The
// key is deliberately not re-derived from the server the way a feed's identity
// is (collIdentityOf in mongodb_changestream.go asks hello): that is a round
// trip, and this memo sits on the request path of every read and write. A
// relocation is answered by restarting the process, or by the consumer
// bootstrapping the new database itself, not by paying that round trip on
// every call.
func schemaCacheKey(tenant string, coll *mongo.Collection) string {
	db := coll.Database()

	return tenant + "/" + db.Name() + "/" + coll.Name()
}

// warnSchemaWithoutTenantID is emitted once per Store (noTenantIDWarn bounds
// it), without fields: the message is the whole signal, and the tenant it
// would name is precisely what is missing.
const warnSchemaWithoutTenantID = "tenant-scoped collection arrived without a tenant id; " +
	"re-running the collection bootstrap on every call because the memo key would be ambiguous. " +
	"The tenant-manager middleware sets both context keys"

// ensureSchema memoizes the per-database bootstrap. tenant names the scope the
// collection was resolved for and is empty for the store's own database.
// tenantScoped marks a collection that belongs to a tenant database — carried
// by ctx or resolved through the connector — and is threaded into runSchema.
//
// A tenant-scoped collection with NO tenant id is the one case that cannot be
// memoized, and it is reachable: tmcore.GetMBContext and
// tmcore.GetTenantIDContext read independent context keys, so a caller may
// carry the tenant database and omit the id. The key would then be names only,
// and two tenants on two clusters whose databases share a name would collide
// on one entry — the second tenant reported as already bootstrapped and its
// collection never materialized, which is the failure the tenant in the key
// exists to prevent. Such a call re-runs the bootstrap instead. runSchema is
// idempotent (CreateCollection treats NamespaceExists as success, CreateMany
// is a no-op on existing indexes), so the cost is one extra round trip per
// call, two in polling mode — runSchema issues CreateCollection and, when
// PollInterval > 0, an index CreateMany. No shipped connector takes this path
// (the tenant-manager middleware sets both keys), so it warns once per Store
// rather than staying silent about what it is paying for.
func (s *Store) ensureSchema(ctx context.Context, tenant string, coll *mongo.Collection, tenantScoped bool) error {
	cacheKey := schemaCacheKey(tenant, coll)

	run := func(ctx context.Context) error {
		if s.schemaRunner != nil {
			// Test seam — see the schemaRunner field.
			return s.schemaRunner(ctx, cacheKey)
		}

		return s.runSchema(ctx, coll, tenant, tenantScoped)
	}

	if tenantScoped && tenant == "" {
		s.noTenantIDWarn.Do(func() { s.logWarn(ctx, warnSchemaWithoutTenantID) })

		return run(ctx)
	}

	return s.ensureSchemaByKey(ctx, cacheKey, run)
}

// ensureSchemaByKey is the testable core of ensureSchema. It accepts a stable
// cache key plus a runner closure so tests can drive the once/err cache
// without standing up a real *mongo.Collection. run MUST be non-nil: the
// schemaRunner test seam is consulted by ensureSchema's run closure alone, so
// that a nil runner is a compile-time-visible caller mistake here rather than
// a nil-func panic inside once.Do.
func (s *Store) ensureSchemaByKey(ctx context.Context, cacheKey string, run func(context.Context) error) error {
	// Load first: a bootstrapped database keeps its once entry, so the Load
	// allocates nothing; only the first caller for a key, and a retry after a
	// failure, reaches LoadOrStore. Composing cacheKey upstream does allocate,
	// once per call, on every read and write.
	onceVal, ok := s.schemaOnce.Load(cacheKey)
	if !ok {
		onceVal, _ = s.schemaOnce.LoadOrStore(cacheKey, &sync.Once{})
	}

	once, ok := onceVal.(*sync.Once)
	if !ok {
		// The map is written nowhere else, so this is unreachable today; it is
		// kept because the alternative — discarding the comma-ok — turns any
		// future writer of a different type into a nil-pointer panic on the
		// read path of every read and write, instead of one wasted bootstrap.
		once = &sync.Once{}
		s.schemaOnce.Store(cacheKey, once)
	}

	// runErr captures the error produced by this Do invocation (if any). A
	// transient runSchema failure must NOT cache permanently — callers would
	// be locked out of a healthy tenant for the lifetime of the process. The
	// once entry is evicted on failure so the next ensureSchema call retries;
	// the err tombstone is left in place so concurrent callers that joined
	// the same once.Do can still observe the failure via schemaErr.Load
	// below. It is cleared on the next successful bootstrap.
	var runErr error

	once.Do(func() {
		runErr = run(ctx)
		if runErr != nil {
			// Persist the error so concurrent callers that joined the same
			// once.Do observe the failure via schemaErr.Load below. Evict the
			// once entry so the next ensureSchema call retries. Do NOT delete
			// the err tombstone here — that would race the concurrent callers
			// joined to this Do and they would incorrectly return nil. The
			// tombstone is cleared on the next successful bootstrap (see the
			// success branch below).
			s.schemaErr.Store(cacheKey, runErr)
			s.schemaOnce.Delete(cacheKey)

			return
		}

		// Success — clear any stale error tombstone left by a prior failed
		// attempt so subsequent callers observe success.
		s.schemaErr.Delete(cacheKey)
	})

	if runErr != nil {
		// This goroutine ran the closure and observed the failure directly.
		return runErr
	}

	// Either the closure ran successfully or it was already executed by a
	// previous call. A non-empty schemaErr here means a concurrent caller
	// ran the closure and saw it fail; we joined the same once.Do but our
	// local runErr is nil because we did not execute the closure. Return
	// the stored error so we do not mask the bootstrap failure.
	if errVal, ok := s.schemaErr.Load(cacheKey); ok {
		if err, _ := errVal.(error); err != nil {
			return err
		}
	}

	return nil
}

// logFieldKeyName is the log field a warning names a configuration key under.
// NOT "key": lib-observability's redaction matches that field name exactly, so
// the canonical logger replaces the value with [REDACTED] and the warning whose
// whole purpose is to name the offending key ships without it. Span attributes
// are not redacted and keep the plain "key" name.
const logFieldKeyName = "keyname"

// scopeAttrs names the database a CRUD span hit and the tenant it touched,
// when the call named one. The tenant is a span attribute and never a metric
// label: a tenant id is unbounded, so it belongs where a trace already costs
// one entry per call rather than in a time series per tenant. It goes under
// the fleet-wide constants.AttrKeyTenantID so one trace query selects a tenant
// across every Lerian service.
//
// coll may be nil on a path that never resolved one; the system attribute is
// still stamped, because a span that says nothing about its backend is worse
// than one that says only which backend it was.
func scopeAttrs(coll *mongo.Collection, scope store.Scope, attrs ...attribute.KeyValue) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs)+4)
	out = append(out, attribute.String(obsconstants.AttrDBSystem, obsconstants.DBSystemMongoDB))

	if coll != nil {
		out = append(out,
			attribute.String(obsconstants.AttrDBName, coll.Database().Name()),
			attribute.String(obsconstants.AttrDBMongoDBCollection, coll.Name()),
		)
	}

	out = append(out, attrs...)

	if scope.Tenant == "" {
		return out
	}

	return append(out, attribute.String(obsconstants.AttrKeyTenantID, scope.Tenant))
}

// List returns every entry from the resolved collection ordered by (namespace, key).
func (s *Store) List(ctx context.Context, scope store.Scope) ([]store.Entry, error) {
	if s == nil || s.isClosed() {
		return nil, store.ErrClosed
	}

	coll, err := s.resolveCollection(ctx, scope)
	if err != nil {
		return nil, err
	}

	ctx, span := s.tracer.Start(ctx, "systemplane.mongodb.list")
	defer span.End()

	span.SetAttributes(scopeAttrs(coll, scope)...)

	findOpts := options.Find().SetSort(bson.D{
		{Key: fieldNamespace, Value: 1},
		{Key: fieldKey, Value: 1},
	})

	cursor, err := coll.Find(ctx, bson.D{notDeleted()}, findOpts)
	if err != nil {
		tracing.HandleSpanError(span, "list find failed", err)

		return nil, fmt.Errorf("systemplane/mongodb: list: %w", err)
	}
	defer cursor.Close(ctx)

	// Non-nil even when the scope is empty, matching Postgres: a caller that
	// marshals the result must not get null from one backend and [] from the
	// other. No capacity hint: the cursor below is drained one document at a
	// time, so the count is unknown until it is done.
	entries := make([]store.Entry, 0)

	// Decoded one document at a time, never in bulk. A single foreign-written
	// document with a badly typed field — a value stored as a sub-document
	// rather than as the JSON string this store writes — fails its own decode,
	// and cursor.All would turn that into an error for the WHOLE scope, so the
	// engine's reconcile after every OpResync could never converge again. One
	// bad document costs one key instead: it is skipped with a warning naming
	// it, and that key falls back to its registered default (FC-11).
	for cursor.Next(ctx) {
		var doc entryDoc

		if err := cursor.Decode(&doc); err != nil {
			namespace, key := docIdentity(cursor.Current)

			// The tenant is on this warning, and on every other
			// document-level warning in this backend, because without it an
			// operator reading the logs of a process carrying dozens of
			// tenant feeds cannot tell which database is emitting garbage.
			s.logWarn(ctx, "list decode error, skipping document",
				log.Err(err),
				log.String(fieldNamespace, namespace),
				log.String(logFieldKeyName, key),
				log.String(obsconstants.AttrKeyTenantID, scope.Tenant),
			)

			continue
		}

		entries = append(entries, doc.toEntry())
	}

	if err := cursor.Err(); err != nil {
		tracing.HandleSpanError(span, "list cursor failed", err)

		return nil, fmt.Errorf("systemplane/mongodb: list cursor: %w", err)
	}

	span.SetAttributes(attribute.Int("entries.count", len(entries)))

	return entries, nil
}

// Get returns a single entry by (namespace, key).
func (s *Store) Get(ctx context.Context, scope store.Scope, namespace, key string) (store.Entry, bool, error) {
	if s == nil || s.isClosed() {
		return store.Entry{}, false, store.ErrClosed
	}

	coll, err := s.resolveCollection(ctx, scope)
	if err != nil {
		return store.Entry{}, false, err
	}

	ctx, span := s.tracer.Start(ctx, "systemplane.mongodb.get")
	defer span.End()

	span.SetAttributes(scopeAttrs(coll, scope,
		attribute.String("namespace", namespace),
		attribute.String("key", key),
	)...)

	filter := bson.D{{Key: fieldID, Value: compoundID{Namespace: namespace, Key: key}}, notDeleted()}

	// Raw() rather than Decode(): it separates the QUERY outcome, which is the
	// caller's problem, from the DECODE outcome, which is one document's.
	raw, err := coll.FindOne(ctx, filter).Raw()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return store.Entry{}, false, nil
		}

		tracing.HandleSpanError(span, "get find failed", err)

		return store.Entry{}, false, fmt.Errorf("systemplane/mongodb: get: %w", err)
	}

	var doc entryDoc

	if err := bson.Unmarshal(raw, &doc); err != nil {
		// A document this backend cannot decode reads as ABSENT rather than as
		// an error: the registered default is then what serves the key, which
		// is what FC-11 prescribes for a stored row the ingress rejects. An
		// error here would instead fail every read of that key.
		s.logWarn(ctx, "get decode error, serving the key as absent",
			log.Err(err),
			log.String(fieldNamespace, namespace),
			log.String(logFieldKeyName, key),
			log.String(obsconstants.AttrKeyTenantID, scope.Tenant),
		)

		return store.Entry{}, false, nil
	}

	return doc.toEntry(), true, nil
}

// Set persists an entry using an upsert keyed on the compound _id.
func (s *Store) Set(ctx context.Context, scope store.Scope, e store.Entry) (int64, error) {
	if s == nil || s.isClosed() {
		return 0, store.ErrClosed
	}

	if e.Namespace == "" || e.Key == "" {
		return 0, fmt.Errorf("systemplane/mongodb: %w: namespace and key must be non-empty", store.ErrValidation)
	}

	if e.UpdatedAt.IsZero() {
		e.UpdatedAt = time.Now().UTC()
	}

	coll, err := s.resolveCollection(ctx, scope)
	if err != nil {
		return 0, err
	}

	ctx, span := s.tracer.Start(ctx, "systemplane.mongodb.set")
	defer span.End()

	span.SetAttributes(scopeAttrs(coll, scope,
		attribute.String("namespace", e.Namespace),
		attribute.String("key", e.Key),
	)...)

	revision, err := upsertReturningRevision(ctx, coll, e)
	if err != nil && mongo.IsDuplicateKeyError(err) {
		// Two concurrent upserts of a not-yet-existing _id can both attempt the
		// insert and one loses on the unique _id. Retry exactly once: the
		// document now exists, so the pipeline takes the update path.
		revision, err = upsertReturningRevision(ctx, coll, e)
	}

	if err != nil {
		tracing.HandleSpanError(span, "set upsert failed", err)

		return 0, fmt.Errorf("systemplane/mongodb: set: %w", err)
	}

	return revision, nil
}

// Delete rewrites a single (namespace, key) row as a tombstone: the document
// stays, carrying deleted: true, no value and a bumped revision, so a key that
// is deleted and recreated always comes back above every revision it ever had
// (FC-9, D11). Get and List treat a tombstone as absent.
//
// Idempotent: the filter excludes tombstones, so deleting an already-deleted
// or never-written key matches nothing, writes nothing, emits no change-stream
// event, and still returns nil.
func (s *Store) Delete(ctx context.Context, scope store.Scope, namespace, key, actor string) error {
	if s == nil || s.isClosed() {
		return store.ErrClosed
	}

	if namespace == "" || key == "" {
		return fmt.Errorf("systemplane/mongodb: %w: namespace and key must be non-empty", store.ErrValidation)
	}

	coll, err := s.resolveCollection(ctx, scope)
	if err != nil {
		return err
	}

	ctx, span := s.tracer.Start(ctx, "systemplane.mongodb.delete")
	defer span.End()

	// actor is intentionally NOT a span attribute: it is unbounded caller
	// identity and would create a high-cardinality / potentially PII tag. It
	// is recorded where audit trails read it: the tombstone's updated_by.
	span.SetAttributes(scopeAttrs(coll, scope,
		attribute.String("namespace", namespace),
		attribute.String("key", key),
	)...)

	filter := bson.D{{Key: fieldID, Value: compoundID{Namespace: namespace, Key: key}}, notDeleted()}

	// No upsert, and MatchedCount is deliberately not inspected: a missing key
	// and an existing tombstone both match nothing, and Delete reports that as
	// success exactly as Postgres does. Rewriting a tombstone would reach the
	// change stream as a second delete at revision 0, which nothing
	// deduplicates, so every subscriber would see a duplicate (FC-9).
	if _, err := coll.UpdateOne(ctx, filter, tombstonePipeline(actor, time.Now().UTC())); err != nil {
		tracing.HandleSpanError(span, "delete tombstone failed", err)

		return fmt.Errorf("systemplane/mongodb: delete: %w", err)
	}

	return nil
}

func (s *Store) logWarn(ctx context.Context, msg string, fields ...log.Field) {
	if s == nil || s.cfg.Logger == nil {
		return
	}

	s.cfg.Logger.Log(ctx, log.LevelWarn, msg, fields)
}

func (s *Store) logInfo(ctx context.Context, msg string, fields ...log.Field) {
	if s == nil || s.cfg.Logger == nil {
		return
	}

	s.cfg.Logger.Log(ctx, log.LevelInfo, msg, fields)
}

func (s *Store) logDebug(ctx context.Context, msg string, fields ...log.Field) {
	if s == nil || s.cfg.Logger == nil {
		return
	}

	s.cfg.Logger.Log(ctx, log.LevelDebug, msg, fields)
}

// logStreakFailure narrates a retry loop: the first failure of each DISTINCT
// cause at WARN, every later one of that same cause at DEBUG. A failure class
// that never resolves on its own — a change stream that can never reopen, a
// tenant that no longer resolves — is otherwise invisible at a production Info
// level, because the one WARN emitted when the stream was lost says nothing
// about why every attempt since has failed, and the engine keeps serving the
// scope it last reconciled in the silence. Dropping the repeats to DEBUG keeps
// that signal at one line per cause per outage.
//
// warned is the caller's per-cause flag, and this is the only writer of it:
// the caller declares one bool per message inside the loop it narrates, so a
// streak that opens on a tenant that will not resolve is still loud when it
// turns into a stream that will not open. It deliberately does NOT read the
// backoff counter. That counter is only cleared by a cursor that did some work,
// so one unproductive cycle — the stream opens and dies before delivering an
// event — leaves it non-zero for the life of the feed, and a loud line gated on
// it would never fire again.
func (s *Store) logStreakFailure(warned *bool, msg string, fields ...log.Field) {
	if !*warned {
		*warned = true

		s.logWarn(context.Background(), msg, fields...)

		return
	}

	s.logDebug(context.Background(), msg, fields...)
}
