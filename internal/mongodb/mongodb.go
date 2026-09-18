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
	defaultCollection  = "systemplane_entries"
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

	// Collection is the collection name. Default: "systemplane_entries".
	Collection string

	// PollInterval enables polling mode when positive. A zero value uses
	// change streams (which require a replica set). Polling serves every
	// scope a change stream serves — the zero scope and a named tenant
	// alike — with the same store.OpResync, store.OpDisconnect and revision
	// guarantees.
	PollInterval time.Duration

	// MultiTenantEnabled selects the tmcore-driven dispatch path. When true,
	// Client/Database may be empty; every method resolves the per-tenant
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
	// database. Keyed by a stable string ("<db.Name()>/<collectionName>")
	// rather than the *mongo.Collection pointer — the mongo-driver/v2
	// Database.Collection method MAY return a fresh handle per call, so a
	// pointer-keyed map would miss every time and rerun the schema probe.
	schemaOnce sync.Map // map[string]*sync.Once
	schemaErr  sync.Map // map[string]error

	// schemaRunner allows unit tests to inject a runSchema replacement that
	// fails on demand without standing up a live MongoDB. Production callers
	// leave this nil; ensureSchema falls back to s.runSchema in that case.
	//
	// The signature uses an interface argument so tests don't need to
	// construct a *mongo.Collection; we pass the cacheKey string instead.
	schemaRunner func(ctx context.Context, cacheKey string) error

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

	if s.cfg.MultiTenantEnabled {
		return nil
	}

	if err := s.ensureSchema(ctx, s.coll, false); err != nil {
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

		coll := db.Collection(s.cfg.Collection)
		if err := s.ensureSchema(ctx, coll, true); err != nil {
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

	coll := db.Collection(s.cfg.Collection)
	if err := s.ensureSchema(ctx, coll, true); err != nil {
		return nil, err
	}

	return coll, nil
}

// schemaCacheKey returns the stable key used to memoize the per-database
// schema bootstrap. The mongo-driver/v2 Collection handle is not guaranteed
// to be reused across calls, so we key by the CONNECTION plus the names:
// ("<client>/<db.Name()>/<collection>"). The client identity is load-bearing —
// two tenants on two clusters may both call their database "systemplane", and
// a name-only key would report the second one as already bootstrapped and
// never materialize its collection.
func schemaCacheKey(coll *mongo.Collection) string {
	db := coll.Database()

	return fmt.Sprintf("%p/%s/%s", db.Client(), db.Name(), coll.Name())
}

// ensureSchema memoizes the per-database bootstrap. tenantScoped marks a
// collection that belongs to a tenant database — carried by ctx or resolved
// through the connector — and is threaded into runSchema.
func (s *Store) ensureSchema(ctx context.Context, coll *mongo.Collection, tenantScoped bool) error {
	cacheKey := schemaCacheKey(coll)

	return s.ensureSchemaByKey(ctx, cacheKey, func(ctx context.Context) error {
		return s.runSchema(ctx, coll, tenantScoped)
	})
}

// ensureSchemaByKey is the testable core of ensureSchema. It accepts a stable
// cache key plus a runner closure so tests can drive the once/err cache via a
// stubbed runSchema without standing up a real *mongo.Collection.
func (s *Store) ensureSchemaByKey(ctx context.Context, cacheKey string, run func(context.Context) error) error {
	if s.schemaRunner != nil {
		// Test seam — replace the runner entirely.
		run = func(ctx context.Context) error { return s.schemaRunner(ctx, cacheKey) }
	}

	onceVal, _ := s.schemaOnce.LoadOrStore(cacheKey, &sync.Once{})
	once, _ := onceVal.(*sync.Once)

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

// scopeAttrs names the tenant a CRUD span touched, when the call named one.
// The tenant is a span attribute and never a metric label: a tenant id is
// unbounded, so it belongs where a trace already costs one entry per call
// rather than in a time series per tenant.
func scopeAttrs(scope store.Scope, attrs ...attribute.KeyValue) []attribute.KeyValue {
	if scope.Tenant == "" {
		return attrs
	}

	return append(attrs, attribute.String("tenant", scope.Tenant))
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

	span.SetAttributes(scopeAttrs(scope)...)

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

	var docs []entryDoc
	if err := cursor.All(ctx, &docs); err != nil {
		tracing.HandleSpanError(span, "list decode failed", err)

		return nil, fmt.Errorf("systemplane/mongodb: list decode: %w", err)
	}

	entries := make([]store.Entry, len(docs))
	for i := range docs {
		entries[i] = docs[i].toEntry()
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

	span.SetAttributes(scopeAttrs(scope,
		attribute.String("namespace", namespace),
		attribute.String("key", key),
	)...)

	filter := bson.D{{Key: fieldID, Value: compoundID{Namespace: namespace, Key: key}}, notDeleted()}

	var doc entryDoc
	if err := coll.FindOne(ctx, filter).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return store.Entry{}, false, nil
		}

		tracing.HandleSpanError(span, "get find failed", err)

		return store.Entry{}, false, fmt.Errorf("systemplane/mongodb: get: %w", err)
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

	span.SetAttributes(scopeAttrs(scope,
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
	span.SetAttributes(scopeAttrs(scope,
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
