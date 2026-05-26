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
//     database, lazily ensures the collection's compound _id index on first
//     use, and returns the collection to the CRUD helpers. Change streams are
//     disabled in this mode — Subscribe returns
//     store.ErrNotSupportedInMultiTenant.
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

	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/tracing"
	"github.com/LerianStudio/lib-systemplane/internal/store"
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
	// change streams (which require a replica set). Polling mode is only
	// available in single-tenant mode.
	PollInterval time.Duration

	// MultiTenantEnabled selects the tmcore-driven dispatch path. When true,
	// Client/Database may be empty; every method resolves the per-tenant
	// database from ctx via tmcore.GetMBContext(ctx, Module).
	MultiTenantEnabled bool

	// Module is the tenant-manager module name used as the context key for
	// dispatch. Default: "systemplane".
	Module string

	Logger    log.Logger
	Telemetry *tracing.Telemetry
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
	UpdatedAt time.Time  `bson:"updated_at"`
	UpdatedBy string     `bson:"updated_by"`
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

	// subscriberMu / subscribers serve the single-tenant change-stream path.
	subscriberMu sync.Mutex
	subscribers  map[uint64]func(store.Event)
	nextSubID    uint64
	streamStop   chan struct{}
	streamDone   chan struct{}

	mu     sync.Mutex
	closed bool

	droppedEvents atomic.Int64
}

// Start performs single-tenant collection bootstrap and opens the change
// stream. In multi-tenant mode it is a no-op.
func (s *Store) Start(ctx context.Context) error {
	if s == nil || s.isClosed() {
		return store.ErrClosed
	}

	if s.cfg.MultiTenantEnabled {
		return nil
	}

	if err := s.ensureSchema(ctx, s.coll); err != nil {
		return err
	}

	return s.startListener(ctx)
}

// Close releases backend resources. Idempotent. Does NOT close the externally
// supplied mongo client.
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
	s.mu.Unlock()

	s.stopListener()

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
func (s *Store) resolveCollection(ctx context.Context) (*mongo.Collection, error) {
	if !s.cfg.MultiTenantEnabled {
		return s.coll, nil
	}

	db := tmcore.GetMBContext(ctx, s.cfg.Module)
	if db == nil {
		return nil, store.ErrTenantConnectionMissing
	}

	coll := db.Collection(s.cfg.Collection)
	if err := s.ensureSchema(ctx, coll); err != nil {
		return nil, err
	}

	return coll, nil
}

// schemaCacheKey returns the stable key used to memoize the per-database
// schema bootstrap. The mongo-driver/v2 Collection handle is not guaranteed
// to be reused across calls, so we key by ("<db.Name()>/<collection>").
func schemaCacheKey(coll *mongo.Collection) string {
	return coll.Database().Name() + "/" + coll.Name()
}

func (s *Store) ensureSchema(ctx context.Context, coll *mongo.Collection) error {
	cacheKey := schemaCacheKey(coll)

	return s.ensureSchemaByKey(ctx, cacheKey, func(ctx context.Context) error {
		return s.runSchema(ctx, coll)
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

// List returns every entry from the resolved collection ordered by (namespace, key).
func (s *Store) List(ctx context.Context) ([]store.Entry, error) {
	if s == nil || s.isClosed() {
		return nil, store.ErrClosed
	}

	coll, err := s.resolveCollection(ctx)
	if err != nil {
		return nil, err
	}

	ctx, span := s.tracer.Start(ctx, "systemplane.mongodb.list")
	defer span.End()

	findOpts := options.Find().SetSort(bson.D{
		{Key: fieldNamespace, Value: 1},
		{Key: fieldKey, Value: 1},
	})

	cursor, err := coll.Find(ctx, bson.D{}, findOpts)
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
func (s *Store) Get(ctx context.Context, namespace, key string) (store.Entry, bool, error) {
	if s == nil || s.isClosed() {
		return store.Entry{}, false, store.ErrClosed
	}

	coll, err := s.resolveCollection(ctx)
	if err != nil {
		return store.Entry{}, false, err
	}

	ctx, span := s.tracer.Start(ctx, "systemplane.mongodb.get")
	defer span.End()

	span.SetAttributes(
		attribute.String("namespace", namespace),
		attribute.String("key", key),
	)

	filter := bson.D{{Key: fieldID, Value: compoundID{Namespace: namespace, Key: key}}}

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
func (s *Store) Set(ctx context.Context, e store.Entry) error {
	if s == nil || s.isClosed() {
		return store.ErrClosed
	}

	if e.Namespace == "" || e.Key == "" {
		return fmt.Errorf("systemplane/mongodb: %w: namespace and key must be non-empty", store.ErrValidation)
	}

	if e.UpdatedAt.IsZero() {
		e.UpdatedAt = time.Now().UTC()
	}

	coll, err := s.resolveCollection(ctx)
	if err != nil {
		return err
	}

	ctx, span := s.tracer.Start(ctx, "systemplane.mongodb.set")
	defer span.End()

	span.SetAttributes(
		attribute.String("namespace", e.Namespace),
		attribute.String("key", e.Key),
	)

	if err := upsert(ctx, coll, e); err != nil {
		tracing.HandleSpanError(span, "set upsert failed", err)

		return fmt.Errorf("systemplane/mongodb: set: %w", err)
	}

	return nil
}

// Delete removes a single (namespace, key) row. Idempotent.
func (s *Store) Delete(ctx context.Context, namespace, key, actor string) error {
	if s == nil || s.isClosed() {
		return store.ErrClosed
	}

	if namespace == "" || key == "" {
		return fmt.Errorf("systemplane/mongodb: %w: namespace and key must be non-empty", store.ErrValidation)
	}

	coll, err := s.resolveCollection(ctx)
	if err != nil {
		return err
	}

	ctx, span := s.tracer.Start(ctx, "systemplane.mongodb.delete")
	defer span.End()

	// actor is intentionally NOT a span attribute: it is unbounded caller
	// identity and would create a high-cardinality / potentially PII tag.
	// Audit trails capture it via the UpdatedBy column on writes.
	_ = actor

	span.SetAttributes(
		attribute.String("namespace", namespace),
		attribute.String("key", key),
	)

	filter := bson.D{{Key: fieldID, Value: compoundID{Namespace: namespace, Key: key}}}
	if _, err := coll.DeleteOne(ctx, filter); err != nil {
		tracing.HandleSpanError(span, "delete failed", err)

		return fmt.Errorf("systemplane/mongodb: delete: %w", err)
	}

	return nil
}

func (s *Store) logWarn(ctx context.Context, msg string, fields ...log.Field) {
	if s == nil || s.cfg.Logger == nil {
		return
	}

	s.cfg.Logger.Log(ctx, log.LevelWarn, msg, fields...)
}

func (s *Store) logInfo(ctx context.Context, msg string, fields ...log.Field) {
	if s == nil || s.cfg.Logger == nil {
		return
	}

	s.cfg.Logger.Log(ctx, log.LevelInfo, msg, fields...)
}
