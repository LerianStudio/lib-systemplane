package client

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/v2/mongo"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/engine"
	mongoDB "github.com/LerianStudio/lib-systemplane/v4/internal/mongodb"
	"github.com/LerianStudio/lib-systemplane/v4/internal/postgres"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// nskey is the composite map key for registry lookups.
type nskey struct {
	Namespace string
	Key       string
}

// Client is the runtime-config handle. Read methods are nil-receiver safe,
// returning zero values when the Client is nil or not yet started.
type Client struct {
	store  store.Store
	engine *engine.Engine
	// logger is the consumer's own, unwrapped, because Logger() hands it back.
	// Nothing in this package logs through it: guarded is what every line goes
	// out on, so a consumer logger that panics cannot unwind out of a library
	// call — logRead runs on the CALLER's goroutine in multi-tenant mode, so
	// an unguarded one took a Get or a List down with it.
	logger  log.Logger
	guarded log.Logger

	multiTenant bool
	// tenantManaged: a tenant manager was configured, so named scopes resolve
	// through a connector.
	tenantManaged  bool
	catalogService string

	registryMu sync.RWMutex
	registry   map[nskey]keyDef

	startMu sync.Mutex
	started atomic.Bool
	// registryFrozen keeps Register refused after a Start that timed out with
	// its first reconcile pending: the next Start waits on that reconcile, which
	// may already have read the registry. Guarded by startMu.
	registryFrozen bool
	closeOnce      sync.Once
	closed         atomic.Bool
	// closeErr is the first Close's outcome, replayed by every later Close
	// the way the engine replays its own.
	closeErr error
}

// NewPostgres creates a Client backed by Postgres.
//
// In single-tenant mode db and listenDSN are required. In multi-tenant mode
// (see WithMultiTenantEnabled) neither is used and both MAY be nil/empty:
// tenant databases come from ctx or from the tenant manager.
func NewPostgres(db *sql.DB, listenDSN string, opts ...Option) (*Client, error) {
	cfg := defaultClientConfig()
	applyClientOptions(&cfg, opts)

	if cfg.mbTenantManager != nil {
		return nil, fmt.Errorf("%w: WithMongoTenantManager passed to NewPostgres", ErrTenantManagerBackendMismatch)
	}

	if !cfg.multiTenantEnabled && db == nil {
		return nil, store.ErrNilBackend
	}

	pgStore, err := postgres.New(postgresConfig(db, listenDSN, cfg))
	if err != nil {
		return nil, err
	}

	return newClient(pgStore, cfg), nil
}

// NewMongoDB creates a Client backed by MongoDB.
//
// In single-tenant mode both client and database are required.
// In multi-tenant mode they MAY be nil/empty.
func NewMongoDB(client *mongo.Client, database string, opts ...Option) (*Client, error) {
	cfg := defaultClientConfig()
	applyClientOptions(&cfg, opts)

	if cfg.pgTenantManager != nil {
		return nil, fmt.Errorf("%w: WithPostgresTenantManager passed to NewMongoDB", ErrTenantManagerBackendMismatch)
	}

	if !cfg.multiTenantEnabled && client == nil {
		return nil, store.ErrNilBackend
	}

	mStore, err := mongoDB.New(mongoConfig(client, database, cfg))
	if err != nil {
		return nil, err
	}

	return newClient(mStore, cfg), nil
}

// postgresConfig builds the Postgres backend's configuration from the
// Client's own. It and mongoConfig below exist for two fields: the logger, and
// a Connector set only for a non-nil tenant manager, so a nil one leaves the
// backend refusing named scopes with store.ErrTenantConnectorMissing.
//
// cfg.logger is the GUARDED logger — the backends log from their changefeed
// goroutines, where a consumer logger that panics takes the process down — and
// cfg.consumerLogger, the raw one Logger() hands back, sits beside it in the
// same struct. Inline in the constructors the choice between them was a
// literal nobody could hold: swapping it compiled and left the suite green,
// because no unit test runs a live changefeed. Named, it is one value a test
// can assert is already guarded.
func postgresConfig(db *sql.DB, listenDSN string, cfg clientConfig) postgres.Config {
	pc := postgres.Config{
		DB:                 db,
		ListenDSN:          listenDSN,
		Logger:             cfg.logger,
		Telemetry:          cfg.telemetry,
		MultiTenantEnabled: cfg.multiTenantEnabled,
		Module:             cfg.module,
	}

	if cfg.pgTenantManager != nil {
		pc.Connector = postgres.NewTenantManagerConnector(cfg.pgTenantManager)
	}

	return pc
}

// mongoConfig is postgresConfig's twin for the MongoDB backend, and carries
// the same guarded logger for the same reason.
func mongoConfig(client *mongo.Client, database string, cfg clientConfig) mongoDB.Config {
	mc := mongoDB.Config{
		Client:             client,
		Database:           database,
		PollInterval:       cfg.pollInterval,
		Logger:             cfg.logger,
		Telemetry:          cfg.telemetry,
		MultiTenantEnabled: cfg.multiTenantEnabled,
		Module:             cfg.module,
	}

	if cfg.mbTenantManager != nil {
		mc.Connector = mongoDB.NewTenantManagerConnector(cfg.mbTenantManager)
	}

	return mc
}

func newClient(s store.Store, cfg clientConfig) *Client {
	// cfg.logger is guarded by applyClientOptions; the Client's own field
	// keeps the consumer's logger unwrapped, because Logger() hands it back.
	// A config built by hand — every test that skips applyClientOptions — has
	// neither, so both normalise to a no-op here.
	guarded := cfg.logger
	if log.IsNil(guarded) {
		guarded = log.NewNop()
	}

	own := cfg.consumerLogger
	if log.IsNil(own) {
		own = log.NewNop()
	}

	c := &Client{
		store:          s,
		logger:         own,
		guarded:        guarded,
		multiTenant:    cfg.multiTenantEnabled,
		tenantManaged:  cfg.pgTenantManager != nil || cfg.mbTenantManager != nil,
		catalogService: cfg.catalogService,
		registry:       make(map[nskey]keyDef),
	}

	// Built in both modes so Close stays uniform: engine.New opens no
	// connection and starts no goroutine, and only Start creates a scope.
	// Registry is the Client itself, which is why this cannot be a field of
	// the literal above.
	c.engine = engine.New(engine.Config{
		Store:    s,
		Registry: c,
		Logger:   guarded,
		Debounce: cfg.debounce,

		// Left zero when the caller set no WithCloseTimeout, so the engine's
		// own default is the single place that names a duration.
		CloseTimeout: cfg.closeTimeout,

		// A tenant scope's read-back grades under its tenant, as a Set for it does.
		ValidatorContext: func(ctx context.Context, scope store.Scope) context.Context {
			return tmcore.ContextWithTenantID(ctx, scope.Tenant)
		},

		Telemetry:                cfg.telemetry,
		AggregateTenantThreshold: cfg.aggregateTenantThreshold,
	})

	return c
}

// Start, in single-tenant mode, starts the engine once MongoDB has ensured its
// collection: it opens the changefeed and returns once the first reconcile has
// confirmed every registered key against the store, so every read taken after
// it serves what is stored rather than the registered default. That reconcile
// also queues the Start announcement for every subscriber registered
// beforehand; the delivery runs on the key's own goroutine, so it may land
// just after Start returns. In multi-tenant mode it only marks the Client
// started; a tenant's first read activates its scope under a tenant manager,
// MongoDB bootstraps each tenant database lazily, and Postgres issues no DDL.
//
// The Client counts as started from the moment that first reconcile begins
// rather than from when it ends, so a [Client.Set] racing Start inside that
// window persists its row and then reports [ErrNotStarted] because no scope
// exists yet to publish into — where before it refused the write outright and
// wrote nothing.
//
// Start and Close are mutually exclusive: both take startMu for the duration
// of their work, and Start re-checks `closed` under the lock so a concurrent
// Close that arrived first cannot be interleaved with Start's wiring.
func (c *Client) Start(ctx context.Context) error {
	if c == nil {
		return ErrClosed
	}

	if ctx == nil {
		return ErrNilContext
	}

	c.startMu.Lock()
	defer c.startMu.Unlock()

	// Recheck under the lock: Close may have raced past the unlocked check.
	if c.closed.Load() {
		return ErrClosed
	}

	if c.store == nil {
		return ErrClosed
	}

	if c.started.Load() {
		return nil
	}

	if err := c.store.Start(ctx); err != nil {
		return err
	}

	// Marked started BEFORE the first reconcile, because that reconcile is
	// what delivers the Start announcement: a subscriber registered before
	// Start is called while Start is still on the stack, and a callback that
	// answers the announcement by writing must not be refused for a Client the
	// consumer considers running. The scope exists by then and the write is
	// fenced by the reconcile window exactly like a feed publication, so
	// read-your-writes holds. Rolled back below when the engine never comes
	// up, so a failed Start leaves the flag exactly as it found it.
	c.started.Store(true)

	// The engine subscribes before it reconciles and rolls a failed Subscribe
	// back itself, so a write landing between the snapshot and the first feed
	// event is still observed. A failed Start leaves the Client usable: a
	// failed reconcile is retried from nothing, a ctx expiry is waited on again.
	if !c.multiTenant {
		if err := c.engine.Start(ctx); err != nil {
			c.started.Store(false)
			c.registryFrozen = ctx.Err() != nil

			return err
		}
	}

	return nil
}

// Close cancels the context handed to running callbacks, waits for them up to
// WithCloseTimeout and stops every changefeed; the database handle passed to
// the constructor stays open. Later calls return the first call's result.
//
// Start and Close are mutually exclusive: both hold startMu, and Close sets
// closed under it, which Start re-checks before any wiring.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}

	c.closeOnce.Do(func() {
		c.startMu.Lock()
		defer c.startMu.Unlock()

		c.closed.Store(true)

		// Engine first, store second, and the order is load-bearing: the
		// engine cancels its lifecycle, unsubscribes every scope, drops
		// pending re-reads and drains its dispatch workers before returning,
		// so the store is closed with nothing still reading through it.
		engineErr := c.engine.Close()

		var storeErr error

		if c.store != nil {
			if err := c.store.Close(); err != nil {
				storeErr = fmt.Errorf("systemplane: close store: %w", err)
			}
		}

		// errors.Join(nil, nil) is nil, so the clean path is unchanged, and a
		// subscriber that refused to stop does not swallow a store failure.
		c.closeErr = errors.Join(engineErr, storeErr)
	})

	return c.closeErr
}
