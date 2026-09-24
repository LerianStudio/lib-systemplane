// Client lifecycle: construction, start, and close for systemplane.
package client

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/v2/mongo"

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
	logger log.Logger

	multiTenant    bool
	catalogService string

	registryMu sync.RWMutex
	registry   map[nskey]keyDef

	startMu   sync.Mutex
	started   atomic.Bool
	closeOnce sync.Once
	closed    atomic.Bool
}

// NewPostgres creates a Client backed by Postgres.
//
// In single-tenant mode both db and listenDSN are required.
// In multi-tenant mode (see WithMultiTenantEnabled) they MAY be nil/empty —
// every method resolves the tenant database from ctx.
func NewPostgres(db *sql.DB, listenDSN string, opts ...Option) (*Client, error) {
	cfg := defaultClientConfig()
	applyClientOptions(&cfg, opts)

	if !cfg.multiTenantEnabled && db == nil {
		return nil, store.ErrNilBackend
	}

	pgStore, err := postgres.New(postgres.Config{
		DB:                 db,
		ListenDSN:          listenDSN,
		Channel:            cfg.listenChannel,
		Table:              cfg.table,
		Logger:             cfg.logger,
		Telemetry:          cfg.telemetry,
		MultiTenantEnabled: cfg.multiTenantEnabled,
		Module:             cfg.module,
	})
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

	if !cfg.multiTenantEnabled && client == nil {
		return nil, store.ErrNilBackend
	}

	mStore, err := mongoDB.New(mongoDB.Config{
		Client:             client,
		Database:           database,
		Collection:         cfg.collection,
		PollInterval:       cfg.pollInterval,
		Logger:             cfg.logger,
		Telemetry:          cfg.telemetry,
		MultiTenantEnabled: cfg.multiTenantEnabled,
		Module:             cfg.module,
	})
	if err != nil {
		return nil, err
	}

	return newClient(mStore, cfg), nil
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
		multiTenant:    cfg.multiTenantEnabled,
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
	})

	return c
}

// Start performs backend bootstrap and, in single-tenant mode, starts the
// engine: it opens the changefeed and returns once the first reconcile has
// confirmed every registered key against the store, so every read taken after
// it serves what is stored rather than the registered default. That reconcile
// also queues the FC-11 announcement for every subscriber registered
// beforehand; the delivery runs on the key's own goroutine, so it may land
// just after Start returns. In multi-tenant mode it only marks the Client
// started; schema bootstrap and reads run lazily against the per-request
// tenant DB.
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
	// what delivers the FC-11 announcement: a subscriber registered before
	// Start is called while Start is still on the stack, and a callback that
	// answers the announcement by writing must not be refused for a Client the
	// consumer considers running. The scope exists by then and the write is
	// fenced by the reconcile window exactly like a feed publication, so
	// read-your-writes holds. Rolled back below when the engine never comes
	// up, so a failed Start leaves the flag exactly as it found it.
	c.started.Store(true)

	// The engine subscribes before it reconciles and rolls a failed Subscribe
	// back itself, so a write landing between the snapshot and the first feed
	// event is still observed. A failed Start leaves the Client usable: the
	// engine retries the scope from nothing on the next Start.
	if !c.multiTenant {
		if err := c.engine.Start(ctx); err != nil {
			c.started.Store(false)

			return err
		}
	}

	return nil
}

// Close unsubscribes from the changefeed and releases backend resources.
//
// Start and Close are mutually exclusive: Close takes startMu so it cannot
// interleave with a concurrent Start that is mid-wiring. closed is set
// inside the same lock, and Start re-checks it under startMu before any
// teardown-visible state mutation.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}

	var closeErr error

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
		closeErr = errors.Join(engineErr, storeErr)
	})

	return closeErr
}
