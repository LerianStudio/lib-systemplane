// Client lifecycle: construction, start, and close for systemplane.
package client

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/runtime"
	"github.com/LerianStudio/lib-observability/tracing"
	"github.com/LerianStudio/lib-systemplane/internal/debounce"
	mongoDB "github.com/LerianStudio/lib-systemplane/internal/mongodb"
	"github.com/LerianStudio/lib-systemplane/internal/postgres"
	"github.com/LerianStudio/lib-systemplane/internal/store"
)

// refreshTimeout bounds Get calls made by the changefeed-driven refresh loop.
const refreshTimeout = 5 * time.Second

// nskey is the composite map key for registry/cache/subscriber lookups.
type nskey struct {
	Namespace string
	Key       string
}

type subscription struct {
	id uint64
	fn func(newValue any)
}

// Client is the runtime-config handle. Read methods are nil-receiver safe,
// returning zero values when the Client is nil or not yet started.
type Client struct {
	store     store.Store
	debouncer *debounce.Debouncer[nskey]
	logger    log.Logger
	telemetry *tracing.Telemetry

	multiTenant bool

	registryMu sync.RWMutex
	registry   map[nskey]keyDef

	// cache is only populated in single-tenant mode. Multi-tenant mode reads
	// directly from the resolved tenant DB.
	cacheMu sync.RWMutex
	cache   map[nskey]any

	subsMu      sync.RWMutex
	subscribers map[nskey][]subscription
	nextSubID   atomic.Uint64

	storeUnsubscribe func()

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
	logger := cfg.logger
	if logger == nil {
		logger = log.NewNop()
	}

	c := &Client{
		store:       s,
		logger:      logger,
		telemetry:   cfg.telemetry,
		multiTenant: cfg.multiTenantEnabled,
		registry:    make(map[nskey]keyDef),
		cache:       make(map[nskey]any),
		subscribers: make(map[nskey][]subscription),
	}

	if !cfg.multiTenantEnabled {
		c.debouncer = debounce.New[nskey](cfg.debounce, debounce.WithLogger[nskey](logger))
	}

	return c
}

// Start performs backend bootstrap and (in single-tenant mode) hydrates the
// in-process cache. In multi-tenant mode it only marks the Client started;
// schema bootstrap and reads run lazily against the per-request tenant DB.
func (c *Client) Start(ctx context.Context) error {
	if c == nil || c.closed.Load() {
		return ErrClosed
	}

	if ctx == nil {
		return ErrNilContext
	}

	if c.store == nil {
		return ErrClosed
	}

	c.startMu.Lock()
	defer c.startMu.Unlock()

	if c.started.Load() {
		return nil
	}

	if err := c.store.Start(ctx); err != nil {
		return err
	}

	// Seed cache with registered defaults in single-tenant mode.
	if !c.multiTenant {
		c.registryMu.RLock()
		c.cacheMu.Lock()

		for nk, def := range c.registry {
			c.cache[nk] = cloneValue(def.defaultValue)
		}

		c.cacheMu.Unlock()
		c.registryMu.RUnlock()

		// Subscribe BEFORE hydration so writes that land between List() and
		// the change feed's first event are still observed.
		unsub, err := c.store.Subscribe(ctx, c.onEvent)
		if err != nil {
			return err
		}

		c.storeUnsubscribe = unsub

		if err := c.hydrate(ctx); err != nil {
			if c.storeUnsubscribe != nil {
				c.storeUnsubscribe()
				c.storeUnsubscribe = nil
			}

			return err
		}
	}

	c.started.Store(true)

	return nil
}

func (c *Client) hydrate(ctx context.Context) error {
	entries, err := c.store.List(ctx)
	if err != nil {
		return err
	}

	c.registryMu.RLock()

	for _, entry := range entries {
		nk := nskey{Namespace: entry.Namespace, Key: entry.Key}

		if _, registered := c.registry[nk]; !registered {
			c.logWarn(ctx, "unregistered key in store, skipping",
				log.String("namespace", entry.Namespace),
				log.String("key", entry.Key),
			)

			continue
		}

		var decoded any
		if err := json.Unmarshal(entry.Value, &decoded); err != nil {
			c.logWarn(ctx, "failed to unmarshal stored value, keeping default",
				log.String("namespace", entry.Namespace),
				log.String("key", entry.Key),
				log.Err(err),
			)

			continue
		}

		c.cacheMu.Lock()
		c.cache[nk] = decoded
		c.cacheMu.Unlock()
	}

	c.registryMu.RUnlock()

	return nil
}

// Close unsubscribes from the changefeed and releases backend resources.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}

	c.closeOnce.Do(func() {
		c.closed.Store(true)

		if c.storeUnsubscribe != nil {
			c.storeUnsubscribe()
			c.storeUnsubscribe = nil
		}

		if c.debouncer != nil {
			c.debouncer.Close()
		}

		if c.store != nil {
			_ = c.store.Close()
		}
	})

	return nil
}

// onEvent debounces a backend event by (namespace, key) and refreshes the
// cache when the debounce window closes. Single-tenant mode only.
func (c *Client) onEvent(evt store.Event) {
	nk := nskey{Namespace: evt.Namespace, Key: evt.Key}

	if c.debouncer == nil {
		c.refreshFromStore(nk, evt.Op)

		return
	}

	op := evt.Op

	c.debouncer.Submit(nk, func() {
		c.refreshFromStore(nk, op)
	})
}

func (c *Client) refreshFromStore(nk nskey, op string) {
	c.registryMu.RLock()
	def, registered := c.registry[nk]
	c.registryMu.RUnlock()

	if !registered {
		c.logWarn(context.Background(), "changefeed event for unregistered key, skipping",
			log.String("namespace", nk.Namespace),
			log.String("key", nk.Key),
		)

		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), refreshTimeout)
	defer cancel()

	newValue := cloneValue(def.defaultValue)

	if op != store.OpDelete {
		entry, found, err := c.store.Get(ctx, nk.Namespace, nk.Key)
		if err != nil {
			c.logWarn(ctx, "refresh from store failed",
				log.String("namespace", nk.Namespace),
				log.String("key", nk.Key),
				log.Err(err),
			)

			return
		}

		if found {
			var decoded any
			if err := json.Unmarshal(entry.Value, &decoded); err != nil {
				c.logWarn(ctx, "failed to unmarshal refreshed value, keeping current",
					log.String("namespace", nk.Namespace),
					log.String("key", nk.Key),
					log.Err(err),
				)

				return
			}

			newValue = decoded
		}
	}

	c.cacheMu.Lock()
	c.cache[nk] = cloneValue(newValue)
	c.cacheMu.Unlock()

	c.fireSubscribers(nk, cloneValue(newValue))
}

// fireSubscribers invokes all OnChange callbacks for a key with panic recovery.
func (c *Client) fireSubscribers(nk nskey, newValue any) {
	c.subsMu.RLock()
	subs := make([]subscription, len(c.subscribers[nk]))
	copy(subs, c.subscribers[nk])
	c.subsMu.RUnlock()

	for _, sub := range subs {
		fn := sub.fn

		func() {
			defer runtime.RecoverAndLog(c.logger, "systemplane.onchange")

			fn(cloneValue(newValue))
		}()
	}
}
