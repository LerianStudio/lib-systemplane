// Package manager implements the v1.5.0 per-tenant cache and push hot-reload
// changefeed for systemplane in multi-tenant mode.
//
// The Manager is constructed once per process and bound to a Client via
// Client.BindManager. The consumer is expected to wire the Manager's
// lifecycle handlers (OnTenantActivated / Suspended / Deleted /
// CredentialsRotated) into its tenant lifecycle event plumbing — typically
// the lib-commons tenant-manager event dispatcher.
//
// Construction order in the consumer:
//
//	client, err := systemplane.NewPostgres(db, dsn,
//	    systemplane.WithMultiTenantEnabled(),
//	    systemplane.WithLogger(logger),
//	    systemplane.WithTelemetry(t),
//	)
//	// ...
//	manager := systemplane.NewManager(client, pgMgr,
//	    systemplane.WithManagerLogger(logger),
//	    systemplane.WithManagerTelemetry(t),
//	)
//	client.BindManager(manager)
package manager

import (
	"context"
	"sync"

	tmpostgres "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/postgres"
	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/tracing"
)

// MaxEntriesPerTenant is the defensive upper bound on cached entries per tenant.
//
// At current scale (~50 registered keys total) the per-tenant cache is tiny;
// the bound exists only to surface misbehaviour (e.g. a runaway registration
// loop) via the cache_entries metric. Reaching the bound emits a warning and
// further writes for that tenant fall through to the tenant DB.
const MaxEntriesPerTenant = 10_000

// DefaultAggregateTenantThreshold controls the cardinality cap on per-tenant
// metric labels. Once the number of active tenants exceeds this threshold,
// telemetry switches the tenant_id label to "aggregate" to bound Prometheus
// cardinality. Operators with very large fleets may raise this via
// WithAggregateTenantThreshold.
const DefaultAggregateTenantThreshold = 1000

// Manager owns per-tenant LISTEN/NOTIFY bookkeeping and the per-tenant
// in-process cache in multi-tenant deployments.
//
// All exported methods are safe for concurrent use. The Manager keeps a
// dedicated LISTEN goroutine per active tenant; goroutines are launched lazily
// in OnTenantActivated and closed in OnTenantSuspended / OnTenantDeleted /
// OnTenantCredentialsRotated (the latter then re-opens). Drain closes every
// remaining goroutine in one shot for graceful shutdown.
type Manager struct {
	// hooks is the Client side of the binding. It is filled in via Bind()
	// (called by the public Client.BindManager method) after construction so
	// the circular Client<->Manager reference resolves cleanly.
	hooks ClientHooks

	pgMgr     *tmpostgres.Manager
	connector Connector

	// perTenant maps tenantID -> *tenantState. sync.Map handles the read-heavy
	// access pattern (every Get checks the per-tenant cache).
	perTenant sync.Map

	// callbacks maps nsKey -> *callbackList. OnChange registers callbacks here
	// in MT mode when a Manager is bound to the Client.
	callbacks sync.Map

	logger    log.Logger
	telemetry *tracing.Telemetry
	metrics   *metrics

	cfg config

	// closed marks the Manager as drained. After Drain() returns, lifecycle
	// handlers no-op and Gets fall through to the DB read path.
	closedMu sync.RWMutex
	closed   bool
}

// ClientHooks is the subset of the Client surface the Manager calls back into.
//
// Defined here (not in internal/client) so the Manager package can describe
// its dependencies without importing internal/client, which would close the
// import cycle. The Client.BindManager method wires a concrete adapter.
type ClientHooks interface {
	// RegisteredKeys returns a snapshot of every (namespace, key) the Client
	// has registered, with the default value to seed if the tenant DB does
	// not have a row for that key. Order is unspecified.
	RegisteredKeys() []RegisteredKey

	// LifecycleContext returns the Client's lifecycle context. Used by the
	// LISTEN goroutine to derive a child context that is canceled when the
	// Client shuts down. Implementations MUST never return nil; return
	// context.Background() if no lifecycle ctx exists.
	LifecycleContext() context.Context
}

// RegisteredKey is the projection of a Client registry entry the Manager
// needs at tenant activation time.
type RegisteredKey struct {
	Namespace    string
	Key          string
	DefaultValue any
}

// config holds the merged ManagerOption values.
type config struct {
	logger                     log.Logger
	telemetry                  *tracing.Telemetry
	aggregateTenantThreshold   int
	listenBackoffBaseMillis    int
	listenBackoffCapSeconds    int
	listenStaleAfterFailures   int
	maxEntriesPerTenantOverride int
}

func defaultConfig() config {
	return config{
		aggregateTenantThreshold:    DefaultAggregateTenantThreshold,
		listenBackoffBaseMillis:     500,
		listenBackoffCapSeconds:     30,
		listenStaleAfterFailures:    3,
		maxEntriesPerTenantOverride: MaxEntriesPerTenant,
	}
}

// Option configures a Manager at construction time.
type Option func(*config)

// WithLogger sets the structured logger for the Manager.
func WithLogger(l log.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithTelemetry sets the OpenTelemetry provider for spans and metrics.
func WithTelemetry(t *tracing.Telemetry) Option {
	return func(c *config) {
		if t != nil {
			c.telemetry = t
		}
	}
}

// WithAggregateTenantThreshold overrides the active-tenant count above which
// per-tenant telemetry labels collapse into a single "aggregate" rollup. A
// non-positive value keeps per-tenant labels regardless of cardinality.
func WithAggregateTenantThreshold(n int) Option {
	return func(c *config) {
		c.aggregateTenantThreshold = n
	}
}

// New constructs a Manager bound to a tenant-manager Postgres Manager.
//
// New is non-blocking: it does not open any LISTEN connections. LISTEN
// goroutines are launched lazily on the first OnTenantActivated call for each
// tenant. New MAY return a non-nil Manager even when pgMgr is nil — the
// Manager will then short-circuit lifecycle handlers and the Get path will
// fall through to the existing v1.4.0 DB-read behaviour. (This shape keeps
// tests simple; production callers should always pass a non-nil pgMgr.)
func New(pgMgr *tmpostgres.Manager, opts ...Option) *Manager {
	cfg := defaultConfig()

	for _, opt := range opts {
		if opt == nil {
			continue
		}

		opt(&cfg)
	}

	logger := cfg.logger
	if logger == nil {
		logger = log.NewNop()
	}

	m := &Manager{
		pgMgr:     pgMgr,
		logger:    logger,
		telemetry: cfg.telemetry,
		metrics:   newMetrics(cfg.telemetry, logger, cfg.aggregateTenantThreshold),
		cfg:       cfg,
	}

	if pgMgr != nil {
		m.connector = &pgMgrConnector{mgr: pgMgr}
	}

	return m
}

// SetConnector replaces the Connector used to resolve tenant handles. Tests
// use this to inject in-memory fakes; production callers should rely on the
// pgMgr-driven default wired in New. Goroutine-safe via the close lock so
// the connector swap synchronizes with Drain.
func (m *Manager) SetConnector(c Connector) {
	if m == nil {
		return
	}

	m.closedMu.Lock()
	defer m.closedMu.Unlock()

	m.connector = c
}

// Bind wires the Client hooks into the Manager. Called from
// Client.BindManager; safe to call exactly once. Subsequent calls are
// ignored.
func (m *Manager) Bind(hooks ClientHooks) {
	if m == nil {
		return
	}

	m.closedMu.Lock()
	defer m.closedMu.Unlock()

	if m.hooks != nil {
		return
	}

	m.hooks = hooks
}

// IsClosed reports whether Drain has been called.
func (m *Manager) IsClosed() bool {
	if m == nil {
		return true
	}

	m.closedMu.RLock()
	defer m.closedMu.RUnlock()

	return m.closed
}

// markClosed marks the Manager closed; idempotent.
func (m *Manager) markClosed() {
	m.closedMu.Lock()
	defer m.closedMu.Unlock()
	m.closed = true
}
