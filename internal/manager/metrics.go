// OpenTelemetry instruments for the Manager.
//
// All recordXxx methods are nil-receiver safe. Instruments are initialized
// lazily on first use via sync.Once so a Manager constructed without
// telemetry stays a no-op (and tests do not need a live MeterProvider).
//
// Tenant-id cardinality is bounded by aggregateTenantThreshold: once the
// active-tenant count exceeds the threshold, recordXxx replaces the
// per-tenant label with the constant "aggregate" so Prometheus does not
// explode under thousands of tenants. Operators with very large fleets can
// raise the threshold via WithManagerAggregateTenantThreshold.
package manager

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/LerianStudio/lib-observability/v2/log"
	"github.com/LerianStudio/lib-observability/v2/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	meterName = "systemplane.manager"

	metricTenantsActive       = "systemplane.manager.tenants_active"
	metricCacheEntries        = "systemplane.manager.cache_entries"
	metricNotifyReceivedTotal = "systemplane.manager.notify_received_total"
	metricListenDisconnects   = "systemplane.manager.listen_disconnects_total"
	metricWarmloadLatency     = "systemplane.manager.warmload_latency_seconds"
	metricCacheHitsTotal      = "systemplane.manager.get_cache_hits_total"

	aggregateTenantLabel = "aggregate"
)

// metrics holds the OpenTelemetry instruments used by the Manager.
type metrics struct {
	telemetry                *tracing.Telemetry
	logger                   log.Logger
	aggregateTenantThreshold int

	// activeCount is the live tenant count, used both as the source for
	// the tenants_active gauge and to drive the aggregate-label decision.
	activeCount int64

	initOnce sync.Once

	notifyReceived   metric.Int64Counter
	listenDisconnect metric.Int64Counter
	cacheHits        metric.Int64Counter
	cacheEntries     metric.Int64UpDownCounter
	warmload         metric.Float64Histogram
	tenantsActive    metric.Int64UpDownCounter
}

// newMetrics constructs a metrics holder. Instruments are created lazily.
func newMetrics(t *tracing.Telemetry, logger log.Logger, aggregateThreshold int) *metrics {
	if logger == nil {
		logger = log.NewNop()
	}

	return &metrics{
		telemetry:                t,
		logger:                   logger,
		aggregateTenantThreshold: aggregateThreshold,
	}
}

// init lazily creates every instrument. Errors are logged at debug and
// the affected instrument stays nil (recordXxx then no-ops).
func (m *metrics) init() {
	if m == nil || m.telemetry == nil {
		return
	}

	m.initOnce.Do(func() {
		meter, err := m.telemetry.Meter(meterName)
		if err != nil || meter == nil {
			m.logger.Log(context.Background(), log.LevelDebug,
				"manager metrics: meter unavailable; instruments disabled",
				log.Err(err),
			)

			return
		}

		if c, err := meter.Int64Counter(metricNotifyReceivedTotal,
			metric.WithDescription("NOTIFYs received per tenant")); err == nil {
			m.notifyReceived = c
		}

		if c, err := meter.Int64Counter(metricListenDisconnects,
			metric.WithDescription("LISTEN connection drops per tenant")); err == nil {
			m.listenDisconnect = c
		}

		if c, err := meter.Int64Counter(metricCacheHitsTotal,
			metric.WithDescription("Per-tenant cache get outcomes (hit/miss)")); err == nil {
			m.cacheHits = c
		}

		if c, err := meter.Int64UpDownCounter(metricCacheEntries,
			metric.WithDescription("Entries cached per tenant")); err == nil {
			m.cacheEntries = c
		}

		if h, err := meter.Float64Histogram(metricWarmloadLatency,
			metric.WithDescription("OnTenantActivated end-to-end latency in seconds")); err == nil {
			m.warmload = h
		}

		if g, err := meter.Int64UpDownCounter(metricTenantsActive,
			metric.WithDescription("Tenants with a live LISTEN goroutine")); err == nil {
			m.tenantsActive = g
		}
	})
}

// tenantLabel returns the value to use for the tenant_id label. Once the
// active tenant count exceeds the threshold, the label collapses to
// "aggregate" to bound Prometheus cardinality.
func (m *metrics) tenantLabel(tenantID string) string {
	if m == nil || m.aggregateTenantThreshold <= 0 {
		return tenantID
	}

	if atomic.LoadInt64(&m.activeCount) > int64(m.aggregateTenantThreshold) {
		return aggregateTenantLabel
	}

	return tenantID
}

func (m *metrics) recordTenantActivated(ctx context.Context, _ string) {
	if m == nil {
		return
	}

	atomic.AddInt64(&m.activeCount, 1)

	m.init()

	if m.tenantsActive != nil {
		m.tenantsActive.Add(ctx, 1)
	}
}

func (m *metrics) recordTenantDeactivated(ctx context.Context, _ string) {
	if m == nil {
		return
	}

	atomic.AddInt64(&m.activeCount, -1)

	m.init()

	if m.tenantsActive != nil {
		m.tenantsActive.Add(ctx, -1)
	}
}

func (m *metrics) recordCacheEntries(ctx context.Context, tenantID string, count int) {
	if m == nil {
		return
	}

	m.init()

	if m.cacheEntries == nil {
		return
	}

	// We can't observe an absolute value with an UpDownCounter directly; emit
	// a single absolute observation via the attribute path. Operators
	// typically scrape this as a gauge through prom view; the snapshot value
	// is what matters.
	m.cacheEntries.Add(ctx, int64(count),
		metric.WithAttributes(attribute.String("tenant_id", m.tenantLabel(tenantID))),
	)
}

func (m *metrics) recordNotifyReceived(ctx context.Context, tenantID, op string) {
	if m == nil {
		return
	}

	m.init()

	if m.notifyReceived == nil {
		return
	}

	m.notifyReceived.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("tenant_id", m.tenantLabel(tenantID)),
			attribute.String("op", op),
		),
	)
}

func (m *metrics) recordListenDisconnect(ctx context.Context, tenantID, reason string) {
	if m == nil {
		return
	}

	m.init()

	if m.listenDisconnect == nil {
		return
	}

	m.listenDisconnect.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("tenant_id", m.tenantLabel(tenantID)),
			attribute.String("reason", reason),
		),
	)
}

func (m *metrics) recordWarmloadLatency(ctx context.Context, tenantID string, seconds float64) {
	if m == nil {
		return
	}

	m.init()

	if m.warmload == nil {
		return
	}

	m.warmload.Record(ctx, seconds,
		metric.WithAttributes(attribute.String("tenant_id", m.tenantLabel(tenantID))),
	)
}

func (m *metrics) recordGetCacheOutcome(ctx context.Context, tenantID, outcome string) {
	if m == nil {
		return
	}

	m.init()

	if m.cacheHits == nil {
		return
	}

	m.cacheHits.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("tenant_id", m.tenantLabel(tenantID)),
			attribute.String("outcome", outcome),
		),
	)
}
