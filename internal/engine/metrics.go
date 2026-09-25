package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	obsmetrics "github.com/LerianStudio/lib-observability/v4/metrics"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// aggregateTenant is the tenant_id every instrument reports once more tenant
// scopes are active than a positive threshold allows (FC-10).
const aggregateTenant = "aggregate"

// metrics holds FC-12's instruments. A nil *metrics records nothing, which is
// what an engine built without a meter carries.
type metrics struct {
	threshold int64
	// tenants counts the tenant scopes in e.scopes; it decides the collapse.
	tenants   atomic.Int64
	aggregate readOptions // what every tenant scope reads under while collapsed

	events, disconnects, reads metric.Int64Counter
	activation                 metric.Float64Histogram
	gauges                     metric.Registration
}

// readOptions are one tenant_id's cache_reads_total options, built once so a
// metered read allocates nothing.
type readOptions struct{ hit, miss []metric.AddOption }

// newMetrics builds the instruments from meter systemplane.engine, or returns
// nil when there is no telemetry or the meter refuses any of them. scopes is
// what the gauge callback reports: every scope the engine tracks.
func newMetrics(t store.Telemetry, threshold int, logger log.Logger, scopes func() []*scopeState) *metrics {
	if t == nil {
		return nil
	}

	meter, err := t.Meter("systemplane.engine")
	if err != nil {
		logger.Log(context.Background(), log.LevelDebug, "engine metrics disabled: no meter", []log.Field{log.Err(err)})

		return nil
	}

	m := &metrics{threshold: int64(threshold)}
	m.aggregate = m.readOptions(aggregateTenant)

	var errEvents, errDisconnects, errReads, errActivation, errGauges error

	m.events, errEvents = meter.Int64Counter("systemplane.changefeed_events_total",
		metric.WithDescription("Changefeed events received, per scope"))
	m.disconnects, errDisconnects = meter.Int64Counter("systemplane.changefeed_disconnects_total",
		metric.WithDescription("Changefeed connections lost, per scope"))
	m.reads, errReads = meter.Int64Counter("systemplane.cache_reads_total",
		metric.WithDescription("Cached reads, per scope and result (hit or miss)"))
	m.activation, errActivation = meter.Float64Histogram("systemplane.activation_latency_seconds", metric.WithUnit("s"),
		metric.WithDescription("From a tenant's first activating read until its scope is fresh"),
		metric.WithExplicitBucketBoundaries(obsmetrics.DefaultLatencyBuckets...))
	active, errActive := meter.Int64ObservableGauge("systemplane.scopes_active",
		metric.WithDescription("Scopes the engine tracks"))
	entries, errEntries := meter.Int64ObservableGauge("systemplane.cache_entries",
		metric.WithDescription("Entries cached, per scope"))

	m.gauges, errGauges = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		m.observe(o, active, entries, scopes())

		return nil
	}, active, entries)

	if err := errors.Join(errEvents, errDisconnects, errReads, errActivation, errActive, errEntries, errGauges); err != nil {
		logger.Log(context.Background(), log.LevelDebug, "engine metrics disabled: instrument refused", []log.Field{log.Err(err)})

		return nil
	}

	return m
}

// observe reports scopes_active as one count, as v3's tenants_active was, and
// cache_entries summed per tenant_id under one collapse decision, so aggregate
// carries every tenant's share.
func (m *metrics) observe(o metric.Observer, active, entries metric.Int64Observable, scopes []*scopeState) {
	o.ObserveInt64(active, int64(len(scopes)))

	collapsed := m.collapsed()
	sums := make(map[string]int64, len(scopes))

	for _, sc := range scopes {
		sc.mu.RLock()
		sums[tenantID(sc.scope, collapsed)] += int64(len(sc.entries))
		sc.mu.RUnlock()
	}

	for label, n := range sums {
		o.ObserveInt64(entries, n, withTenant(label))
	}
}

// collapsed reports that more tenant scopes are active than a positive
// threshold allows, so every one of them reports tenant_id=aggregate.
func (m *metrics) collapsed() bool {
	return m.threshold > 0 && m.tenants.Load() > m.threshold
}

// tenantID is scope's tenant_id: none for the single-tenant scope, the literal
// aggregate for a tenant scope while collapsed.
func tenantID(scope store.Scope, collapsed bool) string {
	if scope.Tenant != "" && collapsed {
		return aggregateTenant
	}

	return scope.Tenant
}

func withTenant(label string, kv ...attribute.KeyValue) metric.MeasurementOption {
	if label != "" {
		kv = append(kv, attribute.String("tenant_id", label))
	}

	return metric.WithAttributes(kv...)
}

// readOptions builds label's cache_reads_total options; a nil *metrics builds
// none, as it records none.
func (m *metrics) readOptions(label string) readOptions {
	if m == nil {
		return readOptions{}
	}

	return readOptions{
		hit:  []metric.AddOption{withTenant(label, attribute.String("result", "hit"))},
		miss: []metric.AddOption{withTenant(label, attribute.String("result", "miss"))},
	}
}

// countTenant moves the active tenant count as a tenant scope enters (+1) or
// leaves (-1) e.scopes; the caller holds scopesMu. A nil sc was never tracked.
func (m *metrics) countTenant(sc *scopeState, delta int64) {
	if m != nil && sc != nil && sc.scope.Tenant != "" {
		m.tenants.Add(delta)
	}
}

// recordEvent counts one changefeed event, and a disconnect twice over.
func (m *metrics) recordEvent(evt store.Event) {
	if m == nil {
		return
	}

	opt := withTenant(tenantID(evt.Scope, m.collapsed()))
	m.events.Add(context.Background(), 1, opt)

	if evt.Op == store.OpDisconnect {
		m.disconnects.Add(context.Background(), 1, opt)
	}
}

// recordRead counts one read of tracked scope sc with the options built as sc
// entered the engine.
func (m *metrics) recordRead(sc *scopeState, hit bool) {
	if m == nil {
		return
	}

	opts := sc.reads
	if sc.scope.Tenant != "" && m.collapsed() {
		opts = m.aggregate
	}

	read := opts.miss
	if hit {
		read = opts.hit
	}

	m.reads.Add(context.Background(), 1, read...)
}

func (m *metrics) recordActivation(scope store.Scope, begun time.Time) {
	if m != nil {
		m.activation.Record(context.Background(), time.Since(begun).Seconds(), withTenant(tenantID(scope, m.collapsed())))
	}
}

// unregister stops the gauge callback, so a closed engine reports no scope.
func (m *metrics) unregister(logger log.Logger) {
	if m == nil {
		return
	}

	if err := m.gauges.Unregister(); err != nil {
		logger.Log(context.Background(), log.LevelDebug, "engine metrics: gauge callback not unregistered", []log.Field{log.Err(err)})
	}
}
