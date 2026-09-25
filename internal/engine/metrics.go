package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
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
	tenants atomic.Int64

	events, disconnects, reads metric.Int64Counter
	activation                 metric.Float64Histogram
	gauges                     metric.Registration
}

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

	var errEvents, errDisconnects, errReads, errActivation, errGauges error

	m.events, errEvents = meter.Int64Counter("systemplane.changefeed_events_total",
		metric.WithDescription("Changefeed events received, per scope"))
	m.disconnects, errDisconnects = meter.Int64Counter("systemplane.changefeed_disconnects_total",
		metric.WithDescription("Changefeed connections lost, per scope"))
	m.reads, errReads = meter.Int64Counter("systemplane.cache_reads_total",
		metric.WithDescription("Cached reads, per scope and result (hit or miss)"))
	m.activation, errActivation = meter.Float64Histogram("systemplane.activation_latency_seconds", metric.WithUnit("s"),
		metric.WithDescription("From a tenant's first activating read until its scope is fresh"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10))
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

// observe reports both gauges summed per attribute set after the collapse, so
// an aggregated tenant_id carries every tenant's share.
func (m *metrics) observe(o metric.Observer, active, entries metric.Int64Observable, scopes []*scopeState) {
	type tally struct{ scopes, entries int64 }

	sums := make(map[string]tally, len(scopes))

	for _, sc := range scopes {
		sc.mu.RLock()
		n := int64(len(sc.entries))
		sc.mu.RUnlock()

		label := m.label(sc.scope)
		sum := sums[label]
		sums[label] = tally{scopes: sum.scopes + 1, entries: sum.entries + n}
	}

	for label, sum := range sums {
		o.ObserveInt64(active, sum.scopes, withTenant(label))
		o.ObserveInt64(entries, sum.entries, withTenant(label))
	}
}

// label is scope's tenant_id: none for the single-tenant scope, the literal
// aggregate while more tenant scopes are active than a positive threshold.
func (m *metrics) label(scope store.Scope) string {
	if scope.Tenant != "" && m.threshold > 0 && m.tenants.Load() > m.threshold {
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

// countTenant moves the active tenant count as a tenant scope enters (+1) or
// leaves (-1) e.scopes; the caller holds scopesMu. A nil sc was never tracked.
func (m *metrics) countTenant(sc *scopeState, delta int64) {
	if m != nil && sc != nil && sc.scope.Tenant != "" {
		m.tenants.Add(delta)
	}
}

// feedCallback is what bringUpScope hands Store.Subscribe: onEvent, counting
// every event the changefeed delivers (a disconnect twice over) given a meter.
func (e *Engine) feedCallback() func(store.Event) {
	m := e.metrics
	if m == nil {
		return e.onEvent
	}

	return func(evt store.Event) {
		opt := withTenant(m.label(evt.Scope))
		m.events.Add(context.Background(), 1, opt)

		if evt.Op == store.OpDisconnect {
			m.disconnects.Add(context.Background(), 1, opt)
		}

		e.onEvent(evt)
	}
}

func (m *metrics) recordRead(scope store.Scope, hit bool) {
	if m == nil {
		return
	}

	result := "miss"
	if hit {
		result = "hit"
	}

	m.reads.Add(context.Background(), 1, withTenant(m.label(scope), attribute.String("result", result)))
}

func (m *metrics) recordActivation(scope store.Scope, begun time.Time) {
	if m != nil {
		m.activation.Record(context.Background(), time.Since(begun).Seconds(), withTenant(m.label(scope)))
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
