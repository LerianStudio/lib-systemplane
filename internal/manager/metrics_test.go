//go:build unit

// Production-path coverage for the Manager's OpenTelemetry instruments.
// Builds a real *tracing.Telemetry whose MeterProvider is wired to an
// in-process manual reader so every recordXxx method drives a live
// instrument and the emitted metric points can be asserted.
//
// Also pins the tenantLabel cardinality cap: below the aggregate threshold
// the per-tenant label is preserved; above the threshold every emission
// collapses to tenant_id="aggregate" so Prometheus cardinality stays
// bounded under thousands of tenants.
//
// The newTestTelemetry helper lives in metrics_helper_test.go (build tag
// unit || integration) so the integration-tagged listen tests can reuse it.
package manager

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/LerianStudio/lib-observability/v2/log"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collect snapshots every metric point currently held by the manual reader.
func collect(t *testing.T, r *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("manual reader Collect: %v", err)
	}

	return rm
}

// findScopeMetric returns the scope+metric matching name, or fails the test.
func findScopeMetric(t *testing.T, rm metricdata.ResourceMetrics, scope, name string) metricdata.Metrics {
	t.Helper()

	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name != scope {
			continue
		}

		for _, m := range sm.Metrics {
			if m.Name == name {
				return m
			}
		}
	}

	t.Fatalf("metric %s/%s not found in %#v", scope, name, rm.ScopeMetrics)
	return metricdata.Metrics{}
}

// sumInt64 returns the aggregate int64 sum across all data points for a
// counter metric. Used to assert recordXxx Add behaviour without coupling
// to the OTel attribute-set ordering.
func sumInt64(t *testing.T, m metricdata.Metrics) int64 {
	t.Helper()

	switch agg := m.Data.(type) {
	case metricdata.Sum[int64]:
		var total int64
		for _, dp := range agg.DataPoints {
			total += dp.Value
		}

		return total
	default:
		t.Fatalf("metric %s is not Sum[int64], got %T", m.Name, m.Data)
		return 0
	}
}

// dataPointForAttr returns the int64 data-point matching the given attribute
// key/value pair, or fails the test.
func dataPointForAttr(t *testing.T, m metricdata.Metrics, key, value string) metricdata.DataPoint[int64] {
	t.Helper()

	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("metric %s is not Sum[int64], got %T", m.Name, m.Data)
	}

	for _, dp := range sum.DataPoints {
		if v, ok := dp.Attributes.Value(attribute.Key(key)); ok && v.AsString() == value {
			return dp
		}
	}

	t.Fatalf("data point with %s=%s not found in %s", key, value, m.Name)
	return metricdata.DataPoint[int64]{}
}

func TestMetrics_Init_NilReceiver_IsSafe(t *testing.T) {
	t.Parallel()

	var m *metrics

	m.init() // must not panic
	m.recordTenantActivated(context.Background(), "t")
	m.recordTenantDeactivated(context.Background(), "t")
	m.recordCacheEntries(context.Background(), "t", 1)
	m.recordNotifyReceived(context.Background(), "t", "upsert")
	m.recordListenDisconnect(context.Background(), "t", "wait_failed")
	m.recordWarmloadLatency(context.Background(), "t", 0.1)
	m.recordGetCacheOutcome(context.Background(), "t", "hit")
}

func TestMetrics_Init_NoTelemetry_IsNoOp(t *testing.T) {
	t.Parallel()

	// newMetrics tolerates nil telemetry; init() short-circuits and every
	// recordXxx must stay a no-op (no panic, no instrument allocation).
	m := newMetrics(nil, log.NewNop(), DefaultAggregateTenantThreshold)

	m.init()

	if m.notifyReceived != nil ||
		m.listenDisconnect != nil ||
		m.cacheHits != nil ||
		m.cacheEntries != nil ||
		m.warmload != nil ||
		m.tenantsActive != nil {
		t.Fatal("instruments must remain nil when telemetry is nil")
	}

	// Every record method must accept the no-instrument state.
	ctx := context.Background()
	m.recordTenantActivated(ctx, "t")
	m.recordTenantDeactivated(ctx, "t")
	m.recordCacheEntries(ctx, "t", 5)
	m.recordNotifyReceived(ctx, "t", "upsert")
	m.recordListenDisconnect(ctx, "t", "wait_failed")
	m.recordWarmloadLatency(ctx, "t", 0.1)
	m.recordGetCacheOutcome(ctx, "t", "hit")
}

func TestMetrics_Init_InstantiatesAllInstruments(t *testing.T) {
	t.Parallel()

	tel, _ := newTestTelemetry(t)

	m := newMetrics(tel, log.NewNop(), DefaultAggregateTenantThreshold)

	m.init()

	if m.notifyReceived == nil ||
		m.listenDisconnect == nil ||
		m.cacheHits == nil ||
		m.cacheEntries == nil ||
		m.warmload == nil ||
		m.tenantsActive == nil {
		t.Fatal("init must create every instrument when telemetry is wired")
	}

	// initOnce must guarantee instruments are created exactly once even
	// under concurrent invocation. Calling init() repeatedly is safe.
	first := m.notifyReceived
	m.init()
	m.init()

	if m.notifyReceived != first {
		t.Fatal("init must reuse the same Counter under sync.Once")
	}
}

func TestMetrics_RecordTenantActivatedDeactivated_UpdatesGauge(t *testing.T) {
	t.Parallel()

	tel, reader := newTestTelemetry(t)
	m := newMetrics(tel, log.NewNop(), DefaultAggregateTenantThreshold)

	ctx := context.Background()
	m.recordTenantActivated(ctx, "tenant-A")
	m.recordTenantActivated(ctx, "tenant-B")
	m.recordTenantDeactivated(ctx, "tenant-A")

	if got := atomic.LoadInt64(&m.activeCount); got != 1 {
		t.Fatalf("activeCount: got %d want 1", got)
	}

	rm := collect(t, reader)
	met := findScopeMetric(t, rm, meterName, metricTenantsActive)

	// tenants_active is an Int64UpDownCounter — its data is Sum[int64].
	// We added +1 +1 -1 = 1 across the lifetime.
	if total := sumInt64(t, met); total != 1 {
		t.Fatalf("tenants_active sum: got %d want 1", total)
	}
}

func TestMetrics_RecordCacheEntries_EmitsTenantLabel(t *testing.T) {
	t.Parallel()

	tel, reader := newTestTelemetry(t)
	m := newMetrics(tel, log.NewNop(), DefaultAggregateTenantThreshold)

	m.recordCacheEntries(context.Background(), "tenant-X", 7)

	rm := collect(t, reader)
	met := findScopeMetric(t, rm, meterName, metricCacheEntries)
	dp := dataPointForAttr(t, met, "tenant_id", "tenant-X")

	if dp.Value != 7 {
		t.Fatalf("cache_entries value: got %d want 7", dp.Value)
	}
}

func TestMetrics_RecordNotifyReceived_EmitsLabels(t *testing.T) {
	t.Parallel()

	tel, reader := newTestTelemetry(t)
	m := newMetrics(tel, log.NewNop(), DefaultAggregateTenantThreshold)

	m.recordNotifyReceived(context.Background(), "tenant-X", "upsert")
	m.recordNotifyReceived(context.Background(), "tenant-X", "upsert")
	m.recordNotifyReceived(context.Background(), "tenant-X", "delete")

	rm := collect(t, reader)
	met := findScopeMetric(t, rm, meterName, metricNotifyReceivedTotal)

	// Each (op, tenant_id) pair is its own attribute set.
	sum, ok := met.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("notify_received_total is not Sum[int64], got %T", met.Data)
	}

	var (
		upsertCount int64
		deleteCount int64
	)

	for _, dp := range sum.DataPoints {
		op, _ := dp.Attributes.Value(attribute.Key("op"))
		tID, _ := dp.Attributes.Value(attribute.Key("tenant_id"))

		if tID.AsString() != "tenant-X" {
			t.Fatalf("unexpected tenant_id %q", tID.AsString())
		}

		switch op.AsString() {
		case "upsert":
			upsertCount = dp.Value
		case "delete":
			deleteCount = dp.Value
		}
	}

	if upsertCount != 2 {
		t.Fatalf("upsert count: got %d want 2", upsertCount)
	}

	if deleteCount != 1 {
		t.Fatalf("delete count: got %d want 1", deleteCount)
	}
}

func TestMetrics_RecordListenDisconnect_EmitsReason(t *testing.T) {
	t.Parallel()

	tel, reader := newTestTelemetry(t)
	m := newMetrics(tel, log.NewNop(), DefaultAggregateTenantThreshold)

	m.recordListenDisconnect(context.Background(), "tenant-X", "wait_failed")
	m.recordListenDisconnect(context.Background(), "tenant-X", "wait_failed")

	rm := collect(t, reader)
	met := findScopeMetric(t, rm, meterName, metricListenDisconnects)

	if got := sumInt64(t, met); got != 2 {
		t.Fatalf("listen_disconnects_total: got %d want 2", got)
	}

	sum := met.Data.(metricdata.Sum[int64])
	if reason, _ := sum.DataPoints[0].Attributes.Value(attribute.Key("reason")); reason.AsString() != "wait_failed" {
		t.Fatalf("reason attribute: got %q want wait_failed", reason.AsString())
	}
}

func TestMetrics_RecordWarmloadLatency_RecordsHistogram(t *testing.T) {
	t.Parallel()

	tel, reader := newTestTelemetry(t)
	m := newMetrics(tel, log.NewNop(), DefaultAggregateTenantThreshold)

	m.recordWarmloadLatency(context.Background(), "tenant-X", 0.05)
	m.recordWarmloadLatency(context.Background(), "tenant-X", 0.15)

	rm := collect(t, reader)
	met := findScopeMetric(t, rm, meterName, metricWarmloadLatency)

	hist, ok := met.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("warmload_latency is not Histogram[float64], got %T", met.Data)
	}

	if len(hist.DataPoints) == 0 {
		t.Fatal("warmload histogram has no data points")
	}

	dp := hist.DataPoints[0]
	if dp.Count != 2 {
		t.Fatalf("histogram count: got %d want 2", dp.Count)
	}

	if dp.Sum < 0.19 || dp.Sum > 0.21 {
		t.Fatalf("histogram sum: got %v want ~0.20", dp.Sum)
	}
}

func TestMetrics_RecordGetCacheOutcome_EmitsHitMiss(t *testing.T) {
	t.Parallel()

	tel, reader := newTestTelemetry(t)
	m := newMetrics(tel, log.NewNop(), DefaultAggregateTenantThreshold)

	m.recordGetCacheOutcome(context.Background(), "tenant-X", "hit")
	m.recordGetCacheOutcome(context.Background(), "tenant-X", "hit")
	m.recordGetCacheOutcome(context.Background(), "tenant-X", "miss")

	rm := collect(t, reader)
	met := findScopeMetric(t, rm, meterName, metricCacheHitsTotal)

	sum, ok := met.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("cache_hits_total is not Sum[int64], got %T", met.Data)
	}

	var (
		hit  int64
		miss int64
	)

	for _, dp := range sum.DataPoints {
		out, _ := dp.Attributes.Value(attribute.Key("outcome"))
		switch out.AsString() {
		case "hit":
			hit = dp.Value
		case "miss":
			miss = dp.Value
		}
	}

	if hit != 2 || miss != 1 {
		t.Fatalf("cache_hits_total: got hit=%d miss=%d want 2/1", hit, miss)
	}
}

func TestMetrics_TenantLabel_BelowThreshold_KeepsTenantID(t *testing.T) {
	t.Parallel()

	m := newMetrics(nil, log.NewNop(), 10)

	// Below the threshold, each tenant ID is preserved on the label.
	atomic.StoreInt64(&m.activeCount, 5)

	if got := m.tenantLabel("tenant-X"); got != "tenant-X" {
		t.Fatalf("tenantLabel below threshold: got %q want tenant-X", got)
	}
}

func TestMetrics_TenantLabel_AboveThreshold_CollapsesToAggregate(t *testing.T) {
	t.Parallel()

	m := newMetrics(nil, log.NewNop(), 10)

	// Once activeCount exceeds the threshold the label collapses to
	// "aggregate" so Prometheus cardinality stays bounded.
	atomic.StoreInt64(&m.activeCount, 11)

	if got := m.tenantLabel("tenant-X"); got != aggregateTenantLabel {
		t.Fatalf("tenantLabel above threshold: got %q want %q", got, aggregateTenantLabel)
	}
}

func TestMetrics_TenantLabel_NilReceiver_ReturnsTenantID(t *testing.T) {
	t.Parallel()

	var m *metrics

	if got := m.tenantLabel("tenant-X"); got != "tenant-X" {
		t.Fatalf("nil-receiver tenantLabel: got %q want tenant-X", got)
	}
}

func TestMetrics_TenantLabel_NonPositiveThreshold_KeepsTenantID(t *testing.T) {
	t.Parallel()

	// A non-positive threshold disables the cardinality cap entirely.
	m := newMetrics(nil, log.NewNop(), 0)
	atomic.StoreInt64(&m.activeCount, 1_000_000)

	if got := m.tenantLabel("tenant-X"); got != "tenant-X" {
		t.Fatalf("tenantLabel with zero threshold: got %q want tenant-X", got)
	}
}

func TestMetrics_RecordCacheEntries_AboveThreshold_RollsUpLabel(t *testing.T) {
	t.Parallel()

	tel, reader := newTestTelemetry(t)
	m := newMetrics(tel, log.NewNop(), 1)

	// Drive activeCount above the threshold (=1) so every recordXxx that
	// emits a tenant_id label switches to "aggregate".
	atomic.StoreInt64(&m.activeCount, 5)

	m.recordCacheEntries(context.Background(), "tenant-X", 3)
	m.recordCacheEntries(context.Background(), "tenant-Y", 4)
	m.recordNotifyReceived(context.Background(), "tenant-X", "upsert")
	m.recordWarmloadLatency(context.Background(), "tenant-Y", 0.01)
	m.recordGetCacheOutcome(context.Background(), "tenant-Z", "hit")
	m.recordListenDisconnect(context.Background(), "tenant-Y", "wait_failed")

	rm := collect(t, reader)

	for _, sm := range rm.ScopeMetrics {
		for _, met := range sm.Metrics {
			// Skip tenants_active — it has no tenant_id label.
			if met.Name == metricTenantsActive {
				continue
			}

			switch agg := met.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range agg.DataPoints {
					if v, ok := dp.Attributes.Value(attribute.Key("tenant_id")); ok && v.AsString() != aggregateTenantLabel {
						t.Errorf("metric %s: tenant_id label %q must be %q above threshold",
							met.Name, v.AsString(), aggregateTenantLabel)
					}
				}
			case metricdata.Histogram[float64]:
				for _, dp := range agg.DataPoints {
					if v, ok := dp.Attributes.Value(attribute.Key("tenant_id")); ok && v.AsString() != aggregateTenantLabel {
						t.Errorf("metric %s: tenant_id label %q must be %q above threshold",
							met.Name, v.AsString(), aggregateTenantLabel)
					}
				}
			}
		}
	}
}

func TestMetrics_NewMetrics_NilLogger_FallsBackToNop(t *testing.T) {
	t.Parallel()

	// newMetrics must accept nil logger and substitute a no-op so init's
	// "meter unavailable" debug log path stays panic-safe.
	m := newMetrics(nil, nil, DefaultAggregateTenantThreshold)
	if m.logger == nil {
		t.Fatal("newMetrics must substitute a non-nil logger when given nil")
	}
}
