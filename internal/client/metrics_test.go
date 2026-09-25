//go:build unit

package client

import (
	"context"
	"maps"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/tracing"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestTelemetryAndAggregateThresholdReachTheEngineMetrics: the default is
// DefaultAggregateTenantThreshold, and two activated tenants above a threshold
// of 1 report their cached entries under one tenant_id=aggregate.
func TestTelemetryAndAggregateThresholdReachTheEngineMetrics(t *testing.T) {
	if got := defaultClientConfig().aggregateTenantThreshold; got != DefaultAggregateTenantThreshold {
		t.Fatalf("default threshold = %d, want DefaultAggregateTenantThreshold", got)
	}

	reader := sdkmetric.NewManualReader()
	telemetry := &tracing.Telemetry{MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}
	c := newTenantClient(t, newTenantStore(), registerKey("ns", "k", "default"),
		WithTelemetry(telemetry), WithAggregateTenantThreshold(1))

	for _, tenant := range []string{"t1", "t2"} {
		mustEntry(t, c, tenantCtx(tenant), "ns", "k")
		waitSettled(t, c, tenant, "ns", "k")
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	got := map[string]int64{}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if g, ok := m.Data.(metricdata.Gauge[int64]); ok && sm.Scope.Name == "systemplane.engine" && m.Name == "systemplane.cache_entries" {
				for _, p := range g.DataPoints {
					got[p.Attributes.Encoded(attribute.DefaultEncoder())] = p.Value
				}
			}
		}
	}

	if want := map[string]int64{"tenant_id=aggregate": 2}; !maps.Equal(got, want) {
		t.Errorf("systemplane.cache_entries = %v, want %v", got, want)
	}
}
