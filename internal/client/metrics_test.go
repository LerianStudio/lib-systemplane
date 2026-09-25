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

// TestTelemetryAndAggregateThresholdReachTheEngineMetrics: two activated
// tenants report under one tenant_id=aggregate above a threshold of 1, and
// under their own ids at the default or with the collapse disabled.
func TestTelemetryAndAggregateThresholdReachTheEngineMetrics(t *testing.T) {
	if got := defaultClientConfig().aggregateTenantThreshold; got != DefaultAggregateTenantThreshold {
		t.Fatalf("default threshold = %d, want DefaultAggregateTenantThreshold", got)
	}

	perTenant := map[string]int64{"tenant_id=t1": 1, "tenant_id=t2": 1}

	for _, tc := range []struct {
		name string
		opts []Option
		want map[string]int64
	}{
		{"above a threshold of 1", []Option{WithAggregateTenantThreshold(1)}, map[string]int64{"tenant_id=aggregate": 2}},
		{"the default threshold", nil, perTenant},
		{"last wins, and 0 disables the collapse", []Option{WithAggregateTenantThreshold(1), WithAggregateTenantThreshold(0)}, perTenant},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			telemetry := &tracing.Telemetry{MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}
			opts := append([]Option{WithTelemetry(telemetry)}, tc.opts...)
			c := newTenantClient(t, newTenantStore(), registerKey("ns", "k", "default"), opts...)

			for _, tenant := range []string{"t1", "t2"} {
				mustEntry(t, c, tenantCtx(tenant), "ns", "k")
				waitSettled(t, c, tenant, "ns", "k")
			}

			if got := scopesActive(t, reader); !maps.Equal(got, tc.want) {
				t.Errorf("systemplane.scopes_active = %v, want %v", got, tc.want)
			}
		})
	}
}

// scopesActive collects meter systemplane.engine's scopes gauge as encoded
// attribute set -> tracked scopes.
func scopesActive(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	got := map[string]int64{}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			g, ok := m.Data.(metricdata.Gauge[int64])
			if !ok || sm.Scope.Name != "systemplane.engine" || m.Name != "systemplane.scopes_active" {
				continue
			}

			for _, p := range g.DataPoints {
				got[p.Attributes.Encoded(attribute.DefaultEncoder())] = p.Value
			}
		}
	}

	return got
}
