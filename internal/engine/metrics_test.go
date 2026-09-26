//go:build unit

package engine

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/v4/tracing"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	metricScopesActive = "systemplane.scopes_active"
	metricCacheEntries = "systemplane.cache_entries"
	metricEvents       = "systemplane.changefeed_events_total"
	metricDisconnects  = "systemplane.changefeed_disconnects_total"
	metricActivation   = "systemplane.activation_latency_seconds"
	metricReads        = "systemplane.cache_reads_total"
)

// meteredEngine builds an engine over fs, with activateKey registered, whose
// instruments reader collects through the production Telemetry type.
func meteredEngine(t *testing.T, fs *fakeStore, threshold int) (*Engine, *sdkmetric.ManualReader) {
	t.Helper()

	fs.resyncOnSubscribe()

	reader := sdkmetric.NewManualReader()
	e := New(Config{
		Store:                    fs,
		Registry:                 fakeRegistry{defs: map[NSKey]KeyDef{activateKey: {Default: "fallback"}}},
		CloseTimeout:             5 * time.Second,
		Telemetry:                &tracing.Telemetry{MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))},
		AggregateTenantThreshold: threshold,
	})

	noDeliveryOutlivesTheTest(t, e)

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	return e, reader
}

// points collects meter systemplane.engine and renders instrument name's data
// points as encoded attribute set -> value (a histogram's value is its count).
// An absent instrument has no points; one of another kind or unit fails.
func points(t *testing.T, reader *sdkmetric.ManualReader, name string, want metricdata.Aggregation, unit string) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	got := map[string]int64{}
	enc := attribute.DefaultEncoder()

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sm.Scope.Name != "systemplane.engine" || m.Name != name {
				continue
			}

			if m.Unit != unit {
				t.Errorf("%s: unit %q, want %q", name, m.Unit, unit)
			}

			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				if _, ok := want.(metricdata.Sum[int64]); !ok || !d.IsMonotonic {
					t.Fatalf("%s: got a sum (monotonic %v), want %T", name, d.IsMonotonic, want)
				}

				for _, p := range d.DataPoints {
					got[p.Attributes.Encoded(enc)] = p.Value
				}
			case metricdata.Gauge[int64]:
				if _, ok := want.(metricdata.Gauge[int64]); !ok {
					t.Fatalf("%s: got a gauge, want %T", name, want)
				}

				for _, p := range d.DataPoints {
					got[p.Attributes.Encoded(enc)] = p.Value
				}
			case metricdata.Histogram[float64]:
				if _, ok := want.(metricdata.Histogram[float64]); !ok {
					t.Fatalf("%s: got a histogram, want %T", name, want)
				}

				for _, p := range d.DataPoints {
					got[p.Attributes.Encoded(enc)] = int64(p.Count)
				}
			default:
				t.Fatalf("%s: got %T, want %T", name, m.Data, want)
			}
		}
	}

	return got
}

var (
	counter   = metricdata.Sum[int64]{}
	gauge     = metricdata.Gauge[int64]{}
	histogram = metricdata.Histogram[float64]{}
)

func wantPoints(t *testing.T, reader *sdkmetric.ManualReader, name string, kind metricdata.Aggregation, unit string, want map[string]int64) {
	t.Helper()

	if got := points(t, reader, name, kind, unit); !maps.Equal(got, want) {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

func TestMetricsReportEachInstrumentAfterItsEngineEvent(t *testing.T) {
	zero, tenant := store.Scope{}, store.Scope{Tenant: "t1"}
	fs := newFakeStore()
	fs.seed(tenant, jsonRow(activateKey, 7, `"stored"`, "alice"))

	e, reader := meteredEngine(t, fs, 0)

	if err := e.Start(startCtx(t, hangGuard)); err != nil {
		t.Fatalf("Start: %v", err)
	}

	e.Lookup(zero, activateKey)
	e.Lookup(zero, NSKey{Namespace: "billing", Key: "unregistered"})
	e.Lookup(tenant, activateKey) // untracked: no cache, so no read counted

	e.Activate(tenant)
	activationDone(t, e, tenant)
	e.Lookup(tenant, activateKey)

	fs.emit(store.Event{Scope: tenant, Op: store.OpDisconnect})

	e.Reactivate(tenant) // a rebuild: counted as events, never timed
	activationDone(t, e, tenant)

	wantPoints(t, reader, metricReads, counter, "", map[string]int64{
		"result=hit": 1, "result=miss": 1, "result=hit,tenant_id=t1": 1,
	})
	// Each Subscribe's resync, plus the tenant's disconnect.
	wantPoints(t, reader, metricEvents, counter, "", map[string]int64{"": 1, "tenant_id=t1": 3})
	wantPoints(t, reader, metricDisconnects, counter, "", map[string]int64{"tenant_id=t1": 1})
	wantPoints(t, reader, metricActivation, histogram, "s", map[string]int64{"tenant_id=t1": 1})
	wantPoints(t, reader, metricScopesActive, gauge, "", map[string]int64{"": 2})
	wantPoints(t, reader, metricCacheEntries, gauge, "", map[string]int64{"": 1, "tenant_id=t1": 1})

	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	wantPoints(t, reader, metricScopesActive, gauge, "", map[string]int64{})
	wantPoints(t, reader, metricCacheEntries, gauge, "", map[string]int64{})
}

func TestMetricsCollapseTenantIDAboveTheThreshold(t *testing.T) {
	t1, t2 := store.Scope{Tenant: "t1"}, store.Scope{Tenant: "t2"}

	for _, tc := range []struct {
		name      string
		threshold int
		want      map[string]int64
	}{
		{"two tenants above a threshold of 1", 1, map[string]int64{"tenant_id=aggregate": 2}},
		{"a threshold of 0 never collapses", 0, map[string]int64{"tenant_id=t1": 1, "tenant_id=t2": 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, reader := meteredEngine(t, newFakeStore(), tc.threshold)

			for _, scope := range []store.Scope{t1, t2} {
				e.Activate(scope)
				activationDone(t, e, scope)
			}

			e.Lookup(t1, activateKey)
			e.Lookup(t2, activateKey)

			reads := map[string]int64{}
			for label, n := range tc.want {
				reads["result=hit,"+label] = n
			}

			wantPoints(t, reader, metricScopesActive, gauge, "", map[string]int64{"": 2})
			wantPoints(t, reader, metricCacheEntries, gauge, "", tc.want)
			wantPoints(t, reader, metricReads, counter, "", reads)
		})
	}
}

// nilMeterTelemetry hands out a nil meter without an error.
type nilMeterTelemetry struct{ store.Telemetry }

func (nilMeterTelemetry) Meter(string) (metric.Meter, error) { return nil, nil }

func TestMetricsWithoutAMeterRecordNothing(t *testing.T) {
	tenant := store.Scope{Tenant: "t1"}

	for _, tc := range []struct {
		name      string
		telemetry store.Telemetry
	}{
		{"no telemetry", nil},
		{"a telemetry whose Meter fails", (*tracing.Telemetry)(nil)},
		{"a telemetry whose Meter returns nil", nilMeterTelemetry{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFakeStore()
			fs.resyncOnSubscribe()

			e := New(Config{
				Store:     fs,
				Registry:  fakeRegistry{defs: map[NSKey]KeyDef{activateKey: {Default: "fallback"}}},
				Telemetry: tc.telemetry,
			})

			if e.metrics != nil {
				t.Fatal("an engine without a meter built instruments")
			}

			if err := e.Start(startCtx(t, hangGuard)); err != nil {
				t.Fatalf("Start: %v", err)
			}

			e.Activate(tenant)
			activationDone(t, e, tenant)
			e.Lookup(tenant, activateKey)
			fs.emit(store.Event{Scope: tenant, Op: store.OpDisconnect})

			if err := e.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}
