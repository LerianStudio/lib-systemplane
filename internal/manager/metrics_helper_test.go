//go:build unit || integration

// Shared test helper for constructing an in-process *tracing.Telemetry so
// every Manager record method can be exercised with a real OTel
// instrument. Lives under both build tags because the unit metrics tests
// and the integration listen tests both need it.
package manager

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/tracing"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func newTestTelemetry(t *testing.T) (*tracing.Telemetry, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
	})

	return &tracing.Telemetry{MeterProvider: mp}, reader
}
