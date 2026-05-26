// Metrics scaffolding for the Manager.
//
// Slice 1 ships a no-op stub so the surrounding code can reference
// m.metrics safely. The real instrumentation is filled in by slice 6
// (`feat(manager): telemetry`).
package manager

import (
	"context"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/tracing"
)

// metrics holds the OpenTelemetry instruments used by the Manager.
//
// All recordXxx methods are nil-receiver safe so callers do not need to
// guard their call sites. The fields are populated in slice 6; for now they
// stay zero and every recordXxx is a no-op.
type metrics struct {
	telemetry                *tracing.Telemetry
	logger                   log.Logger
	aggregateTenantThreshold int

	// active count is tracked here for the tenants_active gauge in slice 6.
	// It also drives the aggregate-tenant rollup decision.
	activeCount int64
}

// newMetrics constructs a metrics holder. Slice 1 returns a stub that
// performs no instrument registration; slice 6 fills in real instruments.
func newMetrics(t *tracing.Telemetry, logger log.Logger, aggregateThreshold int) *metrics {
	return &metrics{
		telemetry:                t,
		logger:                   logger,
		aggregateTenantThreshold: aggregateThreshold,
	}
}

func (m *metrics) recordTenantActivated(_ context.Context, _ string)             {}
func (m *metrics) recordTenantDeactivated(_ context.Context, _ string)           {}
func (m *metrics) recordCacheEntries(_ context.Context, _ string, _ int)         {}
func (m *metrics) recordNotifyReceived(_ context.Context, _ string, _ string)    {}
func (m *metrics) recordListenDisconnect(_ context.Context, _ string, _ string)  {}
func (m *metrics) recordWarmloadLatency(_ context.Context, _ string, _ float64)  {}
func (m *metrics) recordGetCacheOutcome(_ context.Context, _ string, _ string)   {}
