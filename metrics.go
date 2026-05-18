// OpenTelemetry metrics used by the systemplane Client.
//
// Instruments are lazily initialized on first access via Client.metricsOnce
// so constructing a Client without telemetry (e.g. NewForTesting with no
// WithTelemetry option) pays zero setup cost. When c.telemetry is nil every
// accessor returns nil; callers guard with a nil check before recording.
package systemplane

import (
	"context"

	"github.com/LerianStudio/lib-observability/log"
	obsmetrics "github.com/LerianStudio/lib-observability/metrics"
)

// meterName is the OpenTelemetry instrumentation scope name for the Client's
// metrics. Mirrors tracerName (see client.go) so spans and metrics share
// a consistent scope.
const meterName = "systemplane.client"

// clientMetrics groups the Client's metric builders. Every field
// may be nil when instrumentation setup fails; accessors on the Client
// (see recordTenantLazyFetchError) guard against nil before recording.
type clientMetrics struct {
	// tenantLazyFetchErrors counts lazy-mode GetForTenant store fetches that
	// errored and fell through to the global/default cascade. A non-zero
	// rate here with an otherwise healthy backend usually signals timeouts
	// (tenantStoreTimeout) or transient connectivity issues. Attributes:
	// namespace, key.
	tenantLazyFetchErrors *obsmetrics.CounterBuilder
}

// ensureMetrics initializes the Client's metric instruments exactly once.
// Safe to call concurrently; safe on a nil c.telemetry (returns a
// zero-value clientMetrics so every counter field stays nil and recordings
// no-op cleanly).
func (c *Client) ensureMetrics() {
	c.metricsOnce.Do(func() {
		if c.telemetry == nil {
			c.metrics = &clientMetrics{}

			return
		}

		meter, err := c.telemetry.Meter(meterName)
		if err != nil || meter == nil {
			c.logWarn(context.Background(), "systemplane: failed to obtain meter, metrics disabled",
				log.Err(err),
			)

			c.metrics = &clientMetrics{}

			return
		}

		factory, err := obsmetrics.NewMetricsFactory(meter, c.logger)
		if err != nil {
			c.logWarn(context.Background(), "systemplane: failed to create metrics factory, metrics disabled",
				log.Err(err),
			)

			c.metrics = &clientMetrics{}

			return
		}

		m := &clientMetrics{}

		m.tenantLazyFetchErrors, err = factory.Counter(obsmetrics.Metric{
			Name:        "systemplane_tenant_lazy_fetch_errors_total",
			Description: "Lazy-mode GetForTenant backend fetches that errored and fell through to the global/default cascade",
			Unit:        "{error}",
		})
		if err != nil {
			c.logWarn(context.Background(), "systemplane: failed to create tenantLazyFetchErrors counter",
				log.Err(err),
			)

			m.tenantLazyFetchErrors = nil
		}

		c.metrics = m
	})
}

// recordTenantLazyFetchError increments the tenant_lazy_fetch_errors counter
// with (namespace, key) attributes. No-ops when telemetry is not configured
// or the counter failed to initialize — both are captured by a nil guard on
// the instrument field.
func (c *Client) recordTenantLazyFetchError(ctx context.Context, namespace, key string) {
	if c == nil {
		return
	}

	c.ensureMetrics()

	if c.metrics == nil || c.metrics.tenantLazyFetchErrors == nil {
		return
	}

	if err := c.metrics.tenantLazyFetchErrors.WithLabels(map[string]string{
		"namespace": namespace,
		"key":       key,
	}).AddOne(ctx); err != nil {
		c.logWarn(ctx, "systemplane: failed to record tenantLazyFetchErrors counter",
			log.Err(err),
		)
	}
}
