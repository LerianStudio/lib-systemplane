// Public Manager surface for systemplane v1.5.0.
//
// The Manager closes the ST↔MT asymmetry left behind by v1.4.0 by
// providing a per-tenant in-process cache plus a per-tenant LISTEN/NOTIFY
// goroutine. It is constructed once per process, bound to a Client running
// in multi-tenant mode, and driven by the consumer's tenant lifecycle event
// plumbing through the four On* handlers.
//
// Callers that do NOT bind a Manager observe identical v1.4.0 behaviour —
// every Get hits the resolved tenant DB and OnChange returns
// ErrNotSupportedInMultiTenant. The Manager is strictly opt-in.
package systemplane

import (
	tmpostgres "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/postgres"
	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-observability/tracing"
	internalclient "github.com/LerianStudio/lib-systemplane/internal/client"
	internalmanager "github.com/LerianStudio/lib-systemplane/internal/manager"
)

// Manager owns per-tenant cache + push hot-reload bookkeeping for MT
// deployments. See the package-level documentation for the construction
// recipe and lifecycle expectations.
type Manager internalmanager.Manager

// ManagerOption configures a Manager at construction time.
type ManagerOption = internalmanager.Option

// NewManager constructs a Manager bound to a Postgres tenant-manager.
//
// When client is non-nil, NewManager binds the Manager to that Client
// internally so the Client's MT Get/OnChange paths route through the
// Manager's per-tenant cache. NewManager does NOT open any LISTEN
// connections — the first per-tenant LISTEN goroutine spins up on the first
// OnTenantActivated call for each tenant.
func NewManager(client *Client, pgMgr *tmpostgres.Manager, opts ...ManagerOption) *Manager {
	m := internalmanager.New(pgMgr, opts...)

	if client != nil {
		ic := (*internalclient.Client)(client)
		ic.BindManager(m)
	}

	return (*Manager)(m)
}

// WithManagerLogger sets the structured logger for the Manager.
func WithManagerLogger(l log.Logger) ManagerOption {
	return internalmanager.WithLogger(l)
}

// WithManagerTelemetry sets the OpenTelemetry provider for the Manager.
func WithManagerTelemetry(t *tracing.Telemetry) ManagerOption {
	return internalmanager.WithTelemetry(t)
}

// WithManagerAggregateTenantThreshold overrides the active-tenant count
// above which per-tenant metric labels collapse into a single "aggregate"
// rollup. A non-positive value keeps per-tenant labels regardless of
// cardinality.
func WithManagerAggregateTenantThreshold(n int) ManagerOption {
	return internalmanager.WithAggregateTenantThreshold(n)
}

func asInternalManager(m *Manager) *internalmanager.Manager {
	return (*internalmanager.Manager)(m)
}
