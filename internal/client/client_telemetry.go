// Client telemetry helpers: logger-level shortcuts.
package client

import (
	"context"
	"fmt"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
)

// logError reports a read-through failure, stamped with the tenant it happened
// for.
//
// The stamp is here rather than at the call sites because it is the same fact
// on every one of them and missing it costs the same everywhere: a
// multi-tenant deployment serves one namespace and one key to every tenant in
// the fleet, so a line naming only those two tells an operator that SOMEBODY's
// row is unreadable and nothing about whose. The tenant travels on the
// caller's own context, which is the context every one of these lines is
// already logged with.
//
// The guard survives although every call site today is multi-tenant: it is
// what keeps an empty tenant.id — noise that reads like a missing value — off
// the single-tenant lines the wave-3 engine-tenants lane adds.
func (c *Client) logError(ctx context.Context, msg string, fields ...log.Field) {
	if c == nil || c.logger == nil {
		return
	}

	if c.multiTenant {
		fields = append(fields, log.String(constants.AttrKeyTenantID, tmcore.GetTenantIDContext(ctx)))
	}

	c.logger.Log(ctx, log.LevelError, msg, fields)
}

// decodeErr names the row a multi-tenant read-through could not decode, tenant
// included.
//
// One namespace and one key serve every tenant of a deployment, so an error
// carrying only those two sends a caller looking for a broken row it has no
// way to find. Only the read-through paths use it: the single-tenant ingress
// decodes rows the engine already holds by scope.
func decodeErr(ctx context.Context, namespace, key string, err error) error {
	return fmt.Errorf("systemplane: decode value for %s/%s in tenant %q: %w",
		namespace, key, tmcore.GetTenantIDContext(ctx), err)
}
