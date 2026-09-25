// Client telemetry helpers: logger-level shortcuts.
package client

import (
	"context"
	"fmt"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/safelog"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// logRead reports a read-through failure at level, stamped with the tenant it
// happened for.
//
// The stamp is here rather than at the call sites because it is the same fact
// on every one of them and missing it costs the same everywhere: a
// multi-tenant deployment serves one namespace and one key to every tenant in
// the fleet, so a line naming only those two tells an operator that SOMEBODY's
// row is unreadable and nothing about whose. The tenant travels on the
// caller's own context, which is the context every one of these lines is
// already logged with.
func (c *Client) logRead(ctx context.Context, level int, msg string, fields ...log.Field) {
	if c == nil || c.guarded == nil {
		return
	}

	if c.multiTenant {
		tenant, ok := tenantOf(ctx)
		if !ok {
			tenant = safelog.UnresolvedTenant
		}

		fields = append(fields, log.String(constants.AttrKeyTenantID, tenant))
	}

	c.guarded.Log(ctx, level, msg, fields)
}

// tenantOf returns the tenant id ctx carries and whether it carries one.
func tenantOf(ctx context.Context) (string, bool) {
	tenant := tmcore.GetTenantIDContext(ctx)

	return tenant, tenant != ""
}

// scopeFor names the tenant a report about this call belongs to, and is the
// same rule logRead applies above: the tenant travels on the caller's own
// context, and a single-tenant Client has none, so it reports the zero scope
// rather than an empty tenant.id that reads like a missing value.
//
// It exists because the engine reports a recovered panic with the identity of
// what the panic was about, and the write path grades a value through the
// engine before any scope has been resolved for it.
func (c *Client) scopeFor(ctx context.Context) store.Scope {
	if c == nil || !c.multiTenant {
		return store.Scope{}
	}

	return store.Scope{Tenant: tmcore.GetTenantIDContext(ctx)}
}

// decodeErr names the row a multi-tenant read-through could not decode, tenant
// included.
//
// One namespace and one key serve every tenant of a deployment, so an error
// carrying only those two sends a caller looking for a broken row it has no
// way to find. Only the read-through paths use it: the single-tenant ingress
// decodes rows the engine already holds by scope.
//
// The cause stays wrapped: encoding/json reports what it choked on by quoting
// the byte and carries the offset on the *json.SyntaxError, which is what a
// caller debugging the row reaches for.
func decodeErr(ctx context.Context, namespace, key string, err error) error {
	where := "in an unresolved tenant"
	if tenant, ok := tenantOf(ctx); ok {
		where = fmt.Sprintf("in tenant %q", tenant)
	}

	return fmt.Errorf("systemplane: decode value for %s/%s %s: %w",
		namespace, key, where, err)
}
