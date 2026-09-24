// Client telemetry helpers: logger-level shortcuts.
package client

import (
	"context"
	"fmt"

	tmcore "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
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
	if c == nil || c.guarded == nil {
		return
	}

	if c.multiTenant {
		tenant, ok := tenantOf(ctx)
		if !ok {
			tenant = unresolvedTenant
		}

		fields = append(fields, log.String(constants.AttrKeyTenantID, tenant))
	}

	c.guarded.Log(ctx, log.LevelError, msg, fields)
}

// unresolvedTenant is the tenant.id a multi-tenant line carries when ctx holds
// no tenant id. The tenant database and the tenant id ride independent context
// keys, so a read can resolve a database with no id; an empty value there
// would read exactly like a single-tenant line.
const unresolvedTenant = "unresolved"

// tenantOf returns the tenant id ctx carries and whether it carries one.
func tenantOf(ctx context.Context) (string, bool) {
	tenant := tmcore.GetTenantIDContext(ctx)

	return tenant, tenant != ""
}

// scopeFor names the tenant a report about this call belongs to, and is the
// same rule logError applies above: the tenant travels on the caller's own
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
// redacted makes the same trade safelog.ErrorDetail makes on the log line
// beside this: encoding/json reports what it choked on by quoting the byte —
// `invalid character 's' looking for beginning of value` — and carries the
// offset on the *json.SyntaxError for anyone who unwraps, so for a key whose
// registration says its value must never be printed the cause is named by its
// dynamic type instead and left out of the chain. An error travels further
// than a log line, into response bodies and error trackers, so withholding it
// there matters at least as much. An ordinary key keeps the json error
// wrapped, which is what a caller debugging the row reaches for.
func decodeErr(ctx context.Context, namespace, key string, redacted bool, err error) error {
	where := "in an unresolved tenant"
	if tenant, ok := tenantOf(ctx); ok {
		where = fmt.Sprintf("in tenant %q", tenant)
	}

	if redacted {
		return fmt.Errorf("systemplane: decode value for %s/%s %s failed (%T, cause withheld: key registered redacted)",
			namespace, key, where, err)
	}

	return fmt.Errorf("systemplane: decode value for %s/%s %s: %w",
		namespace, key, where, err)
}
