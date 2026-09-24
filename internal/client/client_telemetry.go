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
// Single-tenant lines carry no such field: there is one scope, the context
// carries no tenant, and an empty tenant.id on every line is noise that reads
// like a missing value.
func (c *Client) logError(ctx context.Context, msg string, fields ...log.Field) {
	if c == nil || c.logger == nil {
		return
	}

	if c.multiTenant {
		fields = append(fields, log.String(constants.AttrKeyTenantID, tmcore.GetTenantIDContext(ctx)))
	}

	c.logger.Log(ctx, log.LevelError, msg, fields)
}

// errorDetail renders a rejection's cause under the key's registered redaction
// policy, and is the multi-tenant read-through twin of the engine's own helper.
//
// encoding/json needs no help to publish a secret: an unparsable row comes back
// as "invalid character 's' looking for beginning of value", which quotes the
// value's first byte. The single-tenant ingress has run every decode failure
// through the policy since the engine cutover; these read-through paths did not,
// so a key registered RedactMask or RedactFull was protected on one path and
// exposed on the other.
//
// The type alone tells two failures apart and can never carry a byte of the
// value. What the caller receives is unchanged in both cases: this is the log
// stream, not the API.
func errorDetail(redacted bool, what string, err error) log.Field {
	if !redacted {
		return log.Err(err)
	}

	return log.String("error", fmt.Sprintf("%s (%T)", what, err))
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
