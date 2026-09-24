// Client telemetry helpers: logger-level shortcuts.
package client

import (
	"context"
	"fmt"

	"github.com/LerianStudio/lib-observability/v4/log"
)

func (c *Client) logError(ctx context.Context, msg string, fields ...log.Field) {
	if c == nil || c.logger == nil {
		return
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
