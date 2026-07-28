// Client telemetry helpers: logger-level shortcuts.
package client

import (
	"context"

	"github.com/LerianStudio/lib-observability/v2/log"
)

func (c *Client) logWarn(ctx context.Context, msg string, fields ...log.Field) {
	if c == nil || c.logger == nil {
		return
	}

	c.logger.Log(ctx, log.LevelWarn, msg, fields...)
}

func (c *Client) logDebug(ctx context.Context, msg string, fields ...log.Field) {
	if c == nil || c.logger == nil {
		return
	}

	c.logger.Log(ctx, log.LevelDebug, msg, fields...)
}

func (c *Client) logError(ctx context.Context, msg string, fields ...log.Field) {
	if c == nil || c.logger == nil {
		return
	}

	c.logger.Log(ctx, log.LevelError, msg, fields...)
}
