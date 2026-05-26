// Structured logging helpers for the Manager.
package manager

import (
	"context"

	"github.com/LerianStudio/lib-observability/log"
)

func (m *Manager) logDebug(ctx context.Context, msg string, fields ...log.Field) {
	if m == nil || m.logger == nil {
		return
	}

	m.logger.Log(ctx, log.LevelDebug, msg, fields...)
}

func (m *Manager) logInfo(ctx context.Context, msg string, fields ...log.Field) {
	if m == nil || m.logger == nil {
		return
	}

	m.logger.Log(ctx, log.LevelInfo, msg, fields...)
}

func (m *Manager) logWarn(ctx context.Context, msg string, fields ...log.Field) {
	if m == nil || m.logger == nil {
		return
	}

	m.logger.Log(ctx, log.LevelWarn, msg, fields...)
}

func (m *Manager) logError(ctx context.Context, msg string, fields ...log.Field) {
	if m == nil || m.logger == nil {
		return
	}

	m.logger.Log(ctx, log.LevelError, msg, fields...)
}
