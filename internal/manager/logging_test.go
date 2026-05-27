//go:build unit

// Coverage for the m==nil / m.logger==nil guard returns in logging helpers.
// Production wiring always installs log.NewNop(), so the early-return branch
// can only be exercised by direct construction with a nil logger field or by
// invoking on a nil Manager pointer.
package manager

import (
	"context"
	"testing"
)

func TestLogging_NilManager_NoOp(t *testing.T) {
	t.Parallel()

	var m *Manager

	m.logDebug(context.Background(), "debug")
	m.logInfo(context.Background(), "info")
	m.logWarn(context.Background(), "warn")
}

func TestLogging_NilLogger_NoOp(t *testing.T) {
	t.Parallel()

	// Direct construction with logger=nil bypasses New's NewNop substitution
	// so the early-return guard fires on every helper.
	m := &Manager{}

	m.logDebug(context.Background(), "debug")
	m.logInfo(context.Background(), "info")
	m.logWarn(context.Background(), "warn")
}
