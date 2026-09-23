//go:build unit

// Coverage for the m==nil / m.logger==nil guard returns in logging helpers.
// Production wiring always installs log.NewNop(), so the early-return branch
// can only be exercised by direct construction with a nil logger field or by
// invoking on a nil Manager pointer.
package manager

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/logguard"
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

// TestNoLoggedFieldNameIsRedacted reads this package's own source and refuses
// any field name lib-observability erases. The multi-tenant changefeed and
// warm load are the paths a tenant operator reads when a row is skipped, and
// "key" is an exact entry in the default sensitive-field list: under it the
// line names the namespace and withholds the one thing it exists to publish.
func TestNoLoggedFieldNameIsRedacted(t *testing.T) {
	t.Parallel()

	logguard.AssertNoneRedacted(t, ".")
}
