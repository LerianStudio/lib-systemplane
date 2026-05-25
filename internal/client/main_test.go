//go:build unit

package client

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every unit test in this package under goleak.VerifyTestMain so
// that any goroutine spawned by Client (hydration, debouncer, refresh, OnChange
// dispatch) but not cleaned up by Close() fails the package run.
//
// No ignore list is needed today: the only goroutines this package launches
// are the debouncer's worker (Closed by Client.Close → debouncer.Close) and
// the test-local memStore fire goroutines, both of which complete inline. If
// a future change introduces a dependency goroutine that legitimately outlives
// the test process (e.g. an OpenTelemetry exporter), add a
// goleak.IgnoreAnyFunction("...") entry with a comment explaining why.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
