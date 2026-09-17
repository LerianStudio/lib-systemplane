//go:build unit

package engine

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every unit test in this package under goleak.VerifyTestMain so
// that any goroutine the Engine spawns (feed re-reads, reconcile, dispatch
// workers) but does not stop on Close() fails the package run. Making that a
// standing check of the whole package, rather than one dedicated test, is what
// keeps "no goroutine survives Close" true as the engine grows.
//
// No ignore list is needed today. If a future change pulls in a dependency
// goroutine that legitimately outlives the test process (e.g. an OpenTelemetry
// exporter), add a goleak.IgnoreAnyFunction("...") entry with a comment
// explaining why.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
