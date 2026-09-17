//go:build unit

package engine

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every unit test in this package under goleak.VerifyTestMain so
// that any goroutine the Engine owns but does not stop fails the package run.
// Making that a standing check of the whole package, rather than one dedicated
// test, is what keeps "no goroutine survives Close" true as the engine grows.
//
// The engine owns three kinds, and Close accounts for all three: the reconcile
// goroutine of every scope, the debounced re-reads, and the per-(scope, key)
// delivery workers. Each is registered in the WaitGroup Close drains within
// its timeout, so a Close that returned nil means every one of them had
// finished. A dropped scope's goroutines end with the scope rather than
// waiting for Close.
//
// One goroutine survives Close by design: a subscriber callback that ignores
// the context it was handed. Close reports it as ErrCloseTimeout instead of
// hiding it, and a test that provokes one must release it and wait, or this
// check fails the whole package — which is the point.
//
// No ignore list is needed today. If a future change pulls in a dependency
// goroutine that legitimately outlives the test process (e.g. an OpenTelemetry
// exporter), add a goleak.IgnoreAnyFunction("...") entry with a comment
// explaining why.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
