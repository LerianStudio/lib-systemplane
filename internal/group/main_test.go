//go:build unit

package group

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every unit test in this package under goleak.VerifyTestMain so
// that any goroutine spawned here but not cleaned up fails the package run.
//
// This package spawns none: the codec is pure, and the apply coordinator that
// lands on top of it serializes deliveries with locks on the publishing
// goroutine rather than with workers of its own. This is what holds that claim
// to the truth instead of asserting it in prose.
//
// No ignore list is needed today. If a future change introduces a dependency
// goroutine that legitimately outlives the test process, add a
// goleak.IgnoreAnyFunction("...") entry with a comment explaining why.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
