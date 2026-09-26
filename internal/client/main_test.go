//go:build unit || integration

package client_test

import (
	"fmt"
	"os"
	"testing"

	"go.uber.org/goleak"
)

// afterRun holds the teardown of what outlives every test: the integration
// harness appends each shared container's termination. Empty under unit.
var afterRun []func()

// TestMain fails an otherwise green run on any goroutine left behind. It
// inlines goleak.VerifyTestMain because that exits the process itself, which
// would skip the container teardown the leak check has to wait for.
func TestMain(m *testing.M) {
	code := m.Run()

	for _, teardown := range afterRun {
		teardown()
	}

	if code == 0 {
		if err := goleak.Find(
			// testcontainers' Reaper lives for the whole process by design.
			goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
			// HTTP keep-alive of testcontainers' Docker client.
			goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
			goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
		); err != nil {
			fmt.Fprintf(os.Stderr, "goleak: Errors on successful test run: %v\n", err)

			code = 1
		}
	}

	os.Exit(code)
}
