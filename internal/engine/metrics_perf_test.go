//go:build unit && !race

package engine

import (
	"math"
	"testing"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// TestPerf_MeteredLookupAllocatesNothing guards the read options a scope builds
// as it enters the engine: a metered cache read reuses them, per tenant or
// collapsed, rather than building an attribute set per read.
func TestPerf_MeteredLookupAllocatesNothing(t *testing.T) {
	const wantAllocs = 0

	for name, threshold := range map[string]int{"per tenant": 0, "collapsed": 1} {
		t.Run(name, func(t *testing.T) {
			e, _ := meteredEngine(t, newFakeStore(), threshold)

			for _, scope := range []store.Scope{{Tenant: "t1"}, {Tenant: "t2"}} {
				e.Activate(scope)
				activationDone(t, e, scope)
			}

			// The minimum of three runs is the one least charged with another
			// goroutine's allocations, as in the foreign-event gate.
			got := math.Inf(1)
			for range 3 {
				got = math.Min(got, testing.AllocsPerRun(100, func() { e.Lookup(store.Scope{Tenant: "t1"}, activateKey) }))
			}

			if got > wantAllocs {
				t.Errorf("a metered cache read allocates %v times, want %d: its attributes are built per read", got, wantAllocs)
			}
		})
	}
}
