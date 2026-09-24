//go:build unit && !race

package engine

import (
	"context"
	"math"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// TestPerf_ForeignSnapshotRowAllocatesNothing is the enforceable half of the
// rule that a reconcile costs nothing per row it does not own.
//
// `systemplane_entries` is one table per database, so a scope's snapshot
// carries every other consumer's namespaces, and every OpResync walks all of
// them. Two things used to charge each of those rows: a formatted error built
// for a caller that discards it — a reconcile has nobody to report to — and a
// second Registry lookup asking again what the ingress had just answered. A
// snapshot of 10 own rows next to 200 foreign ones cost 834 allocations where
// the value-only cache it replaces cost none.
//
// The file is excluded under -race rather than skipped inside the test, for
// the reason TestPerf_ForeignEventDropAllocatesNothing states: the race
// detector accounts allocations its own way, so the number below is only
// meaningful in the build the AC15 perf gate runs
// (`go test -tags=unit -run=^TestPerf_ ./...`, no -race).
//
// wantAllocs is one-sided for the same reason as the other gates: it guards
// against the per-row cost coming back, not against an improvement.
func TestPerf_ForeignSnapshotRowAllocatesNothing(t *testing.T) {
	const wantAllocs = 0

	foreign := NSKey{Namespace: "billing", Key: "another-service"}
	mine := NSKey{Namespace: "billing", Key: "limits"}

	rec := &recordingLogger{Logger: log.NewNop(), debugOff: true}
	e := loggingEngineWith(t, map[NSKey]KeyDef{mine: {Default: "fallback"}}, newFakeStore(), rec)

	sc := e.trackedScope(store.Scope{})
	if sc == nil {
		t.Fatal("the zero scope is not tracked, so no snapshot row can be applied")
	}

	arm := reconcileArming{
		reconcile: sc.reconcileGen,
		window: &reconcileWindow{
			touched:  make(map[NSKey]struct{}),
			unusable: make(map[NSKey]struct{}),
		},
	}

	ctx := context.Background()
	row := jsonRow(foreign, 1, `"written-by-someone-else"`, "ops")

	// AllocsPerRun reads the process-wide Mallocs counter, so an allocation
	// made by another goroutine between its two samples is charged to this
	// one; the minimum of three measurements is the one least polluted by it.
	got := math.Inf(1)
	for range 3 {
		got = math.Min(got, testing.AllocsPerRun(100, func() {
			if superseded := e.applySnapshotRow(ctx, sc, arm, row); superseded {
				t.Fatal("the reconcile reported itself superseded, so the row was never applied")
			}
		}))
	}

	if got > wantAllocs {
		t.Errorf("skipping one foreign snapshot row allocates %v times, up from %d: the reconcile "+
			"is formatting an error nobody reads, or asking the registry twice", got, wantAllocs)
	}

	if n := len(rec.all()); n != 0 {
		t.Errorf("the logger with DEBUG off received %d entries, want 0: the measurement above was "+
			"taken against a line that was emitted, not dropped", n)
	}
}
