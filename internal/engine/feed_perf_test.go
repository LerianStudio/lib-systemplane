//go:build unit && !race

package engine

import (
	"math"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// TestPerf_ForeignEventDropAllocatesNothing is the enforceable half of the
// DEBUG guard on the engine's per-event drop line.
//
// `systemplane_entries` is one table per database, so every write by every
// other consumer sharing it reaches this feed, and the line announcing the
// drop is DEBUG — off in every production deployment. Its fields are built at
// the call site and boxed into the ...any the Logger takes, so without the
// guard each foreign write still costs that []log.Field before the logger
// throws the line away. TestPerEventDropLinesCostNothingWhenDebugIsOff proves
// only that Enabled is consulted; a guard that asks and then builds the fields
// anyway passes it, and this is what does not.
//
// The recorder implements Enabled itself rather than inheriting the embedded
// no-op's: this package hands the Logger straight to the engine with no
// lib-observability adapter in between, so the level the engine reads is the
// one this test sets.
//
// The file is excluded under -race rather than skipped inside the test: the
// race detector accounts allocations its own way, so the number below is only
// meaningful in the build the AC15 perf gate runs
// (`go test -tags=unit -run=^TestPerf_ ./...`, no -race).
//
// wantAllocs is one-sided for the same reason as the clone gate: the
// regression it guards against is the field slice coming back on every foreign
// event, and an exact equality would also fail on an improvement it is not
// here to police.
func TestPerf_ForeignEventDropAllocatesNothing(t *testing.T) {
	const wantAllocs = 0

	foreign := NSKey{Namespace: "billing", Key: "another-service"}
	mine := NSKey{Namespace: "billing", Key: "limits"}

	rec := &recordingLogger{Logger: log.NewNop(), debugOff: true}
	e := loggingEngineWith(t, map[NSKey]KeyDef{mine: {Default: "fallback"}}, newFakeStore(), rec)

	evt := upsertEvent(store.Scope{}, foreign, 1)

	// AllocsPerRun reads the process-wide Mallocs counter, so an allocation
	// made by another goroutine between its two samples is charged to this
	// one; the minimum of three measurements is the one least polluted by it.
	got := math.Inf(1)
	for range 3 {
		got = math.Min(got, testing.AllocsPerRun(100, func() { e.onEvent(evt) }))
	}

	if got > wantAllocs {
		t.Errorf("dropping one foreign changefeed event allocates %v times, up from %d: the DEBUG "+
			"drop line builds its fields before asking whether anyone will read them", got, wantAllocs)
	}

	if n := len(rec.all()); n != 0 {
		t.Errorf("the logger with DEBUG off received %d entries, want 0: the measurement above was "+
			"taken against a line that was emitted, not dropped", n)
	}
}
