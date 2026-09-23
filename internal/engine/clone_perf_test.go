//go:build unit && !race

package engine

import (
	"math"
	"testing"
)

// TestPerf_CloneJSONMap is BenchmarkCloneJSONMap with a number CI enforces.
// The benchmark asserts nothing and no job runs it, so the guard against
// falling off the JSON fast path — back onto the generic reflective walk, on
// every consumer read and every subscriber delivery — guarded nothing. An
// allocation count is the half of that cost that IS deterministic, so it can
// be pinned where a wall-clock threshold could only flake.
//
// The file is excluded under -race rather than skipped inside the test: the
// race detector accounts allocations its own way, so the number below is only
// meaningful in the build the AC15 perf gate runs
// (`go test -tags=unit -run=^TestPerf_ ./...`, no -race).
//
// wantAllocs is measured, not derived: it is the ceiling the fast path costs
// for this tree today. The bound is one-sided on purpose — the regression this
// guards against is the fall back to the reflective walk, which MULTIPLIES the
// count, and an exact equality would also fail on the improvement it is not
// here to police.
func TestPerf_CloneJSONMap(t *testing.T) {
	const wantAllocs = 6

	value := jsonCloneTree()

	// AllocsPerRun reads the process-wide Mallocs counter, so an allocation
	// made by another goroutine between its two samples is charged to this
	// one; the minimum of three measurements is the one least polluted by it.
	got := math.Inf(1)
	for range 3 {
		got = math.Min(got, testing.AllocsPerRun(100, func() { _ = Clone(value) }))
	}

	if got > wantAllocs {
		t.Errorf("Clone over a decoded JSON tree allocates %v times, up from %d: the read and "+
			"delivery paths left the JSON fast path", got, wantAllocs)
	}
}
