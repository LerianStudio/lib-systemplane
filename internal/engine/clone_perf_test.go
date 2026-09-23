//go:build unit && !race

package engine

import "testing"

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
// wantAllocs is measured, not derived: it is what the fast path costs for this
// tree today, and it is here to change loudly. A regression to the reflective
// walk multiplies it; a genuine improvement lowers it, and then the number
// moves in a commit that says why.
func TestPerf_CloneJSONMap(t *testing.T) {
	const wantAllocs = 6

	value := jsonCloneTree()

	if got := testing.AllocsPerRun(100, func() { _ = Clone(value) }); got != wantAllocs {
		t.Errorf("Clone over a decoded JSON tree allocates %v times, want %d: the read and "+
			"delivery paths left the JSON fast path", got, wantAllocs)
	}
}
