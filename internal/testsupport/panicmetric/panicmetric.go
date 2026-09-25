// Package panicmetric stands in for the host's metrics factory behind
// lib-observability's panic counter, so a test can prove a recovered panic was
// counted and under which labels — the half of the report a log line cannot
// show.
//
// The counter is process-wide and first-install-wins, so a test that installs
// this recorder must not run in parallel with any test of its binary that can
// panic under recovery.
package panicmetric

import (
	"context"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/runtime"
)

// Increment is one panic_recovered_total increment and the two labels
// lib-observability gives it.
type Increment struct {
	Component string
	Name      string
}

// Recorder counts the increments lib-observability records through it.
type Recorder struct {
	mu         sync.Mutex
	increments []Increment
}

// AddCounter satisfies runtime.Recorder.
func (r *Recorder) AddCounter(_ context.Context, _, _, _ string, attrs map[string]string, delta int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for range delta {
		r.increments = append(r.increments, Increment{Component: attrs["component"], Name: attrs["goroutine_name"]})
	}

	return nil
}

// Increments returns a copy of what has been recorded so far.
func (r *Recorder) Increments() []Increment {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]Increment(nil), r.increments...)
}

// Install makes a fresh Recorder the process's panic counter for the rest of
// t, and removes it when t ends.
func Install(t testing.TB) *Recorder {
	t.Helper()

	r := &Recorder{}

	runtime.ResetPanicMetrics()
	runtime.InitPanicMetrics(r)
	t.Cleanup(runtime.ResetPanicMetrics)

	return r
}

// RequireOnly fails t unless exactly one increment was recorded, under
// component and name.
func (r *Recorder) RequireOnly(t testing.TB, component, name string) {
	t.Helper()

	want := Increment{Component: component, Name: name}

	if got := r.Increments(); len(got) != 1 || got[0] != want {
		t.Errorf("panic counter increments = %+v, want exactly [%+v]: the panic was not counted, or was counted under another site", got, want)
	}
}
