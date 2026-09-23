//go:build unit

package logguard

import (
	"fmt"
	"strings"
	"testing"
)

// fakeTB captures what the guard reports instead of failing this run.
//
// testing.TB is closed to outside implementations, so the embedded interface
// stays nil and only the three methods the guard calls are defined: any other
// call panics, which is louder than a silent pass.
type fakeTB struct {
	testing.TB

	errs   []string
	fatals []string
}

func (f *fakeTB) Helper() {}

func (f *fakeTB) Errorf(format string, args ...any) {
	f.errs = append(f.errs, fmt.Sprintf(format, args...))
}

func (f *fakeTB) Fatalf(format string, args ...any) {
	f.fatals = append(f.fatals, fmt.Sprintf(format, args...))
}

// TestAssertNoneRedacted pins what the guard sees, on the fixtures under
// testdata. The guard is the only thing standing between a renamed field and
// key=[REDACTED] reaching an operator, and it is itself untested code running
// under a build tag — an alias on the import was enough to blind it silently.
func TestAssertNoneRedacted(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		dir    string
		errs   []string
		fatals []string
	}{
		{
			name: "a plain import is scanned",
			dir:  "testdata/unaliased",
			errs: []string{`field name "key"`},
		},
		{
			name: "an aliased import is scanned",
			dir:  "testdata/aliased",
			errs: []string{`field name "key"`},
		},
		{
			name: "every call site of a repeated name is reported, in source order",
			dir:  "testdata/repeated",
			errs: []string{"source.go:8:", "source.go:12:"},
		},
		{
			name: "a name the scan cannot read is skipped",
			dir:  "testdata/nonliteral",
		},
		{
			name:   "another package's log is not scanned, and reading nothing fails",
			dir:    "testdata/foreignlog",
			fatals: []string{"the scan matched nothing"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tb := &fakeTB{}
			AssertNoneRedacted(tb, tc.dir)

			requireLines(t, "error", tb.errs, tc.errs)
			requireLines(t, "fatal", tb.fatals, tc.fatals)
		})
	}
}

// requireLines asserts the guard reported exactly one line per entry of want,
// each containing its entry, in order; an empty want means no line at all.
func requireLines(t *testing.T, kind string, got, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d %s lines, want %d containing %q: %v", len(got), kind, len(want), want, got)
	}

	for i := range want {
		if !strings.Contains(got[i], want[i]) {
			t.Errorf("%s line %d = %q, want it to contain %q", kind, i, got[i], want[i])
		}
	}
}
