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
		name  string
		dir   string
		err   string
		fatal string
	}{
		{
			name: "a plain import is scanned",
			dir:  "testdata/unaliased",
			err:  `field name "key"`,
		},
		{
			name: "an aliased import is scanned",
			dir:  "testdata/aliased",
			err:  `field name "key"`,
		},
		{
			name: "a name the scan cannot read is skipped",
			dir:  "testdata/nonliteral",
		},
		{
			name:  "another package's log is not scanned, and reading nothing fails",
			dir:   "testdata/foreignlog",
			fatal: "the scan matched nothing",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tb := &fakeTB{}
			AssertNoneRedacted(tb, tc.dir)

			requireOne(t, "error", tb.errs, tc.err)
			requireOne(t, "fatal", tb.fatals, tc.fatal)
		})
	}
}

// requireOne asserts the guard reported exactly one line containing want, or
// none at all when want is empty.
func requireOne(t *testing.T, kind string, got []string, want string) {
	t.Helper()

	if want == "" {
		if len(got) != 0 {
			t.Errorf("got %d %s lines, want none: %v", len(got), kind, got)
		}

		return
	}

	if len(got) != 1 {
		t.Fatalf("got %d %s lines, want exactly 1 containing %q: %v", len(got), kind, want, got)
	}

	if !strings.Contains(got[0], want) {
		t.Errorf("%s line = %q, want it to contain %q", kind, got[0], want)
	}
}
