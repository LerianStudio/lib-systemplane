//go:build unit

package safelog

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
)

// deadLogger is the consumer logger that is broken rather than slow: every
// entry panics, and so does every level check. A nil field dereferenced in a
// custom Log, a sink closed at shutdown and written to afterwards — it is the
// ordinary way a consumer's observability code fails, and it arrives at this
// library on library-owned goroutines where the consumer cannot recover it.
type deadLogger struct{}

func (deadLogger) Log(context.Context, int, string, ...any) { panic("the consumer's logger blew up") }

func (deadLogger) Enabled(int) bool { panic("the consumer's logger blew up") }

func (l deadLogger) With(...any) log.Logger { return l }

func (l deadLogger) WithGroup(string) log.Logger { return l }

func (deadLogger) Sync(context.Context) error { return nil }

// TestGuardIsIdempotentAndSwallows pins the two properties every caller leans
// on: a consumer logger that explodes is absorbed rather than unwound into the
// library, and guarding a logger a constructor will guard again costs one
// wrapper rather than two.
func TestGuardIsIdempotentAndSwallows(t *testing.T) {
	guarded := Guard(deadLogger{})

	// Both of the consumer's panics, neither reaching this frame.
	guarded.Log(context.Background(), log.LevelError, "a line the consumer's logger explodes on")

	if guarded.Enabled(log.LevelDebug) {
		t.Error("Enabled reported true for a logger that panics on its level check")
	}

	if again := Guard(guarded); again != guarded {
		t.Error("Guard wrapped an already-guarded logger a second time")
	}

	if Guard(nil) == nil {
		t.Error("Guard(nil) returned nil, want a no-op logger")
	}
}

// TestWithheldPanicNamesTheTypeAndNeverTheValue pins the wording the engine and
// the group coordinator both report a redacted key's panic as. Two callers, one
// sentence: an operator reading either line learns the same thing, and an
// assertion anywhere in the repo matches the same text.
func TestWithheldPanicNamesTheTypeAndNeverTheValue(t *testing.T) {
	const secret = "probe-secret-Rk9Tz"

	cases := []struct {
		name      string
		what      string
		recovered any
		want      string
	}{
		{
			name:      "a string panic",
			what:      "apply function panicked",
			recovered: "cannot apply " + secret,
			want:      "apply function panicked (string, value withheld: key registered redacted)",
		},
		{
			name:      "an error panic",
			what:      "validator panicked",
			recovered: errors.New(secret),
			want:      "validator panicked (*errors.errorString, value withheld: key registered redacted)",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := WithheldPanic(tc.what, tc.recovered); got != tc.want {
				t.Errorf("WithheldPanic = %q, want %q", got, tc.want)
			}
		})
	}
}
