//go:build unit

package safelog

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"

	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/logguard"
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
	t.Parallel()

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
	t.Parallel()

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
			t.Parallel()

			if got := WithheldPanic(tc.what, tc.recovered); got != tc.want {
				t.Errorf("WithheldPanic = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestErrorDetailWithholdsTheCauseForARedactedKey pins the twin of
// WithheldPanic: the rendering a rejection somebody ELSE wrote is reported as.
//
// Every rejection a stored document can produce carries the value in its
// message. A consumer's validator names what it refused; encoding/json needs no
// prompting at all, quoting the row byte by byte and measuring it with an
// offset. So a redacted key gets one exact sentence — what refused the document
// and the cause's dynamic type, nothing else — while an ordinary key keeps
// log.Err(err): the conventional field, the error itself, untouched.
//
// The wanted strings are spelled out rather than rebuilt from the format, so a
// change to either rendering fails here instead of agreeing with itself.
func TestErrorDetailWithholdsTheCauseForARedactedKey(t *testing.T) {
	t.Parallel()

	const secret = "probe-secret-Qw83Lm"

	// The real leak, not a stand-in: encoding/json refusing a row that opens
	// with the secret's first byte, which is what an undecodable stored
	// document produces at an ingress, offset and all.
	syntaxErr := json.Unmarshal([]byte(secret), new(map[string]any))
	if syntaxErr == nil {
		t.Fatal("json.Unmarshal accepted a malformed document, want a syntax error to render")
	}

	cases := []struct {
		name string
		what string
		err  error
		want string
	}{
		{
			name: "a validator rejection naming what it refused",
			what: "validation failed",
			err:  errors.New("token " + secret + " is too short"),
			want: "validation failed (*errors.errorString)",
		},
		{
			name: "encoding/json quoting the row it could not parse",
			what: "decode failed",
			err:  syntaxErr,
			want: "decode failed (*json.SyntaxError)",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			plain := ErrorDetail(false, tc.what, tc.err)
			if plain.Key != "error" || plain.Value != tc.err {
				t.Errorf("ErrorDetail(false, …) = %+v, want log.Err(err): the conventional field carrying the error itself", plain)
			}

			withheld := ErrorDetail(true, tc.what, tc.err)
			if withheld.Key != "error" {
				t.Errorf("ErrorDetail(true, …).Key = %q, want %q", withheld.Key, "error")
			}

			text, ok := withheld.Value.(string)
			if !ok {
				t.Fatalf("ErrorDetail(true, …).Value is %T, want a string: the cause itself must not reach the log", withheld.Value)
			}

			// Exact, so the assertion is about what an operator reads and not
			// merely about the absence of the secret: the offset a
			// *json.SyntaxError carries is a measurement of the value and is
			// withheld with the message it came in.
			if text != tc.want {
				t.Errorf("ErrorDetail(true, …) = %q, want %q", text, tc.want)
			}

			if strings.Contains(text, secret) {
				t.Errorf("ErrorDetail(true, …) = %q, want no byte of the value it refused", text)
			}
		})
	}
}

// TestNoLoggedFieldNameIsRedacted reads this package's own source and refuses
// any field name lib-observability erases. ErrorDetail is the one place that
// builds a field name here, and a "key" slipping into it would erase the very
// detail this package exists to render.
func TestNoLoggedFieldNameIsRedacted(t *testing.T) {
	t.Parallel()

	logguard.AssertNoneRedacted(t, ".")
}

// typedNilLogger is the shape a `== nil` check does not catch: a nil POINTER
// inside a non-nil interface. It is the ordinary way a consumer arrives with
// no logger — a struct field of its own logger type left unset, handed to
// WithLogger — and every method here dereferences the receiver, which is what
// such a logger does in production.
type typedNilLogger struct{ sink []string }

func (l *typedNilLogger) Log(_ context.Context, _ int, msg string, _ ...any) {
	l.sink = append(l.sink, msg)
}

func (l *typedNilLogger) Enabled(int) bool { return len(l.sink) > 0 }

func (l *typedNilLogger) With(...any) log.Logger { l.sink = nil; return l }

func (l *typedNilLogger) WithGroup(string) log.Logger { l.sink = nil; return l }

func (l *typedNilLogger) Sync(context.Context) error { l.sink = nil; return nil }

// TestGuardNormalisesATypedNilLogger pins the guard against the nil every other
// nil check in this module already uses log.IsNil for. Guard runs FIRST — the
// Client's option pass, the engine's constructor and the group coordinator all
// hand it the consumer's raw logger — so a typed nil it wraps rather than
// replaces is no longer log.IsNil to anything downstream, and the
// normalisations that used to catch it never fire.
//
// Every method is exercised, not only the two the wrapper overrides: With,
// WithGroup and Sync are forwarded to the wrapped logger, so a typed nil that
// survives Guard panics on the caller's own stack with no recover in front of
// it.
func TestGuardNormalisesATypedNilLogger(t *testing.T) {
	t.Parallel()

	var typedNil *typedNilLogger

	guarded := Guard(typedNil)

	if log.IsNil(guarded) {
		t.Fatal("Guard returned a nil logger for a typed nil, want a usable no-op")
	}

	guarded.Log(context.Background(), log.LevelError, "a line nobody has a logger for")

	if guarded.Enabled(log.LevelDebug) {
		t.Error("Enabled reported true for a typed-nil logger, want false")
	}

	if with := guarded.With("k", "v"); log.IsNil(with) {
		t.Error("With returned a nil logger")
	}

	if group := guarded.WithGroup("g"); log.IsNil(group) {
		t.Error("WithGroup returned a nil logger")
	}

	if err := guarded.Sync(context.Background()); err != nil {
		t.Errorf("Sync = %v, want nil", err)
	}
}
