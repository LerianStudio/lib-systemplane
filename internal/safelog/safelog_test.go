//go:build unit

package safelog

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/logguard"
)

// TestWithheldPanicNamesTheTypeAndNeverTheValue pins the wording the engine and
// the group coordinator both report a redacted key's panic as. Two callers, one
// sentence: an operator reading either line learns the same thing, and an
// assertion anywhere in the repo matches the same text.
func TestWithheldPanicNamesTheTypeAndNeverTheValue(t *testing.T) {
	t.Parallel()

	const marker = "probe-secret-Rk9Tz"

	cases := []struct {
		name      string
		what      string
		recovered any
		want      string
	}{
		{
			name:      "a string panic",
			what:      "apply function panicked",
			recovered: "cannot apply " + marker,
			want:      "apply function panicked (string, value withheld: key registered redacted)",
		},
		{
			name:      "an error panic",
			what:      "validator panicked",
			recovered: errors.New(marker),
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

	const marker = "probe-secret-Qw83Lm"

	// The real leak, not a stand-in: encoding/json refusing a row that opens
	// with the secret's first byte, which is what an undecodable stored
	// document produces at an ingress, offset and all.
	syntaxErr := json.Unmarshal([]byte(marker), new(map[string]any))
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
			err:  errors.New("token " + marker + " is too short"),
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

			if strings.Contains(text, marker) {
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
