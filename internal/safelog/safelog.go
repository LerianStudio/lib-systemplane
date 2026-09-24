// Package safelog holds the things this library does to keep a consumer's own
// observability code from costing it either the process or a secret: a guard
// that makes a panicking logger harmless, and the two renderings a failure is
// reported as when the key it belongs to is registered redacted — one for a
// recovered panic, one for an error somebody else wrote.
//
// It is a leaf — lib-observability's log package and the standard library,
// nothing else — because both internal/engine and internal/group need it, and
// a group that reached into the engine for it inverted the layering the two
// packages are meant to keep.
package safelog

import (
	"context"
	"fmt"

	"github.com/LerianStudio/lib-observability/v4/log"
)

// UnresolvedTenant is the tenant.id a multi-tenant line carries when it has
// no tenant id to name: the Client's read-through errors when the tenant
// database resolved but ctx carries no tenant id (the two ride independent
// context keys), and the group coordinator's reports about a publication that
// names no tenant. An empty tenant.id there would read exactly like a
// single-tenant line, which carries no tenant field at all.
const UnresolvedTenant = "unresolved"

// guardedLogger is the consumer's logger with a net under it: neither an entry
// nor a level check can unwind into the library.
//
// It is what makes the guard hold everywhere at once. The engine hands its
// logger to lib-observability's recovery handlers on three goroutines it
// owns — the reconcile worker, a debounced changefeed re-read and a delivery
// worker — and the group coordinator hands it to the same handler from an
// applier's recovery; every one of those handlers LOGS the panic it just
// recovered before it counts it. A consumer logger that panics on that line
// turns a recovered panic into an unrecovered one on a goroutine the consumer
// cannot reach, and takes the process down; on the v3 path the same broken
// logger panicked on the caller's own stack, where the caller could see it.
// Enabled is guarded too, because the DEBUG level check runs per changefeed
// event, outside every recovery.
//
// Only Log and Enabled are overridden: With, WithGroup and Sync are forwarded
// to the consumer's logger because nothing in this library or in
// lib-observability's recovery path calls them.
type guardedLogger struct{ log.Logger }

func (l guardedLogger) Log(ctx context.Context, level int, msg string, fields ...any) {
	defer Swallow()

	l.Logger.Log(ctx, level, msg, fields...)
}

// Enabled reports false for a logger that panics on its level check: the
// recovered return never runs the assignment, so the zero value stands, and a
// guarded DEBUG line is skipped rather than built for a logger that cannot
// take it.
func (l guardedLogger) Enabled(level int) bool {
	defer Swallow()

	return l.Logger.Enabled(level)
}

// Guard wraps a consumer's logger so a panic raised inside it cannot unwind
// into the caller, and returns a no-op logger for nil. Everything that logs
// from a goroutine the consumer cannot recover on runs under it: the engine,
// the group coordinator, and the store whose changefeed goroutines log from
// their own recoveries and from the listener loop.
//
// Nil means log.IsNil, not l == nil: a consumer whose own logger field is an
// unset pointer arrives here as a nil POINTER inside a non-nil interface, and
// == nil does not see it. Guard runs FIRST on every path — the Client's option
// pass, the engine's constructor, the group coordinator — so wrapping such a
// logger rather than replacing it hides it from every log.IsNil downstream,
// and the two backend configs and the Client that normalise a nil logger to a
// no-op stop firing. Only the two overridden methods would be covered by the
// recover below; With, WithGroup and Sync are forwarded and would panic on the
// caller's own stack.
//
// Idempotent: a logger already guarded is handed back as it is, so a caller
// that guards early and a constructor that guards again cost one wrapper, not
// two.
func Guard(l log.Logger) log.Logger {
	if log.IsNil(l) {
		return log.NewNop()
	}

	if _, guarded := l.(guardedLogger); guarded {
		return l
	}

	return guardedLogger{l}
}

// WithheldPanic renders a recovered panic for a key registered redacted: what
// panicked, and the panic value's dynamic type, which is enough to tell two
// panics apart and can never carry a byte of a secret.
//
// lib-observability's canonical handler logs log.Any("value", recovered) and
// stamps the same rendering on the span event whenever production mode is off,
// and off is its shipped default. So a validator, a subscriber or a group's
// apply hook that panics NAMING the value it was handed —
// panic(fmt.Sprintf("cannot apply %+v", doc)) — publishes that value at ERROR
// for a key whose entire registration says it must never reach a log line, and
// nothing downstream catches it: "value" is not on lib-observability's
// sensitive-field list. Substituting this sentence keeps the handler, and with
// it the panic counter, the span event and the error report; only what they
// carry changes.
//
// One wording, one place, so the engine and the group coordinator report a
// withheld panic identically.
func WithheldPanic(what string, recovered any) string {
	return fmt.Sprintf("%s (%T, value withheld: key registered redacted)", what, recovered)
}

// Swallow discards a panic raised by the consumer's own observability code.
// There is nowhere left to report it — the logger is what panicked — and the
// alternative is unwinding a library goroutine over a log line.
//
// It lives here because every caller of it is a caller of this package: the
// guard's two overridden methods, the engine's log helpers and the group
// coordinator's, which each used to carry a byte-identical copy.
func Swallow() {
	_ = recover()
}

// ErrorDetail renders a rejection's cause under the key's registered redaction
// policy: the error itself for an ordinary key, and for a redacted one only
// what refused it plus the error's dynamic type.
//
// Every rejection a stored document can produce carries the value in its
// message. A consumer's validator or apply hook may name what it refused —
// "token %q is too short". encoding/json is worse, because it needs no help: an
// unparsable row comes back as "invalid character 'h' looking for beginning of
// value", which quotes the value's first byte.
//
// The type alone is enough to tell two failures apart and can never carry a
// byte of the value; the offset is withheld for the same reason, being a
// measurement of the secret. What the caller receives is unchanged in every
// case: this is the log stream, not the API, and FC-7's Status keeps the
// untouched error.
//
// One wording, one place, so the engine and the group coordinator report a
// withheld cause identically.
func ErrorDetail(redacted bool, what string, err error) log.Field {
	if !redacted {
		return log.Err(err)
	}

	return log.String("error", fmt.Sprintf("%s (%T)", what, err))
}
