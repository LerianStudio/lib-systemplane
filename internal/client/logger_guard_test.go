//go:build unit

package client

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/engine"
)

// explodingLogger is the consumer logger that is broken rather than slow:
// every entry panics, and so does every level check. A nil field dereferenced
// in a custom Log, a sink closed at shutdown and written to afterwards — it is
// the ordinary way a consumer's observability code fails, and the backends
// reach it from their own changefeed goroutines, where the consumer cannot
// recover it.
type explodingLogger struct{}

func (explodingLogger) Log(context.Context, int, string, ...any) {
	panic("the consumer's logger blew up")
}

func (explodingLogger) Enabled(int) bool { panic("the consumer's logger blew up") }

func (l explodingLogger) With(...any) log.Logger { return l }

func (l explodingLogger) WithGroup(string) log.Logger { return l }

func (explodingLogger) Sync(context.Context) error { return nil }

// TestTheLoggerEveryBackendGetsIsGuarded pins the single place the consumer's
// logger is wrapped.
//
// The Postgres listener, the MongoDB change stream and the engine's workers
// all log from goroutines this library owns, and all three are handed
// cfg.logger. Wrapping at each of those hand-offs is a rule the next one has
// to remember, and the suite stayed green when two of them dropped it. So the
// value itself is guarded, once, where the options are applied: there is no
// unguarded logger left in the config for a later reader to pick up.
//
// Client.Logger() is the exception, and deliberately: it hands back exactly
// what the caller passed, which TestLoggerNeverReturnsNil pins.
func TestTheLoggerEveryBackendGetsIsGuarded(t *testing.T) {
	cfg := defaultClientConfig()
	applyClientOptions(&cfg, []Option{WithLogger(explodingLogger{})})

	// Both of the consumer's panics, neither reaching this frame.
	cfg.logger.Log(context.Background(), log.LevelError, "a line the consumer's logger explodes on")

	if cfg.logger.Enabled(log.LevelDebug) {
		t.Error("Enabled reported true for a logger that panics on its level check")
	}

	// GuardLogger is idempotent, so a value it hands back unchanged is one it
	// already wrapped: this is what fails if a constructor is ever handed an
	// unguarded logger again.
	if guarded := engine.GuardLogger(cfg.logger); guarded != cfg.logger {
		t.Error("the logger the backends are handed is not guarded")
	}

	if cfg.consumerLogger != (explodingLogger{}) {
		t.Errorf("consumerLogger: got %v, want the logger the caller passed", cfg.consumerLogger)
	}
}
