//go:build unit

package client

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/safelog"
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

	// safelog.Guard is idempotent, so a value it hands back unchanged is one it
	// already wrapped. This pins the VALUE in the config, not the hand-offs:
	// what the Client itself logs through is pinned by the test below.
	if guarded := safelog.Guard(cfg.logger); guarded != cfg.logger {
		t.Error("the logger the backends are handed is not guarded")
	}

	if cfg.consumerLogger != (explodingLogger{}) {
		t.Errorf("consumerLogger: got %v, want the logger the caller passed", cfg.consumerLogger)
	}
}

// TestAPanickingConsumerLoggerDoesNotUnwindOutOfARead pins the half of the
// guard the test above cannot see: what newClient puts on the CLIENT, not what
// it puts in the config.
//
// The Client keeps the consumer's logger unwrapped on purpose — Logger() hands
// it back — and its own error lines used to go out on that field. Both call
// sites are the multi-tenant read-through naming a row it could not decode, and
// both run on the CALLER's goroutine: a consumer logger that panics there took
// the caller's Get or List down with it, past a library boundary that promises
// a returned error. The config-level assertion above stayed green throughout.
func TestAPanickingConsumerLoggerDoesNotUnwindOutOfARead(t *testing.T) {
	m := newMemStore(true)
	c := newMultiTenantClientWithLogger(t, m, explodingLogger{})

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	// A row no decoder accepts, which is what drives the Client to log.
	seedRaw(m, "ns", "k", []byte("{not json"))

	if _, _, err := c.Get(context.Background(), "ns", "k"); err == nil {
		t.Error("Get: want a decode error, got nil")
	}

	if _, err := c.List(context.Background(), "ns"); err == nil {
		t.Error("List: want a decode error, got nil")
	}
}

// TestBackendConfigsAreBuiltWithTheGuardedLogger pins the hand-off neither
// test above can see.
//
// The value in the config is guarded, but each constructor copies it field by
// field into a backend Config, and swapping that one field for the consumer's
// raw logger compiles and leaves the whole suite green — the backends only log
// from a live changefeed, which no unit test runs. Building both Configs
// through one named function each puts the hand-off somewhere a test can hold.
func TestBackendConfigsAreBuiltWithTheGuardedLogger(t *testing.T) {
	cfg := defaultClientConfig()
	applyClientOptions(&cfg, []Option{WithLogger(explodingLogger{})})

	cases := []struct {
		name string
		got  log.Logger
	}{
		{"postgres", postgresConfig(nil, "", cfg).Logger},
		{"mongodb", mongoConfig(nil, "", cfg).Logger},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// safelog.Guard is idempotent, so a logger it hands back unchanged
			// is one it already wrapped.
			if safelog.Guard(tc.got) != tc.got {
				t.Error("the backend is handed the consumer's raw logger, not the guarded one")
			}
		})
	}
}
