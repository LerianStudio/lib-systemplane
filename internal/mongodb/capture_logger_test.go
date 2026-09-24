//go:build unit || integration

package mongodb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/testsupport/panicmetric"
)

// captureLogger records what the store logs so a test can pin the LEVEL a line
// is emitted at, not only its text.
type captureLogger struct {
	mu      sync.Mutex
	entries []captureEntry
}

type captureEntry struct {
	level  int
	msg    string
	fields []log.Field
}

// Log normalizes the ...any variadic the way the library does: the store hands
// its []log.Field over as a single element.
func (c *captureLogger) Log(_ context.Context, level int, msg string, fields ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = append(c.entries, captureEntry{level: level, msg: msg, fields: log.Fields(fields...)})
}

func (c *captureLogger) With(...any) log.Logger      { return c }
func (c *captureLogger) WithGroup(string) log.Logger { return c }
func (c *captureLogger) Enabled(int) bool            { return true }
func (c *captureLogger) Sync(context.Context) error  { return nil }

// waitFor blocks until an entry at level carrying msg has been logged, so a
// test can pin a line a background goroutine produces without racing it.
func (c *captureLogger) waitFor(t *testing.T, level int, msg string) captureEntry {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for {
		c.mu.Lock()

		for _, e := range c.entries {
			if e.level == level && e.msg == msg {
				c.mu.Unlock()

				return e
			}
		}

		seen := len(c.entries)
		c.mu.Unlock()

		if time.Now().After(deadline) {
			t.Fatalf("no %q entry at level %d after 5s (%d entries logged)", msg, level, seen)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

// warnCount reports how many WARN entries carry msg. waitFor only proves that
// at least one was logged, which is not the claim a streak test makes: the
// point of the streak is that the SECOND failure of the same cause is quiet.
// Callers take the count after the goroutine under test has exited, so it is
// final.
func (c *captureLogger) warnCount(msg string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	var n int

	for _, e := range c.entries {
		if e.level == log.LevelWarn && e.msg == msg {
			n++
		}
	}

	return n
}

// field returns the value of the named field, failing when it is absent.
func (e captureEntry) field(t *testing.T, key string) any {
	t.Helper()

	for _, f := range e.fields {
		if f.Key == key {
			return f.Value
		}
	}

	t.Fatalf("log entry %q carries no %q field (%+v)", e.msg, key, e.fields)

	return nil
}

// requirePanicReported asserts the whole report of the one panic recovered so
// far: a single ERROR line, "panic recovered", naming source, and one increment
// of the panic counter under this package's component and that same source.
// The line alone is not enough — the bare recovery the store used before logged
// it too, and counted nothing.
func requirePanicReported(t *testing.T, logger *captureLogger, counter *panicmetric.Recorder, source string) {
	t.Helper()

	logger.mu.Lock()

	var errs []captureEntry

	for _, e := range logger.entries {
		if e.level == log.LevelError {
			errs = append(errs, e)
		}
	}

	logger.mu.Unlock()

	if len(errs) != 1 || errs[0].msg != "panic recovered" {
		t.Fatalf("ERROR entries = %+v, want exactly one %q", errs, "panic recovered")
	}

	if got := errs[0].field(t, "source"); got != source {
		t.Errorf("panic line source = %v, want %q", got, source)
	}

	counter.RequireOnly(t, "systemplane.mongodb", source)
}
