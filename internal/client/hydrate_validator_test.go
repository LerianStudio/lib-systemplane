//go:build unit

package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// logLine is one call recorded by recordingLogger.
type logLine struct {
	level  int
	msg    string
	fields []any
}

// String renders the line the way a sink would. Without it fmt reflects over
// the unexported fields and prints an error as its address, which would make
// the "never log the value" assertion below vacuous.
func (l logLine) String() string {
	return fmt.Sprintf("%s %v", l.msg, l.fields)
}

// recordingLogger is a log.Logger that keeps every line in memory so a test
// can assert both what was said and what was NOT said — a rejected value may
// carry a secret, so the absence of its bytes is part of the contract.
type recordingLogger struct {
	mu    sync.Mutex
	lines []logLine
}

func (l *recordingLogger) Log(_ context.Context, level int, msg string, fields ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, logLine{level: level, msg: msg, fields: fields})
}

func (l *recordingLogger) With(_ ...any) log.Logger      { return l }
func (l *recordingLogger) WithGroup(_ string) log.Logger { return l }
func (l *recordingLogger) Enabled(_ int) bool            { return true }
func (l *recordingLogger) Sync(_ context.Context) error  { return nil }

// warns returns the recorded WARN lines carrying msg.
func (l *recordingLogger) warns(msg string) []logLine {
	l.mu.Lock()
	defer l.mu.Unlock()

	var out []logLine

	for _, line := range l.lines {
		if line.level == log.LevelWarn && line.msg == msg {
			out = append(out, line)
		}
	}

	return out
}

// rendered flattens every recorded line, message and fields alike, to the text
// a log sink would carry.
func (l *recordingLogger) rendered() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return fmt.Sprintf("%v", l.lines)
}

func newSingleTenantClientWithLogger(t *testing.T, s *memStore, logger log.Logger) *Client {
	t.Helper()

	cfg := defaultClientConfig()
	cfg.debounce = 0
	cfg.logger = logger

	return newClient(s, cfg)
}

// seedEntry writes value to the store directly, as a row an older binary or a
// direct SQL write would have left behind.
func seedEntry(t *testing.T, m *memStore, ns, key string, value any) {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal seed: %v", err)
	}

	m.mu.Lock()
	m.entries[memKey(ns, key)] = store.Entry{Namespace: ns, Key: key, Value: raw}
	m.mu.Unlock()
}

// rejectedSecret is the stored value the validators below refuse. It reads
// like a credential on purpose: no log line may reproduce it.
const rejectedSecret = "s3cr3t-token-do-not-log"

var errSchemeRefused = errors.New("cleartext scheme refused on a hardened deployment")

// TestHydrateRunsTheValidatorOnStoredValues covers the defect a consumer hit in
// production: a value persisted before the key had a validator (or written by
// an older binary, or straight into the table) was hydrated into the cache
// unchecked, so the value in force was one the admin write path would refuse.
// Hydration must grade it and keep the registered default when it fails, while
// a sibling key whose stored value passes still hydrates.
func TestHydrateRunsTheValidatorOnStoredValues(t *testing.T) {
	m := newMemStore(false)
	logger := &recordingLogger{}
	c := newSingleTenantClientWithLogger(t, m, logger)

	validator := func(_ context.Context, value any) error {
		if value == rejectedSecret {
			return errSchemeRefused
		}

		return nil
	}

	for _, key := range []string{"bad", "good"} {
		if err := c.Register("ns", key, "default", WithContextValidator(validator)); err != nil {
			t.Fatalf("register %s: %v", key, err)
		}
	}

	seedEntry(t, m, "ns", "bad", rejectedSecret)
	seedEntry(t, m, "ns", "good", "accepted-from-store")

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	cases := []struct {
		key  string
		want any
	}{
		{key: "bad", want: "default"},
		{key: "good", want: "accepted-from-store"},
	}

	for _, tc := range cases {
		got, ok, err := c.Get(context.Background(), "ns", tc.key)
		if err != nil || !ok {
			t.Fatalf("get %s: value=%v ok=%v err=%v", tc.key, got, ok, err)
		}

		if got != tc.want {
			t.Errorf("%s in force = %v, want %v", tc.key, got, tc.want)
		}
	}

	warns := logger.warns("stored value rejected by validator, keeping default")
	if len(warns) != 1 {
		t.Fatalf("got %d WARN lines for the rejected stored value, want exactly 1", len(warns))
	}

	if rendered := fmt.Sprintf("%v", warns[0]); !strings.Contains(rendered, errSchemeRefused.Error()) {
		t.Errorf("WARN line %q does not carry the validator's own error", rendered)
	}

	if rendered := logger.rendered(); strings.Contains(rendered, rejectedSecret) {
		t.Errorf("a log line reproduced the rejected value's bytes: %q", rendered)
	}
}

// TestRefreshRunsTheValidatorOnRefreshedValues covers the second unchecked
// write path: a value that arrives through the changefeed after start. A
// refusal keeps the value already in force, and — when the refresh lands
// during hydration — must not claim the key, or hydration's own List snapshot
// would be dropped on the strength of a value nobody accepted.
func TestRefreshRunsTheValidatorOnRefreshedValues(t *testing.T) {
	t.Run("keeps the current value after start", func(t *testing.T) {
		m := newMemStore(false)
		logger := &recordingLogger{}
		c := newSingleTenantClientWithLogger(t, m, logger)

		rejected := make(chan struct{}, 1)

		validator := func(_ context.Context, value any) error {
			if value != rejectedSecret {
				return nil
			}

			select {
			case rejected <- struct{}{}:
			default:
			}

			return errSchemeRefused
		}

		if err := c.Register("ns", "k", "default", WithContextValidator(validator)); err != nil {
			t.Fatalf("register: %v", err)
		}

		seedEntry(t, m, "ns", "k", "known-good")

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}

		t.Cleanup(func() { _ = c.Close() })

		if got, ok, err := c.Get(context.Background(), "ns", "k"); err != nil || !ok || got != "known-good" {
			t.Fatalf("pre-condition get = (%v, %v, %v); want (known-good, true, nil)", got, ok, err)
		}

		seedEntry(t, m, "ns", "k", rejectedSecret)
		m.fire(store.Event{Namespace: "ns", Key: "k", Op: store.OpUpsert})

		select {
		case <-rejected:
		case <-time.After(2 * time.Second):
			t.Fatal("the refresh never handed the refreshed value to the validator")
		}

		// The refusal happens before the cache write; give a faulty
		// implementation the chance to perform that write anyway.
		time.Sleep(50 * time.Millisecond)

		got, ok, err := c.Get(context.Background(), "ns", "k")
		if err != nil || !ok {
			t.Fatalf("get: value=%v ok=%v err=%v", got, ok, err)
		}

		if got != "known-good" {
			t.Errorf("value in force = %v, want the value that was already in force", got)
		}

		if n := len(logger.warns("refreshed value rejected by validator, keeping current value")); n != 1 {
			t.Errorf("got %d WARN lines for the rejected refresh, want exactly 1", n)
		}

		if rendered := logger.rendered(); strings.Contains(rendered, rejectedSecret) {
			t.Errorf("a log line reproduced the rejected value's bytes: %q", rendered)
		}
	})

	t.Run("does not claim the key against hydration", func(t *testing.T) {
		m := newMemStoreWithListHook(false)
		logger := &recordingLogger{}
		c := newSingleTenantClientWithLogger(t, m, logger)

		validator := func(_ context.Context, value any) error {
			if value == rejectedSecret {
				return errSchemeRefused
			}

			return nil
		}

		if err := c.Register("ns", "k", "default", WithContextValidator(validator)); err != nil {
			t.Fatalf("register: %v", err)
		}

		// The value List() will report: acceptable, and the one that must end
		// up in force.
		seedEntry(t, m, "ns", "k", "accepted-from-list")

		listReady := make(chan struct{})
		listRelease := make(chan struct{})
		m.listHook = func() {
			select {
			case listReady <- struct{}{}:
			default:
			}
			<-listRelease
		}

		startDone := make(chan error, 1)

		go func() {
			startDone <- c.Start(context.Background())
		}()

		<-listReady

		// A changefeed event mid-hydration carrying a value the validator
		// refuses. Marking the key touched here would make hydrate() skip it
		// and leave the registered default in force.
		seedEntry(t, m, "ns", "k", rejectedSecret)
		m.fire(store.Event{Namespace: "ns", Key: "k", Op: store.OpUpsert})

		time.Sleep(50 * time.Millisecond)

		// Restore the value List() is about to read, so hydration sees the
		// acceptable snapshot the changefeed raced.
		seedEntry(t, m, "ns", "k", "accepted-from-list")
		close(listRelease)

		if err := <-startDone; err != nil {
			t.Fatalf("start: %v", err)
		}

		t.Cleanup(func() { _ = c.Close() })

		time.Sleep(50 * time.Millisecond)

		got, ok, err := c.Get(context.Background(), "ns", "k")
		if err != nil || !ok {
			t.Fatalf("get: value=%v ok=%v err=%v", got, ok, err)
		}

		if got != "accepted-from-list" {
			t.Errorf("value in force = %v, want the hydrated snapshot — a rejected refresh claimed the key", got)
		}
	})
}

// TestAcceptingValidatorSeesTheValueBothPathsCache pins that nothing changed
// for a validator that accepts: it is handed exactly the decoded value the
// cache goes on to serve, on hydration and on refresh alike.
func TestAcceptingValidatorSeesTheValueBothPathsCache(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClientWithLogger(t, m, &recordingLogger{})

	var (
		seenMu sync.Mutex
		seen   []any
	)

	record := func(_ context.Context, value any) error {
		seenMu.Lock()
		defer seenMu.Unlock()

		seen = append(seen, value)

		return nil
	}

	if err := c.Register("ns", "k", []any{"https"}, WithContextValidator(record)); err != nil {
		t.Fatalf("register: %v", err)
	}

	fromStore := []any{"https", "wss"}
	seedEntry(t, m, "ns", "k", fromStore)

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	hydrated, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("get after hydrate: value=%v ok=%v err=%v", hydrated, ok, err)
	}

	if !reflect.DeepEqual(hydrated, fromStore) {
		t.Fatalf("hydrated value = %#v, want %#v", hydrated, fromStore)
	}

	fromFeed := []any{"https"}
	seedEntry(t, m, "ns", "k", fromFeed)
	m.fire(store.Event{Namespace: "ns", Key: "k", Op: store.OpUpsert})

	deadline := time.After(2 * time.Second)

	for {
		refreshed, ok, err := c.Get(context.Background(), "ns", "k")
		if err != nil || !ok {
			t.Fatalf("get after refresh: value=%v ok=%v err=%v", refreshed, ok, err)
		}

		if reflect.DeepEqual(refreshed, fromFeed) {
			break
		}

		select {
		case <-deadline:
			t.Fatalf("refreshed value = %#v, want %#v", refreshed, fromFeed)
		case <-time.After(10 * time.Millisecond):
		}
	}

	seenMu.Lock()
	defer seenMu.Unlock()

	// Register (the default), hydrate, refresh — in that order.
	want := []any{[]any{"https"}, fromStore, fromFeed}
	if !reflect.DeepEqual(seen, want) {
		t.Errorf("validator saw %#v, want %#v", seen, want)
	}
}
