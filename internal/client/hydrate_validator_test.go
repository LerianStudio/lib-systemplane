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
	"github.com/LerianStudio/lib-observability/v4/runtime"
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
func (l *recordingLogger) warns(msg string) []logLine { return l.at(log.LevelWarn, msg) }

// errs returns the recorded ERROR lines carrying msg.
func (l *recordingLogger) errs(msg string) []logLine { return l.at(log.LevelError, msg) }

func (l *recordingLogger) at(level int, msg string) []logLine {
	l.mu.Lock()
	defer l.mu.Unlock()

	var out []logLine

	for _, line := range l.lines {
		if line.level == level && line.msg == msg {
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

// waitFor polls cond until it holds or the bound expires, so a test can wait on
// work the engine does on a goroutine of its own without sleeping for a fixed
// slice of time.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()

	deadline := time.After(2 * time.Second)

	for {
		if cond() {
			return
		}

		select {
		case <-deadline:
			t.Fatal(msg)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// productionMode turns lib-observability's production mode on for one test and
// restores it afterwards. Its recovery pipeline prints the recovered value
// otherwise, and the value a validator panics on is the configuration row —
// which is exactly what the rejection contract says must never be logged.
func productionMode(t *testing.T) {
	t.Helper()

	previous := runtime.IsProductionMode()
	runtime.SetProductionMode(true)

	t.Cleanup(func() { runtime.SetProductionMode(previous) })
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

	warns := logger.warns("stored value rejected by validator, keeping cached value")
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

		if n := len(logger.warns("stored value rejected by validator, keeping cached value")); n != 1 {
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

		listReady := make(chan struct{}, 1)
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

// readBackCtxKey marks a context so a validator can tell which context it was
// handed on a read-back path.
type readBackCtxKey struct{}

// TestStoredValueValidatorNeverSeesTheStartContext pins the v4 context
// contract, which reverses what v3 documented. Every read-back now runs on an
// engine-owned goroutine: the first reconcile and every later one validate with
// the engine's lifecycle context — no request values, no tenant, no deadline —
// and a changefeed re-read with a bounded context derived from it. The context
// a caller hands to Start reaches neither, so a validator that expects request
// scope must treat its absence as "cannot verify" rather than read it.
func TestStoredValueValidatorNeverSeesTheStartContext(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClientWithLogger(t, m, &recordingLogger{})

	type observation struct {
		carried  any
		deadline bool
	}

	var (
		seenMu sync.Mutex
		seen   = map[any]observation{}
	)

	validator := func(ctx context.Context, value any) error {
		_, hasDeadline := ctx.Deadline()

		seenMu.Lock()
		defer seenMu.Unlock()

		seen[value] = observation{carried: ctx.Value(readBackCtxKey{}), deadline: hasDeadline}

		return nil
	}

	if err := c.Register("ns", "k", "default", WithContextValidator(validator)); err != nil {
		t.Fatalf("register: %v", err)
	}

	seedEntry(t, m, "ns", "k", "from-list")

	if err := c.Start(context.WithValue(context.Background(), readBackCtxKey{}, "start-context")); err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	seedEntry(t, m, "ns", "k", "from-feed")
	m.fire(store.Event{Namespace: "ns", Key: "k", Op: store.OpUpsert})

	waitFor(t, func() bool {
		seenMu.Lock()
		defer seenMu.Unlock()

		_, ok := seen["from-feed"]

		return ok
	}, "the changefeed re-read never handed the refreshed value to the validator")

	seenMu.Lock()
	defer seenMu.Unlock()

	if got := seen["from-list"].carried; got != nil {
		t.Errorf("the first reconcile handed the validator a context carrying %v, want the engine's own context with no request values", got)
	}

	if seen["from-list"].deadline {
		t.Error("the first reconcile handed the validator a context with a deadline; the engine's lifecycle context has none")
	}

	if got := seen["from-feed"].carried; got != nil {
		t.Errorf("the changefeed re-read handed the validator a context carrying %v, want the engine's own context with no request values", got)
	}

	if !seen["from-feed"].deadline {
		t.Error("the changefeed re-read handed the validator a context with no deadline, want the bounded re-read context")
	}
}

// TestPanickingValidatorIsARefusal covers the row class the read-back grading
// exists for, met by a validator that is not defensive: a legacy row of the
// wrong shape makes a type-asserting validator panic. Hydration runs inside
// Start, so an unrecovered panic there would take down boot — the very deploy
// the fix was written to survive.
func TestPanickingValidatorIsARefusal(t *testing.T) {
	// The engine hands the panic to lib-observability's recovery pipeline,
	// which prints the panic VALUE unless production mode is on — and the
	// value a validator panics on is the configuration row itself. Production
	// mode is process-wide state, so these subtests never run in parallel.
	productionMode(t)

	t.Run("hydration keeps the default and does not abort start", func(t *testing.T) {
		m := newMemStore(false)
		logger := &recordingLogger{}
		c := newSingleTenantClientWithLogger(t, m, logger)

		// Panics with the value itself: no log line may reproduce its bytes.
		validator := func(_ context.Context, value any) error {
			if value == rejectedSecret {
				panic(value)
			}

			return nil
		}

		if err := c.Register("ns", "k", "default", WithContextValidator(validator)); err != nil {
			t.Fatalf("register: %v", err)
		}

		seedEntry(t, m, "ns", "k", rejectedSecret)

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}

		t.Cleanup(func() { _ = c.Close() })

		got, ok, err := c.Get(context.Background(), "ns", "k")
		if err != nil || !ok {
			t.Fatalf("get: value=%v ok=%v err=%v", got, ok, err)
		}

		if got != "default" {
			t.Errorf("value in force = %v, want the registered default", got)
		}

		if n := len(logger.warns("stored value rejected by validator, keeping cached value")); n != 1 {
			t.Errorf("got %d WARN lines for the panicking validator, want exactly 1", n)
		}

		if rendered := logger.rendered(); strings.Contains(rendered, rejectedSecret) {
			t.Errorf("a log line reproduced the panicked value's bytes: %q", rendered)
		}
	})

	t.Run("refresh keeps the value already in force", func(t *testing.T) {
		m := newMemStore(false)
		logger := &recordingLogger{}
		c := newSingleTenantClientWithLogger(t, m, logger)

		validator := func(_ context.Context, value any) error {
			if value == rejectedSecret {
				panic(errSchemeRefused)
			}

			return nil
		}

		if err := c.Register("ns", "k", "default", WithContextValidator(validator)); err != nil {
			t.Fatalf("register: %v", err)
		}

		seedEntry(t, m, "ns", "k", "known-good")

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}

		t.Cleanup(func() { _ = c.Close() })

		seedEntry(t, m, "ns", "k", rejectedSecret)
		m.fire(store.Event{Namespace: "ns", Key: "k", Op: store.OpUpsert})

		got, ok, err := c.Get(context.Background(), "ns", "k")
		if err != nil || !ok {
			t.Fatalf("get: value=%v ok=%v err=%v", got, ok, err)
		}

		if got != "known-good" {
			t.Errorf("value in force = %v, want the value that was already in force", got)
		}

		warns := logger.warns("stored value rejected by validator, keeping cached value")
		if len(warns) != 1 {
			t.Fatalf("got %d WARN lines for the panicking validator, want exactly 1", len(warns))
		}

		// What the validator panicked WITH stays out of this line: it is a
		// value the validator was handed, so the ingress reports only that it
		// panicked and leaves the value to the redacting recovery pipeline.
		if rendered := fmt.Sprintf("%v", warns[0]); !strings.Contains(rendered, "validator panicked") {
			t.Errorf("WARN line %q does not report that the validator panicked", rendered)
		}
	})
}

// TestHydrationYieldsToARefreshThatLandedDuringValidation closes the window the
// grading opened. hydrationTouched exists so a changefeed value that arrives
// mid-hydration is not overwritten by the older List snapshot; reading it
// before the validator runs and writing the cache after leaves that window
// open for as long as the validator takes, which is now consumer time.
func TestHydrationYieldsToARefreshThatLandedDuringValidation(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClientWithLogger(t, m, &recordingLogger{})

	hydrating := make(chan struct{})
	release := make(chan struct{})

	validator := func(_ context.Context, value any) error {
		if value == "old-from-list" {
			close(hydrating)
			<-release
		}

		return nil
	}

	if err := c.Register("ns", "k", "default", WithContextValidator(validator)); err != nil {
		t.Fatalf("register: %v", err)
	}

	seedEntry(t, m, "ns", "k", "old-from-list")

	startDone := make(chan error, 1)

	go func() {
		startDone <- c.Start(context.Background())
	}()

	<-hydrating

	// The whole refresh runs inline on this goroutine (the debounce window is
	// zero), so when fire returns the changefeed value is cached and the key is
	// claimed against hydration.
	seedEntry(t, m, "ns", "k", "new-from-feed")
	m.fire(store.Event{Namespace: "ns", Key: "k", Op: store.OpUpsert})

	close(release)

	if err := <-startDone; err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	got, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("get: value=%v ok=%v err=%v", got, ok, err)
	}

	if got != "new-from-feed" {
		t.Errorf("value in force = %v, want the changefeed value — the List snapshot overwrote a refresh that landed while the validator ran", got)
	}
}
