//go:build unit

package client

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/constants"
	"github.com/LerianStudio/lib-observability/log"
	"github.com/LerianStudio/lib-systemplane/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypedCoercion_Durations(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	// Case 1: Native time.Duration default.
	if err := c.Register("ns", "timeout", 5*time.Second); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	fs.waitForSubscriber(t, time.Second)

	d := c.GetDuration("ns", "timeout")
	if d != 5*time.Second {
		t.Fatalf("expected 5s from native Duration default, got %v", d)
	}

	// Case 2: Set a JSON string duration. After Set, the cached value is a
	// string (because Set stores the Go value, not the JSON-decoded form).
	if err := c.Set(context.Background(), "ns", "timeout", "10s", "ops"); err != nil {
		t.Fatalf("set string duration: %v", err)
	}

	d = c.GetDuration("ns", "timeout")
	if d != 10*time.Second {
		t.Fatalf("expected 10s from string '10s', got %v", d)
	}

	// Case 3: Simulate a changefeed delivering a nanosecond number (JSON float64).
	// json.Unmarshal decodes numbers as float64.
	fs.simulateExternalChange("ns", "timeout", float64(3*time.Second))

	deadline := time.Now().Add(2 * time.Second)
	for c.GetDuration("ns", "timeout") != 3*time.Second {
		if time.Now().After(deadline) {
			t.Fatalf("timed out: expected 3s from float64 nanoseconds, got %v", c.GetDuration("ns", "timeout"))
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func TestStart_IsIdempotent(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	// First Start.
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("first start: %v", err)
	}

	// Second Start — should return nil silently (not ErrAlreadyStarted).
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("second start: expected nil, got %v", err)
	}
}

func TestStart_RetryAfterTransientFailure(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "default-val"); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Inject a transient error so the first Start fails.
	fs.mu.Lock()
	fs.listErr = errors.New("transient DB error")
	fs.mu.Unlock()

	if err := c.Start(context.Background()); err == nil {
		t.Fatal("first start: expected error, got nil")
	}

	// Client must NOT be marked as started after a failed attempt.
	if c.started.Load() {
		t.Fatal("started should be false after failed Start")
	}

	// Clear the error so the retry succeeds.
	fs.mu.Lock()
	fs.listErr = nil
	fs.mu.Unlock()

	// Retry — must succeed now.
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("retry start: expected nil, got %v", err)
	}

	if !c.started.Load() {
		t.Fatal("started should be true after successful retry")
	}

	// Verify the default value was hydrated.
	got, ok := c.Get("ns", "k")
	if !ok {
		t.Fatal("expected key to be present after start")
	}

	if got != "default-val" {
		t.Fatalf("expected default-val, got %v", got)
	}
}

func TestSet_BeforeStartReturnsErrNotStarted(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Set before Start.
	err := c.Set(context.Background(), "ns", "k", "new", "ops")
	if !errors.Is(err, ErrNotStarted) {
		t.Fatalf("expected ErrNotStarted, got %v", err)
	}
}

func TestSet_NilContextReturnsErrNilContext(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	err := c.Set(nil, "ns", "k", "new", "ops")
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("expected ErrNilContext, got %v", err)
	}
}

func TestNilClient_CloseSafe(t *testing.T) {
	t.Parallel()

	var c *Client

	// Close on nil should not panic and return nil.
	if err := c.Close(); err != nil {
		t.Fatalf("Close on nil: expected nil, got %v", err)
	}
}

func TestNilClient_StartReturnsErrClosed(t *testing.T) {
	t.Parallel()

	var c *Client

	if err := c.Start(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start on nil: expected ErrClosed, got %v", err)
	}
}

func TestStart_NilContextReturnsErrNilContext(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Start(nil): expected ErrNilContext, got %v", err)
	}
}

func TestZeroValueClient_StartReturnsErrClosed(t *testing.T) {
	t.Parallel()

	c := &Client{}

	if err := c.Start(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("zero-value Start: expected ErrClosed, got %v", err)
	}
}

func TestZeroValueClient_CloseSafe(t *testing.T) {
	t.Parallel()

	c := &Client{}

	if err := c.Close(); err != nil {
		t.Fatalf("zero-value Close: expected nil, got %v", err)
	}
}

func TestNilClient_RegisterReturnsErrClosed(t *testing.T) {
	t.Parallel()

	var c *Client

	if err := c.Register("ns", "k", "v"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Register on nil: expected ErrClosed, got %v", err)
	}
}

func TestRegister_NilKeyOptionIgnored(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "v", nil); err != nil {
		t.Fatalf("Register with nil KeyOption: %v", err)
	}
}

func TestNilClient_SetReturnsErrClosed(t *testing.T) {
	t.Parallel()

	var c *Client

	if err := c.Set(context.Background(), "ns", "k", "v", "ops"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set on nil: expected ErrClosed, got %v", err)
	}
}

func TestNilClient_GetReturnsFalse(t *testing.T) {
	t.Parallel()

	var c *Client

	v, ok := c.Get("ns", "k")
	if ok || v != nil {
		t.Fatalf("Get on nil: expected (nil, false), got (%v, %v)", v, ok)
	}
}

func TestClosedClient_ReadsReturnZeroValues(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "v", WithDescription("desc"), WithRedaction(RedactMask)); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	v, ok := c.Get("ns", "k")
	if ok || v != nil {
		t.Fatalf("closed Get: expected (nil, false), got (%v, %v)", v, ok)
	}

	if entries := c.List("ns"); entries != nil {
		t.Fatalf("closed List: expected nil, got %#v", entries)
	}

	if desc := c.KeyDescription("ns", "k"); desc != "" {
		t.Fatalf("closed KeyDescription: expected empty, got %q", desc)
	}

	if redaction := c.KeyRedaction("ns", "k"); redaction != RedactNone {
		t.Fatalf("closed KeyRedaction: expected RedactNone, got %v", redaction)
	}

	registered, tenantScoped := c.KeyStatus("ns", "k")
	if registered || tenantScoped {
		t.Fatalf("closed KeyStatus: expected false,false; got %v,%v", registered, tenantScoped)
	}
}

func TestNilClient_OnChangeReturnsNoop(t *testing.T) {
	t.Parallel()

	var c *Client

	unsub := c.OnChange("ns", "k", func(_ any) {})
	unsub() // should not panic
}

func TestOptions_ApplyCorrectly(t *testing.T) {
	t.Parallel()

	cfg := defaultClientConfig()

	// Verify defaults.
	if cfg.listenChannel != "systemplane_changes" {
		t.Fatalf("default listenChannel: expected 'systemplane_changes', got %q", cfg.listenChannel)
	}

	if cfg.debounce != 100*time.Millisecond {
		t.Fatalf("default debounce: expected 100ms, got %v", cfg.debounce)
	}

	if cfg.collection != "systemplane_entries" {
		t.Fatalf("default collection: expected 'systemplane_entries', got %q", cfg.collection)
	}

	if cfg.table != "systemplane_entries" {
		t.Fatalf("default table: expected 'systemplane_entries', got %q", cfg.table)
	}

	// Apply options.
	WithListenChannel("custom_ch")(&cfg)
	WithDebounce(200 * time.Millisecond)(&cfg)
	WithCollection("custom_coll")(&cfg)
	WithTable("custom_tbl")(&cfg)
	WithPollInterval(5 * time.Second)(&cfg)
	WithLogger(log.NewNop())(&cfg)

	if cfg.listenChannel != "custom_ch" {
		t.Fatalf("expected 'custom_ch', got %q", cfg.listenChannel)
	}

	if cfg.debounce != 200*time.Millisecond {
		t.Fatalf("expected 200ms, got %v", cfg.debounce)
	}

	if cfg.collection != "custom_coll" {
		t.Fatalf("expected 'custom_coll', got %q", cfg.collection)
	}

	if cfg.table != "custom_tbl" {
		t.Fatalf("expected 'custom_tbl', got %q", cfg.table)
	}

	if cfg.pollInterval != 5*time.Second {
		t.Fatalf("expected 5s, got %v", cfg.pollInterval)
	}
}

func TestNewForTesting_NilOptionIgnored(t *testing.T) {
	t.Parallel()

	fs := newFakeTenantTestStore()
	c, err := NewForTesting(fs, nil)
	if err != nil {
		t.Fatalf("NewForTesting with nil Option: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })
}

func TestRedaction(t *testing.T) {
	t.Parallel()

	if v := ApplyRedaction("secret", RedactNone); v != "secret" {
		t.Fatalf("RedactNone: expected 'secret', got %v", v)
	}

	if v := ApplyRedaction("secret", RedactMask); v != constants.ObfuscatedValue {
		t.Fatalf("RedactMask: expected %q, got %v", constants.ObfuscatedValue, v)
	}

	if v := ApplyRedaction("secret", RedactFull); v != constants.ObfuscatedValue {
		t.Fatalf("RedactFull: expected %q, got %v", constants.ObfuscatedValue, v)
	}
}

func TestKeyOptions(t *testing.T) {
	t.Parallel()

	def := keyDef{redaction: RedactNone}

	WithDescription("a knob")(&def)
	WithRedaction(RedactMask)(&def)

	if def.description != "a knob" {
		t.Fatalf("expected description 'a knob', got %q", def.description)
	}

	if def.redaction != RedactMask {
		t.Fatalf("expected RedactMask, got %v", def.redaction)
	}
}

func TestMultipleSubscribers_AllFire(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "v"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	fs.waitForSubscriber(t, time.Second)

	var count atomic.Int32

	for range 5 {
		c.OnChange("ns", "k", func(_ any) {
			count.Add(1)
		})
	}

	fs.simulateExternalChange("ns", "k", "new")

	deadline := time.Now().Add(2 * time.Second)
	for count.Load() != 5 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out: expected 5 subscribers to fire, got %d", count.Load())
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func TestGetString_HappyPath(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "name", "alice"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	if got := c.GetString("ns", "name"); got != "alice" {
		t.Fatalf("expected 'alice', got %q", got)
	}

	// Test GetString with unregistered key.
	if got := c.GetString("ns", "nope"); got != "" {
		t.Fatalf("expected '' for unregistered key, got %q", got)
	}
}

func TestGetInt_HappyPath(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "retries", 5); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	if got := c.GetInt("ns", "retries"); got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}

	// Unregistered returns 0.
	if got := c.GetInt("ns", "nope"); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestGetInt_Float64Coercion(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	// When hydrated from JSON, numbers arrive as float64.
	if err := c.Register("ns", "retries", 3); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Pre-populate store with a JSON number (which json.Unmarshal decodes as float64).
	jsonBytes, err := json.Marshal(7)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	fs.entries[nskey{Namespace: "ns", Key: "retries"}] = store.Entry{
		Namespace: "ns",
		Key:       "retries",
		Value:     jsonBytes,
		UpdatedAt: time.Now(),
		UpdatedBy: "seed",
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	got := c.GetInt("ns", "retries")
	if got != 7 {
		t.Fatalf("expected 7 (via float64 coercion), got %d", got)
	}
}

func TestGetInt_WrongTypeReturnsZero(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "not-a-number"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	if got := c.GetInt("ns", "k"); got != 0 {
		t.Fatalf("expected 0 for string value, got %d", got)
	}
}

func TestGetBool_HappyPath(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "enabled", true); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	if got := c.GetBool("ns", "enabled"); !got {
		t.Fatal("expected true, got false")
	}

	if got := c.GetBool("ns", "nope"); got {
		t.Fatal("expected false for unregistered key, got true")
	}
}

func TestGetFloat64_HappyPath(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "rate", 0.75); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	if got := c.GetFloat64("ns", "rate"); got != 0.75 {
		t.Fatalf("expected 0.75, got %f", got)
	}

	if got := c.GetFloat64("ns", "nope"); got != 0 {
		t.Fatalf("expected 0 for unregistered key, got %f", got)
	}
}

func TestGetDuration_StringParseFails(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "d", "not-a-duration"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	if got := c.GetDuration("ns", "d"); got != 0 {
		t.Fatalf("expected 0 for unparseable string, got %v", got)
	}
}

func TestGetDuration_WrongTypeReturnsZero(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "d", true); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	if got := c.GetDuration("ns", "d"); got != 0 {
		t.Fatalf("expected 0 for bool value, got %v", got)
	}
}

func TestRefreshFromStore_UnmarshalError(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	fs.waitForSubscriber(t, time.Second)

	// Inject a corrupt value directly into the fake store and trigger changefeed.
	fs.mu.Lock()
	nk := nskey{Namespace: "ns", Key: "k"}
	fs.entries[nk] = store.Entry{
		Namespace: "ns",
		Key:       "k",
		TenantID:  store.SentinelGlobal,
		Value:     []byte("{{invalid json}}"),
		UpdatedAt: time.Now(),
		UpdatedBy: "bad-actor",
	}
	handlers := make([]func(store.Event), len(fs.handlers))
	copy(handlers, fs.handlers)
	fs.mu.Unlock()

	evt := store.Event{Namespace: "ns", Key: "k", TenantID: store.SentinelGlobal}
	for _, h := range handlers {
		h(evt)
	}

	// Negative assertion: cache should remain unchanged because unmarshal failed.
	// Cannot poll for "nothing changed" — must wait a reasonable window and verify.
	time.Sleep(50 * time.Millisecond)
	v, ok := c.Get("ns", "k")
	if !ok {
		t.Fatal("expected ok=true")
	}

	if v != "default" {
		t.Fatalf("expected 'default' (unchanged after unmarshal error), got %v", v)
	}
}

func TestRefreshFromStore_KeyDeletedFallsBackToDefault(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()
	c := testClient(t, fs)

	if err := c.Register("ns", "k", "default-val"); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Pre-populate with a different value.
	jsonBytes, err := json.Marshal("override")
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	fs.entries[nskey{Namespace: "ns", Key: "k"}] = store.Entry{
		Namespace: "ns",
		Key:       "k",
		Value:     jsonBytes,
		UpdatedAt: time.Now(),
		UpdatedBy: "seed",
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	fs.waitForSubscriber(t, time.Second)

	// Verify the override is active.
	if v, _ := c.Get("ns", "k"); v != "override" {
		t.Fatalf("expected 'override', got %v", v)
	}

	// Now delete the entry from the fake store and simulate a changefeed event.
	// refreshFromStore will find (existed=false) and fall back to default.
	fs.mu.Lock()
	delete(fs.entries, nskey{Namespace: "ns", Key: "k"})
	handlers := make([]func(store.Event), len(fs.handlers))
	copy(handlers, fs.handlers)
	fs.mu.Unlock()

	evt := store.Event{Namespace: "ns", Key: "k", TenantID: store.SentinelGlobal}
	for _, h := range handlers {
		h(evt)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		v, ok := c.Get("ns", "k")
		if ok && v == "default-val" {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out: expected fallback to 'default-val', got (%v, ok=%v)", v, ok)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func TestStart_HydrateSkipsUnregisteredKeys(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()

	// Seed the store with an entry for an unregistered key.
	jsonBytes, err := json.Marshal("orphan")
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	fs.entries[nskey{Namespace: "ns", Key: "orphan"}] = store.Entry{
		Namespace: "ns",
		Key:       "orphan",
		Value:     jsonBytes,
		UpdatedAt: time.Now(),
		UpdatedBy: "seed",
	}

	c := testClient(t, fs)

	if err := c.Register("ns", "known", "val"); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Start should log a warning for the orphan but not error.
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Orphan should not be in cache.
	_, ok := c.Get("ns", "orphan")
	if ok {
		t.Fatal("expected orphan key to NOT be in cache")
	}

	// Known key should have its default.
	v, ok := c.Get("ns", "known")
	if !ok || v != "val" {
		t.Fatalf("expected 'val', got (%v, %v)", v, ok)
	}
}

func TestStart_HydrateSkipsBadJSON(t *testing.T) {
	t.Parallel()

	fs := newFakeStore()

	// Seed with a registered key but corrupt JSON.
	fs.entries[nskey{Namespace: "ns", Key: "k"}] = store.Entry{
		Namespace: "ns",
		Key:       "k",
		Value:     []byte("{{not json}}"),
		UpdatedAt: time.Now(),
		UpdatedBy: "seed",
	}

	c := testClient(t, fs)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Start should log a warning but not error; the default remains.
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	v, ok := c.Get("ns", "k")
	if !ok || v != "default" {
		t.Fatalf("expected default 'default' after bad JSON, got (%v, %v)", v, ok)
	}
}

func TestStart_ReplaysChangefeedEventsAfterHydration(t *testing.T) {
	t.Parallel()

	oldValue, err := json.Marshal("old")
	require.NoError(t, err)

	fs := newStartupRaceStore([]store.Entry{{
		Namespace: "global",
		Key:       "log.level",
		TenantID:  store.SentinelGlobal,
		Value:     oldValue,
		UpdatedAt: time.Now(),
		UpdatedBy: "snapshot",
	}})

	fs.entries[nskey{Namespace: "global", Key: "log.level"}] = store.Entry{
		Namespace: "global",
		Key:       "log.level",
		TenantID:  store.SentinelGlobal,
		Value:     oldValue,
		UpdatedAt: time.Now(),
		UpdatedBy: "current",
	}

	cfg := defaultClientConfig()
	cfg.debounce = 0
	cfg.logger = log.NewNop()

	c := newClient(fs, cfg)
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.Register("global", "log.level", "default"))

	startDone := make(chan error, 1)
	go func() {
		startDone <- c.Start(context.Background())
	}()

	select {
	case <-fs.listStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not reach hydration List")
	}

	fs.simulateExternalChange("global", "log.level", "new")
	close(fs.releaseList)

	select {
	case err := <-startDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not finish")
	}

	assert.Eventually(t, func() bool {
		v, ok := c.Get("global", "log.level")

		return ok && v == "new"
	}, 2*time.Second, 10*time.Millisecond, "buffered changefeed event must win over stale hydration snapshot")
}

func TestRegister_DuringStartReturnsErrRegisterAfterStart(t *testing.T) {
	t.Parallel()

	fs := newStartupRaceStore(nil)
	c := newClient(fs, defaultClientConfig())
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.Register("global", "known", "default"))

	startDone := make(chan error, 1)
	go func() {
		startDone <- c.Start(context.Background())
	}()

	select {
	case <-fs.listStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not reach hydration List")
	}

	registerDone := make(chan error, 1)
	go func() {
		registerDone <- c.Register("global", "late", "value")
	}()

	select {
	case err := <-registerDone:
		t.Fatalf("Register returned while Start was still in progress: %v", err)
	case <-time.After(50 * time.Millisecond):
		// Expected: Register is serialized behind Start.
	}

	close(fs.releaseList)

	select {
	case err := <-startDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not finish")
	}

	select {
	case err := <-registerDone:
		require.ErrorIs(t, err, ErrRegisterAfterStart)
	case <-time.After(2 * time.Second):
		t.Fatal("Register did not return after Start completed")
	}
}
