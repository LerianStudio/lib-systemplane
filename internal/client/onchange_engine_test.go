//go:build unit

package client

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// seedEntryAt writes a row carrying an explicit revision, the way a real
// backend does: FC-2 stamps every stored row with the revision the write was
// assigned, and a subscriber is entitled to see it.
func seedEntryAt(t *testing.T, m *memStore, ns, key string, value any, revision int64) {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal seed: %v", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries[memKey(ns, key)] = store.Entry{
		Namespace: ns,
		Key:       key,
		Value:     raw,
		Revision:  revision,
		UpdatedAt: time.Now(),
		UpdatedBy: "seed",
	}

	if revision > m.revision {
		m.revision = revision
	}
}

// changeRecorder collects deliveries from a subscriber running on the engine's
// dispatch goroutine, so the test goroutine can read them under a lock.
type changeRecorder struct {
	mu      sync.Mutex
	changes []Change
}

func (r *changeRecorder) record(_ context.Context, ch Change) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.changes = append(r.changes, ch)
}

func (r *changeRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.changes)
}

func (r *changeRecorder) all() []Change {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]Change(nil), r.changes...)
}

// stableAt waits for the recorder to reach n deliveries and then proves no
// further one arrives within a bounded window, which is how "exactly once" is
// asserted against an asynchronous dispatcher.
func (r *changeRecorder) stableAt(t *testing.T, n int, msg string) []Change {
	t.Helper()

	waitFor(t, func() bool { return r.count() >= n }, msg)

	time.Sleep(50 * time.Millisecond)

	got := r.all()
	if len(got) != n {
		t.Fatalf("%s: got %d deliveries %v, want exactly %d", msg, len(got), got, n)
	}

	return got
}

// TestOnChangeRegisteredBeforeStartReceivesTheStoredValueAtStart pins FC-11 at
// the Client boundary: a subscriber that exists before Start is told what is
// in force once Start has reconciled, even though nothing changed afterwards.
// Before the engine cutover the Client announced nothing at Start, so a
// consumer that wired its reload in OnChange ran the whole process on the
// registered default until the first write.
func TestOnChangeRegisteredBeforeStartReceivesTheStoredValueAtStart(t *testing.T) {
	s := newMemStore(false)
	seedEntryAt(t, s, "ns", "key", "stored", 7)

	c := newSingleTenantClient(t, s)
	defer func() { _ = c.Close() }()

	if err := c.Register("ns", "key", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	rec := &changeRecorder{}

	unsub, err := c.OnChange("ns", "key", rec.record)
	if err != nil {
		t.Fatalf("OnChange: %v", err)
	}

	defer unsub()

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	got := rec.stableAt(t, 1, "subscriber registered before Start was never told the value in force")

	if got[0].Value != "stored" {
		t.Errorf("value: got %v, want %q — the announcement must carry the stored row, not the default", got[0].Value, "stored")
	}

	if got[0].Revision != 7 {
		t.Errorf("revision: got %d, want 7 — the announcement must carry the stored row's revision", got[0].Revision)
	}

	if got[0].Namespace != "ns" || got[0].Key != "key" {
		t.Errorf("identity: got %s/%s, want ns/key", got[0].Namespace, got[0].Key)
	}
}

// TestOnChangeRegisteredBeforeStartGetsTheDefaultForARejectedStoredRow is the
// rejection half of FC-11 and D1: a row the validator refuses does not silence
// the announcement, it changes what the announcement says. The subscriber is
// told the registered default at revision 0 — never the refused bytes — and
// one WARN names the key and the reason.
func TestOnChangeRegisteredBeforeStartGetsTheDefaultForARejectedStoredRow(t *testing.T) {
	s := newMemStore(false)
	seedEntryAt(t, s, "ns", "key", rejectedSecret, 9)

	logger := &recordingLogger{}
	c := newSingleTenantClientWithLogger(t, s, logger)

	defer func() { _ = c.Close() }()

	err := c.Register("ns", "key", "safe-default", WithValidator(func(v any) error {
		if v == rejectedSecret {
			return errSchemeRefused
		}

		return nil
	}))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	rec := &changeRecorder{}

	unsub, err := c.OnChange("ns", "key", rec.record)
	if err != nil {
		t.Fatalf("OnChange: %v", err)
	}

	defer unsub()

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	got := rec.stableAt(t, 1, "a rejected row must still announce the default, not go silent")

	if got[0].Value != "safe-default" {
		t.Errorf("value: got %v, want the registered default — a refused row never reaches a subscriber", got[0].Value)
	}

	if got[0].Revision != 0 {
		t.Errorf("revision: got %d, want 0 — nothing valid was accepted, so no revision is in force", got[0].Revision)
	}

	warns := logger.warns("stored value rejected by validator, keeping cached value")
	if len(warns) != 1 {
		t.Fatalf("rejection WARN: got %d lines, want 1", len(warns))
	}

	line := warns[0].String()
	for _, want := range []string{"ns", "key", errSchemeRefused.Error()} {
		if !strings.Contains(line, want) {
			t.Errorf("WARN %q does not name %q", line, want)
		}
	}

	if strings.Contains(logger.rendered(), rejectedSecret) {
		t.Errorf("the refused value leaked into the log: %s", logger.rendered())
	}
}

// TestSubscriberCanReadTheClientFromInsideItsCallback is the lock contract seen
// from a consumer's side: a subscriber typically reloads from the Client, so a
// callback must be able to read it. Deadlock here would hang Start itself.
func TestSubscriberCanReadTheClientFromInsideItsCallback(t *testing.T) {
	s := newMemStore(false)
	seedEntryAt(t, s, "ns", "key", "stored", 3)

	c := newSingleTenantClient(t, s)
	defer func() { _ = c.Close() }()

	if err := c.Register("ns", "key", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	type reading struct {
		change Change
		get    any
		entry  Entry
		err    error
	}

	var (
		mu       sync.Mutex
		readings []reading
	)

	unsub, err := c.OnChange("ns", "key", func(ctx context.Context, ch Change) {
		got, _, getErr := c.Get(ctx, "ns", "key")
		entry, _, entryErr := c.GetEntry(ctx, "ns", "key")

		mu.Lock()
		readings = append(readings, reading{change: ch, get: got, entry: entry, err: errors.Join(getErr, entryErr)})
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("OnChange: %v", err)
	}

	defer unsub()

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v — a callback reading the Client must not deadlock it", err)
	}

	if err := c.Set(context.Background(), "ns", "key", "written", "actor"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// The Start announcement and the write may coalesce into one delivery
	// (FC-4), so what is waited on is the newest revision arriving, not a
	// delivery count.
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		for _, r := range readings {
			if r.change.Value == "written" {
				return true
			}
		}

		return false
	}, "a callback reading the Client blocked: the write was never delivered")

	mu.Lock()
	got := append([]reading(nil), readings...)
	mu.Unlock()

	for i, r := range got {
		if r.err != nil {
			t.Errorf("delivery %d: reading the Client from inside the callback failed: %v", i, r.err)
		}

		// The read may legitimately run a beat behind or ahead of its own
		// notification — a newer revision can land while the callback is on
		// the stack — but it must never serve something OLDER than what the
		// subscriber was just told.
		if r.entry.Revision < r.change.Revision {
			t.Errorf("delivery %d: GetEntry returned revision %d while the Change carried %d — a subscriber must not read a value older than its own notification", i, r.entry.Revision, r.change.Revision)
		}

		// Get and GetEntry are two separate reads, so a publication landing
		// between them legitimately makes them differ — asserting they agree
		// was unsound and failed about one run in twenty under -race. What
		// must hold is that Get served a value this key actually held around
		// this delivery: the one the subscriber was told, or the one the
		// second read went on to see.
		if r.get != r.change.Value && r.get != r.entry.Value {
			t.Errorf("delivery %d: Get returned %v, which is neither the delivered value %v nor what GetEntry read a moment later (%v)", i, r.get, r.change.Value, r.entry.Value)
		}
	}
}

// TestGetEntryBeforeStartReportsStale pins FC-5 on the Client's own reads: a
// value nobody has reconciled yet is the registered default and says so, so a
// consumer reading during boot can tell "default because nothing is stored"
// apart from "default because we have not looked yet".
func TestGetEntryBeforeStartReportsStale(t *testing.T) {
	s := newMemStore(false)

	c := newSingleTenantClient(t, s)
	defer func() { _ = c.Close() }()

	if err := c.Register("ns", "key", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	entry, ok, err := c.GetEntry(context.Background(), "ns", "key")
	if err != nil || !ok {
		t.Fatalf("GetEntry before Start: got (%v, %v), want the registered default", ok, err)
	}

	if entry.Value != "default" {
		t.Errorf("value before Start: got %v, want the registered default", entry.Value)
	}

	if !entry.Stale {
		t.Error("Stale before Start: got false, want true — nothing has been reconciled yet")
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	entry, ok, err = c.GetEntry(context.Background(), "ns", "key")
	if err != nil || !ok {
		t.Fatalf("GetEntry after Start: got (%v, %v)", ok, err)
	}

	if entry.Stale {
		t.Error("Stale after Start: got true, want false — the scope reconciled")
	}
}

// TestStartRetriesAfterAFailedFirstReconcile covers a store that blinks while
// the process boots. The failure is reported, but it must not brick the
// Client: a second Start reconciles for real, reads serve stored rows, and a
// subscriber registered before the failed attempt is announced to — losing it
// would mean a consumer whose database was slow to accept connections runs
// forever on defaults with no way back short of a restart.
func TestStartRetriesAfterAFailedFirstReconcile(t *testing.T) {
	s := newMemStore(false)
	seedEntryAt(t, s, "ns", "key", "stored", 4)

	c := newSingleTenantClient(t, s)
	defer func() { _ = c.Close() }()

	if err := c.Register("ns", "key", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	rec := &changeRecorder{}

	unsub, err := c.OnChange("ns", "key", rec.record)
	if err != nil {
		t.Fatalf("OnChange: %v", err)
	}

	defer unsub()

	boom := errors.New("connection refused")
	s.failListOnce(boom)

	if err := c.Start(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("first Start: got %v, want the store failure wrapped", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("second Start: got %v, want nil — a store that blinked once must not brick the Client", err)
	}

	got, ok, err := c.Get(context.Background(), "ns", "key")
	if err != nil || !ok {
		t.Fatalf("Get after the retry: got (%v, %v)", ok, err)
	}

	if got != "stored" {
		t.Errorf("value after the retry: got %v, want the stored row — the retry must actually reconcile", got)
	}

	changes := rec.stableAt(t, 1, "a subscriber registered before the failed Start was never announced to by the retry")

	if changes[0].Value != "stored" || changes[0].Revision != 4 {
		t.Errorf("announcement after the retry: got %v@%d, want stored@4", changes[0].Value, changes[0].Revision)
	}
}

// TestStartRetriesAfterContextExpiry is the same contract driven by the other
// failure a slow boot produces: Start's deadline expires before the changefeed
// announces itself. The subscription is already live, so the Client must stay
// usable and the Start that follows the connection must succeed.
func TestStartRetriesAfterContextExpiry(t *testing.T) {
	s := newMemStore(false)
	seedEntryAt(t, s, "ns", "key", "stored", 2)
	s.staySilent()

	c := newSingleTenantClient(t, s)
	defer func() { _ = c.Close() }()

	if err := c.Register("ns", "key", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := c.Start(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Start: got %v, want the deadline", err)
	}

	// The changefeed finally announces itself, on the subscription the first
	// Start already registered.
	s.fire(store.Event{Op: store.OpResync})

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("second Start: got %v, want nil — a boot that timed out must be retryable", err)
	}

	got, ok, err := c.Get(context.Background(), "ns", "key")
	if err != nil || !ok || got != "stored" {
		t.Fatalf("Get after the retry: got (%v, %v, %v), want the stored row", got, ok, err)
	}
}

// TestSetValidatesTheCanonicalDecodedShape pins what a validator is graded
// against on the write path. A value makes the round trip through JSON before
// anything else sees it, so the validator must judge what will come back out
// of the store — otherwise a write accepted today is refused by its own
// validator the moment the process restarts and reads its own row.
func TestSetValidatesTheCanonicalDecodedShape(t *testing.T) {
	t.Run("a validator written for the decoded shape accepts the write", func(t *testing.T) {
		s := newMemStore(false)
		c := newSingleTenantClient(t, s)

		defer func() { _ = c.Close() }()

		err := c.Register("ns", "key", 0.0, WithValidator(func(v any) error {
			if _, ok := v.(float64); !ok {
				return errors.New("want the decoded number")
			}

			return nil
		}))
		if err != nil {
			t.Fatalf("Register: %v", err)
		}

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if err := c.Set(context.Background(), "ns", "key", 5, "actor"); err != nil {
			t.Fatalf("Set: %v — an int written through JSON comes back a float64, which is what the validator must grade", err)
		}
	})

	t.Run("a validator written for the caller's Go type refuses the write", func(t *testing.T) {
		s := newMemStore(false)
		c := newSingleTenantClient(t, s)

		defer func() { _ = c.Close() }()

		err := c.Register("ns", "key", 0, WithValidator(func(v any) error {
			if _, ok := v.(int); !ok {
				return errors.New("want an int")
			}

			return nil
		}))
		if err != nil {
			t.Fatalf("Register: %v", err)
		}

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if err := c.Set(context.Background(), "ns", "key", 5, "actor"); !errors.Is(err, ErrValidation) {
			t.Fatalf("Set: got %v, want ErrValidation — a validator that cannot grade the stored shape must fail at the write, not after the next restart", err)
		}

		s.mu.Lock()
		_, stored := s.entries[memKey("ns", "key")]
		s.mu.Unlock()

		if stored {
			t.Error("a refused write reached the store")
		}
	})
}
