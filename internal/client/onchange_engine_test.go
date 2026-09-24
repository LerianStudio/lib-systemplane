//go:build unit

package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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
			// The registered default is graded in canonical shape too, so it
			// reaches this validator a float64 and has to be let through: what
			// the validator is written to refuse is the caller's own write.
			if v == float64(0) {
				return nil
			}

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

// writerMarkKey marks a context as the one a caller handed to Set, so a
// validator can tell a write it is grading from a value the engine read back.
type writerMarkKey struct{}

// TestSetGradesALocalWriteExactlyOnce pins where a local write is judged: at
// Set, in canonical shape, under the caller's own context, once.
//
// The engine's ingress used to grade the same value a second time as the
// Client published it. A validator that answered differently on that second
// call — one consulting a system that had moved on, one counting calls — left
// the row written, the publication dropped and Set reporting success, so the
// caller was told its write landed while the next Get still served the
// previous value.
func TestSetGradesALocalWriteExactlyOnce(t *testing.T) {
	s := newMemStore(false)
	// A real quiet window, so the changefeed echo the fake fires inside
	// store.Set cannot do the write path's job for it.
	c := newSingleTenantClientWithDebounce(t, s, 50*time.Millisecond)

	defer func() { _ = c.Close() }()

	var writerGradings atomic.Int32

	err := c.Register("ns", "key", "default", WithContextValidator(func(ctx context.Context, _ any) error {
		if ctx.Value(writerMarkKey{}) == nil {
			// A read-back grading (Register, the reconcile, a re-read): not
			// what this test counts.
			return nil
		}

		if writerGradings.Add(1) > 1 {
			return errors.New("the same write was graded a second time")
		}

		return nil
	}))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx := context.WithValue(context.Background(), writerMarkKey{}, true)

	if err := c.Set(ctx, "ns", "key", "written", "actor"); err != nil {
		t.Fatalf("Set: %v, want nil", err)
	}

	if got := writerGradings.Load(); got != 1 {
		t.Errorf("the write path graded the value %d times, want 1", got)
	}

	got, ok, err := c.Get(ctx, "ns", "key")
	if err != nil || !ok || got != "written" {
		t.Fatalf("Get after Set: got (%v, %v, %v), want the value Set reported as written", got, ok, err)
	}
}

// TestSetAfterCloseReportsItClosed pins that a write can never be reported as
// landed once nothing is left to publish it.
func TestSetAfterCloseReportsItClosed(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("ns", "key", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := c.Set(context.Background(), "ns", "key", "written", "actor"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set after Close: got %v, want ErrClosed", err)
	}
}

// TestRegisterGradesTheDefaultInCanonicalShape pins the registered default to
// the same shape every other ingress grades: what the store hands back.
//
// Register used to grade the caller's raw Go value, so a validator written for
// the canonical shape — the only shape a stored row ever arrives in — refused
// the very default it was registered with, and one written for the Go type
// passed registration and then refused every read-back of its own key.
func TestRegisterGradesTheDefaultInCanonicalShape(t *testing.T) {
	t.Run("a validator written for the decoded shape accepts an int default", func(t *testing.T) {
		c := newSingleTenantClient(t, newMemStore(false))

		defer func() { _ = c.Close() }()

		err := c.Register("ns", "key", 5, WithValidator(func(v any) error {
			if _, ok := v.(float64); !ok {
				return fmt.Errorf("want the decoded number, got %T", v)
			}

			return nil
		}))
		if err != nil {
			t.Fatalf("Register: %v, want nil — an int default comes back from the store a float64", err)
		}
	})

	t.Run("a validator written for the caller's Go type refuses the default", func(t *testing.T) {
		c := newSingleTenantClient(t, newMemStore(false))

		defer func() { _ = c.Close() }()

		err := c.Register("ns", "key", 5, WithValidator(func(v any) error {
			if _, ok := v.(int); !ok {
				return fmt.Errorf("want an int, got %T", v)
			}

			return nil
		}))
		if !errors.Is(err, ErrValidation) {
			t.Fatalf("Register: got %v, want ErrValidation", err)
		}

		if !strings.Contains(err.Error(), "default value") {
			t.Errorf("Register error does not name the default: %v", err)
		}
	})
}

// TestSetFromTheFirstDeliveryLands pins the window FC-11 opened: the first
// reconcile announces every registered key while Start is still on the stack,
// so a subscriber's callback can run BEFORE Start returns. A callback that
// answers that announcement by writing — the ordinary "reconcile my derived
// state" shape — must have its write land rather than be refused for a Client
// that is, from the consumer's point of view, already running.
//
// The two keys are the clock. The fake's List is sorted, so the reconcile
// publishes "a" (queuing its delivery) and only then grades "z", whose
// validator blocks until that delivery has finished. Start is therefore
// provably still inside its first reconcile while the callback writes.
func TestSetFromTheFirstDeliveryLands(t *testing.T) {
	s := newMemStore(false)
	seedEntryAt(t, s, "ns", "a", "stored", 3)
	seedEntryAt(t, s, "ns", "z", "gate", 4)

	c := newSingleTenantClient(t, s)
	defer func() { _ = c.Close() }()

	delivered := make(chan struct{})

	var graded atomic.Int32

	// Call one is Register grading the default; call two is the reconcile
	// grading the stored row, which is where Start gets held.
	gate := func(_ context.Context, _ any) error {
		if graded.Add(1) == 1 {
			return nil
		}

		select {
		case <-delivered:
		case <-time.After(2 * time.Second):
			t.Error("the first delivery never reached the subscriber")
		}

		return nil
	}

	if err := c.Register("ns", "a", "default"); err != nil {
		t.Fatalf("Register a: %v", err)
	}

	if err := c.Register("ns", "z", "default", WithContextValidator(gate)); err != nil {
		t.Fatalf("Register z: %v", err)
	}

	setErr := make(chan error, 1)

	var once sync.Once

	unsub, err := c.OnChange("ns", "a", func(_ context.Context, _ Change) {
		once.Do(func() {
			setErr <- c.Set(context.Background(), "ns", "a", "written-by-the-subscriber", "cb")
			close(delivered)
		})
	})
	if err != nil {
		t.Fatalf("OnChange: %v", err)
	}

	defer unsub()

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	select {
	case err := <-setErr:
		if err != nil {
			t.Fatalf("Set from the first delivery: %v — the write was refused while Start was still reconciling", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the subscriber never ran")
	}

	got, ok, err := c.Get(context.Background(), "ns", "a")
	if err != nil || !ok {
		t.Fatalf("Get after Start: (%v, %v, %v)", got, ok, err)
	}

	if got != "written-by-the-subscriber" {
		t.Errorf("Get after Start = %v, want the value the subscriber wrote", got)
	}
}

// TestSetReportsAnEngineClosedUnderTheWrite is one half of "Set surfaces every
// refusal the publication can still make".
//
// Set reads the closed flag once, on the way in, and Close runs under a lock
// Set never takes, so a Close can land between that guard and the publication.
// The row is persisted by then and nothing in this process will ever serve it,
// which is the one outcome a nil return must never describe.
func TestSetReportsAnEngineClosedUnderTheWrite(t *testing.T) {
	s := newMemStore(false)
	c := newSingleTenantClient(t, s)

	defer func() { _ = c.Close() }()

	if err := c.Register("ns", "key", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Exactly the state that race leaves behind: the Client's own guards see
	// nothing wrong — c.closed is false, c.started is true — and the engine
	// can no longer publish anything.
	if err := c.engine.Close(); err != nil {
		t.Fatalf("closing the engine under the Client: %v", err)
	}

	if err := c.Set(context.Background(), "ns", "key", "written", "actor"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set: got %v, want ErrClosed", err)
	}

	s.mu.Lock()
	_, stored := s.entries[memKey("ns", "key")]
	s.mu.Unlock()

	if !stored {
		t.Error("the row never reached the store, so this test is pinning a refusal that happened " +
			"before the write rather than after it")
	}
}

// TestSetReportsAPublicationTheEngineRefused is the other half, and the one no
// sentinel covers: the engine tracks no scope to publish into, so the write
// lands in the store and is served by nothing.
//
// The window is real rather than contrived. Start flips the started flag
// BEFORE it brings the engine up — FC-11 announces every key while Start is
// still on the stack, and a subscriber that answers by writing must not be
// refused — and a Start that retries a scope whose first reconcile failed
// drops that scope before rebuilding it. A write arriving in between is
// exactly the case this branch exists for.
func TestSetReportsAPublicationTheEngineRefused(t *testing.T) {
	s := newMemStore(false)
	c := newSingleTenantClient(t, s)

	defer func() { _ = c.Close() }()

	if err := c.Register("ns", "key", "default"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// The scope comes up and its first reconcile fails, which is what makes
	// the next Start drop it.
	s.failListOnce(errors.New("list failed"))

	if err := c.Start(context.Background()); err == nil {
		t.Fatal("Start: got nil, want the first reconcile's failure")
	}

	var (
		fired    bool
		setErr   error
		readBack any
	)

	s.mu.Lock()
	s.unsubHook = func() {
		fired = true
		setErr = c.Set(context.Background(), "ns", "key", "written", "actor")
		readBack, _, _ = c.Get(context.Background(), "ns", "key")
	}
	s.mu.Unlock()

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("second Start: %v", err)
	}

	if !fired {
		t.Fatal("the second Start never dropped the failed scope, so no write met an untracked engine")
	}

	if setErr == nil {
		t.Fatal("Set: got nil for a write nothing published")
	}

	if errors.Is(setErr, ErrClosed) {
		t.Errorf("Set: got ErrClosed, want the refusal named: %v", setErr)
	}

	if !strings.Contains(setErr.Error(), "ns/key was written but not published") {
		t.Errorf("Set error does not name the key that was written but not published: %v", setErr)
	}

	if cause := errors.Unwrap(setErr); cause == nil || !strings.Contains(cause.Error(), "does not track") {
		t.Errorf("Set error does not wrap the engine's own reason: %v", setErr)
	}

	if readBack != "default" {
		t.Errorf("the read taken right after that Set served %v, want the registered default: a write "+
			"reported as refused must not also be readable", readBack)
	}

	s.mu.Lock()
	_, stored := s.entries[memKey("ns", "key")]
	s.mu.Unlock()

	if !stored {
		t.Error("the row never reached the store, so this test is pinning the wrong refusal")
	}
}

// TestRegisteredDefaultServesOneGoTypeOnly pins FC-5's shape rule end to end:
// one key answers with ONE Go type, whether a row exists or not.
//
// Register used to cache and announce the caller's raw Go value while every
// other ingress served what the store hands back, so a key registered with an
// int was announced as int at boot, read as float64 after a Set, and int again
// after a delete. A subscriber that type-asserts the shape its own validator
// was told to expect panicked on the boot announcement; the panic is recovered
// and counted, the delivery dropped, and the service boots on a configuration
// it never applied, with no error reaching it.
func TestRegisteredDefaultServesOneGoTypeOnly(t *testing.T) {
	s := newMemStore(false)
	c := newSingleTenantClientWithDebounce(t, s, 50*time.Millisecond)

	defer func() { _ = c.Close() }()

	if err := c.Register("ns", "num", 5); err != nil {
		t.Fatalf("Register: %v", err)
	}

	announced := make(chan any, 8)

	unsub, err := c.OnChange("ns", "num", func(_ context.Context, ch Change) { announced <- ch.Value })
	if err != nil {
		t.Fatalf("OnChange: %v", err)
	}

	defer unsub()

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// FC-11: the first reconcile announces the key to a subscriber registered
	// before Start, and this is the delivery a consumer's reload runs on.
	boot := receiveValue(t, announced, "the boot announcement")
	if boot != 5.0 {
		t.Errorf("the boot announcement carries %v of type %T, want the decoded 5: a callback written "+
			"for the shape the validator grades panics on it", boot, boot)
	}

	got, _, err := c.Get(context.Background(), "ns", "num")
	if err != nil || got != 5.0 {
		t.Errorf("Get with the default in force: got %v of type %T (err %v), want the decoded 5", got, got, err)
	}

	if err := c.Set(context.Background(), "ns", "num", 7, "actor"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, _, err = c.Get(context.Background(), "ns", "num")
	if err != nil || got != 7.0 {
		t.Errorf("Get with a row in force: got %v of type %T (err %v), want the decoded 7", got, got, err)
	}

	if err := c.Delete(context.Background(), "ns", "num", "actor"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got, _, err = c.Get(context.Background(), "ns", "num")
	if err != nil || got != 5.0 {
		t.Errorf("Get with the default back in force: got %v of type %T (err %v), want the decoded 5",
			got, got, err)
	}
}

// receiveValue takes the next delivery or fails, so a missing announcement
// reads as the assertion it is rather than as a hung test.
func receiveValue(t *testing.T, ch <-chan any, what string) any {
	t.Helper()

	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)

		return nil
	}
}

// TestAPanickingValidatorIsRefusedNotFatal pins the two validator call sites
// the Client owns.
//
// v4 grades the canonical shape everywhere, so a validator written against the
// caller's Go type — `v.(int)` with an int default, the shape that passed on
// v3 — now meets a float64. Unrecovered, that assertion took the consumer's
// process down at Register, which is boot. Every ingress inside the engine
// already turns a validator panic into a rejection; these two were the only
// ones that did not.
func TestAPanickingValidatorIsRefusedNotFatal(t *testing.T) {
	t.Run("grading the registered default", func(t *testing.T) {
		c := newSingleTenantClient(t, newMemStore(false))

		defer func() { _ = c.Close() }()

		err := c.Register("ns", "key", 5, WithValidator(func(v any) error {
			_ = v.(int)

			return nil
		}))
		if !errors.Is(err, ErrValidation) {
			t.Fatalf("Register: got %v, want ErrValidation", err)
		}
	})

	t.Run("grading a local write", func(t *testing.T) {
		s := newMemStore(false)
		c := newSingleTenantClient(t, s)

		defer func() { _ = c.Close() }()

		var explode atomic.Bool

		err := c.Register("ns", "key", 5, WithValidator(func(any) error {
			if explode.Load() {
				panic("validator blew up")
			}

			return nil
		}))
		if err != nil {
			t.Fatalf("Register: %v", err)
		}

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		explode.Store(true)

		if err := c.Set(context.Background(), "ns", "key", 7, "actor"); !errors.Is(err, ErrValidation) {
			t.Fatalf("Set: got %v, want ErrValidation", err)
		}

		s.mu.Lock()
		_, stored := s.entries[memKey("ns", "key")]
		s.mu.Unlock()

		if stored {
			t.Error("a write whose validator panicked was persisted anyway")
		}
	})
}
