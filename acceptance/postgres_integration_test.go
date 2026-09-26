//go:build integration

package acceptance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	systemplane "github.com/LerianStudio/lib-systemplane/v4"
)

// newSTPostgres builds an unstarted single-tenant client over a fresh database,
// plus a foreign pool onto it that stands in for any writer but the library.
func newSTPostgres(t *testing.T, opts ...systemplane.Option) (*systemplane.Client, *sql.DB, *recordingLogger) {
	t.Helper()

	dsn, db := freshPostgres(t)

	foreign, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open foreign connection: %v", err)
	}

	t.Cleanup(func() { _ = foreign.Close() })

	logger := &recordingLogger{}

	client, err := systemplane.NewPostgres(db, dsn, append(opts, systemplane.WithLogger(logger))...)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}

	t.Cleanup(func() { _ = client.Close() })

	return client, foreign, logger
}

// Scenario 1: while the changefeed is down the scope serves the last value
// marked Stale, and the gap's final write lands after the reconnect, exactly
// once, with no second write: the reconcile closes the gap, not a NOTIFY.
func TestIntegration_Acceptance01_FeedLossPostgres(t *testing.T) {
	cases := []struct {
		name   string
		writes []string
	}{
		{"one gap write converges without a second write", []string{"during-gap"}},
		{"the last of two gap writes wins and the first is never observed", []string{"gap-first", "gap-second"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const key = "feed-loss"

			client, foreign, _ := newSTPostgres(t)
			mustRegister(t, client, key)

			sink := subscribe(t, client, key)
			mustStart(t, client)

			if initial := sink.next(t, 30*time.Second, "initial publication at Start"); initial.Revision != 0 || initial.Value != defaultValue {
				t.Fatalf("initial publication = %#v, want the registered default at revision 0", initial)
			}

			// Written foreign, so its publication is the feed's own re-read: none is
			// left pending to read a gap write before the reconnect does.
			writeRowDirect(t, foreign, key, `"before-gap"`, "foreign")

			before := sink.next(t, 10*time.Second, "publication of the pre-gap write")
			if before.Value != "before-gap" || before.Revision == 0 {
				t.Fatalf("pre-gap publication = %#v, want before-gap at a store revision", before)
			}

			held, reopen := holdFeedGap(t, foreign)
			awaitCond(t, 30*time.Second, "the scope reports Stale once the feed is severed", func() bool {
				return entryOf(t, client, key).Stale
			})

			for _, v := range tc.writes {
				writeRowDirect(t, held, key, strconv.Quote(v), "foreign")
			}

			// Longer than the listener's first retry: a reconnect that got through would show.
			for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
				if e := entryOf(t, client, key); !e.Stale || e.Value != "before-gap" || e.Revision != before.Revision {
					t.Fatalf("read inside the gap = %#v, want before-gap at revision %d marked Stale", e, before.Revision)
				}
			}

			reopen()

			want := tc.writes[len(tc.writes)-1]

			converged := sink.next(t, 60*time.Second, "publication of the gap write after the reconnect")
			if converged.Value != want || converged.Revision <= before.Revision {
				t.Fatalf("post-reconnect publication = %#v, want %q above revision %d", converged, want, before.Revision)
			}

			sink.expectSilence(t, "a second delivery after convergence")

			if e := entryOf(t, client, key); e.Value != want || e.Revision != converged.Revision || e.Stale || e.UpdatedBy != "foreign" {
				t.Fatalf("converged entry = %#v, want %q at revision %d, fresh, by foreign", e, want, converged.Revision)
			}
		})
	}
}

// Scenario 5: a foreign row the validator rejects leaves the last valid value
// in force, is logged, marks nothing stale and reaches no subscriber.
func TestIntegration_Acceptance05_InvalidExternalRowPostgres(t *testing.T) {
	const key = "invalid-row"

	client, foreign, logger := newSTPostgres(t)
	mustRegister(t, client, key, systemplane.WithValidator(stringValidator))

	sink := subscribe(t, client, key)
	mustStart(t, client)

	_ = sink.next(t, 30*time.Second, "initial publication at Start")

	mustSet(t, client, key, "last-valid")
	valid := sink.next(t, 10*time.Second, "publication of the last valid value")

	writeRowDirect(t, foreign, key, `{"not":"a string"}`, "foreign")
	sink.expectSilence(t, "delivery of a row the validator rejected")

	if got := entryOf(t, client, key); got.Value != "last-valid" || got.Revision != valid.Revision || got.Stale {
		t.Fatalf("read after the invalid row = %#v, want last-valid at revision %d, not stale", got, valid.Revision)
	}

	logger.requireMention(t, key)
}

// Scenario 7: while one key's subscriber blocks for five seconds, changes to
// another key still arrive through the changefeed within 500ms, one after
// another: dispatch never runs on the goroutine that drains the feed.
func TestIntegration_Acceptance07_SlowSubscriberDoesNotStallFeed(t *testing.T) {
	const slowKey, fastKey = "slow-subscriber", "fast-subscriber"

	client, foreign, _ := newSTPostgres(t)
	mustRegister(t, client, slowKey)
	mustRegister(t, client, fastKey)

	var slowReturned atomic.Bool

	slowEntered := make(chan struct{}, 1)

	unsubscribe, err := client.OnChange(accNS, slowKey, func(ctx context.Context, _ systemplane.Change) {
		defer slowReturned.Store(true)

		slowEntered <- struct{}{}

		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
		}
	})
	if err != nil {
		t.Fatalf("OnChange %s: %v", slowKey, err)
	}

	t.Cleanup(unsubscribe)

	fastSink := subscribe(t, client, fastKey)
	mustStart(t, client)

	// Start announces both keys; that announcement puts the slow subscriber in its block.
	_ = fastSink.next(t, 30*time.Second, "initial publication of the fast key")
	awaitSignal(t, slowEntered, "the slow subscriber to start blocking")

	// Foreign writes reach the client only through NOTIFY and re-read.
	for _, v := range []string{"first", "second"} {
		writeRowDirect(t, foreign, fastKey, strconv.Quote(v), "foreign")

		if got := fastSink.next(t, 500*time.Millisecond, "the fast key's "+v+" change while the slow one blocks"); got.Value != v {
			t.Fatalf("fast-key delivery = %#v, want %q", got, v)
		}
	}

	if slowReturned.Load() {
		t.Fatal("the slow subscriber returned before the fast key was checked, so the check proved nothing")
	}
}

// groupDoc is the three-field document scenario 8 writes atomically.
type groupDoc struct {
	Alpha string `json:"alpha"`
	Beta  int    `json:"beta"`
	Gamma bool   `json:"gamma"`
}

var (
	docBefore = groupDoc{Alpha: "before", Beta: 1, Gamma: false}
	docAfter  = groupDoc{Alpha: "after", Beta: 2, Gamma: true}
)

// Scenario 8: a group is one row, so a reader hammering Snapshot while all
// three fields change never sees half of one document and half of the other.
func TestIntegration_Acceptance08_GroupAtomicity(t *testing.T) {
	client, _, _ := newSTPostgres(t)

	group, err := systemplane.Bind(client, accNS, "group-atomicity", docBefore, nil)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	mustStart(t, client)

	var reads atomic.Int64

	stop, readerErr := make(chan struct{}), make(chan error, 1)

	go func() {
		for {
			select {
			case <-stop:
				readerErr <- nil

				return
			default:
			}

			snapshot, err := group.Snapshot(context.Background())
			if err != nil {
				readerErr <- fmt.Errorf("Snapshot: %w", err)

				return
			}

			reads.Add(1)

			if snapshot.Value != docBefore && snapshot.Value != docAfter {
				readerErr <- fmt.Errorf("Snapshot observed a torn document %#v", snapshot.Value)

				return
			}
		}
	}()

	for range 20 {
		for _, doc := range []groupDoc{docAfter, docBefore} {
			if err := group.Set(t.Context(), doc, "test"); err != nil {
				t.Fatalf("Set %#v: %v", doc, err)
			}
		}
	}

	close(stop)

	if err := <-readerErr; err != nil {
		t.Fatal(err)
	}

	if reads.Load() == 0 {
		t.Fatal("the reader never completed a Snapshot, so atomicity was never observed")
	}
}

// Scenario 9: Set publishes before returning, so the next read on the same
// goroutine returns the value just written, and the feed echo of each Set is
// deduplicated by revision rather than delivered again.
func TestIntegration_Acceptance09_ReadYourWritesPostgres(t *testing.T) {
	const key = "read-your-writes"

	client, _, _ := newSTPostgres(t)
	mustRegister(t, client, key)

	sink := subscribe(t, client, key)
	mustStart(t, client)

	_ = sink.next(t, 30*time.Second, "initial publication at Start")

	for _, want := range []string{"first", "second", "third"} {
		mustSet(t, client, key, want)

		if got, ok, err := client.Get(context.Background(), accNS, key); err != nil || !ok || got != want {
			t.Fatalf("Get right after Set(%q) = %#v, ok %v, err %v; want the value just written", want, got, ok, err)
		}

		if change := sink.next(t, 10*time.Second, "publication of "+want); change.Value != want || change.Revision == 0 {
			t.Fatalf("publication = %#v, want %q at a store revision", change, want)
		}
	}

	sink.expectSilence(t, "echo of an already-published write")
}

// Scenario 10: Close cancels in-flight deliveries and waits for them, so every
// callback that honours ctx has returned by the time Close returns nil.
func TestIntegration_Acceptance10_CloseWaitsForCtxHonouringCallbacks(t *testing.T) {
	keys := []string{"shutdown-a", "shutdown-b"}

	client, _, _ := newSTPostgres(t, systemplane.WithCloseTimeout(10*time.Second))

	var returned atomic.Int32

	entered := make(chan struct{}, len(keys))

	for _, key := range keys {
		mustRegister(t, client, key)

		unsubscribe, err := client.OnChange(accNS, key, func(ctx context.Context, _ systemplane.Change) {
			entered <- struct{}{}

			<-ctx.Done()
			// Winding down takes a moment, so a Close that only cancelled returns first.
			time.Sleep(50 * time.Millisecond)
			returned.Add(1)
		})
		if err != nil {
			t.Fatalf("OnChange %s: %v", key, err)
		}

		t.Cleanup(unsubscribe)
	}

	mustStart(t, client)

	for range keys {
		awaitSignal(t, entered, "a subscriber to start running")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close with ctx-honouring callbacks = %v, want nil", err)
	}

	if n := returned.Load(); n != int32(len(keys)) {
		t.Fatalf("Close returned while %d of %d ctx-honouring callbacks were still running", len(keys)-int(n), len(keys))
	}
}

// Scenario 10: a callback that ignores ctx makes Close give up at the
// configured bound with ErrCloseTimeout naming the single-tenant scope and key.
func TestIntegration_Acceptance10_CloseNamesACallbackThatIgnoresCtx(t *testing.T) {
	const (
		key   = "shutdown-stuck"
		bound = 200 * time.Millisecond
	)

	client, _, _ := newSTPostgres(t, systemplane.WithCloseTimeout(bound))
	mustRegister(t, client, key)

	entered, release, left := make(chan struct{}, 1), make(chan struct{}), make(chan struct{})

	unsubscribe, err := client.OnChange(accNS, key, func(context.Context, systemplane.Change) {
		entered <- struct{}{}

		<-release
		close(left)
	})
	if err != nil {
		t.Fatalf("OnChange: %v", err)
	}

	t.Cleanup(unsubscribe)
	mustStart(t, client)
	awaitSignal(t, entered, "the stuck subscriber to start running")

	started := time.Now()
	err = client.Close()
	elapsed := time.Since(started)

	// Join the deliberate leak first, so no assertion below can leave it running.
	close(release)
	awaitSignal(t, left, "the released callback to return")

	if !errors.Is(err, systemplane.ErrCloseTimeout) {
		t.Fatalf("Close with a ctx-ignoring callback = %v, want ErrCloseTimeout", err)
	}

	if elapsed < bound || elapsed > bound+2*time.Second {
		t.Fatalf("Close took %s, want the configured %s bound plus at most 2s", elapsed, bound)
	}

	for _, want := range []string{"single-tenant", accNS, key} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Close error %q does not name %q", err, want)
		}
	}
}

// Scenario 11: a delete publishes the registered default at revision 0 and the
// next write supersedes it at a higher store revision.
func TestIntegration_Acceptance11_DeletePublishesDefaultAtRevisionZeroPostgres(t *testing.T) {
	const key = "revision-zero"

	client, _, _ := newSTPostgres(t)
	mustRegister(t, client, key)

	sink := subscribe(t, client, key)
	mustStart(t, client)

	_ = sink.next(t, 30*time.Second, "initial publication at Start")

	mustSet(t, client, key, "stored")

	stored := sink.next(t, 10*time.Second, "publication of the stored value")
	if stored.Revision == 0 {
		t.Fatal("a stored row published at revision 0; the store must assign a revision")
	}

	deleteToDefault(t, client, sink, key)
	mustSet(t, client, key, "real")

	promoted := sink.next(t, 10*time.Second, "publication of the write after the delete")
	if promoted.Value != "real" || promoted.Revision <= stored.Revision {
		t.Fatalf("publication after the delete = %#v, want real above revision %d", promoted, stored.Revision)
	}

	if e := entryOf(t, client, key); e.Value != "real" || e.Revision != promoted.Revision {
		t.Fatalf("read after the write = %#v, want real at revision %d", e, promoted.Revision)
	}
}
