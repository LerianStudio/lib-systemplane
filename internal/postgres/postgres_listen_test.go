//go:build unit

// Targeted goroutine-lifecycle tests for the postgres Subscribe path. These
// exercise the unsubscribe func returned by Subscribe — they do NOT require a
// live PostgreSQL because Subscribe in this package only manipulates the
// zero-scope feed's subscriber map (the LISTEN connection is owned by
// startListener, which we never call in these tests).
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.uber.org/goleak"
)

// newSubscribeStore builds a Store whose state is sufficient for Subscribe:
// a zero-scope feed with no connection behind it. We bypass New() because that
// constructor validates DB/DSN; Subscribe itself doesn't touch either.
func newSubscribeStore() *Store {
	return &Store{
		cfg:      Config{Channel: defaultChannel, Table: defaultTable, Module: defaultModule},
		feeds:    map[string]*feed{"": newFeed(store.Scope{}, "")},
		closedCh: make(chan struct{}),
	}
}

// subscriberCount reports how many callbacks the zero-scope feed will fan out to.
func subscriberCount(s *Store) int {
	s.feedsMu.Lock()
	f, ok := s.feeds[""]
	s.feedsMu.Unlock()

	if !ok {
		return 0
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.subs)
}

func waitForObserverExit(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)

	for {
		if err := goleak.Find(); err == nil {
			return
		}

		if time.Now().After(deadline) {
			if err := goleak.Find(); err != nil {
				t.Fatalf("postgres subscribe goroutine did not exit: %v", err)
			}

			return
		}

		time.Sleep(5 * time.Millisecond)
	}
}

// Subscribe spawns one ctx-observer goroutine per subscription, so that a
// cancelled scope releases its feed without a caller-driven unsubscribe. We
// assert (a) basic unsubscribe behavior, (b) idempotency under concurrent
// unsubscribe calls, and (c) that the observer exits either way.
func TestPostgresSubscribe_UnsubscribeIsIdempotent(t *testing.T) {
	s := newSubscribeStore()

	unsub, err := s.Subscribe(context.Background(), store.Scope{}, func(_ store.Event) {})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			unsub()
		}()
	}

	wg.Wait()

	// Subscriber map must be empty after teardown.
	n := subscriberCount(s)

	if n != 0 {
		t.Errorf("subscriber map size = %d, want 0 after unsubscribe", n)
	}

	waitForObserverExit(t)
}

// Subscribe with a nil callback returns a harmless no-op closer and must not
// register anything in the subscriber map.
func TestPostgresSubscribe_NilCallback(t *testing.T) {
	s := newSubscribeStore()

	unsub, err := s.Subscribe(context.Background(), store.Scope{}, nil)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if n := subscriberCount(s); n != 0 {
		t.Errorf("nil-cb subscribe registered %d subscribers; want 0", n)
	}

	unsub()
	waitForObserverExit(t)
}

// beginDisconnect is the single atomic decision point for "does this
// connection loss get announced". It must fire exactly once per outage and
// never during a clean teardown.
func TestPostgresFeed_BeginDisconnectSuppressedWhenClosing(t *testing.T) {
	f := newFeed(store.Scope{}, "")
	f.subs[1] = &subscription{fn: func(store.Event) {}}
	f.subs[2] = &subscription{fn: func(store.Event) {}}

	subs, ok := f.beginDisconnect()
	if !ok {
		t.Fatal("beginDisconnect on a live feed = ok false; want the connected->disconnected edge to announce")
	}

	if len(subs) != 2 {
		t.Fatalf("beginDisconnect returned %d subscribers, want 2", len(subs))
	}

	// Edge-triggered: a second failure inside the same outage announces nothing.
	if subs, ok := f.beginDisconnect(); ok || len(subs) != 0 {
		t.Fatalf("second beginDisconnect in one outage = (%d subs, ok %v), want (0, false)", len(subs), ok)
	}

	// A successful reconnect re-arms the edge.
	if subs, ok := f.beginResync(); !ok || len(subs) != 2 {
		t.Fatalf("beginResync after a disconnect = (%d subs, ok %v), want (2, true)", len(subs), ok)
	}

	if subs, ok := f.beginDisconnect(); !ok || len(subs) != 2 {
		t.Fatalf("beginDisconnect after resync = (%d subs, ok %v), want (2, true)", len(subs), ok)
	}

	f.beginResync()

	// Teardown wins: a clean shutdown must emit no OpDisconnect at all.
	f.mu.Lock()
	f.closing = true
	f.mu.Unlock()

	subs, ok = f.beginDisconnect()
	if ok {
		t.Fatal("beginDisconnect during teardown = ok true; a clean shutdown must announce no disconnect")
	}

	if len(subs) != 0 {
		t.Fatalf("beginDisconnect during teardown returned %d subscribers, want 0", len(subs))
	}
}

// The teardown-vs-loss decision is one atomic step under f.mu, not a probe of
// the stop channel: once teardown has released f.mu with closing set, no
// later beginDisconnect may announce anything.
func TestPostgresFeed_CloseRacingConnectionLoss_EmitsNoDisconnect(t *testing.T) {
	f := newFeed(store.Scope{}, "")
	f.subs[1] = &subscription{fn: func(store.Event) {}}

	// tornDown flips only AFTER teardown released f.mu. Any beginDisconnect
	// that starts once it reads true is guaranteed by the mutex to observe
	// f.closing, so it must announce nothing.
	var tornDown atomic.Bool

	start := make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		<-start

		f.mu.Lock()
		f.closing = true
		f.mu.Unlock()

		tornDown.Store(true)

		close(f.stop)
	}()

	close(start)

	for range 10_000 {
		alreadyTornDown := tornDown.Load()

		subs, ok := f.beginDisconnect()
		if ok {
			if alreadyTornDown {
				t.Fatalf("beginDisconnect announced a disconnect to %d subscribers after teardown completed; a clean shutdown must announce none", len(subs))
			}

			f.beginResync() // re-arm the edge so the next iteration races again
		}

		if alreadyTornDown {
			break
		}
	}

	wg.Wait()

	if subs, ok := f.beginDisconnect(); ok {
		t.Fatalf("beginDisconnect after teardown = (%d subs, ok true), want ok false", len(subs))
	}
}

// A subscriber that joins an already-connected feed gets its own OpResync: it
// missed everything published before it joined, and on the engine's path
// Subscribe runs after Start has connected, so without this emission the scope
// would never reconcile.
//
// The emission must go through deliverLocked rather than calling fn directly.
// A callback that panics would otherwise escape through Subscribe to the
// caller — a path the reader goroutine's recovery never covers — and unwind
// past the unlock of sub.mu, wedging every later delivery to that
// subscription. This test pins both halves: Subscribe returns normally after
// the joining callback panics, and a second delivery to the same subscription
// still completes.
func TestPostgresSubscribe_PanickingCallbackDoesNotEscapeOrHoldLock(t *testing.T) {
	s := newSubscribeStore()
	f, err := s.zeroFeed()
	if err != nil {
		t.Fatalf("zeroFeed: %v", err)
	}

	f.beginResync() // mark the feed connected, as a successful LISTEN does

	var (
		mu     sync.Mutex
		events []store.Event
	)

	record := func(evt store.Event) int {
		mu.Lock()
		defer mu.Unlock()

		events = append(events, evt)

		return len(events)
	}

	// Subscribe must return normally; a panic here fails the test by unwinding it.
	unsub, subErr := s.Subscribe(context.Background(), store.Scope{}, func(evt store.Event) {
		if record(evt) == 1 {
			panic("callback exploded on its joining resync")
		}
	})
	if subErr != nil {
		t.Fatalf("subscribe: %v", subErr)
	}

	defer unsub()

	mu.Lock()
	got := append([]store.Event(nil), events...)
	mu.Unlock()

	if len(got) != 1 {
		t.Fatalf("callback saw %d events during Subscribe, want 1 (the joining resync)", len(got))
	}

	if got[0].Op != store.OpResync || got[0].Scope != f.scope {
		t.Fatalf("joining event = %+v, want {Scope:%+v Op:%q}", got[0], f.scope, store.OpResync)
	}

	// sub.mu must be free again: a second delivery has to complete rather than
	// block forever on a mutex the panicking callback unwound past.
	done := make(chan struct{})

	go func() {
		defer close(done)

		f.dispatch(s.cfg.Logger, store.Event{Namespace: "ns", Key: "k", Op: store.OpUpsert, Revision: 7})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("second delivery blocked: the panicking joining resync left sub.mu held")
	}

	mu.Lock()
	n := len(events)
	mu.Unlock()

	if n != 2 {
		t.Fatalf("callback saw %d events, want 2 (joining resync, then the upsert)", n)
	}
}

// unreachableDSN points at a closed loopback port: any dial fails immediately
// instead of hanging, so a test that sees a connect error is a test where the
// shutdown guard failed to run.
const unreachableDSN = "postgres://u:p@127.0.0.1:1/db?sslmode=disable"

func newListenStore(t *testing.T) *Store {
	t.Helper()

	s, err := New(Config{DB: &sql.DB{}, ListenDSN: unreachableDSN})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	return s
}

// closingStore puts a real store into the state Close leaves the instant it
// starts: the store-wide closing flag raised and the feeds map walked clean.
// A Start or a Subscribe that already passed the s.closed check is racing
// exactly this state.
func closingStore(t *testing.T) *Store {
	t.Helper()

	s := newListenStore(t)

	s.feedsMu.Lock()
	s.closing = true

	clear(s.feeds)
	s.feedsMu.Unlock()

	return s
}

func feedCount(s *Store) int {
	s.feedsMu.Lock()
	defer s.feedsMu.Unlock()

	return len(s.feeds)
}

// Start losing the race to Close must open nothing. Without the recheck it
// re-inserts the zero-scope feed into a shut-down store, opens a LISTEN
// connection and launches its reader — and a second Close returns early at
// s.closed, so that connection and goroutine live until the process dies.
func TestPostgresStart_RacingCloseOpensNothing(t *testing.T) {
	s := closingStore(t)

	if err := s.Start(context.Background()); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Start on a closing store = %v, want ErrClosed: it must not resurrect the zero-scope feed", err)
	}

	if n := feedCount(s); n != 0 {
		t.Fatalf("feeds map holds %d entries after Start lost to Close, want 0", n)
	}

	waitForObserverExit(t)
}

// Subscribe losing the same race must refuse rather than hand back a
// subscription on a feed nothing will ever drive.
func TestPostgresSubscribe_ZeroScopeRacingCloseIsRefused(t *testing.T) {
	s := closingStore(t)

	unsub, err := s.Subscribe(context.Background(), store.Scope{}, func(store.Event) {})
	if !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Subscribe on a closing store = %v, want ErrClosed: a dead store must not hand back a live-looking subscription", err)
	}

	if unsub != nil {
		t.Error("Subscribe returned an unsubscribe func alongside its error")
	}

	if n := feedCount(s); n != 0 {
		t.Fatalf("feeds map holds %d entries after Subscribe lost to Close, want 0", n)
	}

	waitForObserverExit(t)
}

// Close reaps every subscription's ctx observer. A subscriber whose ctx
// outlives the store — an engine root ctx, or context.Background() — otherwise
// keeps one goroutine parked forever, holding the feed graph it closes over
// alive with it.
func TestPostgresSubscribe_CloseReapsCtxObservers(t *testing.T) {
	s := newListenStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A cancellable ctx nobody cancels, and a ctx with no Done channel at all.
	if _, err := s.Subscribe(ctx, store.Scope{}, func(store.Event) {}); err != nil {
		t.Fatalf("subscribe (cancellable ctx): %v", err)
	}

	if _, err := s.Subscribe(context.Background(), store.Scope{}, func(store.Event) {}); err != nil {
		t.Fatalf("subscribe (background ctx): %v", err)
	}

	if n := subscriberCount(s); n != 2 {
		t.Fatalf("subscriber map size = %d, want 2 before Close", n)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	waitForObserverExit(t)
}

// A reconnect that succeeds just as Close lands must announce nothing: the
// scope is going away, and an OpResync there would send the engine off to
// reconcile a feed that no longer exists.
func TestPostgresFeed_BeginResyncSuppressedWhenClosing(t *testing.T) {
	f := newFeed(store.Scope{}, "")
	f.subs[1] = &subscription{fn: func(store.Event) {}}

	if subs, ok := f.beginResync(); !ok || len(subs) != 1 {
		t.Fatalf("beginResync on a live feed = (%d subs, ok %v), want (1, true)", len(subs), ok)
	}

	// Teardown wins, exactly as it does for beginDisconnect.
	f.mu.Lock()
	f.closing = true
	f.mu.Unlock()

	if subs, ok := f.beginResync(); ok || len(subs) != 0 {
		t.Fatalf("beginResync during teardown = (%d subs, ok %v), want (0, false)", len(subs), ok)
	}
}

// Markers (OpResync / OpDisconnect) reach subscribers on the reader goroutine
// exactly as key events do, so broadcast has to mark the dispatch window too.
// Without the marker a callback that drops its tenant's last subscription —
// what the engine does on OpDisconnect, and on the OpResync whose reconcile
// then fails — is handed the reader's own done channel and burns the full
// closeTimeout waiting for the goroutine it is itself running on.
func TestPostgresFeed_BroadcastMarksTheDispatchWindow(t *testing.T) {
	s := newSubscribeStore()

	f := newFeed(store.Scope{Tenant: "t1"}, "")
	f.done = make(chan struct{}) // a reader that never exits, as a stalled one would

	var (
		delivered bool
		wait      <-chan struct{}
	)

	f.subs[1] = &subscription{fn: func(store.Event) {
		delivered = true
		wait = s.signalFeed(f, true)
	}}

	subs, ok := f.beginDisconnect()
	if !ok {
		t.Fatal("beginDisconnect on a live feed = ok false; want the connected->disconnected edge to announce")
	}

	s.broadcast(f, subs, store.Event{Scope: f.scope, Op: store.OpDisconnect})

	if !delivered {
		t.Fatal("broadcast delivered no OpDisconnect to the registered subscriber")
	}

	if wait != nil {
		t.Fatal("tearing the feed down from inside an OpDisconnect delivery returned the reader's done channel; the callback would wait closeTimeout on the goroutine running it")
	}
}

// shrinkTimeouts makes the connect and shutdown bounds small enough to assert
// on inside a unit test, and restores them afterwards. The unit tests in this
// package never run in parallel with one another, so a package var is enough.
func shrinkTimeouts(t *testing.T, d time.Duration) {
	t.Helper()

	prevConnect, prevListen, prevClose := connectTimeout, listenTimeout, closeTimeout

	connectTimeout, listenTimeout, closeTimeout = d, d, d

	t.Cleanup(func() { connectTimeout, listenTimeout, closeTimeout = prevConnect, prevListen, prevClose })
}

// stalledFeed stands in for a feed whose reader ignores the stop signal: its
// done channel is never closed, so every waiter on it burns the full
// closeTimeout.
func stalledFeed(tenant string) *feed {
	f := newFeed(store.Scope{Tenant: tenant}, "")
	f.done = make(chan struct{})

	return f
}

// feedSignalled reports whether a teardown reached this feed: closing marked
// under f.mu and stop closed, the two halves signalFeed performs together.
func feedSignalled(f *feed) bool {
	select {
	case <-f.stop:
	default:
		return false
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	return f.closing
}

// Close must cost ONE closeTimeout no matter how many tenants the store
// carries. Tearing feeds down one at a time costs tenants x closeTimeout, so
// six slow tenants overrun the engine's 30s Close budget and, behind it, a
// Kubernetes termination grace period — the pod is killed mid-shutdown.
//
// The assertion is structural rather than a tight stopwatch window: every feed
// must be signalled and must carry a reader for the shutdown to wait on, and
// five of them together must still finish well inside the budget a per-feed
// deadline would spend (5 x closeTimeout). The remaining bound is deliberately
// loose so a busy machine cannot turn correct behavior into a failure.
func TestPostgresClose_SlowFeedsShareOneShutdownDeadline(t *testing.T) {
	shrinkTimeouts(t, 200*time.Millisecond)

	s := newSubscribeStore()

	feeds := map[string]*feed{}

	for _, tenant := range []string{"t1", "t2", "t3", "t4", "t5"} {
		feeds[tenant] = stalledFeed(tenant)
	}

	s.feedsMu.Lock()

	for tenant, f := range feeds {
		s.feeds[tenant] = f
	}

	s.feedsMu.Unlock()

	start := time.Now()

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	elapsed := time.Since(start)

	for tenant, f := range feeds {
		if !feedSignalled(f) {
			t.Errorf("feed %q was not signalled by Close", tenant)
		}

		f.mu.Lock()
		done := f.done
		f.mu.Unlock()

		if done == nil {
			t.Errorf("feed %q carried no reader for Close to wait on; the test no longer proves anything", tenant)
		}
	}

	if elapsed >= 3*closeTimeout {
		t.Fatalf("Close on %d stalled feeds took %v; want one shared closeTimeout (%v), not one per feed", len(feeds), elapsed, closeTimeout)
	}
}

// blackholeDSN points at a listener that accepts a connection and then says
// nothing, so a pgx dial parks in the startup handshake until its own bound
// fires. It is how an unreachable tenant behaves in practice: a dropped packet
// filter, not a closed port. The returned func shuts the listener down; it is
// idempotent and also runs on cleanup.
func blackholeDSN(t *testing.T) (string, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var (
		mu     sync.Mutex
		accept sync.WaitGroup
		conns  []net.Conn
		once   sync.Once
	)

	accept.Add(1)

	go func() {
		defer accept.Done()

		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}

			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()

	stop := func() {
		once.Do(func() {
			_ = ln.Close()

			accept.Wait()

			mu.Lock()
			defer mu.Unlock()

			for _, c := range conns {
				_ = c.Close()
			}
		})
	}

	t.Cleanup(stop)

	return "postgres://u:p@" + ln.Addr().String() + "/db?sslmode=disable", stop
}

// The FIRST connect of a feed is bounded exactly like a reconnect. Without the
// bound it inherits the caller's ctx: an engine activating a tenant on a root
// context would hang in Subscribe for as long as the process lives, holding the
// reserved feed slot so every later Subscribe for that tenant parks behind it.
func TestPostgresSubscribe_FirstConnectIsBounded(t *testing.T) {
	shrinkTimeouts(t, 250*time.Millisecond)

	dsn, stopBlackhole := blackholeDSN(t)
	conn := &stubConnector{resolve: func(int) (string, error) { return dsn, nil }}

	s, err := New(Config{MultiTenantEnabled: true, Connector: conn})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	start := time.Now()

	unsub, subErr := s.Subscribe(context.Background(), store.Scope{Tenant: "t1"}, func(store.Event) {})

	elapsed := time.Since(start)

	if subErr == nil {
		unsub()
		t.Fatal("Subscribe to an unreachable tenant returned nil; want the connect bound to fail it")
	}

	if elapsed > 4*connectTimeout {
		t.Fatalf("Subscribe took %v to give up; want it bounded by connectTimeout (%v)", elapsed, connectTimeout)
	}

	if !errors.Is(subErr, context.DeadlineExceeded) {
		t.Fatalf("Subscribe error = %v, want it to carry context.DeadlineExceeded", subErr)
	}

	if !strings.Contains(subErr.Error(), "tenant t1") {
		t.Errorf("Subscribe error %q must name the tenant", subErr)
	}

	if n := feedCount(s); n != 0 {
		t.Fatalf("feeds map holds %d entries after a failed first connect, want 0", n)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// The blackhole is the test's own goroutine; shut it down before asking
	// whether the STORE left anything behind.
	stopBlackhole()
	waitForObserverExit(t)
}

// A joining subscriber is told whatever the feed last ANNOUNCED, so the engine
// never reports a scope fresh while its changefeed is down. Joining during an
// outage used to deliver nothing at all, which left the scope looking current
// for as long as the backoff took to reconnect — up to 30s.
//
// The third state is the one that must stay silent: a feed whose reader has not
// yet announced anything. Its first OpResync is imminent and this subscriber is
// already in the map, so it receives that one — announcing here too would
// double it.
func TestPostgresSubscribe_JoinerIsToldTheFeedState(t *testing.T) {
	cases := []struct {
		name string
		arm  func(f *feed)
		want []string
	}{
		{
			name: "connected feed announces a resync",
			arm:  func(f *feed) { f.beginResync() },
			want: []string{store.OpResync},
		},
		{
			name: "feed in an announced outage announces a disconnect",
			arm:  func(f *feed) { f.beginResync(); f.beginDisconnect() },
			want: []string{store.OpDisconnect},
		},
		{
			name: "feed that has announced nothing yet stays silent",
			arm:  func(*feed) {},
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSubscribeStore()

			f, err := s.zeroFeed()
			if err != nil {
				t.Fatalf("zeroFeed: %v", err)
			}

			tc.arm(f)

			var got []store.Event

			unsub, subErr := s.Subscribe(context.Background(), store.Scope{}, func(evt store.Event) {
				got = append(got, evt)
			})
			if subErr != nil {
				t.Fatalf("subscribe: %v", subErr)
			}

			defer unsub()

			if len(got) != len(tc.want) {
				t.Fatalf("joining subscriber saw %+v, want %d event(s) %v", got, len(tc.want), tc.want)
			}

			for i, op := range tc.want {
				if got[i].Op != op || got[i].Scope != f.scope {
					t.Fatalf("joining event %d = %+v, want {Scope:%+v Op:%q}", i, got[i], f.scope, op)
				}
			}
		})
	}
}

// Close must wait for a feed whose reader is inside a callback. "Some goroutine
// is mid-callback" is not "the caller IS the reader goroutine": skipping the
// wait on that signal returns from Close with a live LISTEN connection and a
// live reader behind it. Only the last-unsubscribe path, which a callback can
// legitimately reach on the reader's own goroutine, may skip.
func TestPostgresClose_WaitsForAFeedMidDispatch(t *testing.T) {
	shrinkTimeouts(t, 200*time.Millisecond)

	s := newSubscribeStore()

	f := stalledFeed("t1")

	f.mu.Lock()
	f.dispatching = 1
	f.mu.Unlock()

	s.feedsMu.Lock()
	s.feeds["t1"] = f
	s.feedsMu.Unlock()

	start := time.Now()

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if elapsed := time.Since(start); elapsed < closeTimeout {
		t.Fatalf("Close returned in %v on a feed whose reader is mid-callback; it must still wait up to closeTimeout (%v) for that reader to exit", elapsed, closeTimeout)
	}
}

// A closing store must never open an outbound connection. The named-tenant
// branch of feed acquisition reserved its slot and dialed the tenant without
// ever looking at the shutdown flag the zero scope already fenced on.
func TestPostgresSubscribe_ClosingStoreDialsNoTenant(t *testing.T) {
	s := newSubscribeStore()
	s.cfg.MultiTenantEnabled = true
	s.cfg.Connector = &stubConnector{resolve: func(int) (string, error) { return unreachableDSN, nil }}

	s.feedsMu.Lock()
	s.closing = true
	s.feedsMu.Unlock()

	unsub, err := s.Subscribe(context.Background(), store.Scope{Tenant: "t1"}, func(store.Event) {})
	if err == nil {
		unsub()
		t.Fatal("Subscribe on a closing store returned nil; want store.ErrClosed")
	}

	if !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Subscribe error = %v, want store.ErrClosed", err)
	}

	if calls := s.cfg.Connector.(*stubConnector).callCount(); calls != 0 {
		t.Errorf("a closing store resolved %d tenant DSNs; want 0 — it must not dial", calls)
	}

	if n := feedCount(s); n != 1 {
		t.Errorf("feeds map holds %d entries, want only the zero-scope feed", n)
	}
}

// FAIL CLOSED on schema-per-tenant isolation. A tenant DSN that pins a
// search_path means several tenants share ONE database, and NOTIFY is
// database-wide: every tenant's feed would then receive every other tenant's
// events stamped with its own scope, and the engine's revision fence would act
// on them. Refusing costs a tenant its changefeed; accepting corrupts every
// tenant in that database.
//
// unreachableDSN is the discriminator for "nothing was dialed": without the
// refusal the call reaches pgx.Connect and fails with a connect error instead
// of the sentinel.
func TestPostgresSubscribe_SchemaIsolatedDSNIsRefused(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
	}{
		{name: "search_path parameter", dsn: unreachableDSN + "&search_path=tenant_a"},
		{name: "options -c search_path", dsn: unreachableDSN + "&options=-csearch_path%3Dtenant_a"},
		// Postgres resolves GUC names case-insensitively while pgconn keeps the
		// connection string verbatim, so an upper- or mixed-case spelling pins
		// exactly the same schema through a check that only knew the lowercase
		// one.
		{name: "SEARCH_PATH parameter", dsn: unreachableDSN + "&SEARCH_PATH=tenant_a"},
		{name: "Search_Path parameter", dsn: unreachableDSN + "&Search_Path=tenant_a"},
		{name: "options -c SEARCH_PATH", dsn: unreachableDSN + "&options=-cSEARCH_PATH%3Dtenant_a"},
		{name: "options -c Search_Path", dsn: unreachableDSN + "&options=-cSearch_Path%3Dtenant_a"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := &stubConnector{resolve: func(int) (string, error) { return tc.dsn, nil }}

			s, err := New(Config{MultiTenantEnabled: true, Connector: conn})
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			unsub, subErr := s.Subscribe(context.Background(), store.Scope{Tenant: "t1"}, func(store.Event) {})
			if subErr == nil {
				unsub()
				t.Fatal("Subscribe with a schema-isolated tenant DSN returned nil; want ErrSchemaIsolationUnsupported")
			}

			if !errors.Is(subErr, ErrSchemaIsolationUnsupported) {
				t.Fatalf("Subscribe error = %v, want ErrSchemaIsolationUnsupported", subErr)
			}

			if !strings.Contains(subErr.Error(), "tenant t1") {
				t.Errorf("Subscribe error %q must name the tenant", subErr)
			}

			if n := feedCount(s); n != 0 {
				t.Errorf("feeds map holds %d entries after a refused DSN, want 0", n)
			}

			if err := s.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			waitForObserverExit(t)
		})
	}
}

// The refusal is narrow: a tenant DSN without a search_path still reaches the
// dialer, which is what makes the sentinel above meaningful.
func TestPostgresSubscribe_PlainTenantDSNStillDials(t *testing.T) {
	conn := &stubConnector{resolve: func(int) (string, error) { return unreachableDSN, nil }}

	s, err := New(Config{MultiTenantEnabled: true, Connector: conn})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	unsub, subErr := s.Subscribe(context.Background(), store.Scope{Tenant: "t1"}, func(store.Event) {})
	if subErr == nil {
		unsub()
		t.Fatal("Subscribe to a closed port returned nil; want the dial to fail")
	}

	if errors.Is(subErr, ErrSchemaIsolationUnsupported) {
		t.Fatalf("Subscribe error = %v; a DSN without a search_path must not be refused as schema-isolated", subErr)
	}

	if !strings.Contains(subErr.Error(), "listen connect") {
		t.Errorf("Subscribe error %q must come from the dialer", subErr)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	waitForObserverExit(t)
}

// A nil ctx is a caller bug the store absorbs rather than panics on: the
// subscription is registered and usable, and it spawns no observer goroutine
// because there is no Done channel to watch.
func TestPostgresSubscribe_NilContext(t *testing.T) {
	s := newSubscribeStore()

	// Passed through a variable so the nil is not a literal: linters flag a
	// literal nil context, and the point here is exactly that one cannot crash
	// the store.
	var nilCtx context.Context

	unsub, err := s.Subscribe(nilCtx, store.Scope{}, func(store.Event) {})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if n := subscriberCount(s); n != 1 {
		t.Fatalf("nil-ctx subscribe registered %d subscribers; want 1", n)
	}

	// No ctx to observe means no goroutine: assert it before unsubscribing,
	// while a leaked observer would still be parked.
	if err := goleak.Find(); err != nil {
		t.Fatalf("nil-ctx subscribe spawned a goroutine: %v", err)
	}

	unsub()

	if n := subscriberCount(s); n != 0 {
		t.Errorf("subscriber map size = %d, want 0 after unsubscribe", n)
	}

	waitForObserverExit(t)
}

// The one-database-per-tenant rule is not a tenant-only rule. Two installations
// that share one database through per-schema search_paths cross-contaminate
// through the single-tenant ListenDSN exactly as two tenants would: NOTIFY is
// database-wide and both feeds listen on the same channel. Start must refuse
// the DSN before it opens anything.
func TestPostgresStart_SchemaIsolatedListenDSNIsRefused(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
	}{
		{name: "search_path parameter", dsn: unreachableDSN + "&search_path=app"},
		{name: "SEARCH_PATH parameter", dsn: unreachableDSN + "&SEARCH_PATH=app"},
		{name: "options -c search_path", dsn: unreachableDSN + "&options=-csearch_path%3Dapp"},
		{name: "options -c SEARCH_PATH", dsn: unreachableDSN + "&options=-cSEARCH_PATH%3Dapp"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Store{
				cfg:      Config{Channel: defaultChannel, Table: defaultTable, Module: defaultModule, ListenDSN: tc.dsn},
				feeds:    map[string]*feed{},
				closedCh: make(chan struct{}),
			}

			err := s.Start(context.Background())
			if err == nil {
				t.Fatal("Start with a schema-isolated ListenDSN returned nil; want ErrSchemaIsolationUnsupported")
			}

			if !errors.Is(err, ErrSchemaIsolationUnsupported) {
				t.Fatalf("Start error = %v, want ErrSchemaIsolationUnsupported", err)
			}

			if !strings.Contains(err.Error(), "listen dsn") {
				t.Errorf("Start error %q must name the listen DSN", err)
			}

			if n := feedCount(s); n != 0 {
				t.Errorf("feeds map holds %d entries after a refused ListenDSN, want 0", n)
			}

			if err := s.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
		})
	}
}

// A ListenDSN without a search_path still reaches the dialer, which is what
// makes the refusal above meaningful rather than a blanket Start failure.
func TestPostgresStart_PlainListenDSNStillDials(t *testing.T) {
	s := &Store{
		cfg:      Config{Channel: defaultChannel, Table: defaultTable, Module: defaultModule, ListenDSN: unreachableDSN},
		feeds:    map[string]*feed{},
		closedCh: make(chan struct{}),
	}

	err := s.Start(context.Background())
	if err == nil {
		t.Fatal("Start against a closed port returned nil; want the dial to fail")
	}

	if errors.Is(err, ErrSchemaIsolationUnsupported) {
		t.Fatalf("Start error = %v; a DSN without a search_path must not be refused as schema-isolated", err)
	}

	if !strings.Contains(err.Error(), "listen connect") {
		t.Errorf("Start error %q must come from the dialer", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// startListener must not open a second connection for a feed whose reader is
// already running: the zero-scope feed is shared, and two readers on it deliver
// every NOTIFY twice. The DSN here is unreachable, so a second dial surfaces as
// an error instead of passing silently.
func TestPostgresStartListener_SkipsWhenReaderAlreadyRunning(t *testing.T) {
	s := newSubscribeStore()
	s.cfg.ListenDSN = unreachableDSN

	s.feedsMu.Lock()
	f := s.feeds[""]
	s.feedsMu.Unlock()

	f.mu.Lock()
	f.done = make(chan struct{})
	f.mu.Unlock()

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start on a feed whose reader is already running: %v", err)
	}
}

// Close landing while a creator is still resolving the tenant's DSN must stop
// the dial, not merely throw the connection away after it succeeded. The
// connector closes the store from inside ResolveDSN, which is the window
// acquireFeed's shutdown fence cannot cover; unreachableDSN is the
// discriminator, because a dial that happens at all fails with a connect error
// instead of store.ErrClosed.
func TestPostgresSubscribe_CloseDuringResolutionSkipsTheDial(t *testing.T) {
	s := newSubscribeStore()
	s.cfg.MultiTenantEnabled = true

	conn := &stubConnector{}
	conn.resolve = func(int) (string, error) {
		s.feedsMu.Lock()
		s.closing = true
		s.feedsMu.Unlock()

		return unreachableDSN, nil
	}
	s.cfg.Connector = conn

	unsub, err := s.Subscribe(context.Background(), store.Scope{Tenant: "t1"}, func(store.Event) {})
	if err == nil {
		unsub()
		t.Fatal("Subscribe on a store closed mid-resolution returned nil; want store.ErrClosed")
	}

	if !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Subscribe error = %v, want store.ErrClosed", err)
	}

	if n := feedCount(s); n != 1 {
		t.Errorf("feeds map holds %d entries, want only the zero-scope feed", n)
	}

	waitForObserverExit(t)
}
