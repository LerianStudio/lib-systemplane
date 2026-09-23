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

	"github.com/LerianStudio/lib-observability/v4/log"
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
	f, err := s.zeroFeedForStart()
	if err != nil {
		t.Fatalf("zeroFeedForStart: %v", err)
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
// on inside a unit test, and restores them afterwards.
//
// Writing package vars is safe here because every caller is a SEQUENTIAL test:
// Go resumes a parallel test only once the sequential tests have all finished,
// and a test finishes only after its t.Cleanup has run, so no parallel test can
// observe a shrunk bound. A caller that adds t.Parallel() breaks that and has
// to take a different route.
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

			f, err := s.zeroFeedForStart()
			if err != nil {
				t.Fatalf("zeroFeedForStart: %v", err)
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
	conn := &stubConnector{resolve: func(int) (string, error) { return unreachableDSN, nil }}

	s := newSubscribeStore()
	s.cfg.MultiTenantEnabled = true
	s.cfg.Connector = conn

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

	if calls := conn.callCount(); calls != 0 {
		t.Errorf("a closing store resolved %d tenant DSNs; want 0 — it must not dial", calls)
	}

	if n := feedCount(s); n != 1 {
		t.Errorf("feeds map holds %d entries, want only the zero-scope feed", n)
	}
}

// A tenant DSN that pins a schema is NOT refused on that basis: lib-commons
// writes "options=-csearch_path=<schema>" for any tenant whose config declares
// a schema, whatever its isolation mode, so a tenant with its own database
// that merely names a schema must still get its changefeed. What is refused is
// two scopes resolving to the same DATABASE, which needs two live feeds and is
// pinned by TestPostgresFeed_SharedDatabaseIsRefused and, end to end, by
// TestIntegration_PostgresTwoTenantsOnOneDatabase.
//
// unreachableDSN is the discriminator for "it was dialed": reaching the dialer
// is what proves nothing refused the DSN up front.
func TestPostgresSubscribe_SchemaPinnedDSNStillDials(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
	}{
		{name: "search_path parameter", dsn: unreachableDSN + "&search_path=tenant_a"},
		{name: "options -c search_path", dsn: unreachableDSN + "&options=-csearch_path%3Dtenant_a"},
		{name: "plain", dsn: unreachableDSN},
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
				t.Fatal("Subscribe to a closed port returned nil; want the dial to fail")
			}

			if errors.Is(subErr, ErrSharedDatabaseUnsupported) {
				t.Fatalf("Subscribe error = %v; the only tenant on this store shares no database with anything", subErr)
			}

			if !strings.Contains(subErr.Error(), "listen connect") {
				t.Errorf("Subscribe error %q must come from the dialer", subErr)
			}

			if n := feedCount(s); n != 0 {
				t.Errorf("feeds map holds %d entries after a failed dial, want 0", n)
			}

			if err := s.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			waitForObserverExit(t)
		})
	}
}

// refuseSharedDatabaseLocked is the whole one-database-per-scope decision, and
// it needs two LIVE feeds — which a unit test cannot dial — so it is driven
// directly here. A reserved slot still connecting carries no dbKey and must be
// invisible to it: it is not listening yet, so it cannot receive anything.
func TestPostgresFeed_SharedDatabaseIsRefused(t *testing.T) {
	live := newFeed(store.Scope{Tenant: "t1"}, "")
	live.dbKey = "db.example:5432/shared"

	connecting := newFeed(store.Scope{Tenant: "t3"}, "")

	s := &Store{feeds: map[string]*feed{"t1": live, "t3": connecting}}

	joiner := newFeed(store.Scope{Tenant: "t2"}, "")

	err := s.refuseSharedDatabaseLocked(joiner, "db.example:5432/shared")
	if err == nil {
		t.Fatal("a second scope on t1's database was admitted; want ErrSharedDatabaseUnsupported")
	}

	if !errors.Is(err, ErrSharedDatabaseUnsupported) {
		t.Fatalf("error = %v, want ErrSharedDatabaseUnsupported", err)
	}

	for _, want := range []string{`"t1"`, `"t2"`, "db.example:5432/shared"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q must name %s", err, want)
		}
	}

	if err := s.refuseSharedDatabaseLocked(joiner, "db.example:5432/its_own"); err != nil {
		t.Errorf("a scope with its own database was refused: %v", err)
	}

	// The connecting placeholder holds no database yet, so joining on the key
	// it will eventually take is admitted here and refused at publish time.
	if err := s.refuseSharedDatabaseLocked(joiner, ""); err != nil {
		t.Errorf("an unpublished slot collided on the empty key: %v", err)
	}

	// Republishing the same feed must not collide with itself.
	if err := s.refuseSharedDatabaseLocked(live, "db.example:5432/shared"); err != nil {
		t.Errorf("a feed collided with itself: %v", err)
	}
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

// A ListenDSN that pins a schema reaches the dialer like any other: the
// discriminator for the one-database rule is the DATABASE, not the schema, and
// a single-tenant install may legitimately name one.
//
// The PGOPTIONS case is the environment, not the DSN: pgconn.ParseConfig merges
// PGOPTIONS into the parsed runtime parameters before it reads the connection
// string, so a guard that judged a parsed search_path would refuse a
// completely clean DSN on any host that exports one, and the process could not
// boot.
func TestPostgresStart_SchemaPinnedListenDSNStillDials(t *testing.T) {
	cases := []struct {
		name    string
		dsn     string
		options string
	}{
		{name: "search_path parameter", dsn: unreachableDSN + "&search_path=app"},
		{name: "options -c search_path", dsn: unreachableDSN + "&options=-csearch_path%3Dapp"},
		{name: "clean DSN under PGOPTIONS", dsn: unreachableDSN, options: "-csearch_path=app"},
		{name: "plain", dsn: unreachableDSN},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.options != "" {
				t.Setenv("PGOPTIONS", tc.options)
			}

			s := &Store{
				cfg:      Config{Channel: defaultChannel, Table: defaultTable, Module: defaultModule, ListenDSN: tc.dsn},
				feeds:    map[string]*feed{},
				closedCh: make(chan struct{}),
			}

			err := s.Start(context.Background())
			if err == nil {
				t.Fatal("Start against a closed port returned nil; want the dial to fail")
			}

			if errors.Is(err, ErrSharedDatabaseUnsupported) {
				t.Fatalf("Start error = %v; this store has exactly one feed and shares no database", err)
			}

			if !strings.Contains(err.Error(), "listen connect") {
				t.Errorf("Start error %q must come from the dialer", err)
			}

			if err := s.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
		})
	}
}

// A malformed ListenDSN is still refused before anything reaches the network,
// and Start surfaces it instead of looping in the background. The refusal is
// now the dialer's: the database key is read off the connection the server
// answers on, not parsed out of the DSN text, so pgx is the only parser left.
func TestPostgresStart_UnparseableListenDSNIsRefused(t *testing.T) {
	s := &Store{
		cfg:      Config{Channel: defaultChannel, Table: defaultTable, Module: defaultModule, ListenDSN: "postgres://%zz"},
		feeds:    map[string]*feed{},
		closedCh: make(chan struct{}),
	}

	err := s.Start(context.Background())
	if err == nil {
		t.Fatal("Start with an unparseable ListenDSN returned nil; want a parse error")
	}

	if !strings.Contains(err.Error(), "listen connect") {
		t.Errorf("Start error %q must come from the dialer", err)
	}

	// The reserved zero-scope slot may survive a failed Start — Start is
	// retryable and reuses it — but it must carry no reader, or Close would
	// wait out the full timeout on a goroutine that never ran.
	s.feedsMu.Lock()
	f := s.feeds[""]
	s.feedsMu.Unlock()

	if f != nil {
		f.mu.Lock()
		running := f.done != nil
		f.mu.Unlock()

		if running {
			t.Error("a refused ListenDSN left a LISTEN reader running")
		}
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

// captureLogger records what the store logs so a test can pin an operator-
// facing message. Log is the only method with behavior; the rest satisfy the
// interface. Entries are guarded because some of the paths pinned below log
// from a background goroutine (the reconnect loop), which waitFor then polls.
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
// its []log.Field over as a single element, so the assertions below still read
// f.Key/f.Value.
func (c *captureLogger) Log(_ context.Context, level int, msg string, fields ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = append(c.entries, captureEntry{level: level, msg: msg, fields: log.Fields(fields...)})
}

func (c *captureLogger) With(...any) log.Logger      { return c }
func (c *captureLogger) WithGroup(string) log.Logger { return c }
func (c *captureLogger) Enabled(int) bool            { return true }
func (c *captureLogger) Sync(context.Context) error  { return nil }

// only returns the single entry logged at level, failing when the count is not
// exactly one.
func (c *captureLogger) only(t *testing.T, level int, what string) captureEntry {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	var found []captureEntry

	for _, e := range c.entries {
		if e.level == level {
			found = append(found, e)
		}
	}

	if len(found) != 1 {
		t.Fatalf("%s: logged %d entries at level %d, want exactly 1 (%+v)", what, len(found), level, c.entries)
	}

	return found[0]
}

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

// loggingStore builds the Store of newSubscribeStore with a capturing logger.
func loggingStore() (*Store, *captureLogger) {
	logger := &captureLogger{}

	s := newSubscribeStore()
	s.cfg.Logger = logger

	return s, logger
}

// TestFeed_SelfTeardownSkipIsLoggedWithItsTenant pins the one line an operator
// has to explain a feed that was torn down without waiting for its reader: the
// last subscriber of a tenant feed unsubscribed from inside that feed's own
// callback, so waiting would have been the reader waiting on itself. Without
// the tenant on the line, a process carrying dozens of feeds cannot say which
// one skipped the wait.
func TestFeed_SelfTeardownSkipIsLoggedWithItsTenant(t *testing.T) {
	s, logger := loggingStore()

	f := newFeed(store.Scope{Tenant: "t1"}, "")
	f.done = make(chan struct{})
	f.dispatching = 1

	if done := s.signalFeed(f, true); done != nil {
		t.Fatal("signalFeed returned a channel to wait on for a self-teardown")
	}

	entry := logger.only(t, log.LevelDebug, "self-teardown")

	if !strings.Contains(entry.msg, "not waiting for its reader") {
		t.Errorf("skip message = %q, want it to say the reader is not waited for", entry.msg)
	}

	if got := entry.field(t, "tenant"); got != "t1" {
		t.Errorf("tenant field = %v, want t1", got)
	}
}

// TestFeed_TeardownFromOutsideACallbackLogsNothing is the other half: a
// teardown that is NOT the reader tearing itself down waits for the reader and
// says nothing, so the line above stays a signal rather than shutdown noise.
func TestFeed_TeardownFromOutsideACallbackLogsNothing(t *testing.T) {
	s, logger := loggingStore()

	f := newFeed(store.Scope{Tenant: "t1"}, "")
	f.done = make(chan struct{})

	if done := s.signalFeed(f, true); done == nil {
		t.Fatal("signalFeed skipped the wait for a teardown reached from outside a callback")
	}

	if len(logger.entries) != 0 {
		t.Errorf("teardown logged %+v, want nothing", logger.entries)
	}
}

// TestStore_NotifyDecodeWarningNamesItsTenant pins the warning a garbage NOTIFY
// payload produces. The payload cannot name its tenant — the trigger fires in
// the tenant's database and knows nothing about tenants — so the feed that read
// it is the only thing that can, and an operator staring at a process with many
// tenant feeds needs exactly that. The payload rides along truncated: it is
// written by whoever holds NOTIFY rights on the channel and must not be able to
// stretch a log line without bound.
func TestStore_NotifyDecodeWarningNamesItsTenant(t *testing.T) {
	s, logger := loggingStore()

	f := newFeed(store.Scope{Tenant: "t1"}, "")

	var delivered []store.Event

	f.subs[1] = &subscription{fn: func(evt store.Event) { delivered = append(delivered, evt) }}

	s.handleNotification(context.Background(), f, `{"namespace":"ns","key":`+strings.Repeat("x", 500))

	entry := logger.only(t, log.LevelWarn, "undecodable payload")

	if !strings.Contains(entry.msg, "decode NOTIFY payload") {
		t.Errorf("warning = %q, want it to name the NOTIFY payload", entry.msg)
	}

	if got := entry.field(t, "tenant"); got != "t1" {
		t.Errorf("tenant field = %v, want t1", got)
	}

	payload, ok := entry.field(t, "payload").(string)
	if !ok {
		t.Fatalf("payload field = %T, want a string", entry.field(t, "payload"))
	}

	if len(payload) != 203 || !strings.HasSuffix(payload, "...") {
		t.Errorf("payload field is %d chars ending %q, want 200 plus an ellipsis", len(payload), payload[max(0, len(payload)-3):])
	}

	if len(delivered) != 0 {
		t.Errorf("an undecodable payload was delivered as %+v", delivered)
	}

	// A payload that decodes is dispatched and logs nothing.
	s.handleNotification(context.Background(), f, `{"namespace":"ns","key":"k","op":"upsert","revision":7}`)

	if len(logger.entries) != 1 {
		t.Errorf("a valid payload logged %+v, want nothing beyond the warning above", logger.entries[1:])
	}

	if len(delivered) != 1 || delivered[0].Key != "k" || delivered[0].Revision != 7 || delivered[0].Scope != f.scope {
		t.Errorf("delivered = %+v, want one ns/k upsert at revision 7 stamped with the feed's scope", delivered)
	}
}

// A refusal at publish time — a tenant feed already listening on the database
// ListenDSN names, or a Close landing mid-connect — must NOT drop the
// zero-scope slot. Subscribe can run before Start, so subscribers already hold
// that feed: retracting it strands them on a feed nothing reconnects and nothing
// tears down, while the next zero-scope Subscribe silently gets a fresh feed
// that has announced nothing and looks healthy.
func TestPostgresZeroFeed_RefusalKeepsTheSlotAndIsReported(t *testing.T) {
	s := newSubscribeStore()

	s.feedsMu.Lock()
	f := s.feeds[""]
	s.feedsMu.Unlock()

	events := make(chan store.Event, 4)

	unsub, err := s.Subscribe(context.Background(), store.Scope{}, func(evt store.Event) {
		events <- evt
	})
	if err != nil {
		t.Fatalf("subscribe before Start: %v", err)
	}

	t.Cleanup(unsub)

	// Start reaches publishFeed and is refused.
	s.feedsMu.Lock()
	cause := s.failLocked(f, ErrSharedDatabaseUnsupported)
	s.feedsMu.Unlock()

	if !errors.Is(cause, ErrSharedDatabaseUnsupported) {
		t.Fatalf("recorded cause = %v, want ErrSharedDatabaseUnsupported", cause)
	}

	s.feedsMu.Lock()
	kept := s.feeds[""]
	s.feedsMu.Unlock()

	if kept != f {
		t.Fatalf("the zero-scope slot holds %p after a refused publish, want the feed its subscribers hold (%p)", kept, f)
	}

	// A later zero-scope Subscribe must fail loudly rather than attach to a
	// feed nothing is bringing up.
	unsub2, err := s.Subscribe(context.Background(), store.Scope{}, func(store.Event) {})
	if err == nil {
		unsub2()
		t.Fatal("Subscribe after a refused Start returned nil; want the recorded refusal")
	}

	if !errors.Is(err, ErrSharedDatabaseUnsupported) {
		t.Fatalf("Subscribe error = %v, want the recorded ErrSharedDatabaseUnsupported", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	waitForObserverExit(t)
}

// The zero scope is the exception, not the rule: a NAMED tenant's creation
// failure still retracts its slot, so the next Subscribe for that tenant builds
// a fresh placeholder instead of finding a corpse.
func TestPostgresFeed_NamedFailureStillRetractsItsSlot(t *testing.T) {
	s := newSubscribeStore()

	f := newFeed(store.Scope{Tenant: "t1"}, "")
	f.ready = make(chan struct{})

	s.feedsMu.Lock()
	s.feeds["t1"] = f
	_ = s.failLocked(f, ErrSharedDatabaseUnsupported)
	_, still := s.feeds["t1"]
	s.feedsMu.Unlock()

	if still {
		t.Fatal("a failed tenant feed kept its slot; the next Subscribe for that tenant would wait on a corpse")
	}
}

// A closing store must not dial, and the reconnect loop is the path that did:
// openListen fences on the shutdown flag before opening its socket, reconnect
// went straight to pgx.Connect. Without the fence a store torn down between
// the flag and the feed's stop signal keeps reconnecting to a database it will
// never read from again.
func TestPostgresReconnect_ClosingStoreDialsNothing(t *testing.T) {
	shrinkTimeouts(t, 250*time.Millisecond)

	s := newSubscribeStore()

	s.feedsMu.Lock()
	s.closing = true
	s.feedsMu.Unlock()

	f := newFeed(store.Scope{Tenant: "t1"}, unreachableDSN)

	done := make(chan error, 1)

	var retry reconnectBackoff

	go func() {
		_, err := s.reconnect(f, &retry)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, store.ErrClosed) {
			t.Fatalf("reconnect on a closing store = %v, want store.ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		close(f.stop)
		<-done
		t.Fatal("reconnect on a closing store never returned; it keeps dialing a store that is shutting down")
	}
}

// Start RETRIES: it brings up the same feed its subscribers already hold, and
// the refusal a previous attempt recorded belongs to that attempt, not to the
// feed forever.
func TestPostgresZeroFeed_StartRetriesTheSameFeed(t *testing.T) {
	s := newSubscribeStore()

	s.feedsMu.Lock()
	f := s.feeds[""]
	s.feedsMu.Unlock()

	events := make(chan store.Event, 4)

	unsub, err := s.Subscribe(context.Background(), store.Scope{}, func(evt store.Event) {
		events <- evt
	})
	if err != nil {
		t.Fatalf("subscribe before Start: %v", err)
	}

	t.Cleanup(unsub)

	s.feedsMu.Lock()
	_ = s.failLocked(f, ErrSharedDatabaseUnsupported)
	s.feedsMu.Unlock()

	retried, err := s.zeroFeedForStart()
	if err != nil {
		t.Fatalf("a retried Start was refused by the previous attempt's failure: %v", err)
	}

	if retried != f {
		t.Fatalf("a retried Start took feed %p, want the one the subscribers hold (%p)", retried, f)
	}

	// The reader of that retried connection announces its resync, and the
	// subscriber that attached before the refusal is the one that receives it.
	subs, ok := f.beginResync()
	if !ok {
		t.Fatal("beginResync refused on a retried feed")
	}

	s.broadcast(f, subs, store.Event{Scope: f.scope, Op: store.OpResync})

	select {
	case evt := <-events:
		if evt.Op != store.OpResync {
			t.Fatalf("subscriber received %+v, want OpResync", evt)
		}
	default:
		t.Fatal("the subscriber that attached before the refused Start received nothing from the retried feed")
	}

	// With the feed live again, a new zero-scope Subscribe is served.
	unsub2, err := s.Subscribe(context.Background(), store.Scope{}, func(store.Event) {})
	if err != nil {
		t.Fatalf("Subscribe after a successful retry: %v", err)
	}

	unsub2()

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	waitForObserverExit(t)
}

// A backend that accepts a connection and drops it at once — a pgbouncer in
// transaction pooling, an idle_session_timeout shorter than the quiet period
// between notifications, a primary mid-failover — used to hold the feed at the
// FIRST delay forever, because every successful connect reset the sequence
// before the drop was accounted for. Each iteration below is one such cycle:
// the connect succeeds, nothing is consumed, the connection dies immediately.
func TestPostgresReconnect_BackoffEscalatesOnAcceptThenDropCycles(t *testing.T) {
	var retry reconnectBackoff

	prev := time.Duration(0)

	for cycle := range 6 {
		ceiling := retry.ceiling()
		if ceiling <= prev {
			t.Fatalf("cycle %d: delay ceiling %v did not grow past %v; an accept-then-drop backend is being hammered at one delay", cycle, ceiling, prev)
		}

		prev = ceiling

		if delay := retry.next(); delay < 0 || delay >= ceiling {
			t.Fatalf("cycle %d: delay %v outside [0, %v)", cycle, delay, ceiling)
		}

		// The connection came up and died without carrying anything.
		retry.connectionEnded(false, time.Millisecond)
	}

	if retry.ceiling() > backoffCap {
		t.Fatalf("delay ceiling %v exceeded the cap %v", retry.ceiling(), backoffCap)
	}

	// A connection that carried a notification earned a fresh sequence.
	retry.connectionEnded(true, time.Millisecond)

	if got := retry.ceiling(); got != backoffBase {
		t.Fatalf("ceiling after a useful connection = %v, want the base %v", got, backoffBase)
	}

	// So did one that simply lasted: a feed quiet for longer than the cap is
	// healthy, not flapping.
	_ = retry.next()
	retry.connectionEnded(false, backoffCap)

	if got := retry.ceiling(); got != backoffBase {
		t.Fatalf("ceiling after a long-lived connection = %v, want the base %v", got, backoffBase)
	}
}

// The zero scope is held to the one-database rule in BOTH directions: a
// single-tenant ListenDSN pointed at a database a tenant feed already listens
// on is refused, and so is a tenant whose DSN lands on the ListenDSN's
// database. NOTIFY is database-wide, so either pairing would deliver one
// install's notifications to the other stamped with the wrong scope.
func TestPostgresFeed_SharedDatabaseIsRefusedAcrossTheZeroScope(t *testing.T) {
	const dbKey = "db.example:5432/shared"

	t.Run("tenant joining the single-tenant database", func(t *testing.T) {
		live := newFeed(store.Scope{}, "")
		live.dbKey = dbKey

		s := &Store{feeds: map[string]*feed{"": live}}

		err := s.refuseSharedDatabaseLocked(newFeed(store.Scope{Tenant: "t1"}, ""), dbKey)
		if !errors.Is(err, ErrSharedDatabaseUnsupported) {
			t.Fatalf("a tenant on the ListenDSN's database = %v, want ErrSharedDatabaseUnsupported", err)
		}

		if !strings.Contains(err.Error(), `"t1"`) {
			t.Errorf("error %q must name the refused tenant", err)
		}
	})

	t.Run("single-tenant feed joining a tenant database", func(t *testing.T) {
		live := newFeed(store.Scope{Tenant: "t1"}, "")
		live.dbKey = dbKey

		s := &Store{feeds: map[string]*feed{"t1": live}}

		err := s.refuseSharedDatabaseLocked(newFeed(store.Scope{}, ""), dbKey)
		if !errors.Is(err, ErrSharedDatabaseUnsupported) {
			t.Fatalf("a ListenDSN on t1's database = %v, want ErrSharedDatabaseUnsupported", err)
		}

		if !strings.Contains(err.Error(), `"t1"`) {
			t.Errorf("error %q must name the tenant already listening there", err)
		}
	})

	t.Run("its own database is admitted", func(t *testing.T) {
		live := newFeed(store.Scope{}, "")
		live.dbKey = dbKey

		s := &Store{feeds: map[string]*feed{"": live}}

		if err := s.refuseSharedDatabaseLocked(newFeed(store.Scope{Tenant: "t1"}, ""), "db.example:5432/t1"); err != nil {
			t.Errorf("a tenant with its own database was refused: %v", err)
		}
	})
}

// The delay is DRAWN, not taken: full jitter over [0, ceiling). A bounds-only
// assertion passes for an implementation that always returns the ceiling, which
// is precisely the lockstep this jitter exists to break — every feed of a
// process that lost one database would redial on the same tick.
func TestPostgresReconnect_DelayIsDrawnBelowItsCeiling(t *testing.T) {
	// A ceiling several steps up the sequence, so the window is wide enough
	// that repeated draws colliding is not a plausible outcome.
	const attempt = 4

	var retry reconnectBackoff

	retry.attempt = attempt
	ceiling := retry.ceiling()

	draws := make(map[time.Duration]struct{})

	for range 16 {
		retry.attempt = attempt

		delay := retry.next()
		if delay < 0 || delay >= ceiling {
			t.Fatalf("delay %v outside [0, %v)", delay, ceiling)
		}

		draws[delay] = struct{}{}
	}

	if len(draws) == 1 {
		t.Fatalf("16 draws at ceiling %v all returned the same delay; the jitter is gone and every feed of one outage reconnects in lockstep", ceiling)
	}
}

// A zero-scope Start whose connect is REFUSED must record the cause on the slot
// it retains. Without that record f.err stays nil, so the next
// Subscribe(Scope{}) succeeds and attaches to a feed with no reader behind it:
// it never receives OpResync or OpDisconnect, and the engine therefore reads
// that scope as fresh forever — the silent-stale state this feed exists to
// close. The publish-time refusal is pinned by
// TestPostgresZeroFeed_RefusalKeepsTheSlotAndIsReported, which drives failLocked
// directly; the connect path is the one that slipped through.
func TestPostgresZeroFeed_RefusedConnectIsReportedToSubscribe(t *testing.T) {
	shrinkTimeouts(t, 250*time.Millisecond)

	s := newSubscribeStore()
	s.cfg.ListenDSN = unreachableDSN

	s.feedsMu.Lock()
	f := s.feeds[""]
	f.dsn = unreachableDSN
	s.feedsMu.Unlock()

	startErr := s.Start(context.Background())
	if startErr == nil {
		t.Fatal("Start against a refused dialer returned nil")
	}

	unsub, err := s.Subscribe(context.Background(), store.Scope{}, func(store.Event) {})
	if err == nil {
		unsub()
		t.Fatal("Subscribe after a refused Start returned nil; the caller is attached to a changefeed no reader serves and will never be told it is stale")
	}

	if !errors.Is(err, startErr) {
		t.Fatalf("Subscribe error = %v, want the cause Start reported (%v)", err, startErr)
	}

	// The record belongs to the attempt that produced it: a retried Start takes
	// the very feed the subscribers hold, with the refusal cleared.
	retried, err := s.zeroFeedForStart()
	if err != nil {
		t.Fatalf("a retried Start was refused by the previous attempt's failure: %v", err)
	}

	if retried != f {
		t.Fatalf("a retried Start took feed %p, want the one the subscribers hold (%p)", retried, f)
	}

	unsub2, err := s.Subscribe(context.Background(), store.Scope{}, func(store.Event) {})
	if err != nil {
		t.Fatalf("Subscribe once the refusal was cleared: %v", err)
	}

	unsub2()

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	waitForObserverExit(t)
}

// A reconnect attempt that fails has exactly one exit, and it is logged with
// its cause. The LISTEN half used to be discarded with a bare continue: a feed
// that reconnects but can never re-install LISTEN — pgbouncer in transaction
// pooling refuses it, a revoked grant refuses it — then looped forever with
// nothing after the single initial warning to say why it was never delivering.
func TestPostgresReconnect_AttemptFailureIsLoggedWithItsCause(t *testing.T) {
	shrinkTimeouts(t, 250*time.Millisecond)

	s, logger := loggingStore()
	f := newFeed(store.Scope{Tenant: "t1"}, unreachableDSN)

	done := make(chan struct{})

	go func() {
		defer close(done)

		var retry reconnectBackoff

		_, _ = s.reconnect(f, &retry)
	}()

	entry := logger.waitFor(t, log.LevelDebug, "reconnect attempt failed")

	close(f.stop)
	<-done

	if entry.field(t, "tenant") != "t1" {
		t.Errorf("failed attempt logged tenant %v, want t1: a process carrying dozens of feeds cannot tell which one is down", entry.field(t, "tenant"))
	}

	cause, ok := entry.field(t, "error").(error)
	if !ok || cause == nil {
		t.Fatalf("failed attempt logged error field %v, want the cause", entry.field(t, "error"))
	}

	// The cause names the STAGE it died at. That is what tells a dial nothing
	// answered apart from a connection that came up and then refused LISTEN —
	// the case that used to leave no line at all.
	if !strings.Contains(cause.Error(), "listen connect") {
		t.Errorf("failed attempt logged %q; the cause must name the stage so a refused LISTEN reads differently from a refused dial", cause)
	}
}
