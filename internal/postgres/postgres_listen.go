// LISTEN/NOTIFY changefeeds for the Postgres backend.
//
// One feed per scope: the zero-scope feed is opened by Start and lives until
// Close. Multi-tenant deployments resolve a fresh database on every call, so
// the zero scope has no durable DSN to LISTEN on there and Subscribe returns
// store.ErrNotSupportedInMultiTenant.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/LerianStudio/lib-commons/v7/commons/backoff"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"github.com/jackc/pgx/v5"
)

const (
	backoffBase  = 500 * time.Millisecond
	backoffCap   = 30 * time.Second
	closeTimeout = 5 * time.Second
)

// notifyPayload is the JSON shape emitted by the systemplane_notify_v3 trigger.
//
// Revision is optional: a v3 trigger omits it and the event carries revision 0
// ("unknown"), which the engine never deduplicates.
type notifyPayload struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Op        string `json:"op"`
	Revision  int64  `json:"revision"`
}

// feed is one changefeed for one scope. The zero-scope feed is created by
// Start and lives until Close; a named-tenant feed is created by the first
// Subscribe for that tenant and torn down when its last subscriber leaves.
type feed struct {
	scope store.Scope
	dsn   string

	mu           sync.Mutex
	subs         map[uint64]*subscription
	nextID       uint64
	connected    bool // true between a successful LISTEN and the loss of that connection
	disconnected bool // true once OpDisconnect has been emitted for the CURRENT outage
	closing      bool // set by teardown under mu, BEFORE stop is closed

	stop chan struct{}
	done chan struct{}
}

// subscription serializes delivery to one callback. sub.mu is held for the
// whole of fn, which is what lets Subscribe emit the joining subscriber's
// OpResync without racing the reader goroutine.
type subscription struct {
	mu sync.Mutex
	fn func(store.Event)
}

// newFeed builds an unconnected feed. done stays nil until its reader
// goroutine launches, which is what marks the feed as running.
func newFeed(scope store.Scope, dsn string) *feed {
	return &feed{
		scope: scope,
		dsn:   dsn,
		subs:  make(map[uint64]*subscription),
		stop:  make(chan struct{}),
	}
}

// snapshotLocked copies the subscriber set so it can be fanned out to after
// f.mu is released. The caller MUST already hold f.mu.
func (f *feed) snapshotLocked() []*subscription {
	subs := make([]*subscription, 0, len(f.subs))

	for _, sub := range f.subs {
		subs = append(subs, sub)
	}

	return subs
}

// beginDisconnect decides, atomically with any concurrent teardown, whether
// this connection loss must emit OpDisconnect to the returned subscribers.
// It is EDGE-TRIGGERED: ok is true only on the connected→disconnected
// transition. ok is false when f.closing is already set (clean shutdown, see
// below) or when f.disconnected is already set (a reconnect attempt failed
// while the feed was already known to be down — one outage, one disconnect).
// Teardown sets f.closing under f.mu before closing f.stop, so a teardown
// that wins the race is always visible here.
func (f *feed) beginDisconnect() (subs []*subscription, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closing || f.disconnected {
		return nil, false
	}

	f.connected = false
	f.disconnected = true

	return f.snapshotLocked(), true
}

// beginResync marks the feed connected again, clears f.disconnected so the
// next real loss can emit once more, and returns the subscribers to receive
// OpResync. Called after every successful (re)LISTEN.
func (f *feed) beginResync() (subs []*subscription) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.connected = true
	f.disconnected = false

	return f.snapshotLocked()
}

// deliverLocked runs fn under runtime.RecoverAndLog. The caller MUST already
// hold sub.mu; deliver is the variant that takes it. Both routes are
// panic-safe, and every caller unlocks through defer, so a panicking callback
// can never leave sub.mu held.
func (sub *subscription) deliverLocked(logger log.Logger, evt store.Event) {
	defer runtime.RecoverAndLog(logger, "systemplane.postgres.handler")

	sub.fn(evt)
}

func (sub *subscription) deliver(logger log.Logger, evt store.Event) {
	sub.mu.Lock()
	defer sub.mu.Unlock()

	sub.deliverLocked(logger, evt)
}

// broadcast fans a synthesized marker (OpResync / OpDisconnect) out to an
// already-taken snapshot of subscribers, outside f.mu.
func (s *Store) broadcast(subs []*subscription, evt store.Event) {
	for _, sub := range subs {
		sub.deliver(s.cfg.Logger, evt)
	}
}

// zeroFeed returns the zero-scope feed, creating it when Subscribe runs before
// Start. Callers must NOT hold feedsMu.
func (s *Store) zeroFeed() *feed {
	s.feedsMu.Lock()
	defer s.feedsMu.Unlock()

	return s.zeroFeedLocked()
}

func (s *Store) zeroFeedLocked() *feed {
	if f, ok := s.feeds[""]; ok {
		return f
	}

	f := newFeed(store.Scope{}, s.cfg.ListenDSN)
	s.feeds[""] = f

	return f
}

// Subscribe registers fn to be invoked for every change event. The returned
// unsubscribe func removes fn from the dispatch list.
//
// In multi-tenant mode, and for any named tenant scope, the method returns
// store.ErrNotSupportedInMultiTenant — every method resolves a per-call tenant
// database, so there is no shared process-wide changefeed to attach to.
func (s *Store) Subscribe(ctx context.Context, scope store.Scope, fn func(store.Event)) (func(), error) {
	if s == nil || s.isClosed() {
		return nil, store.ErrClosed
	}

	if s.cfg.MultiTenantEnabled || scope.Tenant != "" {
		return nil, store.ErrNotSupportedInMultiTenant
	}

	if fn == nil {
		return func() {}, nil
	}

	f := s.zeroFeed()
	sub := &subscription{fn: fn}

	f.mu.Lock()
	f.nextID++
	id := f.nextID
	f.subs[id] = sub
	f.mu.Unlock()

	var once sync.Once

	return func() {
		once.Do(func() {
			f.mu.Lock()
			delete(f.subs, id)
			f.mu.Unlock()
		})
	}, nil
}

// startListener opens the zero-scope feed's dedicated pgx LISTEN connection
// and launches its reader. It synchronously verifies LISTEN was installed
// before returning, so callers can immediately observe events and a bad DSN
// surfaces as a Start error instead of looping in the background.
func (s *Store) startListener(ctx context.Context) error {
	f := s.zeroFeed()

	f.mu.Lock()
	running := f.done != nil
	f.mu.Unlock()

	if running {
		return nil
	}

	conn, err := pgx.Connect(ctx, f.dsn)
	if err != nil {
		return fmt.Errorf("systemplane/postgres: listen connect: %w", err)
	}

	if _, err := conn.Exec(ctx, "LISTEN "+quoteIdentifier(s.cfg.Channel)); err != nil {
		_ = conn.Close(ctx)

		return fmt.Errorf("systemplane/postgres: listen: %w", err)
	}

	s.logInfo(ctx, "LISTEN connection established",
		log.String("channel", s.cfg.Channel),
	)

	done := make(chan struct{})

	f.mu.Lock()
	f.done = done
	f.mu.Unlock()

	go func() {
		defer close(done)
		defer runtime.RecoverAndLog(s.cfg.Logger, "systemplane.postgres.listener")

		s.runFeed(f, conn)
	}()

	return nil
}

// stopFeeds tears down every feed the store owns. Idempotent.
func (s *Store) stopFeeds() {
	s.feedsMu.Lock()
	feeds := make([]*feed, 0, len(s.feeds))

	for _, f := range s.feeds {
		feeds = append(feeds, f)
	}

	clear(s.feeds)
	s.feedsMu.Unlock()

	for _, f := range feeds {
		s.stopFeed(f)
	}
}

// stopFeed marks the feed closing under f.mu BEFORE closing f.stop, so the
// reader's beginDisconnect can never announce a disconnect for a shutdown,
// then waits up to closeTimeout for the reader to exit.
func (s *Store) stopFeed(f *feed) {
	f.mu.Lock()

	if f.closing {
		f.mu.Unlock()

		return
	}

	f.closing = true
	done := f.done
	f.mu.Unlock()

	close(f.stop)

	if done == nil {
		return
	}

	select {
	case <-done:
	case <-time.After(closeTimeout):
	}
}

// runFeed is THE LISTEN loop, for every scope. conn is the already-connected,
// already-LISTENing first connection. Per connection it emits, in order:
// store.Event{Scope: f.scope, Op: store.OpResync}, then the decoded NOTIFY
// payloads from that connection, then — on connection loss, before the first
// reconnect attempt — exactly one
// store.Event{Scope: f.scope, Op: store.OpDisconnect}. Then it repeats
// through the existing backoff.
func (s *Store) runFeed(f *feed, conn *pgx.Conn) {
	attempt := 0

	for {
		s.broadcast(f.beginResync(), store.Event{Scope: f.scope, Op: store.OpResync})

		s.consumeUntilFailure(f, conn)

		if subs, ok := f.beginDisconnect(); ok {
			s.broadcast(subs, store.Event{Scope: f.scope, Op: store.OpDisconnect})
		}

		// Close the failed (or shutdown-time) connection before reconnect.
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), closeTimeout)

		_ = conn.Close(cleanCtx)

		cleanCancel()

		select {
		case <-f.stop:
			return
		default:
		}

		var err error

		conn, err = s.reconnect(f, &attempt)
		if err != nil {
			return
		}
	}
}

func (s *Store) consumeUntilFailure(f *feed, conn *pgx.Conn) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-f.stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.logDebug(ctx, "LISTEN wait failed", log.Err(err))
			}

			return
		}

		evt, ok := parseNotifyPayload(notification.Payload)
		if !ok {
			s.logWarn(ctx, "failed to decode NOTIFY payload",
				log.String("payload", truncateString(notification.Payload, 200)),
			)

			continue
		}

		f.dispatch(s.cfg.Logger, evt)
	}
}

func (s *Store) reconnect(f *feed, attempt *int) (*pgx.Conn, error) {
	if *attempt == 0 {
		s.logWarn(context.Background(), "LISTEN connection lost, reconnecting",
			log.Int("attempt", *attempt),
		)
	}

	for {
		select {
		case <-f.stop:
			return nil, errors.New("stopped")
		default:
		}

		delay := min(backoff.ExponentialWithJitter(backoffBase, *attempt), backoffCap)
		*attempt++

		select {
		case <-f.stop:
			return nil, errors.New("stopped")
		case <-time.After(delay):
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		conn, err := pgx.Connect(ctx, f.dsn)

		cancel()

		if err != nil {
			s.logDebug(context.Background(), "reconnect attempt failed", log.Err(err))

			continue
		}

		listenCtx, listenCancel := context.WithTimeout(context.Background(), 5*time.Second)

		_, err = conn.Exec(listenCtx, "LISTEN "+quoteIdentifier(s.cfg.Channel))

		listenCancel()

		if err != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), closeTimeout)

			_ = conn.Close(closeCtx)

			closeCancel()

			continue
		}

		*attempt = 0

		return conn, nil
	}
}
