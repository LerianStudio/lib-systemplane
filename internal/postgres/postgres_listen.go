// LISTEN/NOTIFY subscription loop for the single-tenant Postgres backend.
//
// Multi-tenant deployments resolve a fresh database on every call, so there
// is no shared process-wide changefeed to LISTEN on; Subscribe in that mode
// returns store.ErrNotSupportedInMultiTenant.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/LerianStudio/lib-commons/v6/commons/backoff"
	"github.com/LerianStudio/lib-observability/v2/log"
	"github.com/LerianStudio/lib-observability/v2/runtime"
	"github.com/LerianStudio/lib-systemplane/v2/internal/store"
	"github.com/jackc/pgx/v5"
)

const (
	backoffBase  = 500 * time.Millisecond
	backoffCap   = 30 * time.Second
	closeTimeout = 5 * time.Second
)

// notifyPayload is the JSON shape emitted by the systemplane_notify_v3 trigger.
type notifyPayload struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Op        string `json:"op"`
}

// Subscribe registers fn to be invoked for every change event. The returned
// unsubscribe func removes fn from the dispatch list.
//
// In multi-tenant mode the method returns store.ErrNotSupportedInMultiTenant
// — every method resolves a per-call tenant database, so there is no shared
// process-wide changefeed to attach to.
func (s *Store) Subscribe(ctx context.Context, fn func(store.Event)) (func(), error) {
	if s == nil || s.isClosed() {
		return nil, store.ErrClosed
	}

	if s.cfg.MultiTenantEnabled {
		return nil, store.ErrNotSupportedInMultiTenant
	}

	if fn == nil {
		return func() {}, nil
	}

	s.listenerMu.Lock()
	s.nextSubID++
	id := s.nextSubID
	s.subscribers[id] = fn
	s.listenerMu.Unlock()

	var once sync.Once

	return func() {
		once.Do(func() {
			s.listenerMu.Lock()
			delete(s.subscribers, id)
			s.listenerMu.Unlock()
		})
	}, nil
}

// startListener opens the dedicated pgx LISTEN connection and dispatches
// events to subscribers. It synchronously verifies LISTEN was installed
// before returning so callers can immediately observe events.
func (s *Store) startListener(ctx context.Context) error {
	s.listenerMu.Lock()
	if s.listenStop != nil {
		s.listenerMu.Unlock()

		return nil
	}

	s.listenerMu.Unlock()

	// Open the first connection synchronously to surface immediate failures
	// (bad DSN, server down) instead of looping forever in the background.
	conn, err := pgx.Connect(ctx, s.cfg.ListenDSN)
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

	stop := make(chan struct{})
	done := make(chan struct{})

	s.listenerMu.Lock()
	s.listenStop = stop
	s.listenDone = done
	s.listenerMu.Unlock()

	go func() {
		defer close(done)
		defer runtime.RecoverAndLog(s.cfg.Logger, "systemplane.postgres.listener")

		s.consumeAndReconnect(conn, stop)
	}()

	return nil
}

func (s *Store) stopListener() {
	s.listenerMu.Lock()
	stop := s.listenStop
	done := s.listenDone
	s.listenStop = nil
	s.listenDone = nil
	s.listenerMu.Unlock()

	if stop == nil {
		return
	}

	close(stop)

	if done != nil {
		select {
		case <-done:
		case <-time.After(closeTimeout):
		}
	}
}

// consumeAndReconnect handles events on conn and, on connection loss,
// reconnects with exponential backoff.
func (s *Store) consumeAndReconnect(conn *pgx.Conn, stop <-chan struct{}) {
	attempt := 0

	for {
		s.consumeUntilFailure(conn, stop)

		// Close the failed (or shutdown-time) connection before reconnect.
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), closeTimeout)

		_ = conn.Close(cleanCtx)

		cleanCancel()

		select {
		case <-stop:
			return
		default:
		}

		var err error

		conn, err = s.reconnect(stop, &attempt)
		if err != nil {
			return
		}
	}
}

func (s *Store) consumeUntilFailure(conn *pgx.Conn, stop <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-stop:
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

		s.dispatchEvent(evt)
	}
}

func (s *Store) reconnect(stop <-chan struct{}, attempt *int) (*pgx.Conn, error) {
	if *attempt == 0 {
		s.logWarn(context.Background(), "LISTEN connection lost, reconnecting",
			log.Int("attempt", *attempt),
		)
	}

	for {
		select {
		case <-stop:
			return nil, errors.New("stopped")
		default:
		}

		delay := min(backoff.ExponentialWithJitter(backoffBase, *attempt), backoffCap)
		*attempt++

		select {
		case <-stop:
			return nil, errors.New("stopped")
		case <-time.After(delay):
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		conn, err := pgx.Connect(ctx, s.cfg.ListenDSN)

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
