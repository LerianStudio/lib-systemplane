// Per-tenant LISTEN/NOTIFY goroutine for the Manager.
//
// Each active tenant has one dedicated pgx connection running
//
//	LISTEN systemplane_changes
//
// against the tenant's primary database. NOTIFY payloads carry
// {namespace, key, op} JSON; the goroutine decodes them, updates the
// per-tenant cache (or evicts on delete), and dispatches OnChange
// callbacks.
//
// Reconnect uses exponential backoff capped at backoffCapSeconds. After
// staleAfterFailures consecutive reconnect failures the cache is marked
// stale (next Get falls through to the DB) but the loop continues
// retrying — per design, the Manager never gives up entirely.
package manager

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/LerianStudio/lib-commons/v6/commons/backoff"
	"github.com/LerianStudio/lib-observability/v2/log"
	libRuntime "github.com/LerianStudio/lib-observability/v2/runtime"
	"github.com/bxcodec/dbresolver/v2"
	"github.com/jackc/pgx/v5"
)

const (
	listenCloseTimeout = 5 * time.Second
)

// notifyPayload is the JSON shape emitted by the systemplane_notify_v3 trigger.
type notifyPayload struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Op        string `json:"op"`
}

// startListen opens a dedicated LISTEN connection for tenantID and launches
// the per-tenant goroutine. Idempotent — calling twice for the same tenant
// is a no-op. The handle is stored on ts.listen so OnTenantSuspended /
// OnTenantDeleted / OnTenantCredentialsRotated can cancel it.
//
// The caller MUST hold no locks on ts; startListen takes ts.mu internally
// to install the handle.
func (m *Manager) startListen(ctx context.Context, tenantID string, ts *tenantState) error {
	if m == nil || m.connector == nil || ts == nil {
		return ErrPgMgrUnavailable
	}

	ts.mu.Lock()

	if ts.listen != nil {
		ts.mu.Unlock()

		return nil
	}

	// Reserve the slot under the lock so a concurrent startListen does not
	// race to launch a second goroutine.
	handle := &listenHandle{done: make(chan struct{})}
	ts.listen = handle
	ts.mu.Unlock()

	dsn, err := m.tenantDSN(ctx, tenantID)
	if err != nil {
		ts.mu.Lock()
		ts.listen = nil
		ts.mu.Unlock()

		return err
	}

	// Open the first connection synchronously to surface immediate failures
	// instead of looping forever in the background.
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		ts.mu.Lock()
		ts.listen = nil
		ts.mu.Unlock()

		return fmt.Errorf("systemplane/manager: listen connect: %w", err)
	}

	if _, err := conn.Exec(ctx, `LISTEN `+quoteIdentifier(defaultChannel)); err != nil {
		_ = conn.Close(ctx)

		ts.mu.Lock()
		ts.listen = nil
		ts.mu.Unlock()

		return fmt.Errorf("systemplane/manager: listen: %w", err)
	}

	// cancel is stored on the handle and invoked from stopListen() to
	// terminate the goroutine. gosec G118 wrongly flags this as a leaked
	// cancel because it cannot see the deferred invocation across the
	// goroutine boundary.
	loopCtx, cancel := context.WithCancel(m.lifecycleContext())
	handle.cancel = cancel

	m.logInfo(ctx, "manager LISTEN established",
		log.String("tenant_id", tenantID),
	)

	libRuntime.SafeGo(m.logger,
		"systemplane.manager.listen."+tenantID,
		libRuntime.KeepRunning,
		func() {
			defer close(handle.done)

			m.consumeAndReconnect(loopCtx, tenantID, ts, conn, dsn)
		},
	)

	return nil
}

// stopListen cancels the per-tenant LISTEN goroutine and waits up to
// listenCloseTimeout for it to exit. Idempotent.
func (m *Manager) stopListen(ts *tenantState) {
	m.stopListenCtx(context.Background(), ts)
}

// stopListenCtx is the ctx-aware variant of stopListen used by Drain. When
// ctx cancels before the goroutine exits, the wait is abandoned so a
// shutdown budget can bound total drain time even if a single LISTEN
// reader is wedged (e.g. the Postgres server stopped responding).
func (m *Manager) stopListenCtx(ctx context.Context, ts *tenantState) {
	if ts == nil {
		return
	}

	ts.mu.Lock()
	handle := ts.listen
	ts.listen = nil
	ts.mu.Unlock()

	if handle == nil {
		return
	}

	if handle.cancel != nil {
		handle.cancel()
	}

	if handle.done == nil {
		return
	}

	timer := time.NewTimer(listenCloseTimeout)
	defer timer.Stop()

	select {
	case <-handle.done:
	case <-timer.C:
	case <-ctx.Done():
	}
}

// consumeAndReconnect runs the per-tenant LISTEN loop with reconnect.
//
// On each iteration it:
//   - waits for a NOTIFY on conn (or for ctx to cancel)
//   - on a notification: decodes, updates the cache, dispatches callbacks
//   - on a connection drop: closes conn, marks the cache stale after
//     staleAfterFailures consecutive failures, reconnects with exponential
//     backoff capped at backoffCapSeconds, and resumes
//
// Exits only when ctx is canceled.
func (m *Manager) consumeAndReconnect(ctx context.Context, tenantID string, ts *tenantState, conn *pgx.Conn, dsn string) {
	current := conn

	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), listenCloseTimeout)

		_ = current.Close(closeCtx)

		cancel()
	}()

	failures := 0
	staleAfter := m.cfg.listenStaleAfterFailures

	for {
		m.consumeUntilFailure(ctx, tenantID, ts, current)

		select {
		case <-ctx.Done():
			return
		default:
		}

		// Close failed connection before attempting to reopen.
		closeCtx, closeCancel := context.WithTimeout(context.Background(), listenCloseTimeout)

		_ = current.Close(closeCtx)

		closeCancel()

		failures++

		m.metrics.recordListenDisconnect(ctx, tenantID, "wait_failed")

		if staleAfter > 0 && failures >= staleAfter {
			ts.markStale()
			m.logWarn(ctx, "manager LISTEN: cache marked stale after consecutive failures",
				log.String("tenant_id", tenantID),
				log.Int("consecutive_failures", failures),
			)
		}

		next, err := m.reconnect(ctx, tenantID, dsn, failures)
		if err != nil {
			// ctx canceled.
			return
		}

		current = next
		failures = 0

		ts.clearStale()
		m.logInfo(ctx, "manager LISTEN reconnected",
			log.String("tenant_id", tenantID),
		)
	}
}

// consumeUntilFailure pumps notifications from conn until WaitForNotification
// returns an error or ctx is canceled.
func (m *Manager) consumeUntilFailure(ctx context.Context, tenantID string, ts *tenantState, conn *pgx.Conn) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				m.logDebug(ctx, "manager LISTEN wait failed",
					log.String("tenant_id", tenantID),
					log.Err(err),
				)
			}

			return
		}

		evt, ok := decodeNotifyPayload(notification.Payload)
		if !ok {
			m.logWarn(ctx, "manager LISTEN: malformed payload, ignored",
				log.String("tenant_id", tenantID),
			)

			continue
		}

		m.metrics.recordNotifyReceived(ctx, tenantID, evt.Op)
		m.applyEvent(ctx, tenantID, ts, evt)
	}
}

// reconnect retries pgx.Connect + LISTEN with exponential backoff capped at
// backoffCapSeconds. Returns the live conn or an error when ctx is canceled.
//
// Continues retrying forever — per design the Manager never gives up.
func (m *Manager) reconnect(ctx context.Context, tenantID, dsn string, failuresSoFar int) (*pgx.Conn, error) {
	base := time.Duration(m.cfg.listenBackoffBaseMillis) * time.Millisecond
	if base <= 0 {
		base = 500 * time.Millisecond
	}

	maxDelay := time.Duration(m.cfg.listenBackoffCapSeconds) * time.Second
	if maxDelay <= 0 {
		maxDelay = 30 * time.Second
	}

	attempt := failuresSoFar

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		delay := backoff.ExponentialWithJitter(base, attempt)
		if delay > maxDelay || delay < 0 {
			delay = maxDelay
		}

		attempt++

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}

		connectCtx, connectCancel := context.WithTimeout(context.Background(), 10*time.Second)

		conn, err := pgx.Connect(connectCtx, dsn)

		connectCancel()

		if err != nil {
			m.logDebug(ctx, "manager LISTEN reconnect attempt failed",
				log.String("tenant_id", tenantID),
				log.Err(err),
			)

			continue
		}

		listenCtx, listenCancel := context.WithTimeout(context.Background(), 5*time.Second)

		_, err = conn.Exec(listenCtx, `LISTEN `+quoteIdentifier(defaultChannel))

		listenCancel()

		if err != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), listenCloseTimeout)

			_ = conn.Close(closeCtx)

			closeCancel()

			continue
		}

		return conn, nil
	}
}

// readSingle reads one (namespace, key) row from the tenant DB. Returns
// (decoded value, true, nil) on success, (nil, false, nil) when the row is
// absent, or (nil, false, err) on read/decode failure.
func readSingle(ctx context.Context, db dbresolver.DB, namespace, key string) (any, bool, error) {
	query := fmt.Sprintf(`SELECT value FROM %s WHERE namespace = $1 AND key = $2`, defaultTable)

	var raw []byte

	row := db.QueryRowContext(ctx, query, namespace, key)
	if err := row.Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("systemplane/manager: read %s/%s: %w", namespace, key, err)
	}

	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, false, fmt.Errorf("systemplane/manager: decode %s/%s: %w", namespace, key, err)
	}

	return decoded, true, nil
}

// quoteIdentifier wraps a SQL identifier in double quotes. The channel name
// is validated by the existing safeIdentifierRe in internal/postgres; here
// defaultChannel is a constant so no validation is needed.
func quoteIdentifier(name string) string {
	return `"` + name + `"`
}

// tenantDSN returns the primary connection string for tenantID via the
// configured Connector.
func (m *Manager) tenantDSN(ctx context.Context, tenantID string) (string, error) {
	if m == nil || m.connector == nil {
		return "", ErrPgMgrUnavailable
	}

	return m.connector.ResolveDSN(ctx, tenantID)
}

// lifecycleContext returns the bound Client's lifecycle context so the
// LISTEN goroutine exits when the Client closes. Falls back to
// context.Background() if no hook is bound (test contexts).
func (m *Manager) lifecycleContext() context.Context {
	if m.hooks == nil {
		return context.Background()
	}

	if ctx := m.hooks.LifecycleContext(); ctx != nil {
		return ctx
	}

	return context.Background()
}
