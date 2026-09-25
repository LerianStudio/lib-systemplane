package engine

import (
	"context"
	"time"

	"github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/runtime"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// defaultActivationRetryDelay is how long a scope whose activation failed
// refuses the next one, so a tenant that cannot come up costs one attempt per
// window instead of one per read.
// ponytail: fixed cooldown; lib-commons/backoff per scope if a real outage shows it is too coarse
const defaultActivationRetryDelay = 5 * time.Second

// Activate brings scope up in the background and reports whether this call
// started it. It takes no ctx: the feed outlives the request that asked, so it
// runs on the engine's lifecycle context. Refused in flight, tracked or cooling down.
func (e *Engine) Activate(scope store.Scope) (started bool) {
	if e == nil {
		return false
	}

	e.activationsMu.Lock()
	defer e.activationsMu.Unlock()

	if _, inFlight := e.activating[scope]; inFlight || e.closed.Load() || e.trackedScope(scope) != nil {
		return false
	}

	if failed, ok := e.failedAt[scope]; ok && time.Since(failed) < e.activationRetryDelay {
		return false
	}

	if !e.beginWork() {
		return false
	}

	e.activating[scope] = struct{}{}

	runtime.SafeGoWithContextAndComponent(e.dispatchContext(), e.logger,
		"systemplane.engine", "activate", runtime.KeepRunning, func(ctx context.Context) {
			defer e.dispatchWG.Done()

			failed := true
			defer func() { e.endActivation(scope, failed) }()

			failed = e.activate(ctx, scope) != nil
		})

	return true
}

// activate brings scope up and waits for its first reconcile. A failure drops
// the scope whole (D7): unlike Start, a tenant has no caller to hand a stale
// scope to. Close owns the teardown of a scope it interrupts.
func (e *Engine) activate(ctx context.Context, scope store.Scope) error {
	sc, err := e.bringUpScope(scope)
	if err == nil {
		select {
		case <-sc.firstReconcileDone:
		case <-ctx.Done():
			return ctx.Err()
		}

		sc.mu.RLock()
		err = sc.firstReconcileErr
		sc.mu.RUnlock()
	}

	tenant := log.String(constants.AttrKeyTenantID, scope.Tenant)

	if err != nil {
		e.dropScope(scope) // a no-op when bringUpScope already dropped it
		e.logWarn(ctx, "scope activation failed; reads stay per-request until a later attempt", tenant, log.Err(err))

		return err
	}

	e.logger.Log(ctx, log.LevelInfo, "scope activated", []log.Field{tenant})

	return nil
}

// endActivation frees scope's slot and starts or clears its retry cooldown.
func (e *Engine) endActivation(scope store.Scope, failed bool) {
	e.activationsMu.Lock()
	defer e.activationsMu.Unlock()

	delete(e.activating, scope)

	if failed {
		e.failedAt[scope] = time.Now()
	} else {
		delete(e.failedAt, scope)
	}
}
