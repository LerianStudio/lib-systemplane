package engine

import (
	"context"
	"sync"
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

// activations is the per-scope state of the scope verbs. activationsMu is
// never held across Store.Subscribe, a reconcile or a drop.
type activations struct {
	activationsMu sync.Mutex
	// activating holds each scope being brought up, true once Block or
	// Reactivate superseded it: its own goroutine then drops and redoes it.
	activating           map[store.Scope]bool
	blocked              map[store.Scope]struct{}
	failedAt             map[store.Scope]time.Time
	activationRetryDelay time.Duration
}

func newActivations() activations {
	return activations{
		activating:           make(map[store.Scope]bool),
		blocked:              make(map[store.Scope]struct{}),
		failedAt:             make(map[store.Scope]time.Time),
		activationRetryDelay: defaultActivationRetryDelay,
	}
}

// Activate brings scope up in the background on the engine's lifecycle context
// (the feed outlives the request that asked) and reports whether this call
// started it; a blocked, in-flight, tracked or cooling-down scope is refused.
func (e *Engine) Activate(scope store.Scope) (started bool) {
	if e == nil {
		return false
	}

	e.activationsMu.Lock()
	defer e.activationsMu.Unlock()

	_, blocked := e.blocked[scope]
	if _, inFlight := e.activating[scope]; blocked || inFlight || e.closed.Load() || e.trackedScope(scope) != nil {
		return false
	}

	if failed, ok := e.failedAt[scope]; ok && time.Since(failed) < e.activationRetryDelay {
		return false
	}

	if !e.beginWork() {
		return false
	}

	e.activating[scope] = false

	runtime.SafeGoWithContextAndComponent(e.dispatchContext(), e.logger,
		"systemplane.engine", "activate", runtime.KeepRunning, func(ctx context.Context) {
			defer e.dispatchWG.Done()

			failed := true
			defer func() { e.endActivation(ctx, scope, failed) }()

			failed = e.activate(ctx, scope) != nil
		})

	return true
}

// Block drops scope and refuses every later Activate for it until Unblock. A
// scope still being brought up is dropped by its own activation when it ends:
// dropping it mid-Subscribe would orphan the feed that Subscribe opens.
func (e *Engine) Block(scope store.Scope) {
	if e == nil {
		return
	}

	e.activationsMu.Lock()
	e.blocked[scope] = struct{}{}

	_, inFlight := e.activating[scope]
	if inFlight {
		e.activating[scope] = true
	}
	e.activationsMu.Unlock()

	if !inFlight {
		e.dropScope(scope)
	}
}

// Unblock clears that refusal and any retry cooldown. It does NOT activate: a
// read brings the scope up on its own.
func (e *Engine) Unblock(scope store.Scope) {
	if e == nil {
		return
	}

	e.activationsMu.Lock()
	defer e.activationsMu.Unlock()

	delete(e.blocked, scope)
	delete(e.failedAt, scope)
}

// Reactivate drops a tracked scope and activates it again; an untracked one only
// loses its retry cooldown, and a blocked one is left alone. A read racing the
// drop wins or loses the slot through the same connector: one fresh feed.
func (e *Engine) Reactivate(scope store.Scope) (started bool) {
	if e == nil {
		return false
	}

	e.activationsMu.Lock()

	_, blocked := e.blocked[scope]
	_, inFlight := e.activating[scope]
	tracked := e.trackedScope(scope) != nil

	if !blocked {
		delete(e.failedAt, scope)

		if inFlight {
			e.activating[scope] = true
		}
	}
	e.activationsMu.Unlock()

	if blocked || inFlight || !tracked {
		return false
	}

	e.dropScope(scope)

	return e.Activate(scope)
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

	if err != nil {
		e.dropScope(scope) // a no-op when bringUpScope already dropped it
		e.logWarn(ctx, "scope activation failed; reads stay per-request until a later attempt",
			log.String(constants.AttrKeyTenantID, scope.Tenant), log.Err(err))
	}

	return err
}

// endActivation frees scope's slot and starts or clears its retry cooldown. A
// superseded activation's outcome is discarded: its scope is dropped and
// activated again, which a blocked scope refuses.
func (e *Engine) endActivation(ctx context.Context, scope store.Scope, failed bool) {
	e.activationsMu.Lock()
	superseded := e.activating[scope]
	delete(e.activating, scope)

	switch {
	case superseded:
	case failed:
		e.failedAt[scope] = time.Now()
	default:
		delete(e.failedAt, scope)
	}
	e.activationsMu.Unlock()

	switch {
	case superseded:
		e.dropScope(scope)
		e.Activate(scope)
	case !failed:
		e.logger.Log(ctx, log.LevelInfo, "scope activated", []log.Field{log.String(constants.AttrKeyTenantID, scope.Tenant)})
	}
}
