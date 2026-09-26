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
	// activating holds the slot of each scope being brought up or dropped, true
	// once Block or Reactivate superseded what its holder is building.
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

	return e.launchActivation(scope, false)
}

// launchActivation claims scope's slot and brings the scope up on a goroutine
// of its own; a rebuild starts superseded, so the tracked scope is dropped
// first. The caller holds activationsMu.
func (e *Engine) launchActivation(scope store.Scope, rebuild bool) bool {
	if !e.beginWork() {
		return false
	}

	e.activating[scope] = rebuild
	begun := time.Now()

	runtime.SafeGoWithContextAndComponent(e.dispatchContext(), e.logger,
		"systemplane.engine", "activate", runtime.KeepRunning, func(ctx context.Context) {
			defer e.dispatchWG.Done()

			up := false
			defer func() {
				// Metrics time a first activation, a superseded one included; never a rebuild.
				if e.endActivation(ctx, scope, up) && !rebuild {
					e.metrics.recordActivation(scope, begun)
				}
			}()

			up = !rebuild && e.activate(ctx, scope) == nil
		})

	return true
}

// Block drops scope in the background and refuses every later Activate for it
// until Unblock.
func (e *Engine) Block(scope store.Scope) {
	if e == nil {
		return
	}

	e.activationsMu.Lock()
	defer e.activationsMu.Unlock()

	e.blocked[scope] = struct{}{}
	e.supersede(scope)
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

// Reactivate rebuilds a tracked scope on a fresh feed in the background; an
// untracked scope only loses its retry cooldown, and a blocked one is left alone.
func (e *Engine) Reactivate(scope store.Scope) {
	if e == nil {
		return
	}

	e.activationsMu.Lock()
	defer e.activationsMu.Unlock()

	if _, blocked := e.blocked[scope]; blocked {
		return
	}

	delete(e.failedAt, scope)
	e.supersede(scope)
}

// supersede has scope's slot holder drop what it built, launching a holder for
// a tracked scope nothing holds. The holder keeps the slot from the drop to the
// rebuild, so no read opens a feed in between. The caller holds activationsMu.
func (e *Engine) supersede(scope store.Scope) {
	if _, inFlight := e.activating[scope]; inFlight {
		e.activating[scope] = true
	} else if e.trackedScope(scope) != nil {
		e.launchActivation(scope, true)
	}
}

// activate brings scope up and waits for its first reconcile. A failure drops
// the scope whole: unlike Start, a tenant has no caller to hand a stale
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

// endActivation settles scope's activation and reports whether the scope came
// up. While Block or Reactivate has superseded it, the scope is dropped with the
// slot still held and built again unless it is now blocked or the engine closed.
func (e *Engine) endActivation(ctx context.Context, scope store.Scope, up bool) bool {
	for !e.releaseActivation(scope, up) {
		e.dropScope(scope)

		e.activationsMu.Lock()
		_, blocked := e.blocked[scope]
		e.activationsMu.Unlock()

		up = !blocked && !e.closed.Load() && e.activate(ctx, scope) == nil
	}

	if up {
		e.logger.Log(ctx, log.LevelInfo, "scope activated", []log.Field{log.String(constants.AttrKeyTenantID, scope.Tenant)})
	}

	return up
}

// releaseActivation frees scope's slot and starts or clears its retry cooldown,
// unless the activation was superseded: it then clears that mark, keeps the
// slot and reports false.
func (e *Engine) releaseActivation(scope store.Scope, up bool) bool {
	e.activationsMu.Lock()
	defer e.activationsMu.Unlock()

	if e.activating[scope] {
		e.activating[scope] = false

		return false
	}

	delete(e.activating, scope)

	if up {
		delete(e.failedAt, scope)
	} else {
		e.failedAt[scope] = time.Now()
	}

	return true
}
