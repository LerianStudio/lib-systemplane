// Tenant-scoped registration and read/write/delete/list paths for the
// systemplane Client.
//
// This file covers the five primary tenant methods — RegisterTenantScoped,
// SetForTenant, GetForTenant, DeleteForTenant, ListTenantsForKey — plus the
// shared extractTenantID helper. The tenant-aware subscribe surface
// (OnTenantChange / fireTenantSubscribers) lives in tenant_onchange.go; the
// typed accessor mirrors (GetStringForTenant, GetIntForTenant, etc.) live in
// tenant_scoped_accessors.go.
//
// Dataflow summary (see TRD §4.1-4.5 for the full spec):
//
//	Set:    ctx → validate tenant → registry guard → validator → persist
//	        → write-through tenantCache. Subscribers fire from changefeed echo.
//	Get:    ctx → validate tenant → registry guard → tenantCache → legacy
//	        cache → registered default.
//	Delete: ctx → validate tenant → registry guard → persist delete →
//	        write-through tenantCache delete. Subscribers fire from
//	        changefeed echo with newValue = default.
//	List:   registry guard → backend list. No ctx required (admin-style).
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	"github.com/LerianStudio/lib-systemplane/internal/store"
)

// RegisterTenantScoped declares a tenant-scoped configuration key with its
// default value and optional key options. The key behaves identically to one
// registered via Register from the perspective of the existing public API
// (Get, Set, OnChange, List) — the default value and legacy global cache are
// untouched — but also becomes eligible for tenant-specific overrides via the
// Task 5 methods (SetForTenant, GetForTenant, DeleteForTenant, OnTenantChange).
//
// Key options:
//   - WithDescription — human-readable description surfaced in admin responses.
//   - WithValidator   — runs on the default value at registration AND on every
//     per-tenant write.
//   - WithRedaction   — applied by admin handlers AND log renderers; applies
//     identically to the global row and to every tenant override.
//
// Semantics:
//   - Must be called before Start(); returns ErrRegisterAfterStart otherwise.
//   - Registering the same (namespace, key) twice — via any mix of Register
//     and RegisterTenantScoped — returns ErrDuplicateKey.
//   - If a validator is configured and it rejects the defaultValue, returns
//     ErrValidation. A broken default would cause silent misbehavior later.
//   - Seeds the legacy cache[nk] = defaultValue under cacheMu so a pre-Start
//     Get call returns the default (same contract as Register).
//
// Mutable defaults are defensively cloned for cache seeding and fallback
// reads so map/slice defaults are not shared across tenants by reference.
//
// Concurrency: the two writes (registry insert + cacheMu seed) happen under
// separate locks; no other goroutine can observe the in-between state because
// RegisterTenantScoped is only legal before Start (see the started.Load guard
// above). After Start, the registry and tenantScopedRegistry maps are
// read-only, so the single-write-then-freeze pattern matches Register exactly.
func (c *Client) RegisterTenantScoped(namespace, key string, defaultValue any, opts ...KeyOption) error {
	if c == nil || c.closed.Load() {
		return ErrClosed
	}

	c.startMu.Lock()
	defer c.startMu.Unlock()

	if c.started.Load() {
		return ErrRegisterAfterStart
	}

	if err := validateKeyArgs(namespace, key); err != nil {
		return err
	}

	nk := nskey{Namespace: namespace, Key: key}

	// Build the key definition from defaults + options. Same shape as Register
	// so the rest of the Client treats tenant-scoped and globals-only keys
	// uniformly for description, validator, and redaction lookups.
	def := keyDef{
		defaultValue: cloneValue(defaultValue),
		redaction:    RedactNone,
	}

	applyKeyOptions(&def, opts)

	// Validate the default value if a validator is set. Mirrors register.go
	// behavior — we prefer a fail-fast signal at registration over a confusing
	// validation error emerging later from a tenant write.
	if def.validator != nil {
		if err := def.validator(def.defaultValue); err != nil {
			return fmt.Errorf("%w: default value rejected: %w", ErrValidation, err)
		}
	}

	// Atomically insert into both registry and tenantScopedRegistry under the
	// same registryMu write. No other writer runs concurrently (this path is
	// pre-Start and single-threaded for the typical consumer, but the lock is
	// still correct for defensive concurrent registration).
	c.registryMu.Lock()

	if _, exists := c.registry[nk]; exists {
		c.registryMu.Unlock()

		return fmt.Errorf("%w: %s/%s", ErrDuplicateKey, namespace, key)
	}

	c.registry[nk] = def
	c.tenantScopedRegistry[nk] = struct{}{}

	c.registryMu.Unlock()

	// Seed the legacy cache with the default so a pre-Start Get returns the
	// default value (same contract as Register's implicit seeding at Start —
	// see client.go:182-184). We do it eagerly here so the behavior holds
	// even if Start is never called (e.g. in a Client built via NewForTesting
	// and discarded without Start).
	c.cacheMu.Lock()
	c.cache[nk] = cloneValue(def.defaultValue)
	c.cacheMu.Unlock()

	return nil
}

// extractTenantID pulls a tenant ID from ctx and validates it against
// core.IsValidTenantID. Returns the tenant ID verbatim (as stored in ctx
// by core.ContextWithTenantID — no trimming or normalization) on success,
// or an error on any of:
//
//   - ctx has no tenant ID   → ErrMissingTenantContext
//   - tenant ID fails regex  → ErrInvalidTenantID
//   - tenant ID == sentinel  → ErrInvalidTenantID (cannot collide with the
//     "_global" shared-row sentinel per decision D2)
//
// Decision D8 (locked): tenant-scoped reads and writes never silently fall
// back to a shared global when the tenant is missing. This is the only
// extraction path used by every tenant-scoped method — centralizing it here
// ensures uniform fail-closed behavior.
func extractTenantID(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", ErrNilContext
	}

	id := core.GetTenantIDContext(ctx)
	if id == "" {
		return "", fmt.Errorf("%w", ErrMissingTenantContext)
	}

	if id == store.SentinelGlobal {
		return "", fmt.Errorf("%w: tenantID must not be the %q sentinel", ErrInvalidTenantID, store.SentinelGlobal)
	}

	if !core.IsValidTenantID(id) {
		return "", fmt.Errorf("%w", ErrInvalidTenantID)
	}

	return id, nil
}

// requireTenantScoped looks up nk in the registry and verifies both that the
// key is registered AND that it was registered via RegisterTenantScoped.
// Returns the resolved keyDef on success.
//
// Errors:
//   - Missing from registry          → ErrUnknownKey
//   - Registered but not tenant-scoped → ErrTenantScopeNotRegistered
func (c *Client) requireTenantScoped(namespace, key string) (keyDef, nskey, error) {
	nk := nskey{Namespace: namespace, Key: key}

	// Nil-receiver guard: matches the ErrClosed pattern used by every public
	// tenant-scoped entry point. Every caller of requireTenantScoped also
	// guards against nil at the public-method level, but the defensive check
	// here forestalls a nil-pointer panic if a future caller forgets.
	if c == nil {
		return keyDef{}, nk, ErrClosed
	}

	c.registryMu.RLock()
	def, registered := c.registry[nk]
	_, tenantScoped := c.tenantScopedRegistry[nk]
	c.registryMu.RUnlock()

	if !registered {
		return keyDef{}, nk, fmt.Errorf("%w: %s/%s", ErrUnknownKey, namespace, key)
	}

	if !tenantScoped {
		return keyDef{}, nk, fmt.Errorf("%w: %s/%s", ErrTenantScopeNotRegistered, namespace, key)
	}

	return def, nk, nil
}

// SetForTenant persists a tenant-specific override for (namespace, key)
// scoped to the tenant ID carried in ctx. The value is validated against
// the key's registered validator (if any), JSON-marshaled, persisted to
// the backing store, and the in-memory tenantCache is updated immediately
// for same-process read consistency.
//
// Subscribers are NOT fired from SetForTenant. The changefeed echo drives
// OnTenantChange notifications, preserving the set.go:18-21 invariant that
// subscribers observe *backend* state changes, not in-process writes.
//
// Returns:
//   - ErrClosed               — receiver is nil or the Client is closed
//   - ErrNotStarted           — Start has not been called
//   - ErrMissingTenantContext — ctx carries no tenant ID (D8, fail-closed)
//   - ErrInvalidTenantID      — tenant ID fails validation or equals "_global"
//   - ErrUnknownKey           — (namespace, key) was never registered
//   - ErrTenantScopeNotRegistered — key was registered via Register (not
//     RegisterTenantScoped), so tenant overrides are not permitted
//   - ErrValidation           — validator rejected value OR value is not
//     JSON-serializable
//   - any wrapped store error from the backend (Postgres / MongoDB /
//     TestStore)
func (c *Client) SetForTenant(ctx context.Context, namespace, key string, value any, actor string) error {
	if c == nil || c.closed.Load() {
		return ErrClosed
	}

	if !c.started.Load() {
		return ErrNotStarted
	}

	tenantID, err := extractTenantID(ctx)
	if err != nil {
		return err
	}

	def, nk, err := c.requireTenantScoped(namespace, key)
	if err != nil {
		return err
	}

	// Run the registered validator — same chain as global Set at
	// set.go:46-50. A rejected value is reported as ErrValidation with the
	// validator's own error wrapped for diagnosis.
	if def.validator != nil {
		if err := def.validator(value); err != nil {
			return fmt.Errorf("%w: %w", ErrValidation, err)
		}
	}

	// Build the Entry. TenantID here is informational only — the store
	// contract ignores Entry.TenantID and uses the separate tenantID
	// argument as authoritative (internal/store/store.go:72-76).
	entry := store.Entry{
		Namespace: namespace,
		Key:       key,
		TenantID:  tenantID,
		UpdatedBy: actor,
	}

	ctx, span, finish := c.startSpanWithLabels(ctx, "systemplane.client.set_tenant_value",
		spanString("tenant.id", tenantID),
		spanString("systemplane.namespace", namespace),
		spanString("systemplane.key", key),
	)
	defer finish()

	jsonBytes, err := json.Marshal(value)
	if err != nil {
		span.HandleError("json marshal failed", err)

		return fmt.Errorf("%w: value is not JSON-serializable: %w", ErrValidation, err)
	}

	entry.Value = jsonBytes
	if entry.UpdatedAt.IsZero() {
		entry.UpdatedAt = time.Now().UTC()
	}

	if err := c.store.SetTenantValue(ctx, tenantID, entry); err != nil {
		span.HandleError("store set_tenant_value failed", err)

		return fmt.Errorf("systemplane: SetTenantValue: %w", err)
	}

	var canonical any
	if err := json.Unmarshal(jsonBytes, &canonical); err != nil {
		canonical = value
	}

	// Write-through cache: update immediately so a subsequent GetForTenant
	// in the same process sees the new override without waiting for the
	// changefeed roundtrip. Uses the canonical (JSON round-tripped) value
	// so type agreement with refresh.go is guaranteed (set.go:70-78
	// precedent).
	c.cacheMu.Lock()
	c.tenantCache.set(tenantID, nk, cloneValue(canonical))
	c.cacheMu.Unlock()

	return nil
}

// GetForTenant returns the current value for (namespace, key) scoped to the
// tenant ID carried in ctx.
//
// Resolution order (TRD §4.2):
//  1. tenantCache[tenantID][nk] — per-tenant override (hot path)
//  2. cache[nk] — shared global value (override absent, global was set)
//  3. def.defaultValue — neither override nor global persisted (startup)
//
// found is true whenever a value can be returned. It is only false when
// err is non-nil (a nil-safety or registration error). The "no tenant
// override yet" case always resolves to the global or default and
// therefore returns (value, true, nil).
//
// In lazy mode, a tenantCache miss triggers a single-flight store.GetTenantValue
// with a 5s timeout. Backend errors during the miss-populate path fail closed
// so critical tenant overrides cannot be bypassed during backend degradation.
//
// Errors (value is nil, found is false):
//   - ErrClosed, ErrNotStarted, ErrMissingTenantContext, ErrInvalidTenantID,
//     ErrUnknownKey, ErrTenantScopeNotRegistered
func (c *Client) GetForTenant(ctx context.Context, namespace, key string) (any, bool, error) {
	if c == nil || c.closed.Load() {
		return nil, false, ErrClosed
	}

	if !c.started.Load() {
		return nil, false, ErrNotStarted
	}

	tenantID, err := extractTenantID(ctx)
	if err != nil {
		return nil, false, err
	}

	def, nk, err := c.requireTenantScoped(namespace, key)
	if err != nil {
		return nil, false, err
	}

	// 1. Try the tenant cache under RLock. Per the tenantCache contract in
	// tenant_cache.go, BOTH implementations (eager map and LRU) are safe
	// for read-only get() under cacheMu.RLock: the eager map is read-only
	// from this method's perspective, and the LRU's MRU-promotion happens
	// under the library's OWN internal lock (hashicorp/golang-lru/v2), not
	// the outer cacheMu. Concurrent hits therefore do not serialize
	// through a write lock — critical for lazy hot-path throughput.
	c.cacheMu.RLock()
	v, hit := c.tenantCache.get(tenantID, nk)
	c.cacheMu.RUnlock()

	if hit {
		if isTenantNoOverride(v) {
			return c.getTenantFallbackValue(nk, def)
		}

		return cloneValue(v), true, nil
	}

	// 1b. Lazy-mode miss: delegate to the helper so this method stays
	// focused on the cascade (tenantCache → legacy global → default).
	if c.tenantLoadMode == tenantLoadLazy {
		if val, found, handled, err := c.getForTenantLazyMissLocked(ctx, tenantID, namespace, key, nk); handled {
			return val, found, err
		}
	}

	// 2. Fall through to the legacy global cache. This is the D3 fallthrough
	// contract — tenant without an override sees whatever Set wrote to the
	// global row (or the default if Set was never called).
	return c.getTenantFallbackValue(nk, def)
}

func (c *Client) getTenantFallbackValue(nk nskey, def keyDef) (any, bool, error) {
	c.cacheMu.RLock()
	globalVal, hasGlobal := c.cache[nk]
	c.cacheMu.RUnlock()

	if hasGlobal {
		return cloneValue(globalVal), true, nil
	}

	// Registered default. Never errors on "no override" — GetForTenant
	// always returns a value when the key is correctly registered.
	return cloneValue(def.defaultValue), true, nil
}

// DeleteForTenant removes the tenant-specific override for (namespace, key)
// scoped to the tenant ID carried in ctx. The row is removed from the
// backing store; the in-memory tenantCache entry is cleared immediately for
// same-process read consistency.
//
// Subscribers are NOT fired from DeleteForTenant. The changefeed echo for
// the delete fires OnTenantChange with newValue = def.defaultValue — the
// tenant has reverted to global/default (TRD §4.4). Preserves the
// set.go:18-21 invariant: subscribers observe backend state changes only.
//
// Delete is idempotent at the backend: removing a non-existent override
// returns nil, not an error. This matches the store.Store contract
// documented at internal/store/store.go:82.
//
// If no override row exists for the tenant (idempotent delete), the
// underlying backend emits no change event and OnTenantChange does NOT
// fire. Callers waiting for the echo — e.g. tests that assert a callback
// runs after DeleteForTenant — must ensure a row exists first
// (SetForTenant) or they will wait forever.
//
// Returns the same error set as SetForTenant (except ErrValidation, which
// is not reachable on delete).
func (c *Client) DeleteForTenant(ctx context.Context, namespace, key, actor string) error {
	if c == nil || c.closed.Load() {
		return ErrClosed
	}

	if !c.started.Load() {
		return ErrNotStarted
	}

	tenantID, err := extractTenantID(ctx)
	if err != nil {
		return err
	}

	_, nk, err := c.requireTenantScoped(namespace, key)
	if err != nil {
		return err
	}

	ctx, span, finish := c.startSpanWithLabels(ctx, "systemplane.client.delete_tenant_value",
		spanString("tenant.id", tenantID),
		spanString("systemplane.namespace", namespace),
		spanString("systemplane.key", key),
		spanString("systemplane.actor", actor),
	)
	defer finish()

	if err := c.store.DeleteTenantValue(ctx, tenantID, namespace, key, actor); err != nil {
		span.HandleError("store delete_tenant_value failed", err)

		return fmt.Errorf("systemplane: DeleteTenantValue: %w", err)
	}

	// Write-through cache delete: clear the override immediately so a
	// subsequent GetForTenant in the same process falls through to the
	// global/default cascade without waiting for the changefeed roundtrip.
	c.cacheMu.Lock()
	if c.tenantLoadMode == tenantLoadLazy {
		c.tenantCache.set(tenantID, nk, tenantNoOverride)
	} else {
		c.tenantCache.delete(tenantID, nk)
	}
	c.cacheMu.Unlock()

	return nil
}
