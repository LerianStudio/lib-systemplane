// Write path for systemplane Client.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/engine"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// Set writes a new value for (namespace, key), then publishes it so the
// caller's own next read sees its write before the changefeed echoes it (D4).
//
// The value is JSON-marshaled and the registered validator grades the CANONICAL
// decoded shape — what the store will hand back — not the caller's Go value.
// Every ingress therefore presents the validator the same shapes (float64 for
// numbers, map[string]any, []any, string, bool, nil), so a validator that type-
// asserts a Go type fails loudly here instead of passing Set and being refused
// silently when the row is read back.
//
// It grades the write EXACTLY ONCE, here, before the store is written: a
// refused value is never persisted, and the engine trusts the publication that
// follows rather than running the same validator over the same value a second
// time. A validator that answered differently on that second call used to
// leave the row written, the publication dropped and Set returning nil, so the
// caller was told its write landed while the next Get served the previous
// value.
//
// A nil return therefore means the next read in this process serves this write
// or something newer. Every refusal the publication can still make — a Client
// closing under the write, a scope with no changefeed behind it, bytes that do
// not survive the round trip — comes back as an error naming the key, with the
// row already persisted. A write the engine could not publish because it holds
// no live scope for it matches [ErrNotStarted]; one that met a closing Client
// is [ErrClosed].
//
// [ErrNotStarted] with the row already persisted is reachable by racing Start
// as well: the Client marks itself started before its first reconcile brings
// the scope up, so a Set landing in that window is written and then refused,
// where an unstarted Client refuses before touching the store.
func (c *Client) Set(ctx context.Context, namespace, key string, value any, actor string) error {
	if c == nil || c.closed.Load() {
		return ErrClosed
	}

	if ctx == nil {
		return ErrNilContext
	}

	if !c.started.Load() {
		return ErrNotStarted
	}

	nk := nskey{Namespace: namespace, Key: key}

	c.registryMu.RLock()
	def, registered := c.registry[nk]
	c.registryMu.RUnlock()

	if !registered {
		return fmt.Errorf("%w: %s/%s", ErrUnknownKey, namespace, key)
	}

	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w: value is not JSON-serializable: %w", ErrValidation, err)
	}

	if def.validator != nil {
		var canonical any
		if err := json.Unmarshal(jsonBytes, &canonical); err != nil {
			return fmt.Errorf("%w: value does not survive a JSON round trip: %w", ErrValidation, err)
		}

		// The caller's own context, so a validator can read the tenant, the
		// deadline and the trace the write carried. Through the engine's
		// recovery, the same one every ingress grades under: a validator that
		// panics on the canonical shape refuses the write instead of unwinding
		// into the caller's request goroutine.
		if err := c.engine.RunValidator(ctx, def.validator, canonical); err != nil {
			return fmt.Errorf("%w: %w", ErrValidation, err)
		}
	}

	entry := store.Entry{
		Namespace: namespace,
		Key:       key,
		Value:     jsonBytes,
		UpdatedAt: time.Now().UTC(),
		UpdatedBy: actor,
	}

	revision, err := c.store.Set(ctx, store.Scope{}, entry)
	if err != nil {
		return err
	}

	if !c.multiTenant {
		// Through the engine's own ingress, so the cached shape is the one the
		// feed produces and this write's echo deduplicates by revision. The
		// UpdatedAt stamped above is this process's clock, not the row's: the
		// echo arrives at the same revision with an equal value and refreshes
		// the provenance without firing a callback.
		entry.Revision = revision

		switch err := c.engine.Publish(ctx, store.Scope{}, entry); {
		case err == nil:
		case errors.Is(err, engine.ErrClosed):
			// The Client closed under this write, between the guard above and
			// the publication. The row is persisted; nothing in this process
			// will ever serve it.
			return ErrClosed
		case errors.Is(err, engine.ErrScopeNotTracked):
			// The engine holds no live scope for this write: Start never brought
			// one up, or it was dropped under the write. The Client is not
			// running for that scope, which is what ErrNotStarted means —
			// reported with the row already persisted, so the message says so.
			return fmt.Errorf("%w: %s/%s was written but not published: %w", ErrNotStarted, namespace, key, err)
		default:
			return fmt.Errorf("systemplane: %s/%s was written but not published: %w", namespace, key, err)
		}
	}

	// Multi-tenant holds no in-process cache: the row itself is the only copy,
	// so the caller's next read through the same tenant sees this write.
	return nil
}

// Delete removes a single (namespace, key) row, then publishes the registered
// default at revision 0 so the caller's own next read stops serving the value
// it just removed (D4).
//
// A nil return means the next read in this process serves the registered
// default or something newer. Every refusal the publication can still make —
// a Client closing under the removal, a scope with no changefeed behind it —
// comes back as an error naming the key, with the row already gone from the
// store; the scope case also matches [ErrNotStarted], since the engine holds
// no live scope to publish into.
func (c *Client) Delete(ctx context.Context, namespace, key, actor string) error {
	if c == nil || c.closed.Load() {
		return ErrClosed
	}

	if ctx == nil {
		return ErrNilContext
	}

	if !c.started.Load() {
		return ErrNotStarted
	}

	nk := nskey{Namespace: namespace, Key: key}

	c.registryMu.RLock()
	_, registered := c.registry[nk]
	c.registryMu.RUnlock()

	if !registered {
		return fmt.Errorf("%w: %s/%s", ErrUnknownKey, namespace, key)
	}

	if err := c.store.Delete(ctx, store.Scope{}, namespace, key, actor); err != nil {
		return err
	}

	if !c.multiTenant {
		// The registered default at revision 0, under the engine's delete
		// fence, so a re-read already in flight cannot resurrect the row.
		// Every refusal it can still make is passed on for the reason Set
		// passes its own: the row is gone from the store and the next read in
		// this process still serves the value the caller just removed.
		switch err := c.engine.PublishDelete(store.Scope{}, engine.NSKey{Namespace: namespace, Key: key}); {
		case err == nil:
		case errors.Is(err, engine.ErrClosed):
			return ErrClosed
		case errors.Is(err, engine.ErrScopeNotTracked):
			return fmt.Errorf("%w: %s/%s was deleted but not published: %w", ErrNotStarted, namespace, key, err)
		default:
			return fmt.Errorf("systemplane: %s/%s was deleted but not published: %w", namespace, key, err)
		}
	}

	return nil
}
