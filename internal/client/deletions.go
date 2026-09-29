package client

import (
	"context"
	"fmt"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// Deletion is one record of the deletion history WithDeletionHistory keeps.
type Deletion = store.Deletion

const (
	// DefaultDeletionsLimit is how many records Deletions returns for a
	// non-positive limit.
	DefaultDeletionsLimit = 50

	// MaxDeletionsLimit caps the records one Deletions call returns.
	MaxDeletionsLimit = 500
)

// DeletionHistoryEnabled reports whether the Client was built with
// WithDeletionHistory. False on a nil Client.
func (c *Client) DeletionHistoryEnabled() bool {
	return c != nil && c.deletionHistory
}

// Deletions returns the recorded deletes of (namespace, key), newest first:
// who removed a stored value, when, and at which revision. limit <= 0 means
// DefaultDeletionsLimit; above MaxDeletionsLimit it is capped. A key never
// deleted answers with an empty slice.
//
// It reads through to the database the way a write does: the constructor's in
// single-tenant mode, the tenant database ctx carries in multi-tenant mode
// ([ErrTenantConnectionMissing] without one). Refusals: [ErrClosed] on a nil or
// closed Client, [ErrNilContext], [ErrNotStarted] before Start,
// [ErrDeletionHistoryDisabled] without WithDeletionHistory, and
// [ErrUnknownKey] for a key that was not registered.
//
// Nothing here logs: the records name people, and the caller decides where
// they go.
func (c *Client) Deletions(ctx context.Context, namespace, key string, limit int) ([]Deletion, error) {
	if c == nil || c.closed.Load() {
		return nil, ErrClosed
	}

	if ctx == nil {
		return nil, ErrNilContext
	}

	if !c.started.Load() {
		return nil, ErrNotStarted
	}

	lister, ok := c.store.(store.DeletionLister)
	if !c.deletionHistory || !ok {
		return nil, ErrDeletionHistoryDisabled
	}

	c.registryMu.RLock()
	_, registered := c.registry[nskey{Namespace: namespace, Key: key}]
	c.registryMu.RUnlock()

	if !registered {
		return nil, fmt.Errorf("%w: %s/%s", ErrUnknownKey, namespace, key)
	}

	switch {
	case limit <= 0:
		limit = DefaultDeletionsLimit
	case limit > MaxDeletionsLimit:
		limit = MaxDeletionsLimit
	}

	deletions, err := lister.ListDeletions(ctx, store.Scope{}, namespace, key, limit)
	if err != nil {
		return nil, err
	}

	if deletions == nil {
		deletions = []Deletion{}
	}

	return deletions, nil
}
