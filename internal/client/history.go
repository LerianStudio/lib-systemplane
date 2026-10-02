package client

import (
	"context"
	"fmt"
	"strings"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// ChangeRecord is one record of the change history WithChangeHistory keeps.
type ChangeRecord = store.ChangeRecord

// The operations a ChangeRecord names.
const (
	// ChangeOperationCreate is a Set that found no live value.
	ChangeOperationCreate = store.ChangeCreate
	// ChangeOperationUpdate is a Set over a live value, an identical one
	// included.
	ChangeOperationUpdate = store.ChangeUpdate
	// ChangeOperationDelete is a Delete that removed a live value.
	ChangeOperationDelete = store.ChangeDelete
)

const (
	// DefaultChangeHistoryLimit is how many records ChangeHistory returns for
	// a non-positive limit.
	DefaultChangeHistoryLimit = 50

	// MaxChangeHistoryLimit caps the records one ChangeHistory call returns.
	MaxChangeHistoryLimit = 500
)

// ChangeHistoryEnabled reports whether the Client was built with
// WithChangeHistory. False on a nil Client.
func (c *Client) ChangeHistoryEnabled() bool {
	return c != nil && c.changeHistory
}

// requireActor refuses a write with no actor on a Client that keeps the change
// history, before the store is touched: the history is append-only, so an
// unattributed record could never be corrected. A blank actor is no actor.
// Without the option the actor stays optional.
func (c *Client) requireActor(actor string) error {
	if c.changeHistory && strings.TrimSpace(actor) == "" {
		return fmt.Errorf("%w: the change history records who wrote, and the actor is empty", ErrValidation)
	}

	return nil
}

// ChangeHistory returns the recorded writes of (namespace, key), newest first:
// which operation, the value before and after, who wrote it and when. limit
// <= 0 means DefaultChangeHistoryLimit; above MaxChangeHistoryLimit it is
// capped. A key never written answers with an empty slice.
//
// It reads through to the database the way a write does: the constructor's in
// single-tenant mode, the tenant database ctx carries in multi-tenant mode
// ([ErrTenantConnectionMissing] without one). Refusals: [ErrClosed] on a nil or
// closed Client, [ErrNilContext], [ErrNotStarted] before Start,
// [ErrChangeHistoryDisabled] without WithChangeHistory, and [ErrUnknownKey]
// for a key that was not registered.
//
// Nothing here logs: the records name people and carry values, and the caller
// decides where they go.
func (c *Client) ChangeHistory(ctx context.Context, namespace, key string, limit int) ([]ChangeRecord, error) {
	if c == nil || c.closed.Load() {
		return nil, ErrClosed
	}

	if ctx == nil {
		return nil, ErrNilContext
	}

	if !c.started.Load() {
		return nil, ErrNotStarted
	}

	lister, ok := c.store.(store.HistoryLister)
	if !c.changeHistory || !ok {
		return nil, ErrChangeHistoryDisabled
	}

	c.registryMu.RLock()
	_, registered := c.registry[nskey{Namespace: namespace, Key: key}]
	c.registryMu.RUnlock()

	if !registered {
		return nil, fmt.Errorf("%w: %s/%s", ErrUnknownKey, namespace, key)
	}

	switch {
	case limit <= 0:
		limit = DefaultChangeHistoryLimit
	case limit > MaxChangeHistoryLimit:
		limit = MaxChangeHistoryLimit
	}

	records, err := lister.ListHistory(ctx, store.Scope{}, namespace, key, limit)
	if err != nil {
		return nil, err
	}

	if records == nil {
		records = []ChangeRecord{}
	}

	return records, nil
}
