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

// ChangeHistoryQuery selects one page of a key's change history.
type ChangeHistoryQuery struct {
	// Limit is how many records the page holds at most: <= 0 means
	// DefaultChangeHistoryLimit, and above MaxChangeHistoryLimit it is capped.
	Limit int

	// Before, when positive, starts the page below that Position: the Next of
	// the page before it. 0 starts from the newest record; a negative Before is
	// refused with ErrValidation.
	Before int64
}

// ChangeHistoryPage is one page of a key's change history.
type ChangeHistoryPage struct {
	// Changes holds the page's records, newest first; never nil.
	Changes []ChangeRecord

	// Next is the Before of the next, older page, and 0 when no older record
	// remains.
	Next int64
}

// ChangeHistory returns one page of the recorded writes of (namespace, key),
// newest first: which operation, the value before and after, who wrote it and
// when. Following Next until it is 0 reaches every record the key has, however
// many. A key never written answers with an empty page.
//
// It reads through to the database the way a write does: the constructor's in
// single-tenant mode, the tenant database ctx carries in multi-tenant mode
// ([ErrTenantConnectionMissing] without one). Refusals: [ErrClosed] on a nil or
// closed Client, [ErrNilContext], [ErrNotStarted] before Start,
// [ErrChangeHistoryDisabled] without WithChangeHistory, [ErrUnknownKey] for a
// key that was not registered, and [ErrValidation] for a negative Before.
//
// Nothing here logs: the records name people and carry values, and the caller
// decides where they go.
func (c *Client) ChangeHistory(ctx context.Context, namespace, key string, q ChangeHistoryQuery) (ChangeHistoryPage, error) {
	if c == nil || c.closed.Load() {
		return ChangeHistoryPage{}, ErrClosed
	}

	if ctx == nil {
		return ChangeHistoryPage{}, ErrNilContext
	}

	if !c.started.Load() {
		return ChangeHistoryPage{}, ErrNotStarted
	}

	lister, ok := c.store.(store.HistoryLister)
	if !c.changeHistory || !ok {
		return ChangeHistoryPage{}, ErrChangeHistoryDisabled
	}

	c.registryMu.RLock()
	_, registered := c.registry[nskey{Namespace: namespace, Key: key}]
	c.registryMu.RUnlock()

	if !registered {
		return ChangeHistoryPage{}, fmt.Errorf("%w: %s/%s", ErrUnknownKey, namespace, key)
	}

	if q.Before < 0 {
		return ChangeHistoryPage{}, fmt.Errorf("%w: before must not be negative", ErrValidation)
	}

	limit := q.Limit

	switch {
	case limit <= 0:
		limit = DefaultChangeHistoryLimit
	case limit > MaxChangeHistoryLimit:
		limit = MaxChangeHistoryLimit
	}

	// One record past the page says whether an older page exists, so the last
	// page reports Next 0 instead of sending the caller to an empty one.
	records, err := lister.ListHistory(ctx, store.Scope{}, namespace, key, limit+1, q.Before)
	if err != nil {
		return ChangeHistoryPage{}, err
	}

	page := ChangeHistoryPage{Changes: records}

	if len(records) > limit {
		page.Changes = records[:limit:limit]
		page.Next = page.Changes[limit-1].Position
	}

	if page.Changes == nil {
		page.Changes = []ChangeRecord{}
	}

	return page, nil
}
