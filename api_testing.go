//go:build unit || integration

package systemplane

import internalclient "github.com/LerianStudio/lib-systemplane/v4/internal/client"

// TestStore mirrors the internal store.Store interface for [NewForTesting]. Its
// Subscribe must emit TestEvent{Op: "resync"} when ready and on each reconnect;
// a single-tenant [Client.Start] blocks until the first one or its ctx ends.
type TestStore = internalclient.TestStore

// TestScope is the public mirror of internal store.Scope.
type TestScope = internalclient.TestScope

// TestEntry is the public mirror of internal store.Entry.
type TestEntry = internalclient.TestEntry

// TestEvent is the public mirror of internal store.Event.
type TestEvent = internalclient.TestEvent

// NewForTesting wires a Client from an explicit [TestStore] implementation.
func NewForTesting(s TestStore, opts ...Option) (*Client, error) {
	c, err := internalclient.NewForTesting(s, opts...)
	if err != nil {
		return nil, err
	}

	return (*Client)(c), nil
}
