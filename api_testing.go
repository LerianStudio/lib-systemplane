//go:build unit || integration

package systemplane

import internalclient "github.com/LerianStudio/lib-systemplane/v3/internal/client"

// TestStore is the public mirror of the internal store.Store interface,
// exposed solely for [NewForTesting].
type TestStore = internalclient.TestStore

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
