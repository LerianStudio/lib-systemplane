//go:build unit || integration

// Package-level goleak guard for the mongodb store. Runs under either build tag
// so the unit suite (Subscribe lifecycle, ensureSchema retry) and the
// integration suite (change-stream watcher, polling ticker, full testcontainer
// path) both surface goroutine leaks.
//
// Ignore list rationale — every entry below pins a third-party goroutine that
// legitimately outlives the test process. Add new entries only with a comment
// explaining the source and why it is benign.
package mongodb

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// testcontainers spawns a Reaper goroutine to clean up containers when
		// the parent process exits. It is intentionally process-lifetime and
		// never exits during a test run.
		goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
		// mongo-driver/v2 topology and connection-pool maintenance goroutines
		// are released asynchronously by client.Disconnect; they may still be
		// in their final wind-down at TestMain exit.
		goleak.IgnoreAnyFunction("go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology.(*Server).update"),
		goleak.IgnoreAnyFunction("go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology.(*pool).maintain"),
		goleak.IgnoreAnyFunction("go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology.(*pool).maintain.func1"),
		// HTTP keep-alive used by testcontainers' Docker client.
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
	)
}

// FeedsSnapshot exposes the feeds map to the external mongodb_test package:
// how many changefeed slots the store holds, and how many callers are parked
// on tenant's slot. Test-only — this file never enters a production build.
func (s *Store) FeedsSnapshot(tenant string) (total, refs int) {
	s.feedsMu.Lock()
	defer s.feedsMu.Unlock()

	if f, ok := s.feeds[tenant]; ok {
		refs = f.refs
	}

	return len(s.feeds), refs
}

// zeroFeed returns the zero-scope feed, creating it when the test needs one
// without going through Start. Test-only: production reaches the same slot
// through reserveZeroFeed (which also elects the one caller that opens it) or
// through acquireFeed.
func (s *Store) zeroFeed() (*feed, error) {
	s.feedsMu.Lock()
	defer s.feedsMu.Unlock()

	return s.zeroFeedLocked()
}
