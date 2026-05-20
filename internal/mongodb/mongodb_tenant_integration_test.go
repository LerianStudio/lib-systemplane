//go:build integration

// Tenant-scoped contract integration tests for the MongoDB store.
//
// Two modes are exercised:
//
//   - Change-stream mode (replica set): MongoDB's native change streams
//     surface insert/update/replace/delete events, so the full
//     systemplanetest.Run suite passes unmodified.
//
//   - Polling mode (standalone): the subscribePoll loop polls for documents
//     updated after a watermark. Tenant deletes are represented as tombstone
//     updates so polling subscribers observe the same tenant-revert signal as
//     change-stream subscribers.
//
// Both suites share the helpers (setupMongoDB, newTestStore) defined in
// mongodb_integration_test.go.
package mongodb

import (
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/internal/store"
	"github.com/LerianStudio/lib-systemplane/systemplanetest"
)

// TestIntegration_MongoDBTenantContracts_ChangeStream runs the full
// systemplanetest.Run suite — including the tenant sub-suite — against a
// MongoDB replica set via change-stream subscription. The replica set
// bootstrap and connection validation live in setupMongoDB; if the host
// cannot reach the rs0 advertised address the helper calls t.Skip (Docker
// Desktop on macOS occasionally hits this).
func TestIntegration_MongoDBTenantContracts_ChangeStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)

	systemplanetest.Run(t, func(t *testing.T) store.Store {
		return newTestStore(t, client, 0) // PollInterval=0 -> change streams
	})
}

// TestIntegration_MongoDBTenantContracts_Polling runs the same suite against
// the polling subscription path.
//
// Polling interval is 100ms (same as the existing contract_suite_polling
// test) so the eventually() helper's 10s budget comfortably covers the
// delivery window.
func TestIntegration_MongoDBTenantContracts_Polling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	client, _ := setupMongoDB(t)

	systemplanetest.Run(t, func(t *testing.T) store.Store {
		return newTestStore(t, client, 100*time.Millisecond)
	},
	)
}
