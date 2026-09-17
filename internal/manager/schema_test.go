//go:build unit

package manager

import (
	"context"
	"errors"
	"strings"
	"testing"

	tmpostgres "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/postgres"
	"github.com/bxcodec/dbresolver/v2"
)

func TestApplyEvent_UpsertResolveFailureDoesNotMutateCache(t *testing.T) {
	t.Parallel()

	m := New(nil)
	ts := newTenantState("tenant-a")
	ts.entries[nsKey{Namespace: "ns", Key: "k"}] = "old"

	m.applyEvent(context.Background(), "tenant-a", ts, notifyEvent{Namespace: "ns", Key: "k", Op: "upsert"})

	if got := ts.entries[nsKey{Namespace: "ns", Key: "k"}]; got != "old" {
		t.Fatalf("cache value = %#v, want old", got)
	}
}

func TestResolveTenantDB_NilConnector_ReturnsErr(t *testing.T) {
	t.Parallel()

	m := New(nil)

	_, err := m.resolveTenantDB(context.Background(), "tenant-a")
	if !errors.Is(err, ErrPgMgrUnavailable) {
		t.Fatalf("expected ErrPgMgrUnavailable, got %v", err)
	}
}

func TestTenantDSN_NilConnector_ReturnsErr(t *testing.T) {
	t.Parallel()

	m := New(nil)

	_, err := m.tenantDSN(context.Background(), "tenant-a")
	if !errors.Is(err, ErrPgMgrUnavailable) {
		t.Fatalf("expected ErrPgMgrUnavailable, got %v", err)
	}
}

func TestStartListen_NilConnector_ReturnsErr(t *testing.T) {
	t.Parallel()

	m := New(nil)
	ts := newTenantState("t")

	if err := m.startListen(context.Background(), "t", ts); !errors.Is(err, ErrPgMgrUnavailable) {
		t.Fatalf("expected ErrPgMgrUnavailable, got %v", err)
	}
}

func TestStartListen_NilTenantState_ReturnsErr(t *testing.T) {
	t.Parallel()

	m := New(nil)
	m.SetConnector(&connectorStub{})

	if err := m.startListen(context.Background(), "t", nil); !errors.Is(err, ErrPgMgrUnavailable) {
		t.Fatalf("expected ErrPgMgrUnavailable for nil ts, got %v", err)
	}
}

func TestStartListen_ResolveDSNFails_ReleasesSlot(t *testing.T) {
	t.Parallel()

	m := New(nil)
	m.SetConnector(&connectorStub{})

	ts := newTenantState("t")

	if err := m.startListen(context.Background(), "t", ts); err == nil {
		t.Fatal("expected error when ResolveDSN fails")
	}

	if ts.listen != nil {
		t.Fatal("listen handle must be released after ResolveDSN failure")
	}
}

// connectorStub returns errors for both ResolveDB and ResolveDSN; used to
// pin sentinel error propagation without spinning up a real Postgres.
type connectorStub struct{}

func (*connectorStub) ResolveDB(_ context.Context, _ string) (dbresolver.DB, error) {
	return nil, errors.New("stub: no DB")
}

func (*connectorStub) ResolveDSN(_ context.Context, _ string) (string, error) {
	return "", errors.New("stub: no DSN")
}

func TestNew_WiresTenantManagerConnector(t *testing.T) {
	t.Parallel()

	// Pin the constructor's pgMgr→connector wiring: New(pgMgr) MUST install a
	// tenant-manager connector that talks to the supplied manager. Confirms a
	// real production path (not a SetConnector test seam) is exercised
	// end-to-end without needing a live tenant-manager. The connector's
	// concrete type lives in internal/postgres and is asserted there; here we
	// assert the behaviour it gives the Manager.
	pg := tmpostgres.NewManager(nil, "systemplane.manager.test")
	m := New(pg)

	if m.connector == nil {
		t.Fatal("expected New to wire a connector for non-nil pgMgr")
	}

	// It must fail predictably (no gRPC client) — flows through the same
	// error branches as the connector's own tests, confirming wiring.
	if _, err := m.connector.ResolveDB(context.Background(), "x"); err == nil ||
		!strings.Contains(err.Error(), "systemplane/postgres: get tenant connection") {
		t.Fatalf("wired connector must surface GetConnection error, got %v", err)
	}

	// Make sure the manager isn't accidentally marked closed.
	if m.IsClosed() {
		t.Fatal("New must not return a closed Manager")
	}

	// Use errors.Is to assert ErrPgMgrUnavailable is NOT returned (the wired
	// connector talks to a real tmpostgres.Manager, so the sentinel applies
	// only to the nil-mgr case).
	_, err := m.connector.ResolveDSN(context.Background(), "x")
	if errors.Is(err, ErrPgMgrUnavailable) {
		t.Fatal("ErrPgMgrUnavailable must not surface from a wired connector")
	}
}
