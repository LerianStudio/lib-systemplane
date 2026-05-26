//go:build unit

package manager

import (
	"context"
	"errors"
	"testing"

	"github.com/bxcodec/dbresolver/v2"
)

func TestSeedDefaults_EmptyRegistered_NoOp(t *testing.T) {
	t.Parallel()

	m := New(nil)

	if err := m.seedDefaults(context.Background(), nil, nil); err != nil {
		t.Fatalf("seedDefaults empty: %v", err)
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
