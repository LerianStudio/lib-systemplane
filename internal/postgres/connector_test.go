//go:build unit

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	tmpostgres "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/postgres"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

func TestPgMgrConnector_NilReceiver_ReturnsErr(t *testing.T) {
	t.Parallel()

	var c *pgMgrConnector

	if _, err := c.ResolveDB(context.Background(), "t"); !errors.Is(err, ErrPgMgrUnavailable) {
		t.Fatalf("nil receiver ResolveDB: got %v", err)
	}

	if _, err := c.ResolveDSN(context.Background(), "t"); !errors.Is(err, ErrPgMgrUnavailable) {
		t.Fatalf("nil receiver ResolveDSN: got %v", err)
	}
}

func TestPgMgrConnector_NilManager_ReturnsErr(t *testing.T) {
	t.Parallel()

	c := &pgMgrConnector{mgr: nil}

	if _, err := c.ResolveDB(context.Background(), "t"); !errors.Is(err, ErrPgMgrUnavailable) {
		t.Fatalf("nil mgr ResolveDB: got %v", err)
	}

	if _, err := c.ResolveDSN(context.Background(), "t"); !errors.Is(err, ErrPgMgrUnavailable) {
		t.Fatalf("nil mgr ResolveDSN: got %v", err)
	}
}

func TestNewTenantManagerConnector_NilManager_ReturnsErr(t *testing.T) {
	t.Parallel()

	c := NewTenantManagerConnector(nil)
	if c == nil {
		t.Fatal("NewTenantManagerConnector must return a Connector")
	}

	if _, err := c.ResolveDB(context.Background(), "t"); !errors.Is(err, ErrPgMgrUnavailable) {
		t.Fatalf("nil mgr ResolveDB: got %v", err)
	}

	if _, err := c.ResolveDSN(context.Background(), "t"); !errors.Is(err, ErrPgMgrUnavailable) {
		t.Fatalf("nil mgr ResolveDSN: got %v", err)
	}
}

func TestConfig_CarriesConnector(t *testing.T) {
	t.Parallel()

	c := NewTenantManagerConnector(nil)

	s, err := New(Config{MultiTenantEnabled: true, Connector: c})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if s.cfg.Connector != c {
		t.Fatal("Config.Connector must survive construction")
	}

	// A configured connector is not yet enough to serve a named tenant: FC-3
	// leaves scoped resolution to the storage lane. Every entry point must
	// refuse the call rather than silently fall back to the zero scope, which
	// would read one tenant's rows on another tenant's behalf.
	ctx := context.Background()
	scope := store.Scope{Tenant: "t1"}

	_, _, getErr := s.Get(ctx, scope, "ns", "k")
	_, setErr := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`"v"`)})
	delErr := s.Delete(ctx, scope, "ns", "k", "actor")
	_, listErr := s.List(ctx, scope)

	for _, tc := range []struct {
		op  string
		err error
	}{
		{"Get", getErr},
		{"Set", setErr},
		{"Delete", delErr},
		{"List", listErr},
	} {
		if !errors.Is(tc.err, store.ErrTenantConnectorMissing) {
			t.Errorf("%s with a named tenant: got %v, want ErrTenantConnectorMissing", tc.op, tc.err)

			continue
		}

		if !strings.Contains(tc.err.Error(), "scoped resolution not implemented") {
			t.Errorf("%s error %q must say scoped resolution is not implemented yet", tc.op, tc.err)
		}
	}
}

// TestNewTenantManagerConnector_WrapsSuppliedManager pins the wiring the
// Manager's own constructor depends on: the returned Connector must talk to
// the manager it was handed, not to a zero value that would resolve every
// tenant against nothing.
func TestNewTenantManagerConnector_WrapsSuppliedManager(t *testing.T) {
	t.Parallel()

	mgr := tmpostgres.NewManager(nil, "systemplane.postgres.test")

	c, ok := NewTenantManagerConnector(mgr).(*pgMgrConnector)
	if !ok {
		t.Fatalf("NewTenantManagerConnector returned %T, want *pgMgrConnector", c)
	}

	if c.mgr != mgr {
		t.Fatal("connector must wrap the supplied tenant-manager Manager")
	}
}
