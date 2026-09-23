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

	// Every entry point resolves a named tenant THROUGH the connector: this
	// connector wraps a nil manager, so each call must surface that connector's
	// own failure, named with the tenant, rather than falling back to the zero
	// scope — which would read one tenant's rows on another tenant's behalf.
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
		if !errors.Is(tc.err, ErrPgMgrUnavailable) {
			t.Errorf("%s with a named tenant: got %v, want the connector's own failure", tc.op, tc.err)

			continue
		}

		if !strings.Contains(tc.err.Error(), "resolve tenant t1") {
			t.Errorf("%s error %q must name the tenant it failed to resolve", tc.op, tc.err)
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

// TestFormatServerDatabaseKey pins the identity format serverDatabaseKey
// produces once the server has answered, including the socket branch no
// container test reaches: every test here connects over TCP, so the branch
// that decides whether two feeds are one database would otherwise never run.
func TestFormatServerDatabaseKey(t *testing.T) {
	for _, tc := range []struct {
		name     string
		database string
		addr     string
		port     int32
		started  string
		dsn      string
		want     string
		wantErr  bool
	}{
		{
			name:     "tcp address reported by the server wins over the DSN text",
			database: "app",
			addr:     "10.0.0.5",
			port:     5432,
			started:  "1790000000.123456",
			dsn:      "postgres://an-alias.example:5432/app",
			want:     "started:1790000000.123456/tcp:10.0.0.5:5432/app",
		},
		{
			name:     "another server (or a clone) with the same address, port and database starts at another time",
			database: "app",
			addr:     "10.0.0.5",
			port:     5432,
			started:  "1790000000.123457",
			dsn:      "postgres://an-alias.example:5432/app",
			want:     "started:1790000000.123457/tcp:10.0.0.5:5432/app",
		},
		{
			name:     "the same server reached through another DSN spelling is the same key",
			database: "app",
			addr:     "10.0.0.5",
			port:     5432,
			started:  "1790000000.123456",
			dsn:      "postgres://10.0.0.5:5432/app?options=-csearch_path%3Dtenant_b",
			want:     "started:1790000000.123456/tcp:10.0.0.5:5432/app",
		},
		{
			name:     "no address means a unix socket: the socket directory identifies it",
			database: "app",
			started:  "1790000000.123456",
			dsn:      "postgres:///app?host=/var/run/postgresql",
			want:     "started:1790000000.123456/unix:/var/run/postgresql/app",
		},
		{
			name:     "no address and an unparseable DSN is an error, never a bare key",
			database: "app",
			dsn:      "postgres://%zz/app",
			wantErr:  true,
		},
		{
			name:     "no address with a TCP DSN falls back to the DSN host",
			database: "app",
			started:  "1790000000.123456",
			dsn:      "postgres://localhost:5432/app?sslmode=disable",
			want:     "started:1790000000.123456/unix:localhost/app",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := formatServerDatabaseKey(tc.database, tc.addr, tc.port, tc.started, tc.dsn)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("formatServerDatabaseKey = %q, want an error", got)
				}

				if !strings.Contains(err.Error(), "parse DSN for socket identity") {
					t.Fatalf("error %q must name the parse step it failed in", err)
				}

				return
			}

			if err != nil {
				t.Fatalf("formatServerDatabaseKey: %v", err)
			}

			if got != tc.want {
				t.Fatalf("formatServerDatabaseKey = %q, want %q", got, tc.want)
			}
		})
	}
}
