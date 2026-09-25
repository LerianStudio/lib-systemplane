//go:build unit

package client

import (
	"errors"
	"testing"

	tmmongo "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/mongo"
	tmpostgres "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/postgres"
)

// TestWithPostgresTenantManagerImpliesMultiTenant constructs with a nil
// backend handle, which ErrNilBackend permits only in multi-tenant mode, so
// the option alone must have flipped the mode.
func TestWithPostgresTenantManagerImpliesMultiTenant(t *testing.T) {
	cases := []struct {
		name        string
		build       func() (*Client, error)
		wantManaged bool
	}{
		{"postgres", func() (*Client, error) {
			return NewPostgres(nil, "", WithPostgresTenantManager(tmpostgres.NewManager(nil, "svc")))
		}, true},
		{"mongodb", func() (*Client, error) {
			return NewMongoDB(nil, "", WithMongoTenantManager(tmmongo.NewManager(nil, "svc")))
		}, true},
		{"postgres nil manager", func() (*Client, error) {
			return NewPostgres(nil, "", WithPostgresTenantManager(nil))
		}, false},
		{"mongodb nil manager", func() (*Client, error) {
			return NewMongoDB(nil, "", WithMongoTenantManager(nil))
		}, false},
		{"test store", func() (*Client, error) {
			return NewForTesting(&facadeTestStore{}, WithPostgresTenantManager(tmpostgres.NewManager(nil, "svc")))
		}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := tc.build()
			if err != nil {
				t.Fatalf("construct: %v", err)
			}

			defer c.Close()

			if !c.multiTenant {
				t.Error("the tenant-manager option did not switch the Client to multi-tenant mode")
			}

			if c.tenantManaged != tc.wantManaged {
				t.Errorf("tenantManaged = %v, want %v", c.tenantManaged, tc.wantManaged)
			}
		})
	}
}

// TestTenantManagerBackendMismatchIsRefused pins the wiring error to
// construction: a manager for the other backend would resolve nothing and
// fail only at the first read.
func TestTenantManagerBackendMismatchIsRefused(t *testing.T) {
	pg := tmpostgres.NewManager(nil, "svc")
	mb := tmmongo.NewManager(nil, "svc")

	cases := []struct {
		name  string
		build func() (*Client, error)
	}{
		{"mongo manager on postgres", func() (*Client, error) {
			return NewPostgres(nil, "", WithMongoTenantManager(mb))
		}},
		{"postgres manager on mongodb", func() (*Client, error) {
			return NewMongoDB(nil, "", WithPostgresTenantManager(pg))
		}},
		{"both managers on postgres", func() (*Client, error) {
			return NewPostgres(nil, "", WithPostgresTenantManager(pg), WithMongoTenantManager(mb))
		}},
		{"both managers on mongodb", func() (*Client, error) {
			return NewMongoDB(nil, "", WithPostgresTenantManager(pg), WithMongoTenantManager(mb))
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := tc.build()
			if !errors.Is(err, ErrTenantManagerBackendMismatch) {
				t.Fatalf("err = %v, want ErrTenantManagerBackendMismatch", err)
			}

			if c != nil {
				t.Error("a refused construction returned a Client")
			}
		})
	}
}

// TestBackendConfigsCarryAConnectorOnlyForAManager: a nil manager must leave
// Connector nil so the backend answers a named scope with
// ErrTenantConnectorMissing instead of a connector that fails on every call.
func TestBackendConfigsCarryAConnectorOnlyForAManager(t *testing.T) {
	managed := defaultClientConfig()
	applyClientOptions(&managed, []Option{
		WithPostgresTenantManager(tmpostgres.NewManager(nil, "svc")),
		WithMongoTenantManager(tmmongo.NewManager(nil, "svc")),
	})

	bare := defaultClientConfig()
	applyClientOptions(&bare, []Option{WithPostgresTenantManager(nil), WithMongoTenantManager(nil)})

	if postgresConfig(nil, "", managed).Connector == nil {
		t.Error("postgres: a tenant manager did not reach the backend as a Connector")
	}

	if mongoConfig(nil, "", managed).Connector == nil {
		t.Error("mongodb: a tenant manager did not reach the backend as a Connector")
	}

	if postgresConfig(nil, "", bare).Connector != nil {
		t.Error("postgres: a nil tenant manager produced a Connector")
	}

	if mongoConfig(nil, "", bare).Connector != nil {
		t.Error("mongodb: a nil tenant manager produced a Connector")
	}
}
