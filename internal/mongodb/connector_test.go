//go:build unit

package mongodb

import (
	"context"
	"errors"
	"testing"
)

func TestConnectorWithNilManagerFailsWithSentinel(t *testing.T) {
	t.Parallel()

	c := NewTenantManagerConnector(nil)
	if c == nil {
		t.Fatal("NewTenantManagerConnector(nil) = nil, want a non-nil Connector")
	}

	db, err := c.ResolveDatabase(context.Background(), "t1")
	if !errors.Is(err, ErrMongoMgrUnavailable) {
		t.Fatalf("ResolveDatabase error = %v, want ErrMongoMgrUnavailable", err)
	}

	if db != nil {
		t.Fatalf("ResolveDatabase database = %v, want nil", db)
	}
}

func TestConnectorNilReceiverFailsWithSentinel(t *testing.T) {
	t.Parallel()

	var c *mbMgrConnector

	db, err := c.ResolveDatabase(context.Background(), "t1")
	if !errors.Is(err, ErrMongoMgrUnavailable) {
		t.Fatalf("ResolveDatabase on nil receiver error = %v, want ErrMongoMgrUnavailable", err)
	}

	if db != nil {
		t.Fatalf("ResolveDatabase database = %v, want nil", db)
	}
}

func TestNew_CarriesConnector(t *testing.T) {
	t.Parallel()

	c := NewTenantManagerConnector(nil)

	s, err := New(Config{MultiTenantEnabled: true, Connector: c})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if s.cfg.Connector != c {
		t.Fatalf("cfg.Connector = %v, want the configured connector", s.cfg.Connector)
	}
}
