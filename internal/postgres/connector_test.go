//go:build unit

package postgres

import (
	"context"
	"errors"
	"testing"
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
}
