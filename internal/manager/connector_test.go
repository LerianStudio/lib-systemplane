//go:build unit

package manager

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
