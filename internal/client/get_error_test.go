//go:build unit

package client

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// newMultiTenantClientWithLogger is newMultiTenantClient with a logger the
// test can read back.
func newMultiTenantClientWithLogger(t *testing.T, s *memStore, logger log.Logger) *Client {
	t.Helper()

	cfg := defaultClientConfig()
	cfg.multiTenantEnabled = true

	// Through the option rather than the field, so the logger reaching the
	// engine is the guarded one a real constructor hands it.
	applyClientOptions(&cfg, []Option{WithLogger(logger)})

	return newClient(s, cfg)
}

// seedRaw plants bytes the store hands back verbatim, standing in for a row a
// hand-edit or a foreign writer left behind.
func seedRaw(m *memStore, ns, key string, raw []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries[memKey(ns, key)] = store.Entry{Namespace: ns, Key: key, Value: raw}
}

// TestMultiTenantListDecodeErrorWrapsTheCause pins List's read-through
// rejection for an undecodable row: the encoding/json failure stays in the
// chain, offset and all. Nothing in this library masks a value, so the caller
// debugging the row keeps the one detail that locates it. Get's half of the
// same path is pinned by TestMultiTenantDecodeFailureWithNoTenantIDSaysSo.
func TestMultiTenantListDecodeErrorWrapsTheCause(t *testing.T) {
	m := newMemStore(true)
	c := newMultiTenantClient(t, m)

	if err := c.Register("ns", "k", "default"); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })
	seedRaw(m, "ns", "k", []byte(rejectedSecret))

	_, err := c.List(context.Background(), "ns")
	if err == nil {
		t.Fatal("want a decode error, got nil")
	}

	var syntax *json.SyntaxError
	if !errors.As(err, &syntax) {
		t.Errorf("the json cause callers debug the row with is gone: %v", err)
	}

	if !strings.Contains(err.Error(), "ns/k") {
		t.Errorf("the error does not name the namespace/key pair: %v", err)
	}
}
