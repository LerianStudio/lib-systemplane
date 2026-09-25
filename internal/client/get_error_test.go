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

// TestMultiTenantDecodeErrorWrapsTheCause pins the read-through rejection for
// an undecodable row: the encoding/json failure stays in the chain, offset and
// all, for every key. Nothing in this library masks a value, so the caller
// debugging the row keeps the one detail that locates it.
func TestMultiTenantDecodeErrorWrapsTheCause(t *testing.T) {
	raw := []byte(rejectedSecret)

	read := map[string]func(*Client) error{
		"Get": func(c *Client) error {
			_, _, err := c.Get(context.Background(), "ns", "k")

			return err
		},
		"List": func(c *Client) error {
			_, err := c.List(context.Background(), "ns")

			return err
		},
	}

	for name, call := range read {
		t.Run(name, func(t *testing.T) {
			m := newMemStore(true)
			c := newMultiTenantClientWithLogger(t, m, &recordingLogger{})

			if err := c.Register("ns", "k", "default"); err != nil {
				t.Fatalf("register: %v", err)
			}

			if err := c.Start(context.Background()); err != nil {
				t.Fatalf("start: %v", err)
			}

			t.Cleanup(func() { _ = c.Close() })
			seedRaw(m, "ns", "k", raw)

			err := call(c)
			if err == nil {
				t.Fatal("want a decode error, got nil")
			}

			var syntax *json.SyntaxError
			if !errors.As(err, &syntax) {
				t.Errorf("the json cause callers debug the row with is gone: %v", err)
			}

			if !strings.Contains(err.Error(), "ns") || !strings.Contains(err.Error(), "k") {
				t.Errorf("the error names neither namespace nor key: %v", err)
			}
		})
	}
}

// TestTypedGetterErrorNamesTheValue pins the other half: GetDuration and GetInt
// are the two getters whose rejection is actionable only when it says which
// value it rejected, and they say it for every key.
func TestTypedGetterErrorNamesTheValue(t *testing.T) {
	const unparseable = "4 fortnights"

	for _, tt := range []struct {
		name  string
		value any
		call  func(*Client) error
		names string
	}{
		{
			name:  "GetDuration",
			value: unparseable,
			call: func(c *Client) error {
				_, _, err := c.GetDuration(context.Background(), "ns", "k")

				return err
			},
			names: unparseable,
		},
		{
			name:  "GetInt",
			value: 1.5,
			call: func(c *Client) error {
				_, _, err := c.GetInt(context.Background(), "ns", "k")

				return err
			},
			names: "1.5",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newSingleTenantClient(t, newMemStore(false))

			if err := c.Register("ns", "k", tt.value); err != nil {
				t.Fatalf("register: %v", err)
			}

			err := tt.call(c)
			if err == nil {
				t.Fatal("want a validation error, got nil")
			}

			if !errors.Is(err, ErrValidation) {
				t.Errorf("error %v does not wrap ErrValidation", err)
			}

			if !strings.Contains(err.Error(), tt.names) {
				t.Errorf("error %q does not name the value it rejected", err)
			}
		})
	}
}
