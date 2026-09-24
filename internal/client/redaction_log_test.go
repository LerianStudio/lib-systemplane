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
// test can read back, which is the whole subject below.
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

// decodeErrText is what encoding/json says about raw, which is what must never
// reach the log for a redacted key: the message quotes the row's first byte.
func decodeErrText(t *testing.T, raw []byte) string {
	t.Helper()

	var decoded any

	err := json.Unmarshal(raw, &decoded)
	if err == nil {
		t.Fatalf("seed %q decodes cleanly; the test needs a row that does not", raw)
	}

	return err.Error()
}

// TestMultiTenantDecodeFailureHonorsTheKeyRedaction closes the asymmetry the
// Phase 2 review found: every single-tenant ingress renders a decode failure
// through the engine's policy-aware detail, while the two multi-tenant
// read-through paths rendered the raw encoding/json error. That error quotes
// the offending byte of the row, so a key the consumer registered as redacted
// was protected on one path and published on the other.
func TestMultiTenantDecodeFailureHonorsTheKeyRedaction(t *testing.T) {
	raw := []byte(rejectedSecret)
	leak := decodeErrText(t, raw)

	assertProtected := func(t *testing.T, logger *recordingLogger) {
		t.Helper()

		lines := logger.errs("failed to unmarshal stored value")
		if len(lines) != 1 {
			t.Fatalf("got %d ERROR lines for the undecodable row, want exactly 1: %s", len(lines), logger.rendered())
		}

		rendered := logger.rendered()
		if strings.Contains(rendered, rejectedSecret) {
			t.Errorf("the log carries the stored row of a redacted key: %s", rendered)
		}

		if strings.Contains(rendered, leak) {
			t.Errorf("the log carries the json error text, which quotes the row's bytes: %s", rendered)
		}

		var detail string

		for _, f := range lines[0].structured() {
			if f.Key == "error" {
				detail, _ = f.Value.(string)
			}
		}

		if !strings.Contains(detail, "decode failed") {
			t.Errorf(`the line carries error = %q; a redacted key must still say WHAT failed and its type`, detail)
		}
	}

	t.Run("Get", func(t *testing.T) {
		m := newMemStore(true)
		logger := &recordingLogger{}
		c := newMultiTenantClientWithLogger(t, m, logger)

		if err := c.Register("ns", "k", "default", WithRedaction(RedactFull)); err != nil {
			t.Fatalf("register: %v", err)
		}

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}

		t.Cleanup(func() { _ = c.Close() })

		seedRaw(m, "ns", "k", raw)

		if _, _, err := c.Get(context.Background(), "ns", "k"); err == nil {
			t.Fatal("Get: want a decode error, got nil")
		}

		assertProtected(t, logger)
	})

	t.Run("List", func(t *testing.T) {
		m := newMemStore(true)
		logger := &recordingLogger{}
		c := newMultiTenantClientWithLogger(t, m, logger)

		if err := c.Register("ns", "k", "default", WithRedaction(RedactFull)); err != nil {
			t.Fatalf("register: %v", err)
		}

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}

		t.Cleanup(func() { _ = c.Close() })

		seedRaw(m, "ns", "k", raw)

		if _, err := c.List(context.Background(), "ns"); err == nil {
			t.Fatal("List: want a decode error, got nil")
		}

		assertProtected(t, logger)
	})

	// The policy is consulted, not applied blindly: an ordinary key keeps the
	// cause an operator needs to debug the row.
	t.Run("UnredactedKeyKeepsTheCause", func(t *testing.T) {
		m := newMemStore(true)
		logger := &recordingLogger{}
		c := newMultiTenantClientWithLogger(t, m, logger)

		if err := c.Register("ns", "k", "default"); err != nil {
			t.Fatalf("register: %v", err)
		}

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}

		t.Cleanup(func() { _ = c.Close() })

		seedRaw(m, "ns", "k", raw)

		if _, _, err := c.Get(context.Background(), "ns", "k"); err == nil {
			t.Fatal("Get: want a decode error, got nil")
		}

		if !strings.Contains(logger.rendered(), leak) {
			t.Errorf("an unredacted key lost the decode cause: %s", logger.rendered())
		}
	})
}

// TestSingleTenantDecodeFailureWithholdsTheRedactedRow is the Client-level end
// of the redaction port. Task 2.1.3 pinned the mapping alone — RedactFull on
// Register becomes Redacted on the engine's KeyDef — and nothing proved the
// policy survived the rest of the way: a key the consumer registered as
// redacted, whose stored row is not JSON at all, must leave no fragment of that
// row in the line the operator reads, and the read must still answer with the
// registered default rather than with an error the consumer cannot act on.
func TestSingleTenantDecodeFailureWithholdsTheRedactedRow(t *testing.T) {
	raw := []byte(rejectedSecret)
	leak := decodeErrText(t, raw)

	m := newMemStore(false)
	logger := &recordingLogger{}
	c := newSingleTenantClientWithLogger(t, m, logger)

	if err := c.Register("ns", "k", "default", WithRedaction(RedactFull)); err != nil {
		t.Fatalf("register: %v", err)
	}

	seedRaw(m, "ns", "k", raw)

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	lines := logger.warns("failed to unmarshal stored value, keeping cached value")
	if len(lines) != 1 {
		t.Fatalf("got %d WARN lines for the undecodable row, want exactly 1: %s", len(lines), logger.rendered())
	}

	// Not merely the whole row: any run of it is a fragment of a secret handed
	// to whatever ships the logs, and the json error quotes the row byte by
	// byte rather than wholesale.
	rendered := logger.rendered()

	for i := 0; i+4 <= len(rejectedSecret); i++ {
		if fragment := rejectedSecret[i : i+4]; strings.Contains(rendered, fragment) {
			t.Fatalf("the log carries %q, a fragment of the stored row of a redacted key: %s", fragment, rendered)
		}
	}

	if strings.Contains(rendered, leak) {
		t.Errorf("the log carries the json error text, which quotes the row's bytes: %s", rendered)
	}

	var detail string

	for _, f := range lines[0].structured() {
		if f.Key == "error" {
			detail, _ = f.Value.(string)
		}
	}

	if !strings.Contains(detail, "decode failed") || !strings.Contains(detail, "json.SyntaxError") {
		t.Errorf(`the line carries error = %q; a redacted key must still say WHAT failed and its type`, detail)
	}

	// The row is unusable, so the key falls back to what the consumer
	// registered instead of reporting a miss.
	v, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("get: value=%v ok=%v err=%v", v, ok, err)
	}

	if v != "default" {
		t.Errorf("value in force = %v, want the registered default", v)
	}
}

// TestMultiTenantDecodeErrorWithholdsTheRedactedRow closes the leak beside the
// one the test above closed.
//
// Both read-through paths render their LOG line through the engine's
// policy-aware detail, and then return an error that wraps the raw
// encoding/json failure — which quotes the offending byte of the row and
// carries its offset for anyone who unwraps it. An error is the half of the
// report the consumer is most likely to persist: it reaches a response body,
// an error tracker, a retry log. A key registered redacted must not have its
// row quoted there either.
func TestMultiTenantDecodeErrorWithholdsTheRedactedRow(t *testing.T) {
	raw := []byte(rejectedSecret)
	leak := decodeErrText(t, raw)

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

	start := func(t *testing.T, opts ...KeyOption) (*Client, *memStore) {
		t.Helper()

		m := newMemStore(true)
		c := newMultiTenantClientWithLogger(t, m, &recordingLogger{})

		if err := c.Register("ns", "k", "default", opts...); err != nil {
			t.Fatalf("register: %v", err)
		}

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}

		t.Cleanup(func() { _ = c.Close() })
		seedRaw(m, "ns", "k", raw)

		return c, m
	}

	for name, call := range read {
		t.Run(name+"/redacted", func(t *testing.T) {
			c, _ := start(t, WithRedaction(RedactFull))

			err := call(c)
			if err == nil {
				t.Fatal("want a decode error, got nil")
			}

			// Not merely the whole row: any run of it is a fragment of a
			// secret, and the json error quotes the row byte by byte.
			for i := 0; i+4 <= len(rejectedSecret); i++ {
				if fragment := rejectedSecret[i : i+4]; strings.Contains(err.Error(), fragment) {
					t.Fatalf("the error carries %q, a fragment of the stored row: %v", fragment, err)
				}
			}

			if strings.Contains(err.Error(), leak) {
				t.Errorf("the error carries the json message, which quotes the row: %v", err)
			}

			// The offset travels on the json error itself, so leaving that
			// error in the chain hands the row's shape to anyone who unwraps.
			var syntax *json.SyntaxError
			if errors.As(err, &syntax) {
				t.Errorf("the json error is still in the chain, offset %d and all: %v", syntax.Offset, err)
			}

			if !strings.Contains(err.Error(), "ns") || !strings.Contains(err.Error(), "k") {
				t.Errorf("the error names neither namespace nor key: %v", err)
			}
		})

		t.Run(name+"/unredacted", func(t *testing.T) {
			c, _ := start(t)

			err := call(c)
			if err == nil {
				t.Fatal("want a decode error, got nil")
			}

			var syntax *json.SyntaxError
			if !errors.As(err, &syntax) {
				t.Errorf("an ordinary key lost the json cause callers debug the row with: %v", err)
			}
		})
	}
}
