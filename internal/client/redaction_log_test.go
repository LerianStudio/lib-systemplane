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

// TestTypedGetterErrorWithholdsTheRedactedValue closes the last ingress that
// still printed a value the registration says must never be printed.
//
// GetDuration and GetInt are the two typed getters whose rejection quotes what
// it rejected: GetDuration renders the raw string with %q AND wraps
// time.ParseDuration, whose own message quotes its input a second time, and
// GetInt renders the number. Every other getter reports a shape mismatch with
// %T and carries nothing. The error is the surface that travels furthest — a
// response body, an error tracker — so a redacted key gets the dynamic type and
// a reason, and never the value.
func TestTypedGetterErrorWithholdsTheRedactedValue(t *testing.T) {
	const unparseable = "s3cr3t-not-a-duration"

	for _, tt := range []struct {
		name   string
		value  any
		call   func(*Client) error
		leaks  []string
		policy RedactPolicy
	}{
		{
			name:  "GetDuration on a redacted key",
			value: unparseable,
			call: func(c *Client) error {
				_, _, err := c.GetDuration(context.Background(), "ns", "k")

				return err
			},
			leaks:  []string{unparseable, "s3cr3t"},
			policy: RedactFull,
		},
		{
			name:  "GetInt on a redacted key",
			value: 1.5,
			call: func(c *Client) error {
				_, _, err := c.GetInt(context.Background(), "ns", "k")

				return err
			},
			leaks:  []string{"1.5"},
			policy: RedactMask,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newSingleTenantClient(t, newMemStore(false))

			if err := c.Register("ns", "k", tt.value, WithRedaction(tt.policy)); err != nil {
				t.Fatalf("register: %v", err)
			}

			err := tt.call(c)
			if err == nil {
				t.Fatal("want a validation error, got nil")
			}

			if !errors.Is(err, ErrValidation) {
				t.Errorf("error %v does not wrap ErrValidation", err)
			}

			for _, leak := range tt.leaks {
				if strings.Contains(err.Error(), leak) {
					t.Errorf("error %q carries %q, the value of a redacted key", err, leak)
				}
			}

			if !strings.Contains(err.Error(), "value withheld") {
				t.Errorf("error %q does not say the value was withheld", err)
			}
		})
	}
}

// TestTypedGetterErrorNamesTheValueOfAnUnredactedKey is the twin: withholding
// is the key's registration speaking, not a blanket loss of the one detail that
// makes the rejection actionable.
func TestTypedGetterErrorNamesTheValueOfAnUnredactedKey(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("ns", "k", "4 fortnights"); err != nil {
		t.Fatalf("register: %v", err)
	}

	_, _, err := c.GetDuration(context.Background(), "ns", "k")
	if err == nil {
		t.Fatal("want a validation error, got nil")
	}

	if !strings.Contains(err.Error(), "4 fortnights") {
		t.Errorf("error %q does not name the value of an unredacted key", err)
	}
}

// TestTypedGetterRedactionSurvivesACloseMidRead pins WHERE the typed getters
// read the key's redaction from.
//
// The gate must carry the fact out of the same registry lookup that produced
// the value. Looking it back up afterwards through the public KeyRedaction
// accessor is a second, differently-timed registry read, and it answers from
// whatever the Client is by then: once closed, it answers RedactFull for every
// key without consulting the registration at all. So a Close landing between
// the read and the check grades the value against a policy that read never
// produced — the key registered in the clear is over-withheld, and its
// rejection loses the one detail that makes it actionable. The carried fact is
// right on both rows, whatever lands under the read.
//
// The store hook closes the Client under the read it is serving, which is the
// race made deterministic.
func TestTypedGetterRedactionSurvivesACloseMidRead(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  string
		// value is the malformed value as the rejection would print it: what a
		// redacted key's error must never carry, and what an unredacted key's
		// must.
		value string
		call  func(*Client) error
	}{
		{
			name:  "GetDuration",
			raw:   `"` + rejectedSecret + `"`,
			value: rejectedSecret,
			call: func(c *Client) error {
				_, _, err := c.GetDuration(context.Background(), "ns", "k")

				return err
			},
		},
		{
			name:  "GetInt",
			raw:   `1.5`,
			value: "1.5",
			call: func(c *Client) error {
				_, _, err := c.GetInt(context.Background(), "ns", "k")

				return err
			},
		},
	} {
		for _, policy := range []RedactPolicy{RedactFull, RedactNone} {
			t.Run(tt.name+"/"+policy.String(), func(t *testing.T) {
				m := newMemStore(true)
				c := newMultiTenantClient(t, m)

				if err := c.Register("ns", "k", "1s", WithRedaction(policy)); err != nil {
					t.Fatalf("register: %v", err)
				}

				m.getHook = func(ns, key string) (store.Entry, bool, bool) {
					_ = c.Close()

					return store.Entry{Namespace: ns, Key: key, Value: []byte(tt.raw)}, true, true
				}

				err := tt.call(c)
				if err == nil {
					t.Fatal("want a validation error, got nil")
				}

				if policy == RedactNone {
					if !strings.Contains(err.Error(), tt.value) {
						t.Errorf("error %q does not name %q, the value of a key nobody registered redacted: "+
							"the gate graded it against a policy its own read never produced", err, tt.value)
					}

					if strings.Contains(err.Error(), "value withheld") {
						t.Errorf("error %q withholds the value of a key registered in the clear", err)
					}

					return
				}

				if strings.Contains(err.Error(), tt.value) {
					t.Errorf("error %q carries %q, the value of a redacted key", err, tt.value)
				}

				if !strings.Contains(err.Error(), "value withheld") {
					t.Errorf("error %q does not say the value was withheld", err)
				}
			})
		}
	}
}

// TestGetEntryCarriesTheRegisteredPolicy pins WHAT the single read path hands
// back beside the value: the key's policy itself, on every path that answers
// with a value.
//
// RedactMask is the subject because it is the answer a collapse cannot fake:
// a boolean, or a gate that re-reads the registration through some other
// accessor, can still come out right on a RedactNone and a RedactFull key and
// be wrong here. ok=false carries RedactNone because there is no value to
// grade.
func TestGetEntryCarriesTheRegisteredPolicy(t *testing.T) {
	register := func(t *testing.T, c *Client) {
		t.Helper()

		if err := c.Register("ns", "k", "1s", WithRedaction(RedactMask)); err != nil {
			t.Fatalf("register: %v", err)
		}
	}

	assertMask := func(t *testing.T, policy RedactPolicy, ok bool, err error) {
		t.Helper()

		if err != nil {
			t.Fatalf("getEntry: %v", err)
		}

		if !ok {
			t.Fatal("getEntry reports the registered key as unknown")
		}

		if policy != RedactMask {
			t.Errorf("getEntry returned policy %v, want %v as registered", policy, RedactMask)
		}
	}

	t.Run("single-tenant hit", func(t *testing.T) {
		m := newMemStore(false)
		c := newSingleTenantClient(t, m)

		register(t, c)
		seedEntry(t, m, "ns", "k", "2s")

		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}

		t.Cleanup(func() { _ = c.Close() })

		e, policy, ok, err := c.getEntry(context.Background(), "ns", "k")
		assertMask(t, policy, ok, err)

		if e.Value != "2s" {
			t.Fatalf("value = %v, want the stored row: the policy was graded off the default, not a hit", e.Value)
		}
	})

	t.Run("multi-tenant row found", func(t *testing.T) {
		m := newMemStore(true)
		c := newMultiTenantClient(t, m)

		register(t, c)
		seedEntry(t, m, "ns", "k", "2s")
		t.Cleanup(func() { _ = c.Close() })

		e, policy, ok, err := c.getEntry(context.Background(), "ns", "k")
		assertMask(t, policy, ok, err)

		if e.Value != "2s" {
			t.Fatalf("value = %v, want the stored row", e.Value)
		}
	})

	t.Run("multi-tenant row absent", func(t *testing.T) {
		m := newMemStore(true)
		c := newMultiTenantClient(t, m)

		register(t, c)
		t.Cleanup(func() { _ = c.Close() })

		e, policy, ok, err := c.getEntry(context.Background(), "ns", "k")
		assertMask(t, policy, ok, err)

		if e.Value != "1s" {
			t.Fatalf("value = %v, want the registered default", e.Value)
		}
	})

	t.Run("unregistered key", func(t *testing.T) {
		c := newSingleTenantClient(t, newMemStore(false))
		t.Cleanup(func() { _ = c.Close() })

		_, policy, ok, err := c.getEntry(context.Background(), "ns", "never-registered")
		if err != nil {
			t.Fatalf("getEntry: %v", err)
		}

		if ok {
			t.Fatal("getEntry reports an unregistered key as known")
		}

		if policy != RedactNone {
			t.Errorf("getEntry returned policy %v for an unregistered key, want %v: nothing declared it sensitive "+
				"and there is no value of it to grade", policy, RedactNone)
		}
	})
}

// TestKeyRedactionOnAClosedClientFailsClosed pins the DIRECTION of the
// accessor's closed short-circuit.
//
// Every caller of KeyRedaction uses the answer to decide what a value may
// show, and the admin HTTP handlers ask for it AFTER their GetEntry read: a
// Close landing in that window used to make the accessor report RedactNone,
// and the handler then rendered a RedactFull key's value in clear into the
// response body — the same degrade-open shape the typed getters were fixed
// for, at a strictly worse sink. Bind reads it once too, to decide whether a
// group's document may ever reach a log line.
//
// A Client that can no longer read its registry must therefore withhold, not
// disclose. The registered key and the unregistered one are asserted together
// because they are different questions: the first has a value to protect, the
// second has none and nothing ever declared it sensitive.
func TestKeyRedactionOnAClosedClientFailsClosed(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	if err := c.Register("ns", "k", "1s", WithRedaction(RedactFull)); err != nil {
		t.Fatalf("register: %v", err)
	}

	if got := c.KeyRedaction("ns", "k"); got != RedactFull {
		t.Fatalf("KeyRedaction on an open Client = %v, want RedactFull", got)
	}

	if got := c.KeyRedaction("ns", "never-registered"); got != RedactNone {
		t.Errorf("KeyRedaction for an unregistered key on an OPEN Client = %v, want RedactNone: this is the "+
			"branch Bind reads to decide whether a group's document may reach a log line, and nothing "+
			"registered the key, so nothing declared it sensitive", got)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := c.KeyRedaction("ns", "k"); got != RedactFull {
		t.Errorf("KeyRedaction after Close = %v, want RedactFull: a closed Client must never widen "+
			"disclosure — the admin GET and list handlers render the value under whatever this returns", got)
	}

	if got := c.KeyRedaction("ns", "never-registered"); got != RedactFull {
		t.Errorf("KeyRedaction for an unregistered key after Close = %v, want RedactFull: the closed "+
			"short-circuit answers before the registry is consulted at all", got)
	}

	var nilClient *Client
	if got := nilClient.KeyRedaction("ns", "k"); got != RedactFull {
		t.Errorf("KeyRedaction on a nil Client = %v, want RedactFull", got)
	}
}
