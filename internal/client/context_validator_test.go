//go:build unit

package client

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
)

// validatorMarkerKey is private to this file, so a value found under it inside
// a validator can only have travelled through the context the test handed to
// Set. A shared key type would leave that open to coincidence.
type validatorMarkerKey struct{}

// startForValidator starts c and closes it on cleanup. Registration stays in
// each test, because the validator under test is a registration option.
func startForValidator(t *testing.T, c *Client) {
	t.Helper()

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })
}

// TestContextValidatorSeesTheSetContext pins the reason this option exists: a
// validator that has to read another system with the tenant the caller carried
// needs the context of the write, and Set already holds it.
func TestContextValidatorSeesTheSetContext(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	// Every observation is kept, not just the last: the write Set persists
	// comes back through the changefeed, and that refresh validates the same
	// value again with its own context, which carries no marker.
	var (
		seenMu sync.Mutex
		seen   []any
	)

	err := c.Register("ns", "k", "default", WithContextValidator(func(ctx context.Context, _ any) error {
		seenMu.Lock()
		defer seenMu.Unlock()

		seen = append(seen, ctx.Value(validatorMarkerKey{}))

		return nil
	}))
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	startForValidator(t, c)

	ctx := context.WithValue(context.Background(), validatorMarkerKey{}, "tenant-42")

	if err := c.Set(ctx, "ns", "k", "new", "operator"); err != nil {
		t.Fatalf("set: %v", err)
	}

	seenMu.Lock()
	defer seenMu.Unlock()

	if !slices.Contains(seen, any("tenant-42")) {
		t.Errorf("validator saw %v, want one call carrying the marker from the Set context", seen)
	}
}

// TestContextValidatorRejectionPersistsNothing pins that a refusal from the
// context validator is an ErrValidation that keeps its own cause and leaves the
// value in force untouched.
func TestContextValidatorRejectionPersistsNothing(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	sentinel := errors.New("rejected by the other system")

	err := c.Register("ns", "k", "default", WithContextValidator(func(_ context.Context, value any) error {
		if value == "bad" {
			return sentinel
		}

		return nil
	}))
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	startForValidator(t, c)

	setErr := c.Set(context.Background(), "ns", "k", "bad", "operator")
	if !errors.Is(setErr, ErrValidation) {
		t.Errorf("set error = %v, want ErrValidation", setErr)
	}

	if !errors.Is(setErr, sentinel) {
		t.Errorf("set error = %v, want the validator's own error preserved", setErr)
	}

	m.mu.Lock()
	_, stored := m.entries[memKey("ns", "k")]
	m.mu.Unlock()

	if stored {
		t.Error("a rejected Set wrote a row to the store")
	}

	got, ok, err := c.Get(context.Background(), "ns", "k")
	if err != nil || !ok {
		t.Fatalf("get: value=%v ok=%v err=%v", got, ok, err)
	}

	if got != "default" {
		t.Errorf("got %v, want the registered default after a rejected write", got)
	}
}

// TestValidatorStillRejectsOnSet is the regression guard on the existing
// option: WithValidator keeps its signature and its behavior, and only stops
// seeing a context it never had.
func TestValidatorStillRejectsOnSet(t *testing.T) {
	m := newMemStore(false)
	c := newSingleTenantClient(t, m)

	err := c.Register("ns", "k", "default", WithValidator(func(value any) error {
		if value == "bad" {
			return errors.New("nope")
		}

		return nil
	}))
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	startForValidator(t, c)

	if err := c.Set(context.Background(), "ns", "k", "bad", "operator"); !errors.Is(err, ErrValidation) {
		t.Errorf("rejected value: got %v, want ErrValidation", err)
	}

	m.mu.Lock()
	_, stored := m.entries[memKey("ns", "k")]
	m.mu.Unlock()

	if stored {
		t.Error("a rejected Set wrote a row to the store")
	}

	if err := c.Set(context.Background(), "ns", "k", "good", "operator"); err != nil {
		t.Fatalf("accepted value: got %v, want nil", err)
	}

	m.mu.Lock()
	_, stored = m.entries[memKey("ns", "k")]
	m.mu.Unlock()

	if !stored {
		t.Error("the accepted value was not persisted")
	}
}

// TestRegisterValidatesTheDefaultWithANonNilContext pins the contract stated on
// WithContextValidator: the registered default is not a write and carries no
// request scope, but the context it is validated with is never nil.
func TestRegisterValidatesTheDefaultWithANonNilContext(t *testing.T) {
	c := newSingleTenantClient(t, newMemStore(false))

	var ran bool

	err := c.Register("ns", "k", "default", WithContextValidator(func(ctx context.Context, value any) error {
		ran = true

		if ctx == nil {
			return errors.New("registration handed the validator a nil context")
		}

		if value != "default" {
			return errors.New("registration validated something other than the default")
		}

		return nil
	}))
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	if !ran {
		t.Error("the context validator never ran against the registered default")
	}
}

// TestLastValidatorOptionAppliedWins pins the resolution rule between the two
// options, in both orders, and that a nil function changes nothing.
func TestLastValidatorOptionAppliedWins(t *testing.T) {
	reject := func(any) error { return errors.New("plain validator rejected") }
	ctxSentinel := errors.New("context validator rejected")
	rejectCtx := func(context.Context, any) error { return ctxSentinel }
	accept := func(any) error { return nil }
	acceptCtx := func(context.Context, any) error { return nil }

	t.Run("context validator applied last replaces the plain one", func(t *testing.T) {
		c := newSingleTenantClient(t, newMemStore(false))

		if err := c.Register("ns", "k", "default", WithValidator(reject), WithContextValidator(acceptCtx)); err != nil {
			t.Errorf("register: got %v, want the accepting context validator to have replaced the rejecting one", err)
		}
	})

	t.Run("plain validator applied last replaces the context one", func(t *testing.T) {
		c := newSingleTenantClient(t, newMemStore(false))

		if err := c.Register("ns", "k", "default", WithContextValidator(rejectCtx), WithValidator(accept)); err != nil {
			t.Errorf("register: got %v, want the accepting plain validator to have replaced the rejecting one", err)
		}
	})

	t.Run("a nil context validator is ignored", func(t *testing.T) {
		c := newSingleTenantClient(t, newMemStore(false))

		err := c.Register("ns", "k", "default", WithContextValidator(rejectCtx), WithContextValidator(nil))
		if !errors.Is(err, ErrValidation) {
			t.Errorf("register: got %v, want ErrValidation — a nil WithContextValidator must not clear the validator", err)
		}

		if !errors.Is(err, ctxSentinel) {
			t.Errorf("register: got %v, want the validator's own error preserved through the \"default value rejected\" wrap", err)
		}
	})
}

// TestValidationFailureIsLabelledOnce pins the shape of the error a refused
// value comes back as.
//
// The engine's own recovery already labels a panicking validator with the
// store's validation sentinel — the very value this package exports as
// ErrValidation — so both call sites wrapping it again reported "validation
// failed: validation failed: validator panicked" to the consumer. The label is
// what errors.Is matches on; saying it twice is noise in every log line and
// every API response built from the message.
func TestValidationFailureIsLabelledOnce(t *testing.T) {
	label := ErrValidation.Error()
	explode := func(context.Context, any) error { panic("the validator blew up") }
	refuse := errors.New("the validator refused")

	assertOnce := func(t *testing.T, err error) {
		t.Helper()

		if err == nil {
			t.Fatal("want a validation error, got nil")
		}

		if !errors.Is(err, ErrValidation) {
			t.Errorf("got %v, want it to match ErrValidation", err)
		}

		if n := strings.Count(err.Error(), label); n != 1 {
			t.Errorf("got %q — %d occurrences of %q, want exactly 1", err, n, label)
		}
	}

	t.Run("register with a panicking validator", func(t *testing.T) {
		c := newSingleTenantClient(t, newMemStore(false))

		assertOnce(t, c.Register("ns", "k", "default", WithContextValidator(explode)))
	})

	t.Run("set with a panicking validator", func(t *testing.T) {
		c := newSingleTenantClient(t, newMemStore(false))

		if err := c.Register("ns", "k", "default", WithContextValidator(func(_ context.Context, v any) error {
			if v == "boom" {
				panic("the validator blew up")
			}

			return nil
		})); err != nil {
			t.Fatalf("register: %v", err)
		}

		startForValidator(t, c)

		assertOnce(t, c.Set(context.Background(), "ns", "k", "boom", "tester"))
	})

	// A validator that returns its own error carries no label of its own, so
	// this branch must still add one — and keep the validator's error reachable.
	t.Run("set with a refusing validator", func(t *testing.T) {
		c := newSingleTenantClient(t, newMemStore(false))

		if err := c.Register("ns", "k", "default", WithContextValidator(func(_ context.Context, v any) error {
			if v == "no" {
				return refuse
			}

			return nil
		})); err != nil {
			t.Fatalf("register: %v", err)
		}

		startForValidator(t, c)

		err := c.Set(context.Background(), "ns", "k", "no", "tester")
		assertOnce(t, err)

		if !errors.Is(err, refuse) {
			t.Errorf("got %v, want the validator's own error preserved", err)
		}
	})
}
