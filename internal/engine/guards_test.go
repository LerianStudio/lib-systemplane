//go:build unit

package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// TestNilEngineIsInert pins every exported method against a nil receiver. The
// Client holds the engine in a field, and a Client built by a path that never
// reached New must degrade to "no cache" rather than take the caller's
// goroutine down.
func TestNilEngineIsInert(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}

	tests := []struct {
		name string
		call func(t *testing.T)
	}{
		{
			name: "Start reports a nil backend",
			call: func(t *testing.T) {
				var e *Engine

				if err := e.Start(context.Background()); !errors.Is(err, store.ErrNilBackend) {
					t.Errorf("Start: got %v, want %v", err, store.ErrNilBackend)
				}
			},
		},
		{
			name: "Publish is dropped",
			call: func(t *testing.T) {
				var e *Engine

				e.Publish(context.Background(), store.Scope{}, store.Entry{Namespace: nk.Namespace, Key: nk.Key})
			},
		},
		{
			name: "PublishDelete reports ErrClosed",
			call: func(t *testing.T) {
				var e *Engine

				if err := e.PublishDelete(store.Scope{}, nk); !errors.Is(err, ErrClosed) {
					t.Errorf("PublishDelete: got %v, want %v", err, ErrClosed)
				}
			},
		},
		{
			name: "Close succeeds",
			call: func(t *testing.T) {
				var e *Engine

				if err := e.Close(); err != nil {
					t.Errorf("Close: got %v, want nil", err)
				}
			},
		},
		{
			name: "Lookup reports a miss",
			call: func(t *testing.T) {
				var e *Engine

				if _, ok := e.Lookup(store.Scope{}, nk); ok {
					t.Error("Lookup: got a hit, want a miss")
				}
			},
		},
		{
			name: "OnChange returns a callable no-op",
			call: func(t *testing.T) {
				var e *Engine

				unsub := e.OnChange(nk, func(context.Context, Change) {})
				if unsub == nil {
					t.Fatal("OnChange returned a nil unsubscribe")
				}

				unsub()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.call(t)
		})
	}
}

// TestStartRefusesAMisbuiltEngine pins the two dependencies Config documents as
// required. Without them the engine cannot reconcile a single key, so Start
// says so instead of subscribing to a feed whose events it would then panic on.
func TestStartRefusesAMisbuiltEngine(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want error
	}{
		{name: "no store and no registry", cfg: Config{}, want: store.ErrNilBackend},
		{name: "no registry", cfg: Config{Store: newFakeStore()}, want: ErrNilRegistry},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(tt.cfg)

			t.Cleanup(func() {
				if err := e.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			})

			// Bounded, so a Start that subscribes instead of refusing fails
			// this test rather than wedging the package run.
			if err := e.Start(startCtx(t, time.Second)); !errors.Is(err, tt.want) {
				t.Errorf("Start: got %v, want %v", err, tt.want)
			}
		})
	}
}

// TestPublishWithoutRegistryIsDropped is the crash this guard exists for: the
// Client publishes on its own caller's goroutine, so a registry the engine was
// never given must read as "nothing registered", not as a nil dereference in
// the middle of somebody's Set.
func TestPublishWithoutRegistryIsDropped(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	e := New(Config{})

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	// The scope is tracked, so the registry guard is what drops this write
	// rather than the scope resolution in front of it.
	track(t, e, store.Scope{})

	e.Publish(context.Background(), store.Scope{}, store.Entry{
		Namespace: nk.Namespace,
		Key:       nk.Key,
		Value:     []byte(`"a"`),
		Revision:  1,
	})

	if _, ok := e.Lookup(store.Scope{}, nk); ok {
		t.Error("Lookup reports a hit: an unregistered key reached the cache")
	}
}

// TestOnChangeAfterCloseRegistersNothing keeps a late subscription from looking
// like a live one. The engine has no workers left to deliver on, so the
// callback would never fire; handing back a usable unsubscribe and an empty
// registry is what makes that visible instead of silently pending.
func TestOnChangeAfterCloseRegistersNothing(t *testing.T) {
	nk := NSKey{Namespace: "billing", Key: "limits"}
	e := New(Config{Store: newFakeStore(), Registry: fakeRegistry{}})

	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	unsub := e.OnChange(nk, func(context.Context, Change) {})
	if unsub == nil {
		t.Fatal("OnChange returned a nil unsubscribe")
	}

	unsub()

	e.subsMu.RLock()
	defer e.subsMu.RUnlock()

	if n := len(e.subscribers[nk]); n != 0 {
		t.Errorf("subscribers after Close: got %d, want 0", n)
	}
}
