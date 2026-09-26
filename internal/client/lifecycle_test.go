//go:build unit

package client

import (
	"context"
	"errors"
	"testing"
	"time"

	tmevent "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/event"
	"github.com/LerianStudio/lib-systemplane/v4/internal/engine"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
)

// t1Events builds one lifecycle event per type, each for tenant t1.
func t1Events(types ...string) []tmevent.TenantLifecycleEvent {
	events := make([]tmevent.TenantLifecycleEvent, len(types))
	for i, eventType := range types {
		events[i] = tmevent.TenantLifecycleEvent{EventType: eventType, TenantID: "t1"}
	}

	return events
}

// TestHandleTenantLifecycleGuards: a nil Client and one without a tenant
// manager ignore an event; a tenant-managed one refuses an event with no tenant.
func TestHandleTenantLifecycleGuards(t *testing.T) {
	unmanaged, err := NewForTesting(newTenantStore(), WithMultiTenantEnabled())
	if err != nil {
		t.Fatalf("NewForTesting: %v", err)
	}

	defer unmanaged.Close()

	event := tmevent.TenantLifecycleEvent{EventType: tmevent.EventTenantSuspended, TenantID: "t1"}
	for _, c := range []*Client{nil, unmanaged} {
		if err := c.HandleTenantLifecycle(context.Background(), event); err != nil {
			t.Errorf("a Client managing no tenant: err = %v, want nil", err)
		}
	}

	event.TenantID = ""
	managed := newTenantClient(t, newTenantStore(), registerKey("ns", "k", "default"))

	if err := managed.HandleTenantLifecycle(context.Background(), event); !errors.Is(err, ErrValidation) {
		t.Errorf("an event with no tenant id: err = %v, want ErrValidation", err)
	}
}

// TestHandleTenantLifecycle sends each row's events to a tenant-managed Client
// serving t1's ns/k from its first feed.
func TestHandleTenantLifecycle(t *testing.T) {
	const (
		untouched  = iota // t1 still served from its first feed
		perRequest        // t1 dropped: a read goes per request and opens no feed
		rebuilt           // t1 served from a second feed
	)

	cases := map[string]struct {
		events []tmevent.TenantLifecycleEvent
		want   int
	}{
		"suspended drops the scope":                {t1Events(tmevent.EventTenantSuspended), perRequest},
		"deleted drops the scope":                  {t1Events(tmevent.EventTenantDeleted), perRequest},
		"activated after suspended lets a read in": {t1Events(tmevent.EventTenantSuspended, tmevent.EventTenantActivated), rebuilt},
		"rotation rebuilds an active scope":        {t1Events(tmevent.EventTenantCredentialsRotated), rebuilt},
		"rotation leaves a blocked tenant blocked": {t1Events(tmevent.EventTenantSuspended, tmevent.EventTenantCredentialsRotated), perRequest},
		"an unrouted type changes nothing":         {t1Events(tmevent.EventTenantServiceSuspended), untouched},
		"activated opens no feed for an unread tenant": {
			[]tmevent.TenantLifecycleEvent{{EventType: tmevent.EventTenantActivated, TenantID: "t2"}}, untouched,
		},
	}

	scope, nk := store.Scope{Tenant: "t1"}, engine.NSKey{Namespace: "ns", Key: "k"}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := newTenantStore()
			s.seed(t, "t1", "ns", "k", "stored", 1, "a")
			c := newTenantClient(t, s, registerKey("ns", "k", "default"))

			mustEntry(t, c, tenantCtx("t1"), "ns", "k")
			waitSettled(t, c, "t1", "ns", "k")

			for _, event := range tc.events {
				if err := c.HandleTenantLifecycle(context.Background(), event); err != nil {
					t.Fatalf("HandleTenantLifecycle(%s): %v", event.EventType, err)
				}
			}

			if tc.want == rebuilt {
				waitFor(t, func() bool {
					mustEntry(t, c, tenantCtx("t1"), "ns", "k")
					e, ok := c.engine.Lookup(scope, nk)
					_, _, subscribes := s.counts()

					return ok && !e.Stale && subscribes == 2
				}, "t1 never served from a second feed")

				return
			}

			if tc.want == perRequest {
				waitFor(t, func() bool {
					_, cached := c.engine.Lookup(scope, nk)
					s.mu.Lock()
					_, open := s.feeds["t1"]
					s.mu.Unlock()

					return !cached && !open
				}, "t1's scope was never dropped and its feed closed")

				if e := mustEntry(t, c, tenantCtx("t1"), "ns", "k"); e.Value != "stored" {
					t.Fatalf("read after the drop = %+v, want the stored row per request", e)
				}
			}

			time.Sleep(100 * time.Millisecond) // a wrongly reached verb, or a read that re-activated t1, has acted by now

			e, ok := c.engine.Lookup(scope, nk)
			if _, _, subscribes := s.counts(); ok != (tc.want == untouched) || e.Stale || subscribes != 1 {
				t.Fatalf("t1 cached = (%+v, %v) over %d feeds, want cached %v on its first feed", e, ok, subscribes, tc.want == untouched)
			}
		})
	}
}
