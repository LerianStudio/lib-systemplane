//go:build unit

package manager

import (
	"context"
	"errors"
	"testing"

	tmevent "github.com/LerianStudio/lib-commons/v6/commons/tenant-manager/event"
	"github.com/LerianStudio/lib-observability/v2/log"
)

// captureLogger records every Log call so tests can assert on emitted
// level/message/fields without a live logging backend.
type captureLogger struct {
	entries []captureEntry
}

type captureEntry struct {
	level  log.Level
	msg    string
	fields []log.Field
}

func (c *captureLogger) Log(_ context.Context, level log.Level, msg string, fields ...log.Field) {
	c.entries = append(c.entries, captureEntry{level: level, msg: msg, fields: fields})
}

func (c *captureLogger) With(...log.Field) log.Logger { return c }
func (c *captureLogger) WithGroup(string) log.Logger  { return c }
func (c *captureLogger) Enabled(log.Level) bool       { return true }
func (c *captureLogger) Sync(context.Context) error   { return nil }

func (c *captureLogger) warnEntries() []captureEntry {
	var out []captureEntry
	for _, e := range c.entries {
		if e.level == log.LevelWarn {
			out = append(out, e)
		}
	}

	return out
}

// installHandlerSpy replaces the four On* handler seams on m with stubs that
// record which one was invoked and return retErr. It returns a pointer to the
// recorded handler name (empty if none fired).
func installHandlerSpy(m *Manager, retErr error) *string {
	var fired string

	got := &fired

	m.onTenantActivated = func(_ context.Context, _ string) error {
		*got = "activated"

		return retErr
	}
	m.onTenantSuspended = func(_ context.Context, _ string) error {
		*got = "suspended"

		return retErr
	}
	m.onTenantDeleted = func(_ context.Context, _ string) error {
		*got = "deleted"

		return retErr
	}
	m.onTenantCredentialsRotated = func(_ context.Context, _ string) error {
		*got = "rotated"

		return retErr
	}

	return got
}

func TestHandleTenantLifecycle_RoutesToCorrectHandler(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		eventType string
		want      string
	}{
		{"activated", tmevent.EventTenantActivated, "activated"},
		{"suspended", tmevent.EventTenantSuspended, "suspended"},
		{"deleted", tmevent.EventTenantDeleted, "deleted"},
		{"rotated", tmevent.EventTenantCredentialsRotated, "rotated"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := New(nil)
			fired := installHandlerSpy(m, nil)

			err := m.HandleTenantLifecycle(context.Background(), tmevent.TenantLifecycleEvent{
				EventType: tc.eventType,
				TenantID:  "tenant-a",
			})
			if err != nil {
				t.Fatalf("HandleTenantLifecycle(%s): unexpected error %v", tc.eventType, err)
			}

			if *fired != tc.want {
				t.Fatalf("event %q routed to %q, want %q", tc.eventType, *fired, tc.want)
			}
		})
	}
}

func TestHandleTenantLifecycle_UnrelatedEvent_NoOp(t *testing.T) {
	t.Parallel()

	for _, et := range []string{tmevent.EventTenantCreated, "tenant.service.associated", ""} {
		m := New(nil)
		fired := installHandlerSpy(m, nil)

		err := m.HandleTenantLifecycle(context.Background(), tmevent.TenantLifecycleEvent{
			EventType: et,
			TenantID:  "tenant-a",
		})
		if err != nil {
			t.Fatalf("HandleTenantLifecycle(%q): unexpected error %v", et, err)
		}

		if *fired != "" {
			t.Fatalf("event %q must be ignored, but routed to %q", et, *fired)
		}
	}
}

func TestHandleTenantLifecycle_HandlerError_SwallowedAndLoggedWarn(t *testing.T) {
	t.Parallel()

	logger := &captureLogger{}
	m := New(nil, WithLogger(logger))

	handlerErr := errors.New("listen reconnect failed")
	installHandlerSpy(m, handlerErr)

	err := m.HandleTenantLifecycle(context.Background(), tmevent.TenantLifecycleEvent{
		EventType: tmevent.EventTenantActivated,
		TenantID:  "tenant-a",
		EventID:   "evt-1",
	})
	if err != nil {
		t.Fatalf("handler error must be swallowed, got %v", err)
	}

	warns := logger.warnEntries()
	if len(warns) != 1 {
		t.Fatalf("expected exactly 1 WARN, got %d (%+v)", len(warns), logger.entries)
	}

	// The WARN must carry the event type, tenant id, and the error.
	var sawEventType, sawTenant, sawErr bool

	for _, f := range warns[0].fields {
		switch f.Key {
		case "event_type":
			if f.Value == tmevent.EventTenantActivated {
				sawEventType = true
			}
		case "tenant_id":
			if f.Value == "tenant-a" {
				sawTenant = true
			}
		case "error":
			sawErr = true
		}
	}

	if !sawEventType || !sawTenant || !sawErr {
		t.Fatalf("WARN missing fields: eventType=%v tenant=%v err=%v fields=%+v",
			sawEventType, sawTenant, sawErr, warns[0].fields)
	}
}

func TestHandleTenantLifecycle_NilReceiver_NoPanic(t *testing.T) {
	t.Parallel()

	var m *Manager

	err := m.HandleTenantLifecycle(context.Background(), tmevent.TenantLifecycleEvent{
		EventType: tmevent.EventTenantActivated,
		TenantID:  "tenant-a",
	})
	if err != nil {
		t.Fatalf("nil receiver must return nil, got %v", err)
	}
}
