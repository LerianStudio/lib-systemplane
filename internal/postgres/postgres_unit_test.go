//go:build unit

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-systemplane/v2/internal/store"
)

func TestNew_ConfigValidationAndDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     Config
		wantErr error
	}{
		{
			name:    "single tenant requires db",
			cfg:     Config{ListenDSN: "postgres://example"},
			wantErr: store.ErrNilBackend,
		},
		{
			name:    "single tenant requires listen dsn",
			cfg:     Config{DB: &sql.DB{}},
			wantErr: errors.New("ListenDSN"),
		},
		{
			name:    "rejects unsafe channel",
			cfg:     Config{DB: &sql.DB{}, ListenDSN: "postgres://example", Channel: "bad channel"},
			wantErr: errors.New("unsafe channel"),
		},
		{
			name:    "rejects channel exceeding 63 bytes",
			cfg:     Config{DB: &sql.DB{}, ListenDSN: "postgres://example", Channel: strings.Repeat("a", 64)},
			wantErr: errors.New("63 bytes"),
		},
		{
			name:    "rejects unsafe table",
			cfg:     Config{DB: &sql.DB{}, ListenDSN: "postgres://example", Table: "bad.table"},
			wantErr: errors.New("unsafe table"),
		},
		{
			name: "multi tenant permits nil db and empty dsn",
			cfg:  Config{MultiTenantEnabled: true},
		},
		{
			name: "single tenant applies defaults",
			cfg:  Config{DB: &sql.DB{}, ListenDSN: "postgres://example"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, err := New(tt.cfg)
			if tt.wantErr != nil {
				if err == nil {
					t.Fatal("New: expected error, got nil")
				}

				if !errors.Is(err, tt.wantErr) && !containsError(err, tt.wantErr.Error()) {
					t.Fatalf("New error = %v, want %v", err, tt.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if s.cfg.Channel != defaultChannel {
				t.Fatalf("channel = %q, want %q", s.cfg.Channel, defaultChannel)
			}
			if s.cfg.Table != defaultTable {
				t.Fatalf("table = %q, want %q", s.cfg.Table, defaultTable)
			}
			if s.cfg.Module != defaultModule {
				t.Fatalf("module = %q, want %q", s.cfg.Module, defaultModule)
			}
		})
	}
}

// TestNew_AcceptsHyphenatedChannel locks the fix: a channel prefixed with a
// hyphenated ApplicationName (e.g. "my-service_systemplane_changes") is accepted
// because the channel is double-quoted at LISTEN time. The table, interpolated
// unquoted, stays strict (see the "rejects unsafe table" case with a dot).
func TestNew_AcceptsHyphenatedChannel(t *testing.T) {
	t.Parallel()

	const hyphenated = "br-consignado-gw_systemplane_changes"

	s, err := New(Config{DB: &sql.DB{}, ListenDSN: "postgres://example", Channel: hyphenated})
	if err != nil {
		t.Fatalf("New with hyphenated channel: unexpected error %v", err)
	}
	if s.cfg.Channel != hyphenated {
		t.Fatalf("channel = %q, want %q", s.cfg.Channel, hyphenated)
	}
}

func TestStore_MultiTenantPreIOPaths(t *testing.T) {
	t.Parallel()

	s, err := New(Config{MultiTenantEnabled: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start in multi-tenant mode: %v", err)
	}

	if _, err := s.Subscribe(context.Background(), func(store.Event) {}); !errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Fatalf("Subscribe error = %v, want ErrNotSupportedInMultiTenant", err)
	}

	if _, err := s.List(context.Background()); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("List error = %v, want ErrTenantConnectionMissing", err)
	}
	if _, _, err := s.Get(context.Background(), "ns", "k"); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("Get error = %v, want ErrTenantConnectionMissing", err)
	}
	if err := s.Set(context.Background(), store.Entry{}); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("Set empty entry error = %v, want ErrValidation", err)
	}
	if err := s.Set(context.Background(), store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`)}); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("Set error = %v, want ErrTenantConnectionMissing", err)
	}
	if err := s.Delete(context.Background(), "", "k", "actor"); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("Delete empty namespace error = %v, want ErrValidation", err)
	}
	if err := s.Delete(context.Background(), "ns", "k", "actor"); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("Delete error = %v, want ErrTenantConnectionMissing", err)
	}
}

func TestStore_ClosedAndNilPaths(t *testing.T) {
	t.Parallel()

	var nilStore *Store
	if err := nilStore.Start(context.Background()); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("nil Start error = %v, want ErrClosed", err)
	}
	if err := nilStore.Close(); err != nil {
		t.Fatalf("nil Close = %v, want nil", err)
	}

	s := newSubscribeStore()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if !s.isClosed() {
		t.Fatal("isClosed = false, want true")
	}

	if err := s.Start(context.Background()); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Start after Close error = %v, want ErrClosed", err)
	}
	if _, err := s.List(context.Background()); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("List after Close error = %v, want ErrClosed", err)
	}
	if _, _, err := s.Get(context.Background(), "ns", "k"); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Get after Close error = %v, want ErrClosed", err)
	}
	if err := s.Set(context.Background(), store.Entry{Namespace: "ns", Key: "k"}); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Set after Close error = %v, want ErrClosed", err)
	}
	if err := s.Delete(context.Background(), "ns", "k", "actor"); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Delete after Close error = %v, want ErrClosed", err)
	}
	if _, err := s.Subscribe(context.Background(), func(store.Event) {}); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Subscribe after Close error = %v, want ErrClosed", err)
	}
}

func TestNotifyPayloadParsingAndDispatch(t *testing.T) {
	t.Parallel()

	valid, ok := parseNotifyPayload(`{"namespace":"ns","key":"k","op":"upsert"}`)
	if !ok || valid.Namespace != "ns" || valid.Key != "k" || valid.Op != store.OpUpsert {
		t.Fatalf("valid payload parsed as (%#v, %v)", valid, ok)
	}

	deleteEvt, ok := parseNotifyPayload(`{"namespace":"ns","key":"k","op":"delete"}`)
	if !ok || deleteEvt.Op != store.OpDelete {
		t.Fatalf("delete payload parsed as (%#v, %v)", deleteEvt, ok)
	}

	for _, payload := range []string{
		`not-json`,
		`{"namespace":"","key":"k","op":"upsert"}`,
		`{"namespace":"ns","key":"","op":"upsert"}`,
		`{"namespace":"ns","key":"k","op":"noop"}`,
	} {
		if evt, ok := parseNotifyPayload(payload); ok {
			t.Fatalf("parseNotifyPayload(%q) = (%#v, true), want false", payload, evt)
		}
	}

	if got := truncateString("abcdef", 3); got != "abc..." {
		t.Fatalf("truncateString = %q, want abc...", got)
	}
	if got := truncateString("abc", 3); got != "abc" {
		t.Fatalf("truncateString exact = %q, want abc", got)
	}
	if got := quoteIdentifier("systemplane_entries"); got != `"systemplane_entries"` {
		t.Fatalf("quoteIdentifier = %q", got)
	}
	// Embedded double quotes are doubled (canonical PG quoting) so the identifier
	// cannot break out of its quoted context.
	if got := quoteIdentifier(`a"b`); got != `"a""b"` {
		t.Fatalf("quoteIdentifier embedded-quote escaping = %q, want %q", got, `"a""b"`)
	}

	s := newSubscribeStore()
	var got []store.Event
	s.subscribers[1] = func(store.Event) { panic("handler panic must be recovered") }
	s.subscribers[2] = func(evt store.Event) { got = append(got, evt) }

	s.dispatchEvent(valid)
	if len(got) != 1 || got[0] != valid {
		t.Fatalf("dispatch events = %#v, want %#v", got, []store.Event{valid})
	}
}

func containsError(err error, want string) bool {
	return err != nil && strings.Contains(err.Error(), want)
}
