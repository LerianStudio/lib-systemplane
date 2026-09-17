//go:build unit

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"github.com/bxcodec/dbresolver/v2"
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

	if _, err := s.Subscribe(context.Background(), store.Scope{}, func(store.Event) {}); !errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Fatalf("Subscribe error = %v, want ErrNotSupportedInMultiTenant", err)
	}

	if _, err := s.List(context.Background(), store.Scope{}); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("List error = %v, want ErrTenantConnectionMissing", err)
	}
	if _, _, err := s.Get(context.Background(), store.Scope{}, "ns", "k"); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("Get error = %v, want ErrTenantConnectionMissing", err)
	}
	if _, err := s.Set(context.Background(), store.Scope{}, store.Entry{}); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("Set empty entry error = %v, want ErrValidation", err)
	}
	if _, err := s.Set(context.Background(), store.Scope{}, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`)}); !errors.Is(err, store.ErrTenantConnectionMissing) {
		t.Fatalf("Set error = %v, want ErrTenantConnectionMissing", err)
	}
	if err := s.Delete(context.Background(), store.Scope{}, "", "k", "actor"); !errors.Is(err, store.ErrValidation) {
		t.Fatalf("Delete empty namespace error = %v, want ErrValidation", err)
	}
	if err := s.Delete(context.Background(), store.Scope{}, "ns", "k", "actor"); !errors.Is(err, store.ErrTenantConnectionMissing) {
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
	if _, err := s.List(context.Background(), store.Scope{}); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("List after Close error = %v, want ErrClosed", err)
	}
	if _, _, err := s.Get(context.Background(), store.Scope{}, "ns", "k"); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Get after Close error = %v, want ErrClosed", err)
	}
	if _, err := s.Set(context.Background(), store.Scope{}, store.Entry{Namespace: "ns", Key: "k"}); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Set after Close error = %v, want ErrClosed", err)
	}
	if err := s.Delete(context.Background(), store.Scope{}, "ns", "k", "actor"); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Delete after Close error = %v, want ErrClosed", err)
	}
	if _, err := s.Subscribe(context.Background(), store.Scope{}, func(store.Event) {}); !errors.Is(err, store.ErrClosed) {
		t.Fatalf("Subscribe after Close error = %v, want ErrClosed", err)
	}
}

func TestNotifyPayloadParsingAndDispatch(t *testing.T) {
	t.Parallel()

	valid, ok := parseNotifyPayload(`{"namespace":"ns","key":"k","op":"upsert"}`)
	if !ok || valid.Namespace != "ns" || valid.Key != "k" || valid.Op != store.OpUpsert {
		t.Fatalf("valid payload parsed as (%#v, %v)", valid, ok)
	}

	if valid.Revision != 0 {
		t.Fatalf("payload without revision parsed Revision = %d, want 0", valid.Revision)
	}

	withRevision, ok := parseNotifyPayload(`{"namespace":"ns","key":"k","op":"upsert","revision":7}`)
	if !ok || withRevision.Revision != 7 {
		t.Fatalf("payload with revision parsed as (%#v, %v), want Revision 7", withRevision, ok)
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

// A named tenant is refused on every method when the Store was built without a
// tenant connector: there is nothing to resolve the tenant's database through.
// (Subscribe still refuses every named scope outright; Epic 1.4 gives it a
// per-tenant feed.)
func TestStore_NamedTenantScopeWithoutConnector(t *testing.T) {
	t.Parallel()

	s, err := New(Config{DB: &sql.DB{}, ListenDSN: "postgres://example"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	scope := store.Scope{Tenant: "t1"}
	ctx := context.Background()

	if _, _, err := s.Get(ctx, scope, "ns", "k"); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Get error = %v, want ErrTenantConnectorMissing", err)
	}

	if _, err := s.Set(ctx, scope, store.Entry{Namespace: "ns", Key: "k", Value: []byte(`1`)}); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Set error = %v, want ErrTenantConnectorMissing", err)
	}

	if err := s.Delete(ctx, scope, "ns", "k", "actor"); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("Delete error = %v, want ErrTenantConnectorMissing", err)
	}

	if _, err := s.List(ctx, scope); !errors.Is(err, store.ErrTenantConnectorMissing) {
		t.Fatalf("List error = %v, want ErrTenantConnectorMissing", err)
	}

	if _, err := s.Subscribe(ctx, scope, func(store.Event) {}); !errors.Is(err, store.ErrNotSupportedInMultiTenant) {
		t.Fatalf("Subscribe error = %v, want ErrNotSupportedInMultiTenant", err)
	}
}

// nilHandleConnector reports success while handing back nothing — the shape a
// buggy connector takes.
type nilHandleConnector struct{}

func (nilHandleConnector) ResolveDB(context.Context, string) (dbresolver.DB, error) {
	return nil, nil
}

func (nilHandleConnector) ResolveDSN(context.Context, string) (string, error) {
	return "", nil
}

// A connector that returns a nil handle with a nil error is refused at
// resolution time rather than passed through to panic on the first query.
func TestStore_NamedTenantScopeNilHandleIsRefused(t *testing.T) {
	t.Parallel()

	s, err := New(Config{MultiTenantEnabled: true, Connector: nilHandleConnector{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, resolveErr := s.resolveDB(context.Background(), store.Scope{Tenant: "t1"})
	if !errors.Is(resolveErr, store.ErrTenantConnectorMissing) {
		t.Fatalf("resolveDB error = %v, want ErrTenantConnectorMissing", resolveErr)
	}

	if !strings.Contains(resolveErr.Error(), "resolve tenant t1") {
		t.Errorf("resolveDB error %q must name the tenant", resolveErr)
	}
}
