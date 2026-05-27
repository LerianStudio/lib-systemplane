//go:build unit

package manager

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestIsUndefinedTable_DetectsSQLState42P01 pins the graceful missing-table
// detection used by warmLoad: only a Postgres "undefined table" (42P01) error
// — possibly wrapped — is tolerated; every other error must propagate.
func TestIsUndefinedTable_DetectsSQLState42P01(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "bare undefined table",
			err:  &pgconn.PgError{Code: "42P01", Message: `relation "systemplane_entries" does not exist`},
			want: true,
		},
		{
			name: "wrapped undefined table",
			err:  fmt.Errorf("warm-load: %w", &pgconn.PgError{Code: "42P01"}),
			want: true,
		},
		{
			name: "other pg error (permission denied)",
			err:  &pgconn.PgError{Code: "42501", Message: "permission denied for schema"},
			want: false,
		},
		{
			name: "non-pg error",
			err:  errors.New("connection refused"),
			want: false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := isUndefinedTable(tt.err); got != tt.want {
				t.Fatalf("isUndefinedTable(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
