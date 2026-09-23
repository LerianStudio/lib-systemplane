//go:build unit

package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"

	obsconstants "github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/redaction"
	"github.com/LerianStudio/lib-systemplane/v4/internal/store"
	"go.opentelemetry.io/otel/attribute"
)

// Every log field name this package emits has to survive the canonical
// logger's redaction pass: a reconnect warning whose tenant ships as
// [REDACTED] cannot tell an operator which database is down. This package
// emits no field literally named "key" — the exact name lib-observability
// redacts — and this test is what fails if one is introduced.
//
// The inventory is READ OUT OF THE SOURCE, not hand-maintained, which is what
// makes the sentence above true: a new log.String("key", ...) anywhere in the
// package fails here the moment it is written, rather than the day someone
// remembers to extend a list.
func TestPostgresLogFieldNames_SurviveRedaction(t *testing.T) {
	t.Parallel()

	if !redaction.IsSensitiveField("key") {
		t.Fatal(`redaction no longer treats "key" as sensitive; the mongodb backend renamed a field only to dodge that match — re-check both backends`)
	}

	// Names that reach log.* through an identifier. The source walk sees the
	// identifier, not the string it resolves to, so these stay listed by hand.
	emitted := []string{
		obsconstants.AttrKeyTenantID,
	}

	literals := logFieldNameLiterals(t)
	if len(literals) == 0 {
		t.Fatal("the source walk found no log field name literals; this package emits several, so the walk is broken and every assertion below is vacuous")
	}

	for _, name := range append(emitted, literals...) {
		if redaction.IsSensitiveField(name) {
			t.Errorf("log field %q is redacted by the canonical logger; the warning carrying it ships blind", name)
		}
	}
}

// logFieldNameLiterals parses this package's non-test sources and returns the
// string literal handed as the field name to every log.<Field>(name, value)
// constructor. Test files are excluded: a field name a test invents is not one
// the package ships.
func logFieldNameLiterals(t *testing.T) []string {
	t.Helper()

	ctors := map[string]bool{
		"String": true, "Any": true, "Int": true, "Int64": true,
		"Bool": true, "Duration": true, "Float64": true, "Strings": true,
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	fset := token.NewFileSet()

	var names []string

	for _, entry := range entries {
		file := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(file, ".go") || strings.HasSuffix(file, "_test.go") {
			continue
		}

		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}

		ast.Inspect(parsed, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}

			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !ctors[sel.Sel.Name] {
				return true
			}

			if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "log" {
				return true
			}

			lit, ok := call.Args[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}

			name, err := strconv.Unquote(lit.Value)
			if err != nil {
				t.Fatalf("%s: unquote field name %s: %v", file, lit.Value, err)
			}

			names = append(names, name)

			return true
		})
	}

	return names
}

// A Postgres CRUD span says which database system it hit, so a trace read
// beside the MongoDB backend's spans is filterable the same way.
func TestPostgresScopeAttrs_NameTheDatabaseSystem(t *testing.T) {
	t.Parallel()

	want := attribute.String(obsconstants.AttrDBSystem, obsconstants.DBSystemPostgreSQL)

	for _, scope := range []string{"", "t1"} {
		attrs := scopeAttrs(store.Scope{Tenant: scope})

		var found bool

		for _, a := range attrs {
			if a == want {
				found = true
			}
		}

		if !found {
			t.Errorf("scope %q attributes = %#v, want %v", scope, attrs, want)
		}
	}
}
