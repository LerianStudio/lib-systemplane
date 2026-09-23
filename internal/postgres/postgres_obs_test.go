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
		// log.Err hard-codes this key (lib-observability log.errorFieldKey);
		// the source walk sees log.Err(err), never the string.
		"error",
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

// obsLogImportPath is the canonical logger this package emits through. The
// walk below matches the IMPORT PATH, not the identifier `log`: an alias, or a
// different package that happens to be named log, would otherwise be read as
// this one.
const obsLogImportPath = "github.com/LerianStudio/lib-observability/v4/log"

// logFieldsCtor is the variadic constructor; every other name in ctors below
// takes the field name as its first argument.
const logFieldsCtor = "Fields"

// logFieldNameLiterals parses this package's non-test sources and returns the
// string literal handed as the field name to every field constructor
// lib-observability/v4/log exports. Test files are excluded: a field name a
// test invents is not one the package ships.
//
// Two shapes are invisible to it, by construction rather than by oversight: a
// name that arrives through an identifier (listed by hand at the call site
// above), and a log.Field{Key: "...", Value: ...} composite literal, which
// names no constructor for the walk to key on. Neither is used in this
// package today; a composite literal would need a case here.
//
// This helper is DUPLICATED verbatim in internal/mongodb/mongodb_obs_test.go
// — the two packages share no test code — so a change here has to be made
// there too.
func logFieldNameLiterals(t *testing.T) []string {
	t.Helper()

	// Exactly what lib-observability/v4/log exports as a named field
	// constructor. Err takes no name, so it is absent; Fields is variadic and
	// handled separately below.
	ctors := map[string]bool{
		"Any": true, "String": true, "Int": true, "Bool": true,
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

		logPkg, ok := logPackageIdent(parsed)
		if !ok {
			continue
		}

		ast.Inspect(parsed, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}

			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}

			if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != logPkg {
				return true
			}

			switch {
			case ctors[sel.Sel.Name]:
				names = append(names, stringLiteralArgs(t, file, call.Args[:1])...)
			case sel.Sel.Name == logFieldsCtor:
				// log.Fields alternates name, value, so an even-indexed
				// string literal is a field name. A Field or []Field argument
				// consumes one slot and shifts the rest, which this cannot
				// see — the cost of a walk without type information, and the
				// reason an odd-indexed literal is never read as a name.
				var even []ast.Expr

				for i := 0; i < len(call.Args); i += 2 {
					even = append(even, call.Args[i])
				}

				names = append(names, stringLiteralArgs(t, file, even)...)
			}

			return true
		})
	}

	return names
}

// logPackageIdent returns the identifier this file refers to obsLogImportPath
// by — its alias when it has one, "log" otherwise — and false when the file
// does not import it.
func logPackageIdent(file *ast.File) (string, bool) {
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil || path != obsLogImportPath {
			continue
		}

		if imp.Name != nil {
			return imp.Name.Name, true
		}

		return "log", true
	}

	return "", false
}

// stringLiteralArgs unquotes every argument that is a string literal and drops
// the rest.
func stringLiteralArgs(t *testing.T, file string, args []ast.Expr) []string {
	t.Helper()

	var out []string

	for _, arg := range args {
		lit, ok := arg.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			continue
		}

		name, err := strconv.Unquote(lit.Value)
		if err != nil {
			t.Fatalf("%s: unquote field name %s: %v", file, lit.Value, err)
		}

		out = append(out, name)
	}

	return out
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
