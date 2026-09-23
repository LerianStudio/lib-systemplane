// Package logguard reads a package's own source and refuses any log field name
// lib-observability erases.
//
// A test that only inspects the lines it happens to drive proves nothing about
// the sites its suite never reaches, and "key" is an exact entry in the default
// sensitive-field list: one log.String("key", …) slipping back in publishes
// namespace=billing key=[REDACTED] to an operator hunting a rejected row — a
// line that survives review because it reads correctly in the source and is
// only wrong in production. Both internal/client and internal/engine log from
// a dozen such sites, so the scan lives here rather than twice.
package logguard

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/redaction"
)

// fieldConstructors are the log.Field constructors whose first argument is the
// field NAME — the string an operator greps by and the string lib-observability
// matches its sensitive list against. log.Err is absent because it names no
// field.
var fieldConstructors = map[string]bool{
	"String":   true,
	"Any":      true,
	"Int":      true,
	"Int64":    true,
	"Bool":     true,
	"Duration": true,
	"Float64":  true,
	"Strings":  true,
}

// AssertNoneRedacted parses every non-test Go file in dir — "." is the package
// directory a test binary runs in — and fails tb for each literal log field
// name on lib-observability's sensitive list, reporting the source position so
// the offending call site is one jump away.
//
// It fails outright when the scan matches nothing: a guard that found no field
// names proves nothing about the package it was pointed at.
//
// A name built from a constant or a variable is skipped: it is not a literal
// this scan can read, and constants.AttrKeyTenantID is the library's own and
// already checked there.
func AssertNoneRedacted(tb testing.TB, dir string) {
	tb.Helper()

	sources, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		tb.Fatalf("list package sources in %s: %v", dir, err)
	}

	fset := token.NewFileSet()
	names := make(map[string]token.Position)

	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, source, nil, 0)
		if err != nil {
			tb.Fatalf("parse %s: %v", source, err)
		}

		ast.Inspect(file, func(n ast.Node) bool {
			if name, lit, ok := fieldName(n); ok {
				names[name] = fset.Position(lit.Pos())
			}

			return true
		})
	}

	if len(names) == 0 {
		tb.Fatalf("no log field names found in %s: the scan matched nothing, so it proves nothing", dir)
	}

	for name, pos := range names {
		if redaction.IsSensitiveField(name) {
			tb.Errorf("%s: field name %q is on lib-observability's sensitive list, so the line "+
				"reaches the operator with its value replaced by [REDACTED]", pos, name)
		}
	}
}

// fieldName reports the literal field name of a log.<Constructor>("name", …)
// call, and false for every other node.
func fieldName(n ast.Node) (string, *ast.BasicLit, bool) {
	call, ok := n.(*ast.CallExpr)
	if !ok || len(call.Args) == 0 {
		return "", nil, false
	}

	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !fieldConstructors[sel.Sel.Name] {
		return "", nil, false
	}

	if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "log" {
		return "", nil, false
	}

	lit, ok := call.Args[0].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", nil, false
	}

	name, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", nil, false
	}

	return name, lit, true
}
