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

// obsLogPath is the package whose Field constructors this scan reads. The
// local name is resolved per file from the import block rather than assumed,
// because an alias is the same call under another identifier: a scan keyed on
// the literal "log" misses obslog.String("key", …) and reads any OTHER package
// imported as log — the standard library's included — as if it were this one.
const obsLogPath = "github.com/LerianStudio/lib-observability/v4/log"

// fieldConstructors are the log.Field constructors whose first argument is the
// field NAME — the string an operator greps by and the string lib-observability
// matches its sensitive list against. Those four are every name-carrying
// constructor the package exports: log.Err names no field, and log.Fields, in
// alternating key/value form, would need its own reader and no production file
// here builds one.
var fieldConstructors = map[string]bool{
	"String": true,
	"Any":    true,
	"Int":    true,
	"Bool":   true,
}

// AssertNoneRedacted parses every non-test Go file in dir — "." is the package
// directory a test binary runs in — and fails tb for each literal log field
// name on lib-observability's sensitive list — once per call site, in source
// order — reporting the source position so the offending call site is one
// jump away.
//
// It fails outright when the scan matches nothing: a guard that found no field
// names proves nothing about the package it was pointed at.
//
// A name built from a constant or a variable is skipped: it is not a literal
// this scan can read, and constants.AttrKeyTenantID is the library's own and
// already checked there. A dot-imported log package is skipped too — no file
// here imports one, and a scan that claimed to read it would be guessing.
func AssertNoneRedacted(tb testing.TB, dir string) {
	tb.Helper()

	sources, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		tb.Fatalf("list package sources in %s: %v", dir, err)
	}

	// Every occurrence, in source order, not one entry per name: the same
	// name logged from two sites is two call sites to fix.
	type occurrence struct {
		name string
		pos  token.Position
	}

	fset := token.NewFileSet()

	var found []occurrence

	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, source, nil, 0)
		if err != nil {
			tb.Fatalf("parse %s: %v", source, err)
		}

		pkg, ok := logPkgName(file)
		if !ok {
			continue
		}

		ast.Inspect(file, func(n ast.Node) bool {
			if name, lit, ok := fieldName(n, pkg); ok {
				found = append(found, occurrence{name: name, pos: fset.Position(lit.Pos())})
			}

			return true
		})
	}

	if len(found) == 0 {
		tb.Fatalf("no log field names found in %s: the scan matched nothing, so it proves nothing", dir)
	}

	for _, o := range found {
		if redaction.IsSensitiveField(o.name) {
			tb.Errorf("%s: field name %q is on lib-observability's sensitive list, so the line "+
				"reaches the operator with its value replaced by [REDACTED]", o.pos, o.name)
		}
	}
}

// logPkgName reports the local name lib-observability's log package is bound
// to in file, and false when the file does not import it.
func logPkgName(file *ast.File) (string, bool) {
	for _, imported := range file.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		if err != nil || path != obsLogPath {
			continue
		}

		if imported.Name != nil {
			return imported.Name.Name, true
		}

		return "log", true
	}

	return "", false
}

// fieldName reports the literal field name of a <pkg>.<Constructor>("name", …)
// call, where pkg is the local name of the log package in the file being
// scanned, and false for every other node.
func fieldName(n ast.Node, pkg string) (string, *ast.BasicLit, bool) {
	call, ok := n.(*ast.CallExpr)
	if !ok || len(call.Args) == 0 {
		return "", nil, false
	}

	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !fieldConstructors[sel.Sel.Name] {
		return "", nil, false
	}

	if ident, ok := sel.X.(*ast.Ident); !ok || ident.Name != pkg {
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
