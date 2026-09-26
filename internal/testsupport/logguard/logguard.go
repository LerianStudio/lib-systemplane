// Package logguard reads a package's own source and refuses any log field name
// lib-observability erases.
//
// "key" is an exact entry in the default sensitive-field list, and a test that
// drives some log sites proves nothing about the rest, so the scan reads every
// site. internal/client, internal/engine and internal/group each log from such
// sites, so the scan lives here once.
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
// matches its sensitive list against. Those four are every single-field
// constructor the package exports that names one; log.Err names none, and
// log.Fields takes its names in alternating key/value form, read separately.
var fieldConstructors = map[string]bool{
	"String": true,
	"Any":    true,
	"Int":    true,
	"Bool":   true,
}

// variadicConstructor takes alternating name/value pairs and resolves each to
// the same Any(name, value) a single-field constructor builds, so a name is
// redacted exactly as it would be spelled out.
const variadicConstructor = "Fields"

// fieldType is the struct a name reaches the logger inside. Written as a
// literal it carries its name in the Key element and reaches no constructor at
// all, which is one edit away from any line that already builds a []log.Field.
const fieldType = "Field"

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
			for _, lit := range fieldNames(n, pkg) {
				name, err := strconv.Unquote(lit.Value)
				if err != nil {
					continue
				}

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

// fieldNames reports the literal field names a node carries, each at its own
// string literal, where pkg is the local name of the log package in the file
// being scanned. Two of the three shapes carry more than one name, so a reader
// that returned a single name per node would report the first and hide the
// rest. A name that is not a string literal is not reported.
func fieldNames(n ast.Node, pkg string) []*ast.BasicLit {
	switch node := n.(type) {
	case *ast.CallExpr:
		return callFieldNames(node, pkg)
	case *ast.CompositeLit:
		return compositeFieldNames(node, pkg)
	}

	return nil
}

// callFieldNames reports the names a <pkg>.<Constructor>(…) call carries: the
// first argument of a single-field constructor, and every even-indexed
// argument of the variadic one.
//
// Even-indexed is where the variadic form's names sit while every element is a
// name/value pair; an argument that consumes one element rather than two — a
// Field, a *Field, a []Field — shifts the rest off that parity and its own
// names are read from its literal instead, by the composite reader.
func callFieldNames(call *ast.CallExpr, pkg string) []*ast.BasicLit {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return nil
	}

	if ident, ok := sel.X.(*ast.Ident); !ok || ident.Name != pkg {
		return nil
	}

	var names []*ast.BasicLit

	switch {
	case fieldConstructors[sel.Sel.Name]:
		if len(call.Args) == 0 {
			return nil
		}

		if lit, ok := stringLit(call.Args[0]); ok {
			names = append(names, lit)
		}

	case sel.Sel.Name == variadicConstructor:
		for i := 0; i < len(call.Args); i += 2 {
			if lit, ok := stringLit(call.Args[i]); ok {
				names = append(names, lit)
			}
		}
	}

	return names
}

// compositeFieldNames reports the names a composite literal carries: the Key
// of a <pkg>.Field literal, and the Key of every element of a []<pkg>.Field
// literal that elides its own type.
//
// An elided element names no type of its own, so it is only recognizable from
// the literal that holds it; an element that spells its type out is read when
// ast.Inspect reaches it, and is skipped here so it is reported once.
func compositeFieldNames(lit *ast.CompositeLit, pkg string) []*ast.BasicLit {
	if isFieldType(lit.Type, pkg) {
		if key, ok := keyName(lit); ok {
			return []*ast.BasicLit{key}
		}

		return nil
	}

	array, ok := lit.Type.(*ast.ArrayType)
	if !ok || !isFieldType(array.Elt, pkg) {
		return nil
	}

	var names []*ast.BasicLit

	for _, elt := range lit.Elts {
		inner, ok := elt.(*ast.CompositeLit)
		if !ok || inner.Type != nil {
			continue
		}

		if key, ok := keyName(inner); ok {
			names = append(names, key)
		}
	}

	return names
}

// isFieldType reports whether expr names <pkg>.Field.
func isFieldType(expr ast.Expr, pkg string) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != fieldType {
		return false
	}

	ident, ok := sel.X.(*ast.Ident)

	return ok && ident.Name == pkg
}

// keyName reports the string literal a Field composite literal assigns to Key,
// and false when the literal sets Key positionally or to anything the scan
// cannot read.
func keyName(lit *ast.CompositeLit) (*ast.BasicLit, bool) {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}

		if ident, ok := kv.Key.(*ast.Ident); !ok || ident.Name != "Key" {
			continue
		}

		return stringLit(kv.Value)
	}

	return nil, false
}

// stringLit reports expr as a string literal, and false for every other
// expression — a constant, a variable, a concatenation: none is a name this
// scan can read.
func stringLit(expr ast.Expr) (*ast.BasicLit, bool) {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return nil, false
	}

	return lit, true
}
