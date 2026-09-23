//go:build unit

package mongodb

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"

	obsconstants "github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/redaction"
)

// Every log field name this package emits has to survive the canonical
// logger's redaction pass, because a warning whose purpose is to name the
// offending document is worthless when the name ships as [REDACTED]. The
// exact field name "key" IS on lib-observability's sensitive list, which is
// why the store emits the configuration key under logFieldKeyName instead.
//
// The inventory is READ OUT OF THE SOURCE, not hand-maintained: a
// log.String("key", ...) written anywhere in the package fails here the moment
// it is written, rather than the day someone remembers to extend a list.
func TestMongoLogFieldNames_SurviveRedaction(t *testing.T) {
	t.Parallel()

	if !redaction.IsSensitiveField("key") {
		t.Fatal(`redaction no longer treats "key" as sensitive; logFieldKeyName exists only to dodge that match — re-check before renaming back`)
	}

	// Names that reach log.* through an identifier. The source walk sees the
	// identifier, not the string it resolves to, so these stay listed by hand.
	emitted := []string{
		fieldNamespace,
		logFieldKeyName,
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

// The configuration key a document-level warning names reaches the logger
// under "keyname". A recording logger pins the emitted name, so flipping
// logFieldKeyName back to "key" fails here rather than silently in production.
func TestMongoWarnNamesTheKeyUnderKeyname(t *testing.T) {
	t.Parallel()

	logger := &captureLogger{}
	s := &Store{cfg: Config{Logger: logger}}

	s.logWarn(context.Background(), "get decode error, serving the key as absent",
		log.String(fieldNamespace, "ns"),
		log.String(logFieldKeyName, "flags.enabled"),
	)

	entry := logger.waitFor(t, log.LevelWarn, "get decode error, serving the key as absent")

	if got := entry.field(t, "keyname"); got != "flags.enabled" {
		t.Errorf("warning carries keyname %v, want flags.enabled", got)
	}
}
