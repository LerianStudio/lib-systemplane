//go:build unit

package client

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
	"github.com/LerianStudio/lib-observability/v4/redaction"
)

// fields unwraps the structured attributes of a recorded line. The logger
// interface takes ...any, so a call site may pass Fields one by one or as a
// slice; both shapes are flattened here.
func (l logLine) structured() []log.Field {
	out := make([]log.Field, 0, len(l.fields))

	for _, arg := range l.fields {
		switch v := arg.(type) {
		case []log.Field:
			out = append(out, v...)
		case log.Field:
			out = append(out, v)
		}
	}

	return out
}

// TestLogLinesNameTheKeyUnderKeyname pins the field name the client publishes
// the configuration key under. "key" is an exact entry in lib-observability's
// default sensitive-field list, so log.String("key", ...) renders as
// key=[REDACTED] and the line reaches the operator naming the namespace and
// withholding the one thing it exists to publish. internal/engine already uses
// "keyname" for the same value; this guard keeps the client from drifting back.
func TestLogLinesNameTheKeyUnderKeyname(t *testing.T) {
	if redaction.IsSensitiveField("keyname") {
		t.Fatal(`lib-observability now redacts "keyname" too: every line below ships its key as [REDACTED]`)
	}

	m := newMemStore(false)
	logger := &recordingLogger{}
	c := newSingleTenantClientWithLogger(t, m, logger)

	validator := func(_ context.Context, value any) error {
		if value == rejectedSecret {
			return errSchemeRefused
		}

		return nil
	}

	if err := c.Register("ns", "k", "default", WithContextValidator(validator)); err != nil {
		t.Fatalf("register: %v", err)
	}

	seedEntry(t, m, "ns", "k", rejectedSecret)

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	warns := logger.warns("stored value rejected by validator, keeping default")
	if len(warns) != 1 {
		t.Fatalf("got %d WARN lines for the rejected stored value, want exactly 1", len(warns))
	}

	var named bool

	for _, f := range warns[0].structured() {
		if redaction.IsSensitiveField(f.Key) {
			t.Errorf("the line carries field %q, which lib-observability redacts: the operator reads it as [REDACTED]", f.Key)
		}

		if f.Key == "keyname" {
			named = true

			if f.Value != "k" {
				t.Errorf(`field "keyname" = %v, want the configuration key`, f.Value)
			}
		}
	}

	if !named {
		t.Errorf(`the line carries no "keyname" field, so an operator cannot tell which key was rejected: %v`, warns[0])
	}
}

// logFieldConstructors are the log.Field constructors whose first argument is
// the field NAME — the string an operator greps by and the string
// lib-observability matches its sensitive list against. log.Err is absent
// because it names no field.
var logFieldConstructors = map[string]bool{
	"String":   true,
	"Any":      true,
	"Int":      true,
	"Int64":    true,
	"Bool":     true,
	"Duration": true,
	"Float64":  true,
	"Strings":  true,
}

// TestNoLoggedFieldNameIsRedacted reads the package's own source and refuses
// any field name lib-observability erases.
//
// The test above drives ONE of the lines that rename touched; this package
// logs from a dozen sites the suite reaches unevenly, and "key" is an exact
// entry in the default sensitive-field list. One log.String("key", …) slipping
// back in publishes namespace=billing key=[REDACTED] to an operator hunting a
// rejected row — a line that survives review because it reads correctly in the
// source and is only wrong in production.
func TestNoLoggedFieldNameIsRedacted(t *testing.T) {
	names := loggedFieldNames(t)

	if len(names) == 0 {
		t.Fatal("no log field names found in this package: the scan matched nothing, so it proves nothing")
	}

	for name, pos := range names {
		if redaction.IsSensitiveField(name) {
			t.Errorf("%s: field name %q is on lib-observability's sensitive list, so the line "+
				"reaches the operator with its value replaced by [REDACTED]", pos, name)
		}
	}
}

// loggedFieldNames parses every non-test file of the package under test — the
// test binary runs with its package directory as cwd — and returns each
// literal field name a log.Field constructor is called with, keyed by name so
// the same name reported twice fails once. A name built from a constant or a
// variable is skipped: it is not a literal this scan can read, and
// constants.AttrKeyTenantID is the library's own and already checked there.
func loggedFieldNames(t *testing.T) map[string]token.Position {
	t.Helper()

	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list package sources: %v", err)
	}

	fset := token.NewFileSet()
	names := make(map[string]token.Position)

	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, source, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", source, err)
		}

		ast.Inspect(file, func(n ast.Node) bool {
			name, lit, ok := logFieldName(n)
			if ok {
				names[name] = fset.Position(lit.Pos())
			}

			return true
		})
	}

	return names
}

// logFieldName reports the literal field name of a log.<Constructor>("name",
// …) call, and false for every other node.
func logFieldName(n ast.Node) (string, *ast.BasicLit, bool) {
	call, ok := n.(*ast.CallExpr)
	if !ok || len(call.Args) == 0 {
		return "", nil, false
	}

	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !logFieldConstructors[sel.Sel.Name] {
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
