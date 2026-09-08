//go:build unit

package systemplane_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// exportedPackages are the directories a consumer can import. internal/ is
// unreachable by construction, examples/ is documentation, so neither is part
// of the boundary this test defends.
var exportedPackages = []string{".", "admin", "systemplanetest"}

// coupledModules are the module paths whose MAJOR version must never become
// part of this library's contract. lib-observability is the one this module's
// v3 exists for: naming its log.Logger and its *tracing.Telemetry in parameters
// forced every consumer onto one exact lib-observability major.
//
// lib-commons is deliberately NOT here. NewManager takes a *tmpostgres.Manager,
// a concrete connection-pool handle with no interface to stand in for it, so
// lib-commons' major is part of this contract by construction. Listing it would
// make this test fail on a shape nobody intends to change, and a gate that
// fails on purpose gets deleted. If that coupling ever becomes worth breaking,
// it is its own change and this list is where it starts.
var coupledModules = []string{"lib-observability"}

// loggerish are the parameter names that indicate a logger or recorder is
// being accepted. They widen the check to parameters that name no coupled type
// today but are the position where one would reappear.
var loggerish = []string{"logger", "log", "l", "recorder", "factory", "metrics", "metricsfactory", "telemetry", "t"}

// maxTypeDepth bounds the interface-method recursion against a cyclic
// declaration.
const maxTypeDepth = 8

// TestExportedBoundaryTakesNoCoupledLoggerType is the regression test for the
// defect class this module's v3 exists to eliminate.
//
// # The defect
//
// Go matches the types inside a method signature NOMINALLY. A parameter
// declared as lib-observability/v2/log.Logger binds the caller to that exact
// major: v2/log.Logger and v4/log.Logger are different types even though the
// source is byte-for-byte identical. A consumer already holding a v4 logger
// therefore could not call WithLogger at all, and the only fix available to it
// was a shim. Worse, log.Logger carries With(...) Logger — a self-returning
// method no foreign package can declare, because it has no way to name the
// return type — so a consumer could not even describe the shape locally.
//
// # The rule
//
// A parameter that accepts a logger must be declared with universal types
// only: predeclared types, stdlib types, or an interface declared by THIS
// module whose own methods are themselves universal and non-self-returning.
// systemplane.Logger is that interface. Conversion to a rich logger happens
// inside the library, at the call, via log.Adapt.
//
// Return types are deliberately NOT checked. Returning a rich logger costs a
// consumer nothing: it can always assign the value to a narrower interface
// declared in its own package. Only parameters propagate a major.
func TestExportedBoundaryTakesNoCoupledLoggerType(t *testing.T) {
	t.Parallel()

	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}

	declared := collectDeclaredTypes(t, root)
	fset := token.NewFileSet()
	violations := 0

	for _, pkgDir := range exportedPackages {
		dir := filepath.Join(root, pkgDir)

		entries, readErr := os.ReadDir(dir)
		if readErr != nil {
			t.Fatalf("read %s: %v", pkgDir, readErr)
		}

		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}

			path := filepath.Join(dir, name)

			file, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if parseErr != nil {
				t.Fatalf("parse %s: %v", path, parseErr)
			}

			rel, _ := filepath.Rel(root, path)

			// A dot import merges another package's identifiers into this
			// file's namespace as bare idents, which this checker would then
			// resolve against the WRONG package and silently pass. Nothing in
			// this module dot-imports and the linter forbids it, so treat one
			// as a defeat of the gate rather than a shape to support.
			if name := dotImport(file); name != "" {
				t.Errorf("%s: dot import of %q defeats this checker; "+
					"import it under a name instead", rel, name)

				violations++
			}

			scope := newPkgScope(declared, file)

			ast.Inspect(file, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.FuncDecl:
					if !typed.Name.IsExported() || !exportedReceiver(typed) {
						return true
					}

					violations += checkParams(t, fset, rel, typed.Name.Name, typed.Type.Params, scope)
				case *ast.InterfaceType:
					for _, method := range typed.Methods.List {
						fn, ok := method.Type.(*ast.FuncType)
						if !ok || len(method.Names) == 0 || !method.Names[0].IsExported() {
							continue
						}

						violations += checkParams(t, fset, rel, method.Names[0].Name, fn.Params, scope)
					}
				}

				return true
			})
		}
	}

	if violations > 0 {
		t.Fatalf("%d exported observability parameter(s) are not universal; "+
			"each one re-couples every consumer to lib-observability's major version", violations)
	}
}

// exportedReceiver reports whether a method is reachable from outside the
// module: a plain function, or a method on an exported type.
func exportedReceiver(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return true
	}

	name := receiverTypeName(fn.Recv.List[0].Type)

	return name != "" && ast.IsExported(name)
}

func receiverTypeName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.StarExpr:
		return receiverTypeName(typed.X)
	case *ast.Ident:
		return typed.Name
	case *ast.IndexExpr:
		return receiverTypeName(typed.X)
	case *ast.IndexListExpr:
		// A generic receiver with more than one type parameter, Cache[K, V].
		// Without this case the name comes back empty, exportedReceiver says
		// false, and every method on such a type is skipped silently.
		return receiverTypeName(typed.X)
	default:
		return ""
	}
}

// checkParams reports every logger-shaped parameter that is not universal, and
// returns how many it found.
func checkParams(t *testing.T, fset *token.FileSet, file, fnName string, params *ast.FieldList, scope *pkgScope) int {
	t.Helper()

	if params == nil {
		return 0
	}

	found := 0

	for _, param := range params.List {
		// Three independent gates, because the defect is not tied to a
		// parameter NAME:
		//
		//   - a type from a coupled module is checked whatever the parameter
		//     is called. This is what catches a concrete struct such as
		//     *tracing.Telemetry, whose parameter no name heuristic would
		//     reliably spot;
		//   - a logger-shaped NAME is checked even when its type looks clean
		//     today, since that is the position a coupled type reappears in;
		//   - an interface declared HERE is checked whatever it is called: a
		//     consumer cannot implement it without importing this module, so
		//     it must at least be implementable;
		//   - an ALIAS declared here is checked, because an alias is a rename
		//     rather than a new type and slips past all three of the above:
		//     "type Alias = log.Logger" is a bare identifier, so it is no
		//     selector for the first gate to see, and it resolves to no
		//     declaration this module owns for the third.
		coupled := scope.coupledQualifier(param.Type) != ""
		if !coupled &&
			!isLoggerish(param.Names) &&
			!namesLocalInterface(param.Type, scope) &&
			!namesLocalAlias(param.Type, scope) {
			continue
		}

		if reason := universalityViolation(param.Type, scope, 0); reason != "" {
			t.Errorf("%s:%d: %s takes parameter %q which is not universal.\n\t%s",
				file, fset.Position(param.Pos()).Line, fnName, paramNames(param.Names), reason)

			found++
		}
	}

	return found
}

// universalityViolation returns a non-empty explanation if expr names a type a
// consumer cannot reproduce in its own package, or "" if the type is universal.
//
//   - a type imported from a coupled module (lib-observability, lib-commons) is
//     never universal: naming it makes that module's major part of our contract;
//   - a predeclared or stdlib type (int, string, any, context.Context) is
//     universal;
//   - a locally-declared INTERFACE is universal if every one of its own method
//     signatures is universal, and NOT universal if it has a self-returning
//     method, which a consumer has no way to name;
//   - an alias is followed to its target, since naming the alias is naming what
//     it points at;
//   - any OTHER locally-declared type is universal here. That differs from
//     lib-observability's own version of this test, deliberately: there, the
//     module's own major was the thing propagating, so naming any of its types
//     was the defect. Here the propagating major belongs to lib-observability,
//     and this module's own types — Client, Catalog, TestEntry — appear
//     throughout its exported signatures by design. Flagging them would demand
//     a redesign that buys no decoupling.
func universalityViolation(expr ast.Expr, scope *pkgScope, depth int) string {
	if depth > maxTypeDepth {
		return ""
	}

	if pkg := scope.coupledQualifier(expr); pkg != "" {
		return "it names a type imported from " + pkg + ", whose major version would " +
			"become part of this library's contract. Accept a local interface built from " +
			"universal types instead (see systemplane.Logger) and convert with log.Adapt."
	}

	// An anonymous interface has no name to resolve, so the loop below would
	// skip it entirely — while its methods can name whatever they like. Walk it
	// directly. Nothing can self-return here: an anonymous interface gives a
	// method no name to return.
	if iface := anonymousInterface(expr); iface != nil {
		return interfaceViolation("it is an anonymous interface", "", iface, scope, depth)
	}

	for _, name := range scope.qualify(expr) {
		decl, isLocal := scope.declared[name]
		if !isLocal {
			continue
		}

		declScope := scope.at(decl)

		// "type X = Y" declares no new identity, but it NAMES one: an alias to
		// a coupled interface is the cheapest way to smuggle that interface
		// back into a parameter, so follow it rather than stop at it.
		if decl.alias {
			if reason := universalityViolation(decl.expr, declScope, depth+1); reason != "" {
				return "it names " + name + ", an alias whose target is not universal: " + reason
			}

			continue
		}

		iface, isInterface := decl.expr.(*ast.InterfaceType)
		if !isInterface {
			continue
		}

		// The method set resolves in the scope of the file that DECLARED the
		// interface, not the file accepting it as a parameter: the two need not
		// import a coupled module under the same alias, or at all.
		if reason := interfaceViolation("it names "+name, name, iface, declScope, depth); reason != "" {
			return reason
		}
	}

	return ""
}

// interfaceViolation walks one interface's method set. selfName is the
// package-qualified name the interface is declared under, or "" for an
// anonymous one, which cannot self-return.
func interfaceViolation(prefix, selfName string, iface *ast.InterfaceType, scope *pkgScope, depth int) string {
	for _, method := range iface.Methods.List {
		fn, ok := method.Type.(*ast.FuncType)
		if !ok {
			// An embedded interface carries the embedded method set whole, so
			// embedding a non-universal interface couples a consumer exactly as
			// naming it directly would.
			if reason := universalityViolation(method.Type, scope, depth+1); reason != "" {
				return prefix + ", whose embedded interface is not universal: " + reason
			}

			continue
		}

		for _, result := range fieldTypes(fn.Results) {
			if selfName != "" {
				for _, resultName := range scope.qualify(result) {
					if resultName == selfName {
						return prefix + ", an interface with a self-returning method. " +
							"A consumer cannot declare that interface in its own package — it has " +
							"no way to name the return type — so it must import this module."
					}
				}
			}

			if reason := universalityViolation(result, scope, depth+1); reason != "" {
				return prefix + ", whose method " + methodName(method) +
					" returns a non-universal type: " + reason
			}
		}

		for _, param := range fieldTypes(fn.Params) {
			if reason := universalityViolation(param, scope, depth+1); reason != "" {
				return prefix + ", whose method " + methodName(method) + " is not universal: " + reason
			}
		}
	}

	return ""
}

// anonymousInterface unwraps an inline interface literal, or returns nil.
func anonymousInterface(expr ast.Expr) *ast.InterfaceType {
	switch typed := expr.(type) {
	case *ast.InterfaceType:
		return typed
	case *ast.StarExpr:
		return anonymousInterface(typed.X)
	case *ast.Ellipsis:
		return anonymousInterface(typed.Elt)
	case *ast.ArrayType:
		return anonymousInterface(typed.Elt)
	default:
		return nil
	}
}

// dotImport returns the path of the first dot import in a file, or "".
func dotImport(file *ast.File) string {
	for _, imp := range file.Imports {
		if imp.Name == nil || imp.Name.Name != "." {
			continue
		}

		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return imp.Path.Value
		}

		return path
	}

	return ""
}

// namesLocalInterface reports whether expr resolves to an interface declared by
// this module. Such a parameter is coupling-critical regardless of its name:
// satisfying it is the consumer's job, and it cannot do that without naming the
// type.
func namesLocalInterface(expr ast.Expr, scope *pkgScope) bool {
	// An inline interface literal is declared here too — it just has no name.
	if iface := anonymousInterface(expr); iface != nil {
		return !sealed(iface, scope, 0)
	}

	for _, name := range scope.qualify(expr) {
		decl, declScope, isLocal := resolve(name, scope)
		if !isLocal {
			continue
		}

		iface, isInterface := decl.expr.(*ast.InterfaceType)
		if !isInterface {
			continue
		}

		// A SEALED interface — one carrying an unexported method — is an
		// opaque token, not a dependency the consumer supplies. That is the
		// functional-option pattern, where the consumer calls a WithX
		// constructor and never implements anything.
		if sealed(iface, declScope, 0) {
			continue
		}

		return true
	}

	return false
}

// namesLocalAlias reports whether expr names a type ALIAS declared by this
// module.
//
// It is its own gate because an alias evades every other one. "type Alias =
// log.Logger" appears in a signature as a bare identifier, so coupledQualifier
// sees no selector to inspect; and resolve cannot follow it to a declaration
// this module owns, because its target lives in another module, so
// namesLocalInterface reports false. A parameter with a neutral name would
// then be skipped entirely, which is the cheapest way to put a coupled
// interface back into the public API.
func namesLocalAlias(expr ast.Expr, scope *pkgScope) bool {
	for _, name := range scope.qualify(expr) {
		decl, isLocal := scope.declared[name]
		if !isLocal || !decl.alias {
			continue
		}

		// An alias to a SEALED interface stays exempt, exactly as naming that
		// interface directly does: the unexported method makes it
		// unimplementable outside its own package, so the consumer receives
		// one from a constructor rather than supplying it.
		if target, targetScope, resolvable := resolve(name, scope); resolvable {
			if iface, isInterface := target.expr.(*ast.InterfaceType); isInterface &&
				sealed(iface, targetScope, 0) {
				continue
			}
		}

		return true
	}

	return false
}

// sealed reports whether an interface carries an unexported method, which makes
// it unimplementable outside the declaring package. Embedding is followed.
func sealed(iface *ast.InterfaceType, scope *pkgScope, depth int) bool {
	if depth > maxTypeDepth {
		return false
	}

	for _, method := range iface.Methods.List {
		for _, name := range method.Names {
			if !name.IsExported() {
				return true
			}
		}

		if _, isFunc := method.Type.(*ast.FuncType); isFunc {
			continue
		}

		for _, name := range scope.qualify(method.Type) {
			decl, declScope, isLocal := resolve(name, scope)
			if !isLocal {
				continue
			}

			embedded, isInterface := decl.expr.(*ast.InterfaceType)
			if !isInterface {
				continue
			}

			if sealed(embedded, declScope, depth+1) {
				return true
			}
		}
	}

	return false
}

// declaration is a type declared by this module, together with the context
// needed to resolve the type expressions inside it.
type declaration struct {
	expr    ast.Expr
	pkg     string
	aliases map[string]string
	coupled map[string]string
	// alias marks "type X = Y". The alias declares no new identity, but it
	// NAMES one: using it in a signature is using its target, so resolution
	// must follow it through rather than stop at it.
	alias bool
}

// pkgScope resolves a type expression appearing in one file to the
// package-qualified key collectDeclaredTypes uses, and knows which import
// qualifiers in that file point at a coupled module.
type pkgScope struct {
	declared map[string]declaration
	pkg      string
	aliases  map[string]string
	coupled  map[string]string
}

func newPkgScope(declared map[string]declaration, file *ast.File) *pkgScope {
	return &pkgScope{
		declared: declared,
		pkg:      file.Name.Name,
		aliases:  localAliases(file),
		coupled:  coupledAliases(file),
	}
}

// at returns the scope as seen from inside the file that declared decl. Both
// halves matter: the package name decides what a bare identifier means, and the
// alias maps must come from the DECLARING file, since the file accepting the
// parameter need not import the same packages under the same names.
func (s *pkgScope) at(decl declaration) *pkgScope {
	return &pkgScope{declared: s.declared, pkg: decl.pkg, aliases: decl.aliases, coupled: decl.coupled}
}

// coupledQualifier returns the module path of the coupled import a type
// expression is qualified by, or "" when it is not one.
func (s *pkgScope) coupledQualifier(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.StarExpr:
		return s.coupledQualifier(typed.X)
	case *ast.Ellipsis:
		return s.coupledQualifier(typed.Elt)
	case *ast.ArrayType:
		return s.coupledQualifier(typed.Elt)
	case *ast.SelectorExpr:
		pkg, ok := typed.X.(*ast.Ident)
		if !ok {
			return ""
		}

		return s.coupled[pkg.Name]
	case *ast.IndexExpr:
		if path := s.coupledQualifier(typed.X); path != "" {
			return path
		}

		return s.coupledQualifier(typed.Index)
	case *ast.IndexListExpr:
		if path := s.coupledQualifier(typed.X); path != "" {
			return path
		}

		for _, index := range typed.Indices {
			if path := s.coupledQualifier(index); path != "" {
				return path
			}
		}

		return ""
	default:
		return ""
	}
}

// qualify turns the identifiers of a type expression into package-qualified
// keys: a bare ident resolves against the current package, a selector against
// its import alias.
func (s *pkgScope) qualify(expr ast.Expr) []string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return []string{s.pkg + "." + typed.Name}
	case *ast.SelectorExpr:
		pkg, ok := typed.X.(*ast.Ident)
		if !ok {
			return nil
		}

		real, aliased := s.aliases[pkg.Name]
		if !aliased {
			return nil
		}

		return []string{real + "." + typed.Sel.Name}
	case *ast.StarExpr:
		return s.qualify(typed.X)
	case *ast.Ellipsis:
		return s.qualify(typed.Elt)
	case *ast.ArrayType:
		return s.qualify(typed.Elt)
	case *ast.IndexExpr:
		// An instantiated generic: a consumer must name BOTH the generic and
		// the argument, so both propagate identity.
		return append(s.qualify(typed.X), s.qualify(typed.Index)...)
	case *ast.IndexListExpr:
		names := s.qualify(typed.X)
		for _, index := range typed.Indices {
			names = append(names, s.qualify(index)...)
		}

		return names
	default:
		return nil
	}
}

// resolve looks up name and follows any chain of type aliases to the
// declaration that actually carries the shape.
func resolve(name string, scope *pkgScope) (declaration, *pkgScope, bool) {
	for hops := 0; hops <= maxTypeDepth; hops++ {
		decl, isLocal := scope.declared[name]
		if !isLocal {
			return declaration{}, nil, false
		}

		inner := scope.at(decl)
		if !decl.alias {
			return decl, inner, true
		}

		targets := inner.qualify(decl.expr)
		if len(targets) == 0 {
			return declaration{}, nil, false
		}

		name, scope = targets[0], inner
	}

	return declaration{}, nil, false
}

// collectDeclaredTypes maps every type declared in this module to the
// expression it is declared as, keyed "package.TypeName". The key MUST be
// package-qualified: "Logger" means the universal one-method interface in the
// root package and something else entirely elsewhere.
//
// internal/ is walked too. A root alias can point into it, and resolution has
// to be able to follow that rather than treat it as foreign.
func collectDeclaredTypes(t *testing.T, root string) map[string]declaration {
	t.Helper()

	declared := make(map[string]declaration)
	fset := token.NewFileSet()

	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() {
			if name := entry.Name(); path != root && strings.HasPrefix(name, ".") {
				return filepath.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		file, parseErr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			return parseErr
		}

		record(declared, file)

		return nil
	})
	if err != nil {
		t.Fatalf("collect declared types: %v", err)
	}

	return declared
}

func record(declared map[string]declaration, file *ast.File) {
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}

		for _, spec := range gen.Specs {
			typeSpec, isType := spec.(*ast.TypeSpec)
			if !isType {
				continue
			}

			declared[file.Name.Name+"."+typeSpec.Name.Name] = declaration{
				expr:    typeSpec.Type,
				pkg:     file.Name.Name,
				aliases: localAliases(file),
				coupled: coupledAliases(file),
				alias:   typeSpec.Assign.IsValid(),
			}
		}
	}
}

// rootPackageName is the package clause of this module's root package.
//
// It cannot be derived from the import path. The path ends in the major-version
// suffix (.../lib-systemplane/v3), and the directory above it is
// "lib-systemplane" while the package is "systemplane" — so the usual
// last-path-element rule yields "v3", which matches no declaration and would
// make every root type referenced from admin/ silently unresolvable.
const rootPackageName = "systemplane"

// majorSuffix matches the /vN element a Go module path carries from v2 onward.
var majorSuffix = regexp.MustCompile(`^v[0-9]+$`)

// localPackageName maps one of THIS module's import paths to the package name
// its declarations are keyed by.
func localPackageName(path string) string {
	if last := lastElement(path); !majorSuffix.MatchString(last) {
		return last
	}

	return rootPackageName
}

func lastElement(path string) string {
	return path[strings.LastIndex(path, "/")+1:]
}

// localAliases maps the name a file refers to each of THIS module's packages
// by, back to the real package name.
func localAliases(file *ast.File) map[string]string {
	return importsMatching(file, localPackageName, func(path string) (string, bool) {
		if !strings.Contains(path, "lib-systemplane") {
			return "", false
		}

		return localPackageName(path), true
	})
}

// coupledAliases maps the name a file refers to a coupled module's package by,
// to that module's identifying path fragment.
func coupledAliases(file *ast.File) map[string]string {
	return importsMatching(file, lastElement, func(path string) (string, bool) {
		for _, module := range coupledModules {
			if strings.Contains(path, module) {
				return module, true
			}
		}

		return "", false
	})
}

// importsMatching keys each selected import by the identifier the file refers
// to it as: the explicit alias when there is one, otherwise defaultName(path).
func importsMatching(
	file *ast.File,
	defaultName func(string) string,
	match func(string) (string, bool),
) map[string]string {
	out := make(map[string]string, len(file.Imports))

	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue
		}

		value, ok := match(path)
		if !ok {
			continue
		}

		name := defaultName(path)
		if imp.Name != nil {
			name = imp.Name.Name
		}

		out[name] = value
	}

	return out
}

func fieldTypes(list *ast.FieldList) []ast.Expr {
	if list == nil {
		return nil
	}

	out := make([]ast.Expr, 0, len(list.List))
	for _, field := range list.List {
		out = append(out, field.Type)
	}

	return out
}

func methodName(field *ast.Field) string {
	if len(field.Names) == 0 {
		return "<embedded>"
	}

	return field.Names[0].Name
}

func isLoggerish(names []*ast.Ident) bool {
	for _, name := range names {
		lowered := strings.ToLower(name.Name)
		for _, candidate := range loggerish {
			if lowered == candidate {
				return true
			}
		}
	}

	return false
}

func paramNames(names []*ast.Ident) string {
	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, name.Name)
	}

	return strings.Join(out, ", ")
}

// TestCheckerCatchesEvasions proves the gate above CAN fail. A checker that
// accepts everything would keep the boundary test green forever while the
// coupling walked straight back in, so each case here is source the checker
// must reject.
//
// The fixtures are parsed in memory rather than written to the tree, because
// the real walk reads real files and a permanent canary on disk would make it
// fail by design.
func TestCheckerCatchesEvasions(t *testing.T) {
	t.Parallel()

	const dependency = `package log

import "context"

type Level uint8

type Field struct {
	Key   string
	Value any
}

type Logger interface {
	Log(ctx context.Context, level int, msg string, fields ...any)
	With(fields ...any) Logger
}
`

	cases := map[string]struct {
		src     string
		wantHit string
	}{
		"the v3 regression itself: a coupled logger back in a parameter": {
			src: `package systemplane

import "github.com/LerianStudio/lib-observability/v4/log"

func WithLogger(l log.Logger) {}
`,
			wantHit: "lib-observability",
		},
		"a coupled type behind a parameter named anything at all": {
			src: `package systemplane

import "github.com/LerianStudio/lib-observability/v4/log"

func WithLogger(sink log.Logger) {}
`,
			wantHit: "lib-observability",
		},
		"a coupled type smuggled through a local interface method": {
			src: `package systemplane

import (
	"context"

	obslog "github.com/LerianStudio/lib-observability/v4/log"
)

type Logger interface {
	Log(ctx context.Context, level int, msg string, fields ...obslog.Field)
}

func WithLogger(l Logger) {}
`,
			wantHit: "lib-observability",
		},
		"the telemetry half of the same break: a concrete coupled struct": {
			src: `package systemplane

import "github.com/LerianStudio/lib-observability/v4/tracing"

func WithTelemetry(t *tracing.Telemetry) {}
`,
			wantHit: "lib-observability",
		},
		"a concrete coupled struct behind a parameter named nothing like a logger": {
			src: `package systemplane

import "github.com/LerianStudio/lib-observability/v4/tracing"

func WithManagerTelemetry(provider *tracing.Telemetry) {}
`,
			wantHit: "lib-observability",
		},
		"a coupled type behind a local alias with a neutral parameter name": {
			src: `package systemplane

import "github.com/LerianStudio/lib-observability/v4/tracing"

type Alias = *tracing.Telemetry

func WithProvider(provider Alias) {}
`,
			wantHit: "lib-observability",
		},
		"a coupled type inside an ANONYMOUS interface's method": {
			src: `package systemplane

import (
	"context"

	obslog "github.com/LerianStudio/lib-observability/v4/log"
)

func WithLogger(l interface {
	Log(ctx context.Context, level int, msg string, fields ...obslog.Field)
}) {
}
`,
			wantHit: "lib-observability",
		},
		"a coupled type returned by an ANONYMOUS interface's method": {
			src: `package systemplane

import "github.com/LerianStudio/lib-observability/v4/tracing"

func WithTelemetry(provider interface{ Unwrap() *tracing.Telemetry }) {}
`,
			wantHit: "lib-observability",
		},
		"a coupled type on a method of a receiver with TWO type parameters": {
			src: `package systemplane

import "github.com/LerianStudio/lib-observability/v4/log"

type Cache[K comparable, V any] struct{}

func (c *Cache[K, V]) WithLogger(l log.Logger) {}
`,
			wantHit: "lib-observability",
		},
		"a coupled type behind a root type imported through the /vN path": {
			src: `package admin

import systemplane "example.test/lib-systemplane/v3"

func Mount(l systemplane.Logger) {}
`,
			wantHit: "lib-observability",
		},
		"a coupled type as a generic argument": {
			src: `package systemplane

import "github.com/LerianStudio/lib-observability/v4/log"

type Box[T any] interface{ Get() T }

func WithLogger(l Box[log.Field]) {}
`,
			wantHit: "lib-observability",
		},
		"a local self-returning interface a consumer cannot declare": {
			src: `package systemplane

import "context"

type Logger interface {
	Log(ctx context.Context, level int, msg string, fields ...any)
	With(fields ...any) Logger
}

func WithLogger(l Logger) {}
`,
			wantHit: "self-returning",
		},
		"an alias to a coupled interface": {
			src: `package systemplane

import "github.com/LerianStudio/lib-observability/v4/log"

type Alias = log.Logger

func WithLogger(l Alias) {}
`,
			wantHit: "lib-observability",
		},
		"a coupled type smuggled through a local interface RESULT": {
			src: `package systemplane

import obslog "github.com/LerianStudio/lib-observability/v4/log"

type Logger interface {
	Unwrap() obslog.Logger
}

func WithLogger(l Logger) {}
`,
			wantHit: "lib-observability",
		},
		"an interface embedding a self-returning one": {
			src: `package systemplane

import "context"

type Rich interface {
	Log(ctx context.Context, level int, msg string, fields ...any)
	With(fields ...any) Rich
}

type Embedder interface{ Rich }

func WithLogger(l Embedder) {}
`,
			wantHit: "self-returning",
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			reason := checkFixture(t, dependency, testCase.src)
			if reason == "" {
				t.Fatalf("checker accepted an evasion it must reject; expected a reason containing %q",
					testCase.wantHit)
			}

			if !strings.Contains(reason, testCase.wantHit) {
				t.Errorf("reason does not explain the violation\n got: %s\nwant substring: %s",
					reason, testCase.wantHit)
			}
		})
	}
}

// TestCheckerAcceptsLegitimateShapes is the other half. Without it, tightening
// the checker could pass by rejecting everything.
func TestCheckerAcceptsLegitimateShapes(t *testing.T) {
	t.Parallel()

	const dependency = `package tracing

type options struct{ verbose bool }

type Option interface{ apply(*options) }
`

	cases := map[string]string{
		"the shape this module actually ships": `package systemplane

import "context"

type Logger interface {
	Log(ctx context.Context, level int, msg string, fields ...any)
}

func WithLogger(l Logger) {}
`,
		"an anonymous universal interface": `package systemplane

import "context"

func WithLogger(l interface {
	Log(ctx context.Context, level int, msg string, fields ...any)
}) {
}
`,
		"a sealed option interface reached through a local alias": `package systemplane

import "example.test/tracing"

type Opt = tracing.Option

func Mount(opts ...Opt) {}
`,
		"a sealed option interface": `package systemplane

import "example.test/tracing"

func Mount(opts ...tracing.Option) {}
`,
		"the telemetry shape this module actually ships": `package systemplane

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type Telemetry interface {
	Tracer(name string) (trace.Tracer, error)
	Meter(name string) (metric.Meter, error)
}

func WithTelemetry(t Telemetry) {}
`,
		"a lib-commons handle, which is out of scope by design": `package systemplane

import tmpostgres "github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/postgres"

func NewManager(pgMgr *tmpostgres.Manager) {}
`,
	}

	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if reason := checkFixture(t, dependency, src); reason != "" {
				t.Errorf("checker rejected a legitimate shape: %s", reason)
			}
		})
	}
}

// checkFixture parses two in-memory files — a dependency package and the
// package under test — and returns the violation the checker reports for the
// first examined parameter, or "" for none.
//
// It mirrors all four gates in checkParams: a parameter is examined when its
// type names a coupled module, OR its name looks loggerish, OR its type is a
// local interface, OR its type is a local alias. Calling universalityViolation directly would bypass the
// sealed-interface exemption, which lives in namesLocalInterface.
func checkFixture(t *testing.T, dependency, subject string) string {
	t.Helper()

	fset := token.NewFileSet()
	declared := make(map[string]declaration)

	for _, src := range []string{dependency, rootPackageFixture, subject} {
		file, err := parser.ParseFile(fset, "fixture.go", src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse fixture: %v", err)
		}

		recordFixture(declared, file)
	}

	file, err := parser.ParseFile(fset, "subject.go", subject, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse subject: %v", err)
	}

	scope := &pkgScope{
		declared: declared,
		pkg:      file.Name.Name,
		aliases:  fixtureAliases(file),
		coupled:  coupledAliases(file),
	}

	for _, decl := range file.Decls {
		fn, isFunc := decl.(*ast.FuncDecl)
		if !isFunc || !fn.Name.IsExported() || !exportedReceiver(fn) || fn.Type.Params == nil {
			continue
		}

		for _, param := range fn.Type.Params.List {
			coupled := scope.coupledQualifier(param.Type) != ""
			if !coupled &&
				!isLoggerish(param.Names) &&
				!namesLocalInterface(param.Type, scope) &&
				!namesLocalAlias(param.Type, scope) {
				continue
			}

			if reason := universalityViolation(param.Type, scope, 0); reason != "" {
				return reason
			}
		}
	}

	return ""
}

func recordFixture(declared map[string]declaration, file *ast.File) {
	aliases := fixtureAliases(file)
	coupled := coupledAliases(file)

	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}

		for _, spec := range gen.Specs {
			typeSpec, isType := spec.(*ast.TypeSpec)
			if !isType {
				continue
			}

			declared[file.Name.Name+"."+typeSpec.Name.Name] = declaration{
				expr:    typeSpec.Type,
				pkg:     file.Name.Name,
				aliases: aliases,
				coupled: coupled,
				alias:   typeSpec.Assign.IsValid(),
			}
		}
	}
}

// rootPackageFixture stands in for this module's root package, so a fixture can
// exercise the .../lib-systemplane/v3 import path whose last element is a
// version rather than a package name.
const rootPackageFixture = `package systemplane

import obslog "github.com/LerianStudio/lib-observability/v4/log"

type Logger interface {
	Log(fields ...obslog.Field)
}
`

// fixtureAliases treats every import in a fixture as local, so the checker can
// resolve the fixture's own dependency package the way it resolves this
// module's real ones.
func fixtureAliases(file *ast.File) map[string]string {
	return importsMatching(file, localPackageName, func(path string) (string, bool) {
		return localPackageName(path), true
	})
}
