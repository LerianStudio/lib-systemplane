# Migrating to lib-systemplane v3

v3 has one job: **stop lib-observability's major version from propagating
through this library's public API.**

Nothing was added to what the library does. Runtime-config semantics, the
storage shape, the admin routes, the tenant dispatch, the Manager lifecycle —
all unchanged. What changed is the *shape of two parameter positions*, plus the
module path that a major always costs.

---

## Why v3 exists

Four exported symbols named a type defined by `lib-observability/v2`:

```go
func WithLogger(l log.Logger) Option
func WithTelemetry(t *tracing.Telemetry) Option
func WithManagerLogger(l log.Logger) ManagerOption
func WithManagerTelemetry(t *tracing.Telemetry) ManagerOption
```

Go matches the types inside a signature **nominally**. `v2/log.Logger` and
`v4/log.Logger` are different types even where the source is byte-for-byte
identical, and the same is true of `*v2/tracing.Telemetry` against
`*v4/tracing.Telemetry`. So a service already holding a v4 logger could not
call `WithLogger` at all: not "it logs differently", it does not compile.
lib-observability's major was, in effect, part of this library's contract, and
every service that wanted lib-systemplane had to agree with it about which
major of lib-observability the whole process would use.

That is the coupling the fleet is dismantling. `lib-observability` v4 widened
its own boundary to universal types for the same reason; this is
lib-systemplane's half of the same move, and it makes lib-systemplane's own
future majors cheap too — the next one costs a consumer an import line, not a
rewrite of its bootstrap.

`log.Logger` also carried `With(fields ...any) Logger`. A **self-returning**
method cannot be declared by a foreign package — it has no way to name the
return type — so a consumer could not even describe the shape locally to work
around the problem. The replacement interfaces have no such method.

---

## From / to

| v2 | v3 | breaking? |
|---|---|---|
| `WithLogger(l log.Logger) Option` | `WithLogger(l Logger) Option` | **yes** — only for a caller that cannot produce a `Logger` |
| `WithManagerLogger(l log.Logger) ManagerOption` | `WithManagerLogger(l Logger) ManagerOption` | same |
| `WithTelemetry(t *tracing.Telemetry) Option` | `WithTelemetry(t Telemetry) Option` | same |
| `WithManagerTelemetry(t *tracing.Telemetry) ManagerOption` | `WithManagerTelemetry(t Telemetry) ManagerOption` | same |
| — | `type systemplane.Logger` (new) | new |
| — | `type systemplane.Telemetry` (new) | new |
| `(*Client).Logger() log.Logger` | unchanged, now v4's `log.Logger` | import path only |
| dependency `lib-observability/v2` | `lib-observability/v4 v4.0.0-beta.1` | **yes** |
| module `…/lib-systemplane/v2` | module `…/lib-systemplane/v3` | **yes — import path** |

Nothing else in the public API moved. `NewPostgres`, `NewMongoDB`, `Register`,
`Start`, `Close`, every typed getter, `Set`, `Delete`, `List`, `Catalog`,
`OnChange`, the sentinel errors, the key options, `admin.Mount`,
`admin.MountCatalog`, `SchemaSQL`, `DefaultSeedSQL` and `NewForTesting` are
untouched.

### The two new types

```go
// systemplane.Logger
type Logger interface {
	Log(ctx context.Context, level int, msg string, fields ...any)
}

// systemplane.Telemetry
type Telemetry interface {
	Tracer(name string) (trace.Tracer, error)
	Meter(name string) (metric.Meter, error)
}
```

Both are built from types this library does not own a major of: stdlib types,
and `go.opentelemetry.io/otel`, a stable v1 module the whole ecosystem shares.

`Logger` is one method because one method is all this library calls. It has no
`With`, `WithGroup`, `Enabled` or `Sync`, so a consumer can declare the same
interface in its own package and satisfy the parameter while importing nothing
from lib-observability. `Telemetry` is two methods for the same reason: `Tracer`
for backend spans, `Meter` for the Manager's instruments, and nothing else was
ever read from the struct.

### One behaviour change beyond the types

The four options are now **last-wins including nil**. In v2 a nil logger or a
nil telemetry provider was silently ignored, so `WithTelemetry(t)` followed by
`WithTelemetry(nil)` kept `t` — the opposite of what the second call asked for.
A nil now clears whatever an earlier option set, and the constructor substitutes
a no-op logger and disables spans and metrics. A single `WithLogger(nil)` or
`WithTelemetry(nil)`, which is the only shape anyone actually writes, behaves
exactly as it did.

---

## The consumer diff

For a service already on lib-observability v4, this is the whole change:

```diff
-	systemplane "github.com/LerianStudio/lib-systemplane/v2"
+	systemplane "github.com/LerianStudio/lib-systemplane/v3"
```

The bootstrap call sites do not move. A `log.Logger` from lib-observability v4
satisfies `systemplane.Logger`, and a `*tracing.Telemetry` from v4 satisfies
`systemplane.Telemetry`, both directly — no adapter, no wrapper:

```go
client, err := systemplane.NewPostgres(db, dsn,
	systemplane.WithLogger(telemetry.Logger),      // unchanged
	systemplane.WithTelemetry(telemetry),           // unchanged
)
```

A service still on lib-observability v2 is equally fine: a v2 logger and a v2
`*tracing.Telemetry` satisfy the new interfaces too. That is the point of
declaring them locally — the parameter no longer cares which major the value
came from.

The only callers that must change are the ones that were passing something
which is *not* a logger or a telemetry provider by shape. There were none in
the fleet at the time of writing.

### If you implement your own logger

Widen it the way lib-observability v4 asks:

```go
-func (l *myLogger) Log(ctx context.Context, level log.Level, msg string, fields ...log.Field)
+func (l *myLogger) Log(ctx context.Context, level int, msg string, fields ...any)
```

`level` is on lib-observability's scale, where **lower is more severe**:
Error=0, Warn=1, Info=2, Debug=3. This is inverted from `log/slog`.

### If you hold a `[]log.Field`

v4's variadic is `...any`, so a slice can no longer be spread into it. Pass it
as a single argument; v4 flattens a `[]Field` in place and the rendered output
is identical:

```diff
-logger.Log(ctx, log.LevelInfo, "msg", fields...)
+logger.Log(ctx, log.LevelInfo, "msg", fields)
```

---

## Why `(*Client).Logger()` still returns a rich `log.Logger`

Only **parameters** propagate a major. A return type does not: a caller can
always assign a rich value to a narrower interface declared in its own package,
so returning `log.Logger` costs it nothing and narrowing the return would only
remove capability. The value returned genuinely is a full logger — `WithLogger`
converts once at the boundary via `log.Adapt`, which returns an already-rich
logger unchanged rather than wrapping it — so the accessor is honest about what
it hands back.

The corollary: `Logger()` now returns lib-observability **v4**'s `log.Logger`.
A consumer that assigns the result to an explicitly typed v2 variable has to
update that line. A consumer that calls a method on it, or assigns it to its own
interface, does not.

---

## Why the module path moved in the same commit as the break

Because on a repository with automated releases it has to. lib-streaming split
the two across separate PRs and semantic-release tagged `v4.0.0-beta.1` against
a module path still saying `/v3` the moment the first one merged; Go rejects
that combination outright (`module path includes a major version suffix, so
major version must match`), and the release was uninstallable until the rename
landed. Here the rename and the API change are one commit.

**This repository does not auto-major, by policy.** `.releaserc.yml` maps
`breaking: true` to `minor`, guarded in both directions by
`admin/release_policy_test.go`, because a `major` rule on a `/vN` line
recomputes a phantom `/vN+1` tag that Go cannot consume — the failure that
wedged lib-commons' beta channel. The policy's own instruction for a real major
is "do the path rename + manual `vN.0.0` tag".

So: **`v3.0.0` must be tagged by hand.** Until that tag exists, the `/v3` path
is not resolvable, whatever version semantic-release computes on merge.

---

## The regression test

`boundary_test.go` walks every exported function, method and interface method in
the importable packages (root, `admin`, `systemplanetest`) with `go/ast` and
fails if a parameter is not universal — one that names a `lib-observability`
type, or a locally-declared interface a consumer could not implement. It is a
denylist of shapes rather than a signature snapshot, so it keeps working as the
API grows, and it carries its own positive controls: `TestCheckerCatchesEvasions`
pins eight shapes the checker must reject, `TestCheckerAcceptsLegitimateShapes`
pins four it must not, so the gate cannot go quietly vacuous.

It closes the bypasses an AST checker collects if nobody looks for them: a
receiver with two type parameters (`ast.IndexListExpr`, which resolves to no
name and skips every method on the type), an anonymous interface literal (which
resolves to no name either, while its methods can name anything), the root
package imported through its `/v3` path (whose last element is a version, not a
package name, so a root type referenced from `admin/` would never resolve), and
a dot import (which the checker cannot resolve at all, so it fails loudly rather
than passing quietly). Each has a fixture that fails without its fix.

`internal/` is deliberately outside the walk. Those packages still name
`log.Logger` and use `tracing.HandleSpanError` freely; no consumer can import
them, so nothing propagates.

`lib-commons` is deliberately outside the denylist. `NewManager` takes a
`*tmpostgres.Manager`, a concrete connection-pool handle with no interface to
stand in for it, so that library's major is part of this contract by
construction. Breaking it is a separate change, not a silent one.
