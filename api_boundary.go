package systemplane

import (
	"context"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Logger is the logging contract this library accepts.
//
// It is declared here, from stdlib types only, and never in terms of a type
// defined by lib-observability. That is deliberate. Go matches the types in a
// method signature nominally, so a parameter typed
// lib-observability/vN/log.Logger makes lib-observability's MAJOR part of this
// library's contract: a consumer holding a v4 logger could not satisfy a v2
// parameter even though the source of the two interfaces is identical. Naming
// the interface locally means a logger built against any lib-observability
// major — or against none at all — satisfies it, and a future major of that
// library costs this one's consumers nothing.
//
// The one method is the whole interface on purpose. With and WithGroup are
// self-returning, and a self-returning method cannot be declared by a foreign
// package: it has no way to name the return type. Nothing here calls them, so
// requiring them would exclude implementers for no gain.
//
// level is on lib-observability's scale, where lower is more severe:
// Error=0, Warn=1, Info=2, Debug=3. This is INVERTED from log/slog.
//
// fields accepts lib-observability log.Field values, a []log.Field carried as
// a single element, or slog-style alternating key/value pairs.
//
// A nil Logger is accepted and discards every entry.
type Logger interface {
	Log(ctx context.Context, level int, msg string, fields ...any)
}

// Telemetry is the OpenTelemetry provider contract this library accepts.
//
// It is declared here for the same reason as [Logger]: naming
// lib-observability's *tracing.Telemetry in a parameter would make that
// library's MAJOR part of this one's contract, and a consumer holding a v4
// Telemetry could not call a v2 parameter even though the struct is identical.
//
// These are the only two methods this library ever calls on a telemetry
// handle. The values they return come from go.opentelemetry.io/otel, a stable
// v1 module shared by the whole ecosystem, so naming them propagates nothing.
// lib-observability's *tracing.Telemetry satisfies this directly, from any
// major, and so does a consumer's own provider wrapper.
//
// Both methods may return an error; this library then falls back to a no-op
// tracer or leaves the affected instruments disabled. A nil Telemetry is
// accepted and disables tracing and metrics entirely.
type Telemetry interface {
	Tracer(name string) (trace.Tracer, error)
	Meter(name string) (metric.Meter, error)
}
