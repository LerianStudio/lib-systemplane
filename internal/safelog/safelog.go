// Package safelog holds the two things this library does to keep a consumer's
// own observability code from costing it more than a log line: the stamp a
// tenant-less multi-tenant report carries, and the swallow that ends a
// panicking logger's report one frame from where it started.
//
// It is a leaf because both internal/engine and internal/group need it, and a
// group that reached into the engine for it inverted their layering.
package safelog

// UnresolvedTenant is the tenant.id a multi-tenant line carries when it has
// no tenant id to name: the Client's read-through errors when the tenant
// database resolved but ctx carries no tenant id (the two ride independent
// context keys), and the group coordinator's reports about a publication that
// names no tenant. An empty tenant.id there would read exactly like a
// single-tenant line, which carries no tenant field at all.
const UnresolvedTenant = "unresolved"

// Swallow discards a panic raised by the consumer's own observability code.
// There is nowhere left to report it — the logger is what panicked — and the
// alternative is unwinding a library goroutine over a log line.
//
// It lives here because every caller of it is a caller of this package: the
// engine's log helpers and the group coordinator's, which each used to carry a
// byte-identical copy.
func Swallow() {
	_ = recover()
}
