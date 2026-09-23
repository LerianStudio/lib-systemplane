// Package engine holds the convergent runtime-configuration engine that backs
// the systemplane client.
//
// One Engine tracks N scopes in a single map keyed by store.Scope. The zero
// scope is the single-tenant scope; every tenant is another key in that same
// map, so nothing about the engine is single- or multi-tenant shaped. Every
// value that reaches a cache — hydration, changefeed refresh, reconcile, Set
// and per-request read — enters through one decode, validate and publish
// ingress, where publish is a per-(scope, key) revision fence. Subscribers are
// invoked by a coalescing worker per (scope, key), never by the goroutine the
// changefeed calls into.
//
// The engine never imports internal/client: the key registry, options, catalog
// and redaction stay there and reach the engine through ports declared here.
package engine
