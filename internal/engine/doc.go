// Package engine holds the convergent runtime-configuration engine that backs
// the systemplane client.
//
// One Engine tracks N scopes in a single map keyed by store.Scope. The zero
// scope is the single-tenant scope; every tenant is another key in that same
// map, so nothing about the engine is single- or multi-tenant shaped. A cache
// has exactly four ingresses — a reconcile, including the snapshot rows it
// applies and the registered defaults it publishes for the keys the snapshot
// did not carry; a changefeed re-read; the echo of a local Set; and the
// registered default a local Delete publishes in place of the row it removed —
// and every value reaching one enters through a single decode, validate and
// publish step, where publish is a per-(scope, key) revision fence. Nothing
// else feeds a cache: a multi-tenant per-request read never enters one at all,
// it reads the tenant row on every call. Subscribers are invoked by a
// coalescing worker per (scope, key), never by the goroutine the changefeed
// calls into.
//
// The engine never imports internal/client: the key registry, options, catalog
// and redaction stay there and reach the engine through ports declared here.
package engine
