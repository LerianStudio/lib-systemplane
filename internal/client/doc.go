// Package client implements the systemplane runtime-configuration client.
//
// It owns lifecycle management, the key registry, options, the catalog and the
// facade the root package exposes. It holds no cache of its own: internal/engine
// serves the reads, takes the writes, runs the reconcile that follows every
// changefeed (re)connect, and dispatches to OnChange subscribers. Two operating
// modes share the same surface:
//
//   - Single-tenant: the Client tracks the zero scope in the engine. Reads
//     serve from the engine's cache; a write upserts through the store and
//     publishes into that cache before returning. The store's changefeed
//     drives refresh and reconcile, and the engine delivers to OnChange
//     subscribers off the changefeed goroutine.
//
//   - Multi-tenant: every write, and every read of a scope that is not
//     cached, resolves a per-request tenant database from ctx via lib-commons
//     tenant-manager. With a tenant manager a tenant's first read activates
//     its own tracked scope, which serves its later reads and delivers to
//     OnChange subscribers; without one there is no cache and no changefeed,
//     and OnChange returns ErrNotSupportedInMultiTenant.
//
// The root systemplane package exposes the public facade; this internal
// package remains the implementation boundary.
package client
