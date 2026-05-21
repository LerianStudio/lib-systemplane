// Package client implements the systemplane runtime-configuration client.
//
// It owns lifecycle management, registration, read/write paths, and
// subscription dispatch. Two operating modes share the same surface:
//
//   - Single-tenant: a process-wide in-memory cache fronts the backing
//     store. Reads hit the cache; writes update the store and the cache.
//     The store's changefeed drives invalidation/refresh and fires
//     OnChange subscribers.
//
//   - Multi-tenant: every read/write resolves a per-request tenant
//     database from ctx via lib-commons tenant-manager. No process-wide
//     cache, no changefeed; OnChange returns ErrNotSupportedInMultiTenant.
//
// The root systemplane package exposes the public facade; this internal
// package remains the implementation boundary.
package client
