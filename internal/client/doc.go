// Package client implements the systemplane runtime-configuration client.
//
// It owns lifecycle management, registration, read/write paths, subscription
// dispatch, telemetry, tenant-scoped overrides, and test-only construction for
// callers that need a controlled store. The root systemplane package exposes the
// public facade; this internal package remains the implementation boundary for
// concrete PostgreSQL, MongoDB, and test stores.
package client
