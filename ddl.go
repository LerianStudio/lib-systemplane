// Copyright 2025 Lerian Studio.

package systemplane

import _ "embed"

// schemaSQL is the canonical systemplane schema DDL, embedded byte-faithfully
// from ddl/schema.sql. It is the same shape the runtime emits via
// internal/postgres/postgres_schema.go and internal/manager/schema.go, with
// the table name fixed to systemplane_entries and the NOTIFY channel fixed to
// systemplane_changes.
//
//go:embed ddl/schema.sql
var schemaSQL string

// defaultSeedSQL is the universal default seed for the runtime_config
// namespace, embedded from ddl/default_seed.sql. Values are neutral baselines
// inserted with ON CONFLICT (namespace, "key") DO NOTHING.
//
//go:embed ddl/default_seed.sql
var defaultSeedSQL string

// SchemaSQL returns the full systemplane schema DDL as an importable artifact.
//
// The returned SQL creates the systemplane_entries table, the
// systemplane_notify_v3() trigger function, and the INSERT/DELETE and UPDATE
// NOTIFY triggers on the systemplane_changes channel. It is idempotent and
// safe to fold into a consumer's own migration pipeline; lib-systemplane does
// not execute it for the caller.
func SchemaSQL() string {
	return schemaSQL
}

// DefaultSeedSQL returns the universal default seed INSERTs as an importable
// artifact.
//
// The returned SQL seeds neutral runtime_config defaults (log level, CORS,
// rate limit, idempotency) with ON CONFLICT (namespace, "key") DO NOTHING so
// operator-set values are never overwritten. Consumers fold this into their
// own migration pipeline; lib-systemplane does not execute it for the caller.
func DefaultSeedSQL() string {
	return defaultSeedSQL
}
