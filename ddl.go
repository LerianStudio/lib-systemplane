// Copyright 2025 Lerian Studio.

package systemplane

import _ "embed"

// schemaSQL is the canonical systemplane schema DDL, embedded byte-faithfully
// from ddl/schema.sql. That file is the published artifact: consumers fold it
// into their own migration pipeline and the runtime never executes it. The
// table name is fixed to systemplane_entries and the NOTIFY channel to
// systemplane_changes.
//
//go:embed ddl/schema.sql
var schemaSQL string

// migrationV3ToV4SQL is the v3 -> v4 delta, embedded byte-faithfully from
// ddl/migrate_v3_to_v4.sql. It is schemaSQL without the table creation.
//
//go:embed ddl/migrate_v3_to_v4.sql
var migrationV3ToV4SQL string

// defaultSeedSQL is the universal default seed for the runtime_config
// namespace, embedded from ddl/default_seed.sql. Values are neutral baselines
// inserted with ON CONFLICT (namespace, "key") DO NOTHING.
//
//go:embed ddl/default_seed.sql
var defaultSeedSQL string

// SchemaSQL returns the full systemplane schema DDL as an importable artifact.
//
// The returned SQL creates the systemplane_entries table, the
// systemplane_bump_revision_v4() and systemplane_notify_v4() trigger
// functions, and the three triggers that bump the revision on UPDATE and
// NOTIFY on the systemplane_changes channel. It is idempotent, upgrades a v3
// database in place, and is safe to fold into a consumer's own migration
// pipeline; lib-systemplane does not execute it for the caller.
func SchemaSQL() string {
	return schemaSQL
}

// MigrationV3ToV4SQL returns the v3 -> v4 delta as an importable artifact.
//
// The returned SQL adds the revision column, installs
// systemplane_bump_revision_v4() and systemplane_notify_v4() with the three v4
// triggers, and drops the v3 notify function. It is idempotent and it does NOT
// create the systemplane_entries table: it upgrades a database that already
// carries the v3 schema. A consumer starting from an empty database applies
// SchemaSQL() instead. lib-systemplane does not execute it for the caller.
func MigrationV3ToV4SQL() string {
	return migrationV3ToV4SQL
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
