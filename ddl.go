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
// ddl/migrate_v3_to_v4.sql. It is schemaSQL minus the guard block and the
// CREATE TABLE.
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
// systemplane_revision_seq sequence, the systemplane_bump_revision_v4() and
// systemplane_notify_v4() trigger functions, and the three triggers that
// assign the revision and NOTIFY on the systemplane_changes channel.
//
// The table and the sequence always live in the SAME schema: the bump trigger
// resolves the sequence as TG_TABLE_SCHEMA.systemplane_revision_seq, so the
// DDL resolves the schema owning systemplane_entries the same way and creates
// and seeds the sequence there rather than in whatever schema the applying
// role's search_path happens to put first.
//
// Apply it to ONE DATABASE PER TENANT. It must never be applied once per
// schema inside a shared database: NOTIFY is database-wide and every feed
// listens on the single systemplane_changes channel, so two installations in
// one database would each receive the other's events, and the unqualified
// DROP FUNCTION of the v3 notify function resolves through the applying role's
// whole search_path.
//
// Every revision comes from the sequence, and only ever through the BEFORE
// INSERT OR UPDATE trigger: an insert draws a new one, an UPDATE that actually
// changes value draws a new one, and an identical rewrite keeps the revision
// it had. The column carries no DEFAULT and the trigger function is SECURITY
// DEFINER on purpose — the sequence is advanced with the privileges of the
// role that applied this DDL, so the runtime role needs plain DML on
// systemplane_entries and no grant at all on systemplane_revision_seq.
// Because the counter is table-level rather than per-row, a key deleted and
// recreated always comes back above every revision it ever had, revisions may
// skip numbers, and on a fresh database the first write lands at revision 2
// rather than 1.
//
// systemplane_entries MUST live in the FIRST schema of the applying role's
// search_path. Every statement but one resolves the table through the whole
// search_path; CREATE TABLE IF NOT EXISTS only ever looks at the first schema,
// so applying this to an install that sits further down the path would create
// a second, empty table there and orphan the populated one — a migration that
// exits 0 and leaves every registered key reading its default. The DDL refuses
// that case loudly instead, scanning the catalog for systemplane_entries in
// every user schema rather than resolving it through search_path, so the
// install it protects is found even when the applier cannot see it. An install
// that cannot be put first in search_path
// is upgraded with MigrationV3ToV4SQL(), which creates no table and therefore
// follows the search_path to wherever the table actually is.
//
// It is idempotent and upgrades a v3 database in place: the ALTER adds the
// column at revision 1 for the rows already there and the setval lifts the
// sequence past the highest revision present, so the first write after the
// upgrade lands at 2 or higher. Every application re-sets the transitional
// column default unconditionally and drops it again last, after the triggers
// are installed, so neither a first nor a repeated untransacted application
// ever leaves a window in which a concurrent insert has neither a default nor
// a trigger to fill the NOT NULL revision — on a re-application the ADD COLUMN
// is a no-op and would restore nothing. It is safe to fold
// into a consumer's own migration pipeline; lib-systemplane does not execute
// it for the caller.
func SchemaSQL() string {
	return schemaSQL
}

// MigrationV3ToV4SQL returns the v3 -> v4 delta as an importable artifact.
//
// The returned SQL adds the revision column at 1 for every row already stored,
// creates and seeds systemplane_revision_seq past the highest revision present
// so the first write after the upgrade lands at 2 or higher, installs
// systemplane_bump_revision_v4() and systemplane_notify_v4() with the three v4
// triggers, drops the v3 notify function, and only then drops the column
// default — last, after the triggers exist, so an untransacted application
// never leaves a window in which a concurrent insert has neither a default nor
// a trigger to fill the NOT NULL revision. The default is re-set
// unconditionally on every application, because once the column exists the ADD
// COLUMN is a no-op and would restore nothing for a second run to lean on.
//
// The table and the sequence always live in the SAME schema: the bump trigger
// resolves the sequence as TG_TABLE_SCHEMA.systemplane_revision_seq, so the
// migration resolves the schema owning systemplane_entries the same way and
// creates and seeds the sequence there rather than in whatever schema the
// applying role's search_path happens to put first.
//
// Apply it to ONE DATABASE PER TENANT. It must never be applied once per
// schema inside a shared database: NOTIFY is database-wide and every feed
// listens on the single systemplane_changes channel, so two installations in
// one database would each receive the other's events, and the unqualified
// DROP FUNCTION of the v3 notify function resolves through the applying role's
// whole search_path.
//
// After the upgrade the SECURITY DEFINER bump trigger is the only thing that
// touches the sequence, so the runtime role needs plain DML on
// systemplane_entries and no grant at all on systemplane_revision_seq.
// Revisions may skip numbers from then on, and a key deleted and recreated
// always comes back above every revision it previously had.
//
// It is idempotent and it is SchemaSQL() minus the guard block and the CREATE
// TABLE: it creates no systemplane_entries and therefore upgrades the install
// wherever search_path finds it, which is what makes it the way out of the
// fork SchemaSQL() refuses. A consumer starting
// from an empty database applies SchemaSQL() instead, where the first write
// lands at revision 2 rather than 1. lib-systemplane does not execute it for
// the caller.
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
