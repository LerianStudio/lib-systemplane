// BSON field and operator name constants used across the MongoDB backend.
//
// Hoisted to a single location so the repeated string literals — which the
// goconst linter flags when the same value appears in three or more places —
// have one canonical definition. Struct tags (e.g. `bson:"namespace"`)
// intentionally still carry the literal: Go struct tags must be compile-time
// string literals and cannot reference these constants.
package mongodb

const (
	// fieldID is the BSON _id field name. On phase-2 rows it carries a
	// compoundID sub-document; on legacy rows it is a bare ObjectId.
	fieldID = "_id"

	// fieldNamespace is the BSON namespace field name (also mirrored into
	// _id.namespace on phase-2 rows).
	fieldNamespace = "namespace"

	// fieldKey is the BSON key field name (also mirrored into _id.key on
	// phase-2 rows).
	fieldKey = "key"

	// fieldTenantID is the BSON tenant_id field name. The sentinel
	// store.SentinelGlobal ("_global") marks rows owned by the legacy
	// (non-tenant-scoped) API surface.
	fieldTenantID = "tenant_id"

	// fieldUpdatedAt is the BSON updated_at field name driving the polling
	// path's watermark and the global sort key on listing endpoints.
	fieldUpdatedAt = "updated_at"

	// fieldUpdatedBy is the BSON updated_by audit field name.
	fieldUpdatedBy = "updated_by"

	// fieldOwner is the BSON owner field name on the migration lease
	// sentinel document (see mongodb_migration_legacy.go).
	fieldOwner = "owner"

	// fieldDeleted marks polling-mode tombstones. Active documents either omit
	// the field or set it to false.
	fieldDeleted = "deleted"

	// opSet is the MongoDB $set update operator.
	opSet = "$set"

	// opGt is the MongoDB $gt comparison operator used by the polling
	// watermark filter and the tenant-override keyset cursor.
	opGt = "$gt"

	opAnd = "$and"
	opOr  = "$or"

	opMatch  = "$match"
	opNe     = "$ne"
	opNin    = "$nin"
	opExists = "$exists"
)
