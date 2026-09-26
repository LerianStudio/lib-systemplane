// BSON field and operator name constants used across the MongoDB backend.
//
// Hoisted to a single location so the repeated string literals — which the
// goconst linter flags when the same value appears in three or more places —
// have one canonical definition. Struct tags (`bson:"namespace"`)
// intentionally still carry the literal because Go struct tags must be
// compile-time string literals and cannot reference these constants.

package mongodb

const (
	fieldID        = "_id"
	fieldNamespace = "namespace"
	fieldKey       = "key"
	fieldValue     = "value"
	fieldRevision  = "revision"
	fieldUpdatedAt = "updated_at"
	fieldUpdatedBy = "updated_by"
	// fieldDeleted marks a tombstone. It is present and true only on a
	// document Delete rewrote; every other document omits it entirely, which
	// is why the reads guard with $ne rather than $exists (FC-9).
	fieldDeleted = "deleted"

	opSet = "$set"
	// opLiteral wraps every caller-supplied STRING written by an
	// aggregation-pipeline update. In a pipeline $set a bare string beginning
	// with "$" is an expression, not a value.
	opLiteral = "$literal"
	opIfNull  = "$ifNull"
	opUnset   = "$unset"
)
