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
	fieldUpdatedAt = "updated_at"
	fieldUpdatedBy = "updated_by"

	opSet = "$set"
)
