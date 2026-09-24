package fieldliteral

import "github.com/LerianStudio/lib-observability/v4/log"

// spelled names its field through a composite literal that spells its type.
func spelled() log.Field {
	return log.Field{Key: "key", Value: "k"}
}

// elided names its field through a composite literal inside a slice that
// spells the type for it — the shape a constructor call is one edit away from,
// and the one a scan reading only calls never sees.
func elided() []log.Field {
	return []log.Field{{Key: "key", Value: "k"}}
}
