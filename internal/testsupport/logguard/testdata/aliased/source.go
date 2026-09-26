package aliased

import obslog "github.com/LerianStudio/lib-observability/v4/log"

// line is the shape that slipped past a scan keyed on the identifier "log":
// the same call, under an alias.
func line() []obslog.Field {
	return []obslog.Field{obslog.String("namespace", "ns"), obslog.String("key", "k")}
}
