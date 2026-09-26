package unaliased

import "github.com/LerianStudio/lib-observability/v4/log"

// line is the plain shape: the package imported under its own name.
func line() []log.Field {
	return []log.Field{log.String("namespace", "ns"), log.String("key", "k")}
}
