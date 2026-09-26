package nonliteral

import "github.com/LerianStudio/lib-observability/v4/log"

const keyField = "key"

// line names two fields the scan cannot read — a constant and a parameter —
// beside one it can. Only the literal is reported.
func line(name string) []log.Field {
	return []log.Field{
		log.String("namespace", "ns"),
		log.String(keyField, "k"),
		log.String(name, "v"),
	}
}
