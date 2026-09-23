package fields

import "github.com/LerianStudio/lib-observability/v4/log"

// line names its fields through the variadic form. lib-observability resolves
// each name/value pair to the same Any(name, value) the single-field
// constructors build, so a name that reaches the output redacted reaches it
// redacted from here too.
func line() []log.Field {
	return log.Fields("namespace", "ns", "key", "k")
}
