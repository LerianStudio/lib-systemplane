package repeated

import "github.com/LerianStudio/lib-observability/v4/log"

// first and second log the same sensitive name from two call sites: the guard
// has to name both, not whichever one it saw last.
func first() []log.Field {
	return []log.Field{log.String("key", "k")}
}

func second() []log.Field {
	return []log.Field{log.String("key", "k")}
}
