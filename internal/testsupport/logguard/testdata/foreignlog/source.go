// Package foreignlog imports the standard library's log under the identifier
// the scan used to key on. It does not compile — stdlib log has no String —
// and never has to: testdata is invisible to the go tool, and the scan only
// parses. It exists to prove the scan resolves the import PATH rather than
// trusting the identifier, so this file contributes no field names at all and
// the guard fails outright for having read nothing.
package foreignlog

import "log"

func line() any { return log.String("key", "k") }
