package mongodb

import (
	"errors"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// isNamespaceExists reports whether err signals that the target collection
// already exists. MongoDB returns server error code 48 ("NamespaceExists") in
// that case; the driver's CommandError wraps that, and string matching covers
// the unstructured fallback path.
func isNamespaceExists(err error) bool {
	if err == nil {
		return false
	}

	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) {
		if cmdErr.Code == 48 {
			return true
		}

		if strings.Contains(cmdErr.Message, "already exists") {
			return true
		}
	}

	msg := err.Error()

	return strings.Contains(msg, "NamespaceExists") || strings.Contains(msg, "already exists")
}
