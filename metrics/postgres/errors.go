package postgres

import (
	"errors"
	"strings"

	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/lib/pq"
)

// errorReason maps an error to one of a small fixed set of reasons suitable for use
// as a metric label value. The full error message must be logged separately.
func errorReason(err error) string {
	if err == nil {
		return ""
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		code := string(pqErr.Code)
		switch {
		case code == "57014": // query_canceled (statement_timeout)
			return dbtracker.ErrorReasonTimeout
		case strings.HasPrefix(code, "28"): // invalid_authorization_specification, invalid_password
			return dbtracker.ErrorReasonAuth
		case strings.HasPrefix(code, "08"), strings.HasPrefix(code, "57P"): // connection_exception, admin_shutdown, cannot_connect_now, ...
			return dbtracker.ErrorReasonConnection
		case code == "42501": // insufficient_privilege
			return dbtracker.ErrorReasonPermission
		case code == "42P01", code == "42883", code == "3D000", code == "58P01": // undefined_table, undefined_function, invalid_catalog_name, undefined_file
			return dbtracker.ErrorReasonNotFound
		}
		return dbtracker.ErrorReasonUnknown
	}
	if r := dbtracker.GenericErrorReason(err); r != "" {
		return r
	}
	return dbtracker.ErrorReasonUnknown
}
