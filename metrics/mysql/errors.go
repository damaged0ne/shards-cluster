package mysql

import (
	"errors"

	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/go-sql-driver/mysql"
)

// errorReason maps an error to one of a small fixed set of reasons suitable for use
// as a metric label value. The full error message must be logged separately.
func errorReason(err error) string {
	if err == nil {
		return ""
	}
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		switch myErr.Number {
		case 1045, 1698, 1251, 1862, 3159: // access denied (bad credentials), auth plugin issues, expired password, insecure transport required
			return dbtracker.ErrorReasonAuth
		case 1044, 1142, 1143, 1227, 1370, 3530: // DB/table/column/routine access denied, missing privilege
			return dbtracker.ErrorReasonPermission
		case 1146, 1049, 1109, 1193: // no such table, unknown database, unknown table, unknown system variable
			return dbtracker.ErrorReasonNotFound
		case 3024, 1205, 1317, 3740: // max_execution_time exceeded, lock wait timeout, query interrupted, statement timeout
			return dbtracker.ErrorReasonTimeout
		case 1040, 1053, 1129, 1130, 1152, 1153, 1158, 1159, 1160, 1161, 2002, 2003, 2006, 2013: // too many connections, shutdown, host blocked/not allowed, aborted/packet errors, gone away
			return dbtracker.ErrorReasonConnection
		}
		return dbtracker.ErrorReasonUnknown
	}
	if errors.Is(err, mysql.ErrInvalidConn) || errors.Is(err, mysql.ErrMalformPkt) || errors.Is(err, mysql.ErrPktSync) || errors.Is(err, mysql.ErrBusyBuffer) {
		return dbtracker.ErrorReasonConnection
	}
	if r := dbtracker.GenericErrorReason(err); r != "" {
		return r
	}
	return dbtracker.ErrorReasonUnknown
}
