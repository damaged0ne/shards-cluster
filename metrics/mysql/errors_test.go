package mysql

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func Test_errorReason(t *testing.T) {
	for _, tc := range []struct {
		err    error
		reason string
	}{
		{context.DeadlineExceeded, dbtracker.ErrorReasonTimeout},
		{&mysql.MySQLError{Number: 1045, Message: "Access denied for user 'foo'@'10.1.2.3' (using password: YES)"}, dbtracker.ErrorReasonAuth},
		{&mysql.MySQLError{Number: 1142, Message: "SELECT command denied to user 'foo'@'10.1.2.3' for table 'error_log'"}, dbtracker.ErrorReasonPermission},
		{&mysql.MySQLError{Number: 1227, Message: "Access denied; you need (at least one of) the PROCESS privilege(s)"}, dbtracker.ErrorReasonPermission},
		{&mysql.MySQLError{Number: 1146, Message: "Table 'performance_schema.data_lock_waits' doesn't exist"}, dbtracker.ErrorReasonNotFound},
		{&mysql.MySQLError{Number: 3024, Message: "Query execution was interrupted, maximum statement execution time exceeded"}, dbtracker.ErrorReasonTimeout},
		{&mysql.MySQLError{Number: 1040, Message: "Too many connections"}, dbtracker.ErrorReasonConnection},
		{&mysql.MySQLError{Number: 1064, Message: "You have an error in your SQL syntax"}, dbtracker.ErrorReasonUnknown},
		{fmt.Errorf("wrapped: %w", mysql.ErrInvalidConn), dbtracker.ErrorReasonConnection},
		{errors.New("no File_size column in SHOW BINARY LOGS output"), dbtracker.ErrorReasonUnknown},
	} {
		assert.Equal(t, tc.reason, errorReason(tc.err), tc.err.Error())
	}
	assert.Equal(t, "", errorReason(nil))
}

func Test_variablesToText(t *testing.T) {
	values := map[string]string{
		"max_connections":                          "151",
		"key_buffer_size":                          "8388608",
		"authentication_ldap_simple_bind_root_pwd": "s3cr3t",
		"replication_sender_password":              "s3cr3t",
		"ssl_key":                                  "/etc/mysql/s3cr3t.pem",
		"init_connect":                             "",
	}
	names := []string{"max_connections", "ssl_key", "key_buffer_size", "authentication_ldap_simple_bind_root_pwd", "replication_sender_password", "init_connect"}
	got := variablesToText(names, values)
	assert.NotContains(t, got, "s3cr3t")
	assert.Equal(t, "authentication_ldap_simple_bind_root_pwd = <redacted>\n"+
		"init_connect = \n"+
		"key_buffer_size = 8388608\n"+
		"max_connections = 151\n"+
		"replication_sender_password = <redacted>\n"+
		"ssl_key = <redacted>\n", got)
	assert.Equal(t, "max_connections", names[0], "the input slice must not be reordered")
}
