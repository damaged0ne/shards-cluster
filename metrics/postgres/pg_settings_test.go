package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func Test_redactConninfo(t *testing.T) {
	for _, tc := range []struct{ in, out string }{
		{
			in:  "user=replicator password=s3cr3t host=10.0.0.1 port=5432 sslmode=prefer",
			out: "user=replicator password=<redacted> host=10.0.0.1 port=5432 sslmode=prefer",
		},
		{
			in:  "host=pg-0 password = 'quoted secret with \\' escape' port=5433",
			out: "host=pg-0 password = <redacted> port=5433",
		},
		{
			in:  "host=pg-0 sslpassword=keypass PASSWORD=Up application_name=walreceiver",
			out: "host=pg-0 sslpassword=<redacted> PASSWORD=<redacted> application_name=walreceiver",
		},
		{
			in:  "host=pg-0 port=5432 user=replicator",
			out: "host=pg-0 port=5432 user=replicator",
		},
		{
			in:  "postgresql://replicator:s3cr3t@10.0.0.1:5432/postgres?sslmode=require&password=other",
			out: "postgresql://replicator:redacted@10.0.0.1:5432/postgres?password=redacted&sslmode=require",
		},
		{
			in:  "postgres://replicator@10.0.0.1:5432",
			out: "postgres://replicator@10.0.0.1:5432",
		},
	} {
		got := redactConninfo(tc.in)
		assert.Equal(t, tc.out, got, tc.in)
		assert.NotContains(t, got, "s3cr3t")
		assert.NotContains(t, got, "keypass")
	}
	host, port, err := (&replicationStatus{primaryConnectionInfo: redactConninfo("user=r password=x host=h1 port=6432")}).primaryHostPort()
	assert.NoError(t, err)
	assert.Equal(t, "h1", host)
	assert.Equal(t, "6432", port)
}

func Test_settingsToText_Redaction(t *testing.T) {
	settings := []Setting{
		{Name: "primary_conninfo", RawValue: "host=10.0.0.1 port=5432 user=replicator password=s3cr3t", Source: "configuration file", Context: "sighup"},
		{Name: "archive_command", RawValue: "aws s3 cp %p s3://bucket/%f --secret=s3cr3t", Source: "configuration file", Context: "sighup"},
		{Name: "restore_command", RawValue: "curl -u admin:s3cr3t https://backup/%f", Source: "configuration file", Context: "postmaster"},
		{Name: "archive_cleanup_command", RawValue: "cleanup --token=s3cr3t", Source: "configuration file", Context: "sighup"},
		{Name: "ssl_passphrase_command", RawValue: "echo s3cr3t", Source: "configuration file", Context: "sighup"},
		{Name: "ssl_key_file", RawValue: "/etc/ssl/private/s3cr3t.key", Source: "configuration file", Context: "sighup"},
		{Name: "my.api_secret", RawValue: "s3cr3t", Source: "configuration file", Context: "user"},
		{Name: "archive_mode", RawValue: "on", Source: "configuration file", Context: "postmaster"},
		{Name: "max_connections", RawValue: "100", Source: "configuration file", Context: "postmaster"},
		{Name: "tcp_keepalives_idle", RawValue: "60", Source: "configuration file", Context: "user"},
		{Name: "recovery_end_command", RawValue: "", Source: "default", Context: "sighup"},
	}
	got := settingsToText(settings)
	assert.NotContains(t, got, "s3cr3t")
	expected := strings.Join([]string{
		"primary_conninfo = host=10.0.0.1 port=5432 user=replicator password=<redacted>",
		"archive_command = <redacted>",
		"restore_command = <redacted>",
		"archive_cleanup_command = <redacted>",
		"ssl_passphrase_command = <redacted>",
		"ssl_key_file = <redacted>",
		"my.api_secret = <redacted>",
		"archive_mode = on",
		"max_connections = 100",
		"tcp_keepalives_idle = 60",
		"recovery_end_command = ",
	}, "\n") + "\n"
	assert.Equal(t, expected, got)
}

func Test_errorReason(t *testing.T) {
	for _, tc := range []struct {
		err    error
		reason string
	}{
		{context.DeadlineExceeded, dbtracker.ErrorReasonTimeout},
		{fmt.Errorf("query: %w", context.DeadlineExceeded), dbtracker.ErrorReasonTimeout},
		{&pq.Error{Code: "57014", Message: "canceling statement due to statement timeout"}, dbtracker.ErrorReasonTimeout},
		{&pq.Error{Code: "28P01", Message: `password authentication failed for user "foo"`}, dbtracker.ErrorReasonAuth},
		{&pq.Error{Code: "28000", Message: "no pg_hba.conf entry"}, dbtracker.ErrorReasonAuth},
		{&pq.Error{Code: "42501", Message: "permission denied for table foo"}, dbtracker.ErrorReasonPermission},
		{&pq.Error{Code: "42P01", Message: `relation "pg_stat_statements" does not exist`}, dbtracker.ErrorReasonNotFound},
		{&pq.Error{Code: "57P03", Message: "the database system is starting up"}, dbtracker.ErrorReasonConnection},
		{&pq.Error{Code: "08006", Message: "connection failure"}, dbtracker.ErrorReasonConnection},
		{&pq.Error{Code: "22P02", Message: "invalid input syntax"}, dbtracker.ErrorReasonUnknown},
		{&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, dbtracker.ErrorReasonConnection},
		{errors.New("postgres version 9.0.0 is not supported"), dbtracker.ErrorReasonUnknown},
	} {
		assert.Equal(t, tc.reason, errorReason(tc.err), tc.err.Error())
	}
	assert.Equal(t, "", errorReason(nil))
}
