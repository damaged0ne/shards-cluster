package dbtracker

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_GenericErrorReason(t *testing.T) {
	assert.Equal(t, "", GenericErrorReason(nil))
	assert.Equal(t, ErrorReasonTimeout, GenericErrorReason(context.DeadlineExceeded))
	assert.Equal(t, ErrorReasonTimeout, GenericErrorReason(fmt.Errorf("x: %w", context.Canceled)))
	assert.Equal(t, ErrorReasonConnection, GenericErrorReason(driver.ErrBadConn))
	assert.Equal(t, ErrorReasonConnection, GenericErrorReason(io.EOF))
	assert.Equal(t, ErrorReasonConnection, GenericErrorReason(&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}))
	assert.Equal(t, ErrorReasonConnection, GenericErrorReason(&net.DNSError{Err: "no such host", Name: "db"}))
	assert.Equal(t, "", GenericErrorReason(errors.New("something else")))
}

func Test_IsSensitiveSettingName(t *testing.T) {
	for _, name := range []string{"password_encryption", "ssl_passphrase_command", "ssl_key_file", "client_secret", "authentication_ldap_sasl_bind_root_pwd", "API_TOKEN"} {
		assert.True(t, IsSensitiveSettingName(name), name)
	}
	for _, name := range []string{"max_connections", "shared_buffers", "tcp_keepalives_idle", "key_buffer_size", "key_cache_block_size", "work_mem"} {
		assert.False(t, IsSensitiveSettingName(name), name)
	}
}
