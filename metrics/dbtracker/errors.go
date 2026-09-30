package dbtracker

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"regexp"
	"strings"
	"syscall"
)

// Scrape error reasons. Errors are exported as metric label values only through
// one of these constants to keep the label cardinality bounded; the full error
// text belongs in the logs.
const (
	ErrorReasonTimeout    = "timeout"
	ErrorReasonAuth       = "auth"
	ErrorReasonConnection = "connection"
	ErrorReasonPermission = "permission"
	ErrorReasonNotFound   = "not_found"
	ErrorReasonUnknown    = "unknown"
)

// GenericErrorReason classifies driver-independent errors (context, network, I/O).
// It returns "" if the error isn't recognized, so drivers can apply their own rules.
func GenericErrorReason(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, syscall.ETIMEDOUT) {
		return ErrorReasonTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ErrorReasonTimeout
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) {
		return ErrorReasonConnection
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return ErrorReasonConnection
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return ErrorReasonConnection
	}
	return ""
}

var sensitiveSettingNameRe = regexp.MustCompile(`(?i)(password|passwd|pwd|secret|passphrase|key|token|credential)`)

// IsSensitiveSettingName reports whether a server setting/variable name suggests that
// its value may hold a secret (or a path/command that may embed one) and must not be
// shipped anywhere verbatim. It errs on the side of redacting.
func IsSensitiveSettingName(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	// MySQL MyISAM key cache tuning knobs: "key" means an index key here.
	if strings.HasPrefix(name, "key_buffer_") || strings.HasPrefix(name, "key_cache_") {
		return false
	}
	return sensitiveSettingNameRe.MatchString(name)
}

const RedactedValue = "<redacted>"
