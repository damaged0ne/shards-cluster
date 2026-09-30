package clickhouse

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"regexp"
	"strconv"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
)

const (
	errorReasonTLS           = "tls"
	errorReasonResourceLimit = "resource_limit"
)

// the HTTP interface returns exceptions as text: "Code: 516. DB::Exception: ..." or "code: 516, message: ..."
var reExceptionCode = regexp.MustCompile(`(?i)\bcode:\s*(\d+)`)

// errorReason maps an error to one of a small fixed set of reasons suitable for use as a metric label value.
// The full error message (which can contain queries, addresses, etc.) must be logged separately.
func errorReason(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, ch.ErrAcquireConnTimeout) {
		return dbtracker.ErrorReasonTimeout
	}
	if r := tlsErrorReason(err); r != "" {
		return r
	}
	code := -1
	var ex *ch.Exception
	if errors.As(err, &ex) {
		code = int(ex.Code)
	} else if m := reExceptionCode.FindStringSubmatch(err.Error()); m != nil {
		code, _ = strconv.Atoi(m[1])
	}
	switch code {
	case -1:
	case 192, 193, 194, 516: // UNKNOWN_USER, WRONG_PASSWORD, REQUIRED_PASSWORD, AUTHENTICATION_FAILED
		return dbtracker.ErrorReasonAuth
	case 164, 291, 497: // READONLY, DATABASE_ACCESS_DENIED, ACCESS_DENIED
		return dbtracker.ErrorReasonPermission
	case 46, 47, 60, 81: // UNKNOWN_FUNCTION, UNKNOWN_IDENTIFIER, UNKNOWN_TABLE, UNKNOWN_DATABASE
		return dbtracker.ErrorReasonNotFound
	case 159, 209, 394: // TIMEOUT_EXCEEDED, SOCKET_TIMEOUT, QUERY_WAS_CANCELLED
		return dbtracker.ErrorReasonTimeout
	case 210: // NETWORK_ERROR
		return dbtracker.ErrorReasonConnection
	case 202, 241: // TOO_MANY_SIMULTANEOUS_QUERIES, MEMORY_LIMIT_EXCEEDED
		return errorReasonResourceLimit
	default:
		return dbtracker.ErrorReasonUnknown
	}
	if r := dbtracker.GenericErrorReason(err); r != "" {
		return r
	}
	return dbtracker.ErrorReasonUnknown
}

func tlsErrorReason(err error) string {
	var (
		unknownAuthority x509.UnknownAuthorityError
		invalidCert      x509.CertificateInvalidError
		hostname         x509.HostnameError
		recordHeader     tls.RecordHeaderError
		certVerification *tls.CertificateVerificationError
	)
	if errors.As(err, &unknownAuthority) || errors.As(err, &invalidCert) || errors.As(err, &hostname) ||
		errors.As(err, &recordHeader) || errors.As(err, &certVerification) {
		return errorReasonTLS
	}
	return ""
}
