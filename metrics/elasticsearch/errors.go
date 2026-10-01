package elasticsearch

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"

	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
)

const (
	errorReasonTLS           = "tls"
	errorReasonServerError   = "server_error"
	errorReasonBadResponse   = "bad_response"
	errorReasonResourceLimit = "resource_limit"
)

type httpStatusError struct {
	path string
	code int
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("GET %s: unexpected status %d", e.path, e.code)
}

type decodeError struct {
	path string
	err  error
}

func (e *decodeError) Error() string {
	return fmt.Sprintf("GET %s: failed to decode the response: %s", e.path, e.err)
}

func (e *decodeError) Unwrap() error {
	return e.err
}

// errorReason maps an error to one of a small fixed set of reasons suitable for use as a metric label value.
// The full error message must be logged separately.
func errorReason(err error) string {
	if err == nil {
		return ""
	}
	var se *httpStatusError
	if errors.As(err, &se) {
		switch {
		case se.code == http.StatusUnauthorized:
			return dbtracker.ErrorReasonAuth
		case se.code == http.StatusForbidden:
			return dbtracker.ErrorReasonPermission
		case se.code == http.StatusNotFound:
			return dbtracker.ErrorReasonNotFound
		case se.code == http.StatusTooManyRequests:
			return errorReasonResourceLimit
		case se.code == http.StatusRequestTimeout || se.code == http.StatusGatewayTimeout:
			return dbtracker.ErrorReasonTimeout
		case se.code >= 500:
			return errorReasonServerError
		}
		return dbtracker.ErrorReasonUnknown
	}
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
	if r := dbtracker.GenericErrorReason(err); r != "" {
		return r
	}
	var de *decodeError
	if errors.As(err, &de) {
		return errorReasonBadResponse
	}
	return dbtracker.ErrorReasonUnknown
}
