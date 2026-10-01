package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"

	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrorReasonTLS is reported for TLS handshake and certificate verification failures.
const ErrorReasonTLS = "tls"

// errorReason maps an error to one of a small fixed set of reasons suitable for use
// as a metric label value. The full error message must be logged separately.
func errorReason(err error) string {
	if err == nil {
		return ""
	}
	var authErr *kadm.AuthError
	if errors.As(err, &authErr) {
		return dbtracker.ErrorReasonPermission
	}
	var kErr *kerr.Error
	if errors.As(err, &kErr) {
		switch kErr {
		case kerr.SaslAuthenticationFailed, kerr.IllegalSaslState, kerr.UnsupportedSaslMechanism:
			return dbtracker.ErrorReasonAuth
		case kerr.TopicAuthorizationFailed, kerr.GroupAuthorizationFailed, kerr.ClusterAuthorizationFailed,
			kerr.TransactionalIDAuthorizationFailed, kerr.DelegationTokenAuthorizationFailed:
			return dbtracker.ErrorReasonPermission
		case kerr.UnknownTopicOrPartition, kerr.GroupIDNotFound, kerr.UnknownTopicID:
			return dbtracker.ErrorReasonNotFound
		case kerr.RequestTimedOut:
			return dbtracker.ErrorReasonTimeout
		case kerr.CoordinatorNotAvailable, kerr.NotCoordinator, kerr.CoordinatorLoadInProgress,
			kerr.LeaderNotAvailable, kerr.NotLeaderForPartition, kerr.BrokerNotAvailable, kerr.NetworkException:
			return dbtracker.ErrorReasonConnection
		}
		return dbtracker.ErrorReasonUnknown
	}
	var (
		unknownAuthority x509.UnknownAuthorityError
		hostnameErr      x509.HostnameError
		certInvalid      x509.CertificateInvalidError
		recordHeaderErr  tls.RecordHeaderError
		certVerifyErr    *tls.CertificateVerificationError
		alertErr         tls.AlertError
	)
	if errors.As(err, &unknownAuthority) || errors.As(err, &hostnameErr) || errors.As(err, &certInvalid) ||
		errors.As(err, &recordHeaderErr) || errors.As(err, &certVerifyErr) || errors.As(err, &alertErr) {
		return ErrorReasonTLS
	}
	if errors.Is(err, kgo.ErrClientClosed) {
		return dbtracker.ErrorReasonConnection
	}
	var brokerErr *kgo.ErrFirstReadEOF
	if errors.As(err, &brokerErr) { // typically a TLS/SASL mismatch: the broker closed the connection right away
		return dbtracker.ErrorReasonConnection
	}
	if r := dbtracker.GenericErrorReason(err); r != "" {
		return r
	}
	return dbtracker.ErrorReasonUnknown
}
