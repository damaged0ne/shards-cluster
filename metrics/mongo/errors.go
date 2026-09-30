package mongo

import (
	"context"
	"errors"
	"net"
	"regexp"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/mongo/driver/auth"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
)

const (
	codeUnauthorized         = 13
	codeAuthenticationFailed = 18
)

var reCodeName = regexp.MustCompile(`^[A-Za-z0-9]{1,64}$`)

// errorReason maps an error to one of a small fixed set of values suitable for a metric label.
// Raw error messages contain addresses, topology descriptions, command documents, etc. and would
// produce unbounded label cardinality; the full error is logged instead.
func errorReason(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, errCollectorClosed) {
		return "closed"
	}
	if isAuthError(err) {
		return "auth"
	}
	var sse topology.ServerSelectionError
	if errors.As(err, &sse) {
		for _, s := range sse.Desc.Servers {
			if s.LastError != nil && isAuthError(s.LastError) {
				return "auth"
			}
		}
		return "unreachable"
	}
	if errors.Is(err, mongo.ErrClientDisconnected) {
		return "disconnected"
	}
	var se mongo.ServerError
	if errors.As(err, &se) {
		switch {
		case se.HasErrorCode(codeUnauthorized):
			return "unauthorized"
		case se.HasErrorCode(codeAuthenticationFailed):
			return "auth"
		}
	}
	if mongo.IsNetworkError(err) {
		return "unreachable"
	}
	if mongo.IsTimeout(err) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var ce mongo.CommandError
	if errors.As(err, &ce) && reCodeName.MatchString(ce.Name) {
		return "command error: " + ce.Name
	}
	var ne net.Error
	if errors.As(err, &ne) {
		if ne.Timeout() {
			return "timeout"
		}
		return "unreachable"
	}
	return "unknown"
}

func isAuthError(err error) bool {
	var ae *auth.Error
	if errors.As(err, &ae) {
		return true
	}
	var se mongo.ServerError
	return errors.As(err, &se) && se.HasErrorCode(codeAuthenticationFailed)
}
