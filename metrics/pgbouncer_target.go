package metrics

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/metrics/pgbouncer"
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
)

const TargetTypePgbouncer TargetType = "pgbouncer"

// pgbouncerDSN returns the DSN of the PgBouncer admin console. Only client-side options are set:
// PgBouncer rejects the startup parameters it doesn't track (e.g. statement_timeout) unless they're
// listed in ignore_startup_parameters. binary_parameters makes lib/pq never prepare statements
// (the admin console doesn't support the extended query protocol), although the collector only runs
// queries without arguments, which lib/pq sends with the simple query protocol anyway.
func pgbouncerDSN(credentials Credentials, addr, sslmode string, connectTimeout time.Duration) string {
	query := url.Values{}
	query.Set("connect_timeout", strconv.Itoa(max(1, int(connectTimeout.Seconds()))))
	query.Set("binary_parameters", "yes")
	query.Set("sslmode", sslmode)
	query.Set("application_name", "coroot-cluster-agent")
	u := url.URL{
		Scheme:   "postgresql",
		User:     url.UserPassword(credentials.Username, credentials.Password),
		Host:     addr,
		Path:     "/pgbouncer",
		RawQuery: query.Encode(),
	}
	return u.String()
}

func (t *Target) newPgbouncerCollector(credentials Credentials, tlsCreds common.TLSCredentials, collectTimeout time.Duration) (prometheus.Collector, func(), error) {
	sslmode := t.Params["sslmode"]
	pqTLSName := ""
	if tlsCreds.CA != "" || (tlsCreds.Cert != "" && tlsCreds.Key != "") {
		cfg, err := common.DatabaseTLSConfig(tlsCreds, false)
		if err != nil {
			return nil, nil, err
		}
		pqTLSName = t.tlsConfigName()
		if err = pq.RegisterTLSConfig(pqTLSName, cfg); err != nil {
			return nil, nil, err
		}
		sslmode = "pqgo-" + pqTLSName
	}
	if sslmode == "" {
		sslmode = "disable"
	}
	unregisterTLS := func() {
		if pqTLSName != "" {
			_ = pq.RegisterTLSConfig(pqTLSName, nil)
		}
	}
	collector, err := pgbouncer.New(pgbouncerDSN(credentials, t.Addr, sslmode, collectTimeout), collectTimeout, t.logger)
	if err != nil {
		unregisterTLS()
		return nil, nil, fmt.Errorf("pgbouncer: %w", err)
	}
	return collector, func() {
		_ = collector.Close()
		unregisterTLS()
	}, nil
}
