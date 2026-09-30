package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/coroot/coroot-cluster-agent/common"
)

const (
	defaultMaxTopics         = 1000
	defaultMaxConsumerGroups = 1000

	ClusterMetricsAll          = "all"
	ClusterMetricsLowestBroker = "lowest-broker"
)

// Options are the per-target settings, parsed from the target params
// (the "params" of a static config entry or the coroot.com/kafka-scrape-param-* pod annotations).
type Options struct {
	Seeds []string // bootstrap brokers: the target address followed by params["brokers"]

	SASLMechanism string // "", plain, scram-sha-256, scram-sha-512, aws-msk-iam
	TLS           *tls.Config

	Topics         *Filter
	ConsumerGroups *Filter

	IncludeInternalTopics bool
	PerPartition          bool
	MaxTopics             int
	MaxConsumerGroups     int

	// ClusterMetrics controls which target reports the cluster-wide metrics when several targets
	// point to the same cluster (e.g. every broker pod is annotated): "all" (default) or
	// "lowest-broker" (only the target whose address is the broker with the lowest node ID).
	ClusterMetrics string
}

// Filter matches names against optional include and exclude regular expressions (anchored).
type Filter struct {
	include, exclude *regexp.Regexp
}

func NewFilter(include, exclude string) (*Filter, error) {
	f := &Filter{}
	var err error
	if include != "" {
		if f.include, err = regexp.Compile("^(?:" + include + ")$"); err != nil {
			return nil, fmt.Errorf("invalid include regexp: %w", err)
		}
	}
	if exclude != "" {
		if f.exclude, err = regexp.Compile("^(?:" + exclude + ")$"); err != nil {
			return nil, fmt.Errorf("invalid exclude regexp: %w", err)
		}
	}
	return f, nil
}

func (f *Filter) Match(name string) bool {
	if f == nil {
		return true
	}
	if f.include != nil && !f.include.MatchString(name) {
		return false
	}
	if f.exclude != nil && f.exclude.MatchString(name) {
		return false
	}
	return true
}

// ParseOptions builds the options of a target. Credentials are only used to validate the SASL settings;
// they're never included in the returned errors.
func ParseOptions(addr, username string, tlsCreds common.TLSCredentials, params map[string]string) (*Options, error) {
	o := &Options{
		Seeds:             []string{addr},
		MaxTopics:         defaultMaxTopics,
		MaxConsumerGroups: defaultMaxConsumerGroups,
		ClusterMetrics:    ClusterMetricsAll,
	}
	for _, b := range strings.Split(params["brokers"], ",") {
		b = strings.TrimSpace(b)
		if b == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(b); err != nil {
			return nil, fmt.Errorf("invalid broker address %q in params.brokers (expected host:port)", b)
		}
		o.Seeds = append(o.Seeds, b)
	}

	switch m := strings.ToLower(strings.TrimSpace(params["sasl"])); m {
	case "", "none":
		if username != "" {
			o.SASLMechanism = "plain"
		}
	case "plain", "scram-sha-256", "scram-sha-512":
		if username == "" {
			return nil, fmt.Errorf("sasl %s requires credentials", m)
		}
		o.SASLMechanism = m
	case "aws-msk-iam":
		o.SASLMechanism = m
	default:
		return nil, fmt.Errorf("unsupported sasl mechanism %q (supported: plain, scram-sha-256, scram-sha-512, aws-msk-iam)", m)
	}

	var err error
	if o.TLS, err = tlsConfig(tlsCreds, params); err != nil {
		return nil, err
	}
	if o.Topics, err = NewFilter(params["topics"], params["excludeTopics"]); err != nil {
		return nil, fmt.Errorf("topics: %w", err)
	}
	if o.ConsumerGroups, err = NewFilter(params["consumerGroups"], params["excludeConsumerGroups"]); err != nil {
		return nil, fmt.Errorf("consumerGroups: %w", err)
	}
	if o.IncludeInternalTopics, err = parseBool(params, "includeInternalTopics"); err != nil {
		return nil, err
	}
	if o.PerPartition, err = parseBool(params, "perPartitionMetrics"); err != nil {
		return nil, err
	}
	if o.MaxTopics, err = parseInt(params, "maxTopics", defaultMaxTopics); err != nil {
		return nil, err
	}
	if o.MaxConsumerGroups, err = parseInt(params, "maxConsumerGroups", defaultMaxConsumerGroups); err != nil {
		return nil, err
	}
	switch v := params["clusterMetrics"]; v {
	case "", ClusterMetricsAll:
	case ClusterMetricsLowestBroker:
		o.ClusterMetrics = v
	default:
		return nil, fmt.Errorf("invalid clusterMetrics %q (expected %s or %s)", v, ClusterMetricsAll, ClusterMetricsLowestBroker)
	}
	return o, nil
}

func parseBool(params map[string]string, key string) (bool, error) {
	v := params[key]
	if v == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("invalid %s %q: expected true or false", key, v)
	}
	return b, nil
}

func parseInt(params map[string]string, key string, def int) (int, error) {
	v := params[key]
	if v == "" {
		return def, nil
	}
	i, err := strconv.Atoi(v)
	if err != nil || i < 0 {
		return 0, fmt.Errorf("invalid %s %q: expected a non-negative integer", key, v)
	}
	return i, nil
}

// tlsConfig returns nil if TLS is not enabled. TLS is enabled by params["tls"] ("true" or "skip-verify"),
// or by TLS credentials: from a Kubernetes secret (tlsCreds) or, for static targets,
// from the files referenced by params tlsCaFile/tlsCertFile/tlsKeyFile.
func tlsConfig(tlsCreds common.TLSCredentials, params map[string]string) (*tls.Config, error) {
	mode := params["tls"]
	switch mode {
	case "", "true", "false", "skip-verify":
	default:
		return nil, fmt.Errorf("invalid tls %q (expected true, false or skip-verify)", mode)
	}
	if mode == "false" {
		return nil, nil
	}
	for _, f := range []struct {
		param string
		dst   *string
	}{{"tlsCaFile", &tlsCreds.CA}, {"tlsCertFile", &tlsCreds.Cert}, {"tlsKeyFile", &tlsCreds.Key}} {
		path := params[f.param]
		if path == "" || *f.dst != "" {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", f.param, err)
		}
		*f.dst = string(data)
	}
	if (tlsCreds.Cert != "") != (tlsCreds.Key != "") {
		return nil, fmt.Errorf("the TLS client certificate and key must be set together")
	}
	hasCreds := tlsCreds.CA != "" || tlsCreds.Cert != ""
	if mode == "" && !hasCreds {
		return nil, nil
	}
	skipVerify := mode == "skip-verify"
	if skipVerify || tlsCreds.CA != "" {
		// with a custom CA the chain is verified, but not the hostname: targets are usually addressed by IP
		return common.DatabaseTLSConfig(tlsCreds, skipVerify)
	}
	cfg, err := common.DatabaseTLSConfig(tlsCreds, false) // loads the client certificate, if any
	if err != nil {
		return nil, err
	}
	// System roots. Kafka clients dial the bootstrap address (usually an IP here) and then the advertised
	// broker hostnames. Verify the chain always, and the hostname whenever a hostname was dialed.
	cfg.InsecureSkipVerify = true
	cfg.VerifyConnection = verifySystemRoots
	return cfg, nil
}

func verifySystemRoots(cs tls.ConnectionState) error {
	if len(cs.PeerCertificates) == 0 {
		return fmt.Errorf("no server certificate")
	}
	opts := x509.VerifyOptions{Intermediates: x509.NewCertPool()}
	if cs.ServerName != "" && net.ParseIP(cs.ServerName) == nil {
		opts.DNSName = cs.ServerName
	}
	for _, c := range cs.PeerCertificates[1:] {
		opts.Intermediates.AddCert(c)
	}
	_, err := cs.PeerCertificates[0].Verify(opts)
	return err
}
