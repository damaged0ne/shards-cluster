package metrics

import (
	"fmt"
	"net"
	"strconv"

	cfgpkg "github.com/coroot/coroot-cluster-agent/config"
	promCommon "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/model/relabel"
)

// Services exposing their metrics natively in the Prometheus format. They aren't collected by an exporter
// running in the agent: their /metrics endpoints are scraped by the Prometheus scrape manager, like the agent's
// own /metrics, and only the allowlisted metrics are kept to keep the cardinality bounded.
const (
	TargetTypeRabbitmq TargetType = "rabbitmq"
	TargetTypeEtcd     TargetType = "etcd"
)

// NativeScrapeSampleLimit is a safety net on top of the metric allowlists:
// a scrape exceeding it is rejected as a whole (and reported as failed by the up metric).
const NativeScrapeSampleLimit = 50000

type nativeService struct {
	defaultPort string
	// allowlist is a regular expression matching the names of the metrics to keep (anchored).
	allowlist string
}

var nativeServices = map[TargetType]nativeService{
	// The rabbitmq_prometheus plugin (enabled by default since RabbitMQ 3.8), aggregated metrics on /metrics.
	// The per-object metrics (/metrics/per-object, /metrics/detailed) are not recommended:
	// their cardinality grows with the number of queues, connections and channels.
	TargetTypeRabbitmq: {
		defaultPort: "15692",
		allowlist: `rabbitmq_(` +
			`build_info|identity_info|erlang_uptime_seconds|` +
			`alarms_.+|` +
			`process_resident_memory_bytes|resident_memory_limit_bytes|` +
			`disk_space_available_bytes|disk_space_available_limit_bytes|` +
			`process_open_fds|process_max_fds|process_open_tcp_sockets|process_max_tcp_sockets|` +
			`connections|connections_opened_total|connections_closed_total|` +
			`channels|channels_opened_total|channels_closed_total|` +
			`consumers|queues|queues_declared_total|queues_created_total|queues_deleted_total|` +
			`queue_messages|queue_messages_ready|queue_messages_unacked|queue_consumers|` +
			`queue_messages_published_total|` +
			`global_messages_.+_total|global_publishers|global_consumers|` +
			`unreachable_cluster_peers_count` +
			`)`,
	},
	// etcd serves /metrics on the client URLs (usually TLS with client certificates) and, if --listen-metrics-urls
	// is set (e.g. http://0.0.0.0:2381, kubeadm's default), without authentication.
	TargetTypeEtcd: {
		defaultPort: "2381",
		allowlist: `etcd_server_(has_leader|is_leader|leader_changes_seen_total|` +
			`proposals_(committed|applied|pending|failed)_total|` +
			`heartbeat_send_failures_total|slow_apply_total|slow_read_indexes_total|` +
			`health_success|health_failures|id|version|quota_backend_bytes)|` +
			`etcd_cluster_version|` +
			`etcd_mvcc_db_total_size_in_bytes|etcd_mvcc_db_total_size_in_use_in_bytes|` +
			`etcd_mvcc_(put|delete|txn|range)_total|etcd_debugging_mvcc_keys_total|` +
			`etcd_disk_(wal_fsync|backend_commit)_duration_seconds_(bucket|sum|count)|` +
			`etcd_network_peer_round_trip_time_seconds_(bucket|sum|count)|` +
			`etcd_network_peer_(sent|received)_bytes_total|etcd_network_peer_sent_failures_total|` +
			`etcd_network_client_grpc_(sent|received)_bytes_total|` +
			`grpc_server_handled_total|` +
			`process_(resident_memory_bytes|cpu_seconds_total|open_fds|max_fds|start_time_seconds)`,
	},
}

// IsNativeScrapeType reports whether the targets of the type are scraped via their native /metrics endpoint.
func IsNativeScrapeType(t string) bool {
	_, ok := nativeServices[TargetType(t)]
	return ok
}

// nativeScrapeConfigs returns a scrape config for each statically configured RabbitMQ/etcd instance
// (each can have its own TLS and auth settings) and, in k8s, a scrape config per service type
// for the pods annotated with coroot.com/<type>-scrape: "true".
func nativeScrapeConfigs(databases []cfgpkg.Database, inK8s bool) ([]*config.ScrapeConfig, error) {
	var res []*config.ScrapeConfig
	for i, d := range databases {
		if !IsNativeScrapeType(d.Type) {
			continue
		}
		sc, err := staticNativeScrapeConfig(i, d)
		if err != nil {
			return nil, fmt.Errorf("databases[%d]: %w", i, err)
		}
		res = append(res, sc)
	}
	if inK8s {
		for _, tt := range []TargetType{TargetTypeRabbitmq, TargetTypeEtcd} {
			res = append(res, podNativeScrapeConfig(tt))
		}
	}
	return res, nil
}

func newNativeScrapeConfig(jobName string, allowlist string) (*config.ScrapeConfig, error) {
	re, err := relabel.NewRegexp(allowlist)
	if err != nil {
		return nil, fmt.Errorf("invalid metrics_allowlist: %w", err)
	}
	return &config.ScrapeConfig{
		JobName:                       jobName,
		HonorLabels:                   false,
		AlwaysScrapeClassicHistograms: ptr(true),
		MetricsPath:                   "/metrics",
		Scheme:                        "http",
		EnableCompression:             true,
		SampleLimit:                   NativeScrapeSampleLimit,
		HTTPClientConfig:              promCommon.DefaultHTTPClientConfig,
		MetricRelabelConfigs: []*relabel.Config{
			{
				SourceLabels: model.LabelNames{model.MetricNameLabel},
				Regex:        re,
				Action:       relabel.Keep,
				Separator:    relabel.DefaultRelabelConfig.Separator,
				Replacement:  relabel.DefaultRelabelConfig.Replacement,
			},
		},
	}, nil
}

// staticNativeScrapeConfig builds the scrape config of a statically configured instance. Supported params:
// scheme (http, https), metrics_path, tls_ca_file, tls_cert_file, tls_key_file, tls_server_name,
// tls_insecure_skip_verify (true/false), metrics_allowlist (replaces the default allowlist).
// The credentials are used for HTTP basic auth.
func staticNativeScrapeConfig(idx int, d cfgpkg.Database) (*config.ScrapeConfig, error) {
	tt := TargetType(d.Type)
	svc := nativeServices[tt]
	if d.Host == "" {
		return nil, fmt.Errorf("%s: host is required", tt)
	}
	allowlist := svc.allowlist
	if a := d.Params["metrics_allowlist"]; a != "" {
		allowlist = a
	}
	sc, err := newNativeScrapeConfig(fmt.Sprintf("%s-%d", tt, idx), allowlist)
	if err != nil {
		return nil, err
	}
	if p := d.Params["metrics_path"]; p != "" {
		sc.MetricsPath = p
	}
	tls := promCommon.TLSConfig{
		CAFile:     d.Params["tls_ca_file"],
		CertFile:   d.Params["tls_cert_file"],
		KeyFile:    d.Params["tls_key_file"],
		ServerName: d.Params["tls_server_name"],
	}
	if v := d.Params["tls_insecure_skip_verify"]; v != "" {
		if tls.InsecureSkipVerify, err = strconv.ParseBool(v); err != nil {
			return nil, fmt.Errorf("invalid tls_insecure_skip_verify: %w", err)
		}
	}
	if (tls.CertFile == "") != (tls.KeyFile == "") {
		return nil, fmt.Errorf("tls_cert_file and tls_key_file must be set together")
	}
	sc.HTTPClientConfig.TLSConfig = tls
	switch scheme := d.Params["scheme"]; scheme {
	case "":
		if tls != (promCommon.TLSConfig{}) {
			sc.Scheme = "https"
		}
	case "http", "https":
		sc.Scheme = scheme
	default:
		return nil, fmt.Errorf("invalid scheme: %q", scheme)
	}
	if d.Credentials.Username != "" || d.Credentials.Password != "" {
		sc.HTTPClientConfig.BasicAuth = &promCommon.BasicAuth{
			Username: d.Credentials.Username,
			Password: promCommon.Secret(d.Credentials.Password),
		}
	}
	if err = sc.HTTPClientConfig.Validate(); err != nil {
		return nil, err
	}
	port := d.Port
	if port == "" {
		port = svc.defaultPort
	}
	addr := net.JoinHostPort(d.Host, port)
	sc.ServiceDiscoveryConfigs = discovery.Configs{
		discovery.StaticConfig{&targetgroup.Group{
			Source: sc.JobName,
			Targets: []model.LabelSet{{
				model.AddressLabel: model.LabelValue(addr),
			}},
			Labels: model.LabelSet{
				model.JobLabel: model.LabelValue(tt), // the job name must be unique, the job label needn't
			},
		}},
	}
	return sc, nil
}

// podNativeScrapeConfig discovers the pods annotated with coroot.com/<type>-scrape: "true".
// Optional annotations: coroot.com/<type>-scrape-port, coroot.com/<type>-scrape-scheme (http or https),
// coroot.com/<type>-scrape-metrics-path. TLS client certificates and basic auth need a static config.
func podNativeScrapeConfig(tt TargetType) *config.ScrapeConfig {
	sc, _ := newNativeScrapeConfig(fmt.Sprintf("%s-k8s-pods", tt), nativeServices[tt].allowlist)
	annotation := func(suffix string) model.LabelName {
		return model.LabelName("__meta_kubernetes_pod_annotation_coroot_com_" + string(tt) + "_scrape" + suffix)
	}
	replace := func(source model.LabelName, target, regex, replacement string) *relabel.Config {
		return &relabel.Config{
			SourceLabels: model.LabelNames{source},
			Separator:    relabel.DefaultRelabelConfig.Separator,
			TargetLabel:  target,
			Regex:        relabel.MustNewRegexp(regex),
			Replacement:  replacement,
			Action:       relabel.Replace,
		}
	}
	sc.RelabelConfigs = []*relabel.Config{
		{
			SourceLabels: model.LabelNames{annotation("")},
			Separator:    relabel.DefaultRelabelConfig.Separator,
			Regex:        relabel.MustNewRegexp("true"),
			Replacement:  relabel.DefaultRelabelConfig.Replacement,
			Action:       relabel.Keep,
		},
		{
			SourceLabels: model.LabelNames{"__meta_kubernetes_pod_phase"},
			Separator:    relabel.DefaultRelabelConfig.Separator,
			Regex:        relabel.MustNewRegexp("Pending|Succeeded|Failed|Completed"),
			Replacement:  relabel.DefaultRelabelConfig.Replacement,
			Action:       relabel.Drop,
		},
		{
			SourceLabels: model.LabelNames{"__meta_kubernetes_pod_ip", annotation("_port")},
			Separator:    ";",
			TargetLabel:  model.AddressLabel,
			Regex:        relabel.MustNewRegexp(`(.+);(\d+)`),
			Replacement:  "$1:$2",
			Action:       relabel.Replace,
		},
		{ // no port annotation: the default port of the service
			SourceLabels: model.LabelNames{"__meta_kubernetes_pod_ip", annotation("_port")},
			Separator:    ";",
			TargetLabel:  model.AddressLabel,
			Regex:        relabel.MustNewRegexp(`(.+);`),
			Replacement:  "$1:" + nativeServices[tt].defaultPort,
			Action:       relabel.Replace,
		},
		replace(annotation("_scheme"), model.SchemeLabel, "(https?)", "$1"),
		replace(annotation("_metrics_path"), model.MetricsPathLabel, "(/.*)", "$1"),
		replace("__meta_kubernetes_namespace", "namespace", "(.+)", "$1"),
		replace("__meta_kubernetes_pod_name", "pod", "(.+)", "$1"),
		replace(model.AddressLabel, model.JobLabel, ".*", string(tt)),
	}
	sc.ServiceDiscoveryConfigs = discovery.Configs{k8sPodSDConfig()}
	return sc
}
