package metrics

import (
	"net/url"
	"strings"
	"testing"

	cfgpkg "github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/scrape"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validated(t *testing.T, sc *config.ScrapeConfig) *config.ScrapeConfig {
	t.Helper()
	cfg, err := config.Load("", nil)
	require.NoError(t, err)
	require.NoError(t, sc.Validate(cfg.GlobalConfig))
	return sc
}

func keptMetric(t *testing.T, sc *config.ScrapeConfig, name string) bool {
	t.Helper()
	lb := labels.NewBuilder(labels.FromStrings(model.MetricNameLabel, name, "instance", "x"))
	return relabel.ProcessBuilder(lb, sc.MetricRelabelConfigs...)
}

// scrapeTargetLabels applies the target relabeling of sc, as the scrape manager does.
func scrapeTargetLabels(t *testing.T, sc *config.ScrapeConfig, target, group model.LabelSet) labels.Labels {
	t.Helper()
	lb := labels.NewBuilder(labels.EmptyLabels())
	res, err := scrape.PopulateLabels(lb, sc, target, group)
	require.NoError(t, err)
	return res
}

func TestStaticNativeScrapeConfigRabbitmq(t *testing.T) {
	d := cfgpkg.Database{
		Type:        "rabbitmq",
		Host:        "rabbitmq.example.internal",
		Credentials: cfgpkg.Credentials{Username: "monitoring", Password: "s3cret"},
		Params:      map[string]string{"tls_ca_file": "/etc/ssl/ca.pem"},
	}
	sc, err := staticNativeScrapeConfig(3, d)
	require.NoError(t, err)
	validated(t, sc)
	assert.Equal(t, "rabbitmq-3", sc.JobName)
	assert.Equal(t, "https", sc.Scheme) // TLS options imply https
	assert.Equal(t, "/metrics", sc.MetricsPath)
	assert.False(t, sc.HonorLabels)
	assert.Equal(t, uint(NativeScrapeSampleLimit), sc.SampleLimit)
	assert.Equal(t, "/etc/ssl/ca.pem", sc.HTTPClientConfig.TLSConfig.CAFile)
	require.NotNil(t, sc.HTTPClientConfig.BasicAuth)
	assert.Equal(t, "monitoring", sc.HTTPClientConfig.BasicAuth.Username)
	assert.Equal(t, "s3cret", string(sc.HTTPClientConfig.BasicAuth.Password))

	require.Len(t, sc.ServiceDiscoveryConfigs, 1)
	groups := sc.ServiceDiscoveryConfigs[0].(discovery.StaticConfig)
	require.Len(t, groups, 1)
	lbls := scrapeTargetLabels(t, sc, groups[0].Targets[0], groups[0].Labels)
	assert.Equal(t, "rabbitmq", lbls.Get(model.JobLabel))
	assert.Equal(t, "rabbitmq.example.internal:15692", lbls.Get(model.InstanceLabel))
	assert.Equal(t, "https", lbls.Get(model.SchemeLabel))

	for _, name := range []string{"rabbitmq_queues", "rabbitmq_queue_messages_ready", "rabbitmq_connections", "rabbitmq_global_messages_received_total", "rabbitmq_alarms_memory_used_watermark"} {
		assert.True(t, keptMetric(t, sc, name), name)
	}
	for _, name := range []string{"rabbitmq_detailed_queue_messages", "erlang_vm_memory_bytes_total", "rabbitmq_queues_extra", "xrabbitmq_queues", "telemetry_scrape_duration_seconds_count"} {
		assert.False(t, keptMetric(t, sc, name), name)
	}
}

func TestStaticNativeScrapeConfigEtcd(t *testing.T) {
	d := cfgpkg.Database{
		Type: "etcd",
		Host: "10.0.0.10",
		Port: "2379",
		Params: map[string]string{
			"tls_ca_file":              "/etc/etcd/ca.crt",
			"tls_cert_file":            "/etc/etcd/client.crt",
			"tls_key_file":             "/etc/etcd/client.key",
			"tls_server_name":          "etcd",
			"tls_insecure_skip_verify": "false",
			"metrics_path":             "/custom/metrics",
		},
	}
	sc, err := staticNativeScrapeConfig(0, d)
	require.NoError(t, err)
	validated(t, sc)
	assert.Equal(t, "https", sc.Scheme)
	assert.Equal(t, "/custom/metrics", sc.MetricsPath)
	assert.Nil(t, sc.HTTPClientConfig.BasicAuth)
	assert.Equal(t, "etcd", sc.HTTPClientConfig.TLSConfig.ServerName)
	groups := sc.ServiceDiscoveryConfigs[0].(discovery.StaticConfig)
	lbls := scrapeTargetLabels(t, sc, groups[0].Targets[0], groups[0].Labels)
	assert.Equal(t, "etcd", lbls.Get(model.JobLabel))
	assert.Equal(t, "10.0.0.10:2379", lbls.Get(model.InstanceLabel))

	for _, name := range []string{"etcd_server_has_leader", "etcd_server_leader_changes_seen_total", "etcd_mvcc_db_total_size_in_bytes", "etcd_disk_wal_fsync_duration_seconds_bucket", "grpc_server_handled_total", "process_resident_memory_bytes"} {
		assert.True(t, keptMetric(t, sc, name), name)
	}
	for _, name := range []string{"etcd_debugging_store_reads_total", "grpc_server_handling_seconds_bucket", "go_gc_duration_seconds", "etcd_disk_wal_write_bytes_total"} {
		assert.False(t, keptMetric(t, sc, name), name)
	}
}

func TestStaticNativeScrapeConfigOptions(t *testing.T) {
	sc, err := staticNativeScrapeConfig(0, cfgpkg.Database{Type: "etcd", Host: "etcd.local"})
	require.NoError(t, err)
	assert.Equal(t, "http", sc.Scheme)
	groups := sc.ServiceDiscoveryConfigs[0].(discovery.StaticConfig)
	assert.Equal(t, model.LabelValue("etcd.local:2381"), groups[0].Targets[0][model.AddressLabel])

	sc, err = staticNativeScrapeConfig(0, cfgpkg.Database{Type: "rabbitmq", Host: "mq", Params: map[string]string{
		"metrics_allowlist": "rabbitmq_queues|rabbitmq_detailed_.+",
		"scheme":            "http",
	}})
	require.NoError(t, err)
	validated(t, sc)
	assert.True(t, keptMetric(t, sc, "rabbitmq_detailed_queue_messages"))
	assert.False(t, keptMetric(t, sc, "rabbitmq_connections"))

	for _, d := range []cfgpkg.Database{
		{Type: "rabbitmq"}, // no host
		{Type: "rabbitmq", Host: "mq", Params: map[string]string{"scheme": "ftp"}},
		{Type: "rabbitmq", Host: "mq", Params: map[string]string{"metrics_allowlist": "("}},
		{Type: "etcd", Host: "etcd", Params: map[string]string{"tls_cert_file": "/c.crt"}},
		{Type: "etcd", Host: "etcd", Params: map[string]string{"tls_insecure_skip_verify": "maybe"}},
	} {
		_, err = staticNativeScrapeConfig(0, d)
		assert.Error(t, err, d)
	}
}

func TestPodNativeScrapeConfig(t *testing.T) {
	sc := validated(t, podNativeScrapeConfig(TargetTypeRabbitmq))
	assert.Equal(t, "rabbitmq-k8s-pods", sc.JobName)

	target := func(annotations map[string]string, phase string) model.LabelSet {
		ls := model.LabelSet{
			model.AddressLabel:             "10.1.0.5:5672",
			"__meta_kubernetes_pod_ip":     "10.1.0.5",
			"__meta_kubernetes_pod_name":   "rabbitmq-0",
			"__meta_kubernetes_namespace":  "mq",
			"__meta_kubernetes_pod_phase":  model.LabelValue(phase),
			"__meta_kubernetes_pod_uid":    "uid",
			"__meta_kubernetes_pod_ready":  "true",
			"__meta_kubernetes_pod_labels": "",
		}
		for k, v := range annotations {
			ls[model.LabelName("__meta_kubernetes_pod_annotation_"+strings.NewReplacer(".", "_", "/", "_", "-", "_").Replace(k))] = model.LabelValue(v)
		}
		return ls
	}

	lbls := scrapeTargetLabels(t, sc, target(map[string]string{"coroot.com/rabbitmq-scrape": "true"}, "Running"), nil)
	assert.Equal(t, "10.1.0.5:15692", lbls.Get(model.InstanceLabel))
	assert.Equal(t, "rabbitmq", lbls.Get(model.JobLabel))
	assert.Equal(t, "mq", lbls.Get("namespace"))
	assert.Equal(t, "rabbitmq-0", lbls.Get("pod"))
	assert.Equal(t, "http", lbls.Get(model.SchemeLabel))
	assert.Equal(t, "/metrics", lbls.Get(model.MetricsPathLabel))

	lbls = scrapeTargetLabels(t, sc, target(map[string]string{
		"coroot.com/rabbitmq-scrape":              "true",
		"coroot.com/rabbitmq-scrape-port":         "9419",
		"coroot.com/rabbitmq-scrape-scheme":       "https",
		"coroot.com/rabbitmq-scrape-metrics-path": "/metrics/memory-breakdown",
	}, "Running"), nil)
	assert.Equal(t, "10.1.0.5:9419", lbls.Get(model.InstanceLabel))
	assert.Equal(t, "https", lbls.Get(model.SchemeLabel))
	assert.Equal(t, "/metrics/memory-breakdown", lbls.Get(model.MetricsPathLabel))

	assert.True(t, scrapeTargetLabels(t, sc, target(nil, "Running"), nil).IsEmpty(), "not annotated")
	assert.True(t, scrapeTargetLabels(t, sc, target(map[string]string{"coroot.com/etcd-scrape": "true"}, "Running"), nil).IsEmpty(), "another type")
	assert.True(t, scrapeTargetLabels(t, sc, target(map[string]string{"coroot.com/rabbitmq-scrape": "true"}, "Pending"), nil).IsEmpty(), "pending")

	etcd := validated(t, podNativeScrapeConfig(TargetTypeEtcd))
	lbls = scrapeTargetLabels(t, etcd, target(map[string]string{"coroot.com/etcd-scrape": "true"}, "Running"), nil)
	assert.Equal(t, "10.1.0.5:2381", lbls.Get(model.InstanceLabel))
	assert.Equal(t, "etcd", lbls.Get(model.JobLabel))
}

func TestPromConfig(t *testing.T) {
	endpoint, _ := url.Parse("http://coroot.example.internal:8080/v1/metrics")
	ms := &Metrics{
		endpoint:       endpoint,
		apiKey:         "key",
		listenAddr:     "127.0.0.1:10301",
		scrapeInterval: 15e9,
		scrapeTimeout:  10e9,
		static: &cfgpkg.Static{Databases: []cfgpkg.Database{
			{Type: "postgres", Host: "pg", Port: "5432"},
			{Type: "rabbitmq", Host: "mq", Credentials: cfgpkg.Credentials{Username: "u", Password: "p"}},
			{Type: "etcd", Host: "etcd"},
		}},
	}

	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	cfg, err := ms.promConfig(NewLogger())
	require.NoError(t, err)
	var jobs []string
	for _, sc := range cfg.ScrapeConfigs {
		jobs = append(jobs, sc.JobName)
	}
	assert.Equal(t, []string{jobName, "rabbitmq-1", "etcd-2"}, jobs)
	scfgs, err := cfg.GetScrapeConfigs() // requires a loaded config
	require.NoError(t, err)
	require.Len(t, scfgs, 3)
	assert.Equal(t, "p", string(scfgs[1].HTTPClientConfig.BasicAuth.Password), "the secrets must be preserved")
	assert.Equal(t, model.Duration(15e9), scfgs[1].ScrapeInterval)
	require.Len(t, cfg.RemoteWriteConfigs, 1)
	assert.Equal(t, "key", cfg.RemoteWriteConfigs[0].Headers["X-Api-Key"])

	t.Setenv("KUBERNETES_SERVICE_HOST", "10.96.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	cfg, err = ms.promConfig(NewLogger())
	require.NoError(t, err)
	jobs = jobs[:0]
	for _, sc := range cfg.ScrapeConfigs {
		jobs = append(jobs, sc.JobName)
	}
	assert.Equal(t, []string{jobName, "custom-metrics-k8s-pods", "rabbitmq-1", "etcd-2", "rabbitmq-k8s-pods", "etcd-k8s-pods"}, jobs)
}

func TestResolveDatabasesSkipsNativeTypes(t *testing.T) {
	ms := newTestMetrics()
	targets := ms.resolveDatabases([]cfgpkg.Database{
		{Type: "rabbitmq", Host: "127.0.0.1"},
		{Type: "etcd", Host: "127.0.0.1"},
		{Type: "pgbouncer", Host: "127.0.0.1", Port: "6432"},
	})
	require.Len(t, targets, 1)
	assert.Equal(t, TargetTypePgbouncer, targets[0].Type)
	assert.Equal(t, "127.0.0.1:6432", targets[0].Addr)
}

func TestPgbouncerTargetFromPod(t *testing.T) {
	pod := &k8s.Pod{
		Id: k8s.PodId{Namespace: "db", Name: "pgbouncer-0"},
		IP: "10.1.0.7",
		Annotations: map[string]string{
			"coroot.com/pgbouncer-scrape":                                 "true",
			"coroot.com/pgbouncer-scrape-credentials-secret-name":         "pgbouncer-stats",
			"coroot.com/pgbouncer-scrape-credentials-secret-username-key": "username",
			"coroot.com/pgbouncer-scrape-credentials-secret-password-key": "password",
		},
	}
	tg := TargetFromPod(pod)
	require.NotNil(t, tg)
	assert.Equal(t, TargetTypePgbouncer, tg.Type)
	assert.Equal(t, "10.1.0.7:6432", tg.Addr)
	assert.Equal(t, CredentialsSecret{Namespace: "db", Name: "pgbouncer-stats", UsernameKey: "username", PasswordKey: "password"}, tg.CredentialsSecret)
}

func TestPgbouncerDSN(t *testing.T) {
	dsn := pgbouncerDSN(Credentials{Username: "stats", Password: "p@ss:w/rd"}, "10.1.0.7:6432", "disable", 4e9)
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	assert.Equal(t, "/pgbouncer", u.Path)
	p, _ := u.User.Password()
	assert.Equal(t, "p@ss:w/rd", p)
	q := u.Query()
	assert.Equal(t, "yes", q.Get("binary_parameters"))
	assert.Equal(t, "4", q.Get("connect_timeout"))
	assert.Equal(t, "disable", q.Get("sslmode"))
	assert.False(t, q.Has("statement_timeout"), "pgbouncer rejects unknown startup parameters")
}
