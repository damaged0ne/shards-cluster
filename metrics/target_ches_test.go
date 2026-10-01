package metrics

import (
	"testing"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTargetFromPodClickhouseElasticsearch(t *testing.T) {
	pod := &k8s.Pod{
		Id: k8s.PodId{Namespace: "ns", Name: "ch-0"},
		IP: "10.0.0.5",
		Annotations: map[string]string{
			"coroot.com/clickhouse-scrape":                                 "true",
			"coroot.com/clickhouse-scrape-credentials-secret-name":         "ch-creds",
			"coroot.com/clickhouse-scrape-credentials-secret-username-key": "user",
			"coroot.com/clickhouse-scrape-credentials-secret-password-key": "password",
			"coroot.com/clickhouse-scrape-param-protocol":                  "http",
			"coroot.com/clickhouse-scrape-port":                            "8123",
		},
	}
	tg := TargetFromPod(pod)
	require.NotNil(t, tg)
	assert.Equal(t, TargetTypeClickhouse, tg.Type)
	assert.Equal(t, "10.0.0.5:8123", tg.Addr)
	assert.Equal(t, CredentialsSecret{Namespace: "ns", Name: "ch-creds", UsernameKey: "user", PasswordKey: "password"}, tg.CredentialsSecret)
	assert.Equal(t, map[string]string{"protocol": "http"}, tg.Params)

	pod = &k8s.Pod{
		Id: k8s.PodId{Namespace: "ns", Name: "es-0"},
		IP: "10.0.0.6",
		Annotations: map[string]string{
			"coroot.com/elasticsearch-scrape":                      "true",
			"coroot.com/elasticsearch-scrape-credentials-username": "monitor",
			"coroot.com/elasticsearch-scrape-param-tls":            "skip-verify",
		},
	}
	tg = TargetFromPod(pod)
	require.NotNil(t, tg)
	assert.Equal(t, TargetTypeElasticsearch, tg.Type)
	assert.Equal(t, "10.0.0.6:9200", tg.Addr)
	assert.Equal(t, "monitor", tg.Credentials.Username)
	assert.Equal(t, map[string]string{"tls": "skip-verify"}, tg.Params)
}

// TestStartClickhouseElasticsearchExporters checks that the static config types are wired to their collectors,
// which start without a reachable server (the errors are reported by the metrics) and stop cleanly.
func TestStartClickhouseElasticsearchExporters(t *testing.T) {
	for _, typ := range []string{"clickhouse", "elasticsearch", "opensearch"} {
		t.Run(typ, func(t *testing.T) {
			tg := TargetFromConfig(config.ApplicationInstrumentation{Type: typ, Host: "127.0.0.1", Port: "1", Params: map[string]string{}})
			reg := prometheus.NewRegistry()
			require.NoError(t, tg.StartExporter(reg, tg.Credentials, common.TLSCredentials{}, time.Hour, 3*time.Second, nil, 100, false, false, nil))
			require.True(t, tg.IsExporterStarted())
			tg.StopExporter(reg)
			assert.False(t, tg.IsExporterStarted())
		})
	}
	tg := TargetFromConfig(config.ApplicationInstrumentation{Type: "clickhouse", Host: "127.0.0.1", Port: "1", Params: map[string]string{"protocol": "grpc"}})
	assert.Error(t, tg.StartExporter(prometheus.NewRegistry(), tg.Credentials, common.TLSCredentials{}, time.Hour, 3*time.Second, nil, 100, false, false, nil))
}
