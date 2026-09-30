package elasticsearch

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testUser     = "monitor"
	testPassword = "s3cr3t-p@ss"
)

// fixtureServer serves the recorded responses of testdata/<dir>, requiring basic auth.
// statusOverrides returns the given status for a path instead of the fixture.
type fixtureServer struct {
	dir             string
	statusOverrides map[string]int
	mu              sync.Mutex
	requests        []*url.URL
}

func (f *fixtureServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r.URL)
	f.mu.Unlock()
	if u, p, ok := r.BasicAuth(); !ok || u != testUser || p != testPassword {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"type":"security_exception","reason":"unable to authenticate user"},"status":401}`))
		return
	}
	if code, ok := f.statusOverrides[r.URL.Path]; ok {
		w.WriteHeader(code)
		return
	}
	var file string
	switch {
	case r.URL.Path == "/":
		file = "root.json"
	case r.URL.Path == "/_cluster/health":
		file = "health.json"
	case strings.HasPrefix(r.URL.Path, "/_nodes/") && strings.HasSuffix(r.URL.Path, "/stats/jvm,fs,indices,thread_pool,breaker,process"):
		file = "nodes_stats.json"
	case r.URL.Path == "/_cat/indices":
		file = "cat_indices.json"
	default:
		w.WriteHeader(http.StatusNotFound)
		return
	}
	data, err := os.ReadFile(filepath.Join("testdata", f.dir, file))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}

func testCollector(t *testing.T, srv *httptest.Server, username, password string, params map[string]string) *Collector {
	opts, err := parseOptions(params)
	require.NoError(t, err)
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	return newCollector(srv.Client(), u, username, password, opts, 5*time.Second, logger.NewKlog("test"))
}

// gather collects the metrics through a pedantic registry (which rejects duplicate and inconsistent series)
// and returns them as "name{label="value",...}" => value.
func gather(t *testing.T, c prometheus.Collector) map[string]float64 {
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(c))
	mfs, err := reg.Gather()
	require.NoError(t, err)
	res := map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			var labels []string
			for _, l := range m.Label {
				labels = append(labels, fmt.Sprintf("%s=%q", l.GetName(), l.GetValue()))
			}
			sort.Strings(labels)
			res[mf.GetName()+"{"+strings.Join(labels, ",")+"}"] = value(m)
		}
	}
	return res
}

func value(m *dto.Metric) float64 {
	if m.Counter != nil {
		return m.Counter.GetValue()
	}
	return m.Gauge.GetValue()
}

func TestElasticsearch8(t *testing.T) {
	fs := &fixtureServer{dir: "es8"}
	srv := httptest.NewServer(fs)
	defer srv.Close()
	c := testCollector(t, srv, testUser, testPassword, nil)
	c.snapshot()
	m := gather(t, c)

	assert.Equal(t, 1.0, m[`elasticsearch_up{}`])
	assert.Equal(t, 0.0, m[`elasticsearch_scrape_error{error="",warning=""}`])
	assert.Equal(t, 1.0, m[`elasticsearch_clusterinfo_version_info{cluster="docker-cluster",distribution="elasticsearch",version="8.15.1"}`])

	cl := `cluster="docker-cluster"`
	assert.Equal(t, 1.0, m[`elasticsearch_cluster_health_status{`+cl+`,color="yellow"}`])
	assert.Equal(t, 0.0, m[`elasticsearch_cluster_health_status{`+cl+`,color="green"}`])
	assert.Equal(t, 0.0, m[`elasticsearch_cluster_health_status{`+cl+`,color="red"}`])
	assert.Equal(t, 1.0, m[`elasticsearch_cluster_health_number_of_nodes{`+cl+`}`])
	assert.Equal(t, 12.0, m[`elasticsearch_cluster_health_active_shards{`+cl+`}`])
	assert.Equal(t, 3.0, m[`elasticsearch_cluster_health_unassigned_shards{`+cl+`}`])
	assert.Equal(t, 1.0, m[`elasticsearch_cluster_health_number_of_pending_tasks{`+cl+`}`])
	assert.Equal(t, 250.0, m[`elasticsearch_cluster_health_task_max_waiting_in_queue_millis{`+cl+`}`])

	n := `cluster="docker-cluster",host="172.18.0.2",name="es01"`
	assert.Equal(t, 536870912.0, m[`elasticsearch_jvm_memory_used_bytes{area="heap",`+n+`}`])
	assert.Equal(t, 1073741824.0, m[`elasticsearch_jvm_memory_max_bytes{area="heap",`+n+`}`])
	assert.Equal(t, 120.0, m[`elasticsearch_jvm_gc_collection_seconds_count{cluster="docker-cluster",gc="young",host="172.18.0.2",name="es01"}`])
	assert.Equal(t, 3.4, m[`elasticsearch_jvm_gc_collection_seconds_sum{cluster="docker-cluster",gc="young",host="172.18.0.2",name="es01"}`])
	assert.Equal(t, 4.0, m[`elasticsearch_jvm_gc_collection_seconds_count{cluster="docker-cluster",gc="G1 Concurrent GC",host="172.18.0.2",name="es01"}`])
	fsl := `cluster="docker-cluster",host="172.18.0.2",mount="/ (overlay)",name="es01",path="/usr/share/elasticsearch/data"`
	assert.Equal(t, 55e9, m[`elasticsearch_filesystem_data_available_bytes{`+fsl+`}`])
	assert.Equal(t, 1e11, m[`elasticsearch_filesystem_data_size_bytes{`+fsl+`}`])
	assert.Equal(t, 104233.0, m[`elasticsearch_indices_docs{`+n+`}`])
	assert.Equal(t, 104300.0, m[`elasticsearch_indices_indexing_index_total{`+n+`}`])
	assert.Equal(t, 23.456, m[`elasticsearch_indices_indexing_index_time_seconds_total{`+n+`}`])
	assert.Equal(t, 5200.0, m[`elasticsearch_indices_search_query_total{`+n+`}`])
	assert.Equal(t, 7.8, m[`elasticsearch_indices_search_query_time_seconds{`+n+`}`])
	assert.Equal(t, 5.0, m[`elasticsearch_thread_pool_rejected_count{`+n+`,type="search"}`])
	assert.Equal(t, 2.0, m[`elasticsearch_thread_pool_queue_count{`+n+`,type="search"}`])
	assert.Equal(t, 2.0, m[`elasticsearch_breakers_tripped{breaker="parent",`+n+`}`])
	assert.Equal(t, 7.0, m[`elasticsearch_process_cpu_percent{`+n+`}`])

	assert.Equal(t, 100000.0, m[`elasticsearch_indices_docs_primary{`+cl+`,index="logs-2024.09.30"}`])
	assert.Equal(t, 52e6, m[`elasticsearch_indices_store_size_bytes_total{`+cl+`,index="logs-2024.09.30"}`])
	assert.Equal(t, 3.0, m[`elasticsearch_indices_shards_primary{`+cl+`,index="orders"}`])
	assert.Equal(t, 1.0, m[`elasticsearch_indices_health_status{`+cl+`,color="yellow",index="logs-2024.09.30"}`])
	// a closed index has no stats, but its health is reported
	assert.Equal(t, 1.0, m[`elasticsearch_indices_health_status{`+cl+`,color="red",index="archived"}`])
	assert.NotContains(t, m, `elasticsearch_indices_docs_primary{`+cl+`,index="archived"}`)
	// hidden/system indices are excluded by default
	for k := range m {
		assert.NotContains(t, k, `index=".security-7"`)
	}

	var nodesReq *url.URL
	for _, u := range fs.requests {
		if strings.HasPrefix(u.Path, "/_nodes/") {
			nodesReq = u
		}
	}
	require.NotNil(t, nodesReq)
	assert.True(t, strings.HasPrefix(nodesReq.Path, "/_nodes/_local/stats/"), nodesReq.Path)
}

func TestOpenSearch2(t *testing.T) {
	fs := &fixtureServer{dir: "opensearch2"}
	srv := httptest.NewServer(fs)
	defer srv.Close()
	c := testCollector(t, srv, testUser, testPassword, map[string]string{"nodes": "_all", "indicesExclude": "", "topIndices": "2"})
	c.snapshot()
	m := gather(t, c)

	assert.Equal(t, 1.0, m[`elasticsearch_up{}`])
	assert.Equal(t, 0.0, m[`elasticsearch_scrape_error{error="",warning=""}`])
	assert.Equal(t, 1.0, m[`elasticsearch_clusterinfo_version_info{cluster="opensearch-cluster",distribution="opensearch",version="2.17.0"}`])

	cl := `cluster="opensearch-cluster"`
	assert.Equal(t, 1.0, m[`elasticsearch_cluster_health_status{`+cl+`,color="green"}`])
	assert.Equal(t, 2.0, m[`elasticsearch_cluster_health_number_of_data_nodes{`+cl+`}`])
	assert.Equal(t, 1.0, m[`elasticsearch_cluster_health_relocating_shards{`+cl+`}`])

	n1 := `cluster="opensearch-cluster",host="172.19.0.3",name="opensearch-node1"`
	n2 := `cluster="opensearch-cluster",host="172.19.0.4",name="opensearch-node2"`
	assert.Equal(t, 268435456.0, m[`elasticsearch_jvm_memory_used_bytes{area="heap",`+n1+`}`])
	assert.Equal(t, 200000000.0, m[`elasticsearch_jvm_memory_used_bytes{area="heap",`+n2+`}`])
	assert.Equal(t, 1.0, m[`elasticsearch_thread_pool_rejected_count{`+n1+`,type="search"}`])
	assert.Equal(t, 1900.0, m[`elasticsearch_indices_docs{`+n2+`}`])
	assert.Equal(t, 0.45, m[`elasticsearch_indices_search_query_time_seconds{`+n1+`}`])
	// node2 has no process stats in the response: not reported rather than reported as zero
	assert.NotContains(t, m, `elasticsearch_process_cpu_percent{`+n2+`}`)

	// the two largest indices (hidden ones included since the exclude is cleared)
	assert.Equal(t, 2.8e6, m[`elasticsearch_indices_store_size_bytes_primary{`+cl+`,index="products"}`])
	assert.Equal(t, 1.0, m[`elasticsearch_indices_replicas{`+cl+`,index="products"}`])
	assert.Equal(t, 10.0, m[`elasticsearch_indices_docs_primary{`+cl+`,index=".opendistro_security"}`])
	assert.NotContains(t, m, `elasticsearch_indices_docs_primary{`+cl+`,index="top_queries-2024.09.30-00001"}`)

	for _, u := range fs.requests {
		if strings.HasPrefix(u.Path, "/_nodes/") {
			assert.True(t, strings.HasPrefix(u.Path, "/_nodes/_all/stats/"), u.Path)
		}
	}
}

func TestErrors(t *testing.T) {
	t.Run("auth", func(t *testing.T) {
		srv := httptest.NewServer(&fixtureServer{dir: "es8"})
		defer srv.Close()
		c := testCollector(t, srv, testUser, "wrong", nil)
		c.snapshot()
		assert.Equal(t, map[string]float64{
			`elasticsearch_up{}`: 0,
			`elasticsearch_scrape_error{error="auth",warning=""}`: 1,
		}, gather(t, c))
	})
	t.Run("forbidden endpoints", func(t *testing.T) {
		srv := httptest.NewServer(&fixtureServer{dir: "es8", statusOverrides: map[string]int{
			"/_cat/indices":    http.StatusForbidden,
			"/_cluster/health": http.StatusServiceUnavailable,
		}})
		defer srv.Close()
		c := testCollector(t, srv, testUser, testPassword, nil)
		c.snapshot()
		m := gather(t, c)
		assert.Equal(t, 1.0, m[`elasticsearch_up{}`])
		assert.Equal(t, 1.0, m[`elasticsearch_scrape_error{error="",warning="indices: permission"}`])
		assert.Equal(t, 1.0, m[`elasticsearch_scrape_error{error="",warning="cluster_health: server_error"}`])
		assert.NotContains(t, m, `elasticsearch_scrape_error{error="",warning=""}`)
		assert.Contains(t, m, `elasticsearch_jvm_memory_max_bytes{area="heap",cluster="docker-cluster",host="172.18.0.2",name="es01"}`)
	})
	t.Run("unreachable", func(t *testing.T) {
		srv := httptest.NewServer(&fixtureServer{dir: "es8"})
		srv.Close()
		c := testCollector(t, srv, testUser, testPassword, nil)
		c.snapshot()
		assert.Equal(t, map[string]float64{
			`elasticsearch_up{}`: 0,
			`elasticsearch_scrape_error{error="connection",warning=""}`: 1,
		}, gather(t, c))
	})
	t.Run("bad response", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("<html>not json</html>"))
		}))
		defer srv.Close()
		c := testCollector(t, srv, "", "", nil)
		c.snapshot()
		assert.Equal(t, 1.0, gather(t, c)[`elasticsearch_scrape_error{error="bad_response",warning=""}`])
	})
}

func TestErrorReason(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{&httpStatusError{code: 401}, "auth"},
		{&httpStatusError{code: 403}, "permission"},
		{&httpStatusError{code: 404}, "not_found"},
		{&httpStatusError{code: 429}, "resource_limit"},
		{&httpStatusError{code: 502}, "server_error"},
		{&httpStatusError{code: 418}, "unknown"},
		{&url.Error{Op: "Get", URL: "http://10.0.0.1:9200/", Err: context.DeadlineExceeded}, "timeout"},
		{fmt.Errorf("dial: %w", syscall.ECONNREFUSED), "connection"},
		{&decodeError{path: "/", err: errors.New("invalid character '<'")}, "bad_response"},
		{errors.New("weird"), "unknown"},
	} {
		assert.Equal(t, tc.want, errorReason(tc.err), tc.err.Error())
	}
}

// TestNewTLS runs the full constructor (background loop, TLS transport) against a TLS server,
// with the CA passed as a file via the params, and checks that Close stops the collector.
func TestNewTLS(t *testing.T) {
	srv := httptest.NewTLSServer(&fixtureServer{dir: "opensearch2"})
	defer srv.Close()
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600))
	addr := strings.TrimPrefix(srv.URL, "https://")

	c, err := New(addr, testUser, testPassword, common.TLSCredentials{}, map[string]string{"tlsCaFile": caFile}, time.Hour, 5*time.Second, logger.NewKlog("test"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return gather(t, c)[`elasticsearch_up{}`] == 1 }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, c.Close())

	// without the CA, the self-signed certificate is rejected
	c, err = New(addr, testUser, testPassword, common.TLSCredentials{}, map[string]string{"tls": "true"}, time.Hour, 5*time.Second, logger.NewKlog("test"))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(gather(t, c)) > 0 }, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, 1.0, gather(t, c)[`elasticsearch_scrape_error{error="tls",warning=""}`])
	require.NoError(t, c.Close())
}

func TestParseOptions(t *testing.T) {
	for _, params := range []map[string]string{
		{"nodes": "_local/../_cluster"},
		{"topIndices": "-1"},
		{"indicesInclude": "["},
		{"indicesExclude": "["},
	} {
		_, err := parseOptions(params)
		assert.Error(t, err, params)
	}
	opts, err := parseOptions(nil)
	require.NoError(t, err)
	assert.Equal(t, "_local", opts.nodes)
	assert.Equal(t, defaultTopIndices, opts.topIndices)
	assert.True(t, opts.indicesExclude.MatchString(".kibana"))
}
