package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	cfgpkg "github.com/coroot/coroot-cluster-agent/config"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

type remoteWriteReceiver struct {
	mu      sync.Mutex
	series  []map[string]string // labels of the received series (with the __name__ label)
	apiKeys map[string]bool
}

func (r *remoteWriteReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	compressed, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	data, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var wr prompb.WriteRequest
	if err = wr.Unmarshal(data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.apiKeys[req.Header.Get("X-Api-Key")] = true
	for _, ts := range wr.Timeseries {
		if len(ts.Samples) == 0 {
			continue
		}
		ls := map[string]string{}
		for _, l := range ts.Labels {
			ls[l.Name] = l.Value
		}
		r.series = append(r.series, ls)
	}
	w.WriteHeader(http.StatusNoContent)
}

// find returns the first received series with the given name, or nil.
func (r *remoteWriteReceiver) find(name string) map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.series {
		if s["__name__"] == name {
			return s
		}
	}
	return nil
}

// TestScrapeToRemoteWrite checks the whole path: the scrape manager scrapes the targets,
// the samples are written to the WAL, and the remote-write queue sends them to the receiver.
func TestScrapeToRemoteWrite(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	agentTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, "# TYPE e2e_test_metric gauge\ne2e_test_metric{foo=\"bar\"} 42\n")
	}))
	defer agentTarget.Close()

	rabbitmq := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u, p, ok := r.BasicAuth(); !ok || u != "monitoring" || p != "secret" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, "# TYPE rabbitmq_queues gauge\nrabbitmq_queues 3\n"+
			"# TYPE rabbitmq_not_allowlisted gauge\nrabbitmq_not_allowlisted 1\n")
	}))
	defer rabbitmq.Close()
	rmqURL, _ := url.Parse(rabbitmq.URL)

	receiver := &remoteWriteReceiver{apiKeys: map[string]bool{}}
	rw := httptest.NewServer(receiver)
	defer rw.Close()
	endpoint, _ := url.Parse(rw.URL + "/v1/metrics")

	agentURL, _ := url.Parse(agentTarget.URL)
	ms := &Metrics{
		endpoint:       endpoint,
		apiKey:         "test-key",
		listenAddr:     agentURL.Host,
		scrapeInterval: time.Second,
		scrapeTimeout:  time.Second,
		walDir:         t.TempDir(),
		static: &cfgpkg.Static{Databases: []cfgpkg.Database{{
			Type:        "rabbitmq",
			Host:        rmqURL.Hostname(),
			Port:        rmqURL.Port(),
			Credentials: cfgpkg.Credentials{Username: "monitoring", Password: "secret"},
		}}},
	}
	if err := ms.runScraper(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ms.stopScraper(ctx)
		ms.closeStorage(ctx)
	}()

	deadline := time.Now().Add(60 * time.Second)
	var agentSeries, rmqSeries map[string]string
	for time.Now().Before(deadline) {
		agentSeries, rmqSeries = receiver.find("e2e_test_metric"), receiver.find("rabbitmq_queues")
		if agentSeries != nil && rmqSeries != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if agentSeries == nil || rmqSeries == nil {
		t.Fatalf("the samples haven't been received: agent=%v, rabbitmq=%v", agentSeries, rmqSeries)
	}
	if agentSeries["foo"] != "bar" || agentSeries["job"] != jobName || agentSeries["instance"] != agentURL.Host {
		t.Errorf("unexpected labels: %v", agentSeries)
	}
	if rmqSeries["job"] != "rabbitmq" || rmqSeries["instance"] != rmqURL.Host {
		t.Errorf("unexpected labels: %v", rmqSeries)
	}
	if up := receiver.find("up"); up == nil {
		t.Error("the up series hasn't been received")
	}
	if s := receiver.find("rabbitmq_not_allowlisted"); s != nil {
		t.Errorf("a metric not in the allowlist has been received: %v", s)
	}
	receiver.mu.Lock()
	if !receiver.apiKeys["test-key"] || len(receiver.apiKeys) != 1 {
		t.Errorf("unexpected API keys: %v", receiver.apiKeys)
	}
	receiver.mu.Unlock()
}
