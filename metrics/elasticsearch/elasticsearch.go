// Package elasticsearch collects Elasticsearch and OpenSearch metrics from the REST API
// (/, /_cluster/health, /_nodes/<nodes>/stats and /_cat/indices).
//
// The metric names mirror prometheus-community/elasticsearch_exporter where possible, so existing
// dashboards and alerts translate. The API is queried by a background goroutine every scrape interval,
// and Collect serves the metrics of the last completed snapshot, so a slow cluster never blocks a scrape.
package elasticsearch

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultTopIndices     = 100
	defaultIndicesExclude = `^\.` // hidden and system indices
	maxResponseSize       = 64 << 20
)

var (
	dUp          = common.Desc("elasticsearch_up", "Whether the last snapshot of the Elasticsearch/OpenSearch metrics succeeded (1) or not (0)")
	dScrapeError = common.Desc("elasticsearch_scrape_error", "Errors of the last snapshot: error is the reason the server is unavailable, warning is a failed API request (what: reason)", "error", "warning")
	dVersionInfo = common.Desc("elasticsearch_clusterinfo_version_info", "Elasticsearch/OpenSearch version information", "cluster", "version", "distribution")
)

var reNodes = regexp.MustCompile(`^[A-Za-z0-9_.,*:-]+$`)

type options struct {
	nodes          string
	topIndices     int
	indicesInclude *regexp.Regexp
	indicesExclude *regexp.Regexp
}

type Collector struct {
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	started bool

	client   *http.Client
	baseURL  *url.URL
	username string
	password string
	opts     options
	logger   logger.Logger
	timeout  time.Duration

	lock    sync.RWMutex
	metrics []prometheus.Metric // the metrics of the last snapshot; never mutated after publishing
}

// New creates a collector for the Elasticsearch/OpenSearch node at addr and starts taking snapshots every
// scrapeInterval, each bounded by collectTimeout. Supported params:
//   - tls: "true", "skip-verify" or "false"; tlsCaFile, tlsCertFile, tlsKeyFile: PEM files
//   - nodes: the nodes to report node stats for: "_local" (default, the node at addr) or "_all"
//   - indicesInclude, indicesExclude: regular expressions matched against index names (the default exclude is `^\.`)
//   - topIndices: the max number of indices to report (the largest ones, default 100)
func New(addr, username, password string, tlsCreds common.TLSCredentials, params map[string]string,
	scrapeInterval, collectTimeout time.Duration, logger logger.Logger) (*Collector, error) {

	opts, err := parseOptions(params)
	if err != nil {
		return nil, err
	}
	tlsCreds, err = common.TLSCredentialsFromParams(tlsCreds, params)
	if err != nil {
		return nil, err
	}
	scheme := "http"
	var tlsCfg *tls.Config
	if common.TLSEnabled(tlsCreds, params["tls"]) {
		scheme = "https"
		if tlsCfg, err = common.DatabaseTLSConfig(tlsCreds, params["tls"] == "skip-verify"); err != nil {
			return nil, err
		}
	}
	transport := &http.Transport{
		Proxy:                 nil, // the cluster is reached directly, like the other database targets
		DialContext:           (&net.Dialer{Timeout: collectTimeout, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig:       tlsCfg,
		TLSHandshakeTimeout:   collectTimeout,
		ResponseHeaderTimeout: collectTimeout,
		MaxIdleConnsPerHost:   2,
		IdleConnTimeout:       2 * scrapeInterval,
		ForceAttemptHTTP2:     tlsCfg != nil,
	}
	c := newCollector(&http.Client{Transport: transport, Timeout: collectTimeout},
		&url.URL{Scheme: scheme, Host: addr}, username, password, opts, collectTimeout, logger)
	c.start(scrapeInterval)
	return c, nil
}

func parseOptions(params map[string]string) (options, error) {
	opts := options{nodes: "_local", topIndices: defaultTopIndices}
	if v := params["nodes"]; v != "" {
		if !reNodes.MatchString(v) {
			return opts, fmt.Errorf("invalid nodes: %q", v)
		}
		opts.nodes = v
	}
	if v := params["topIndices"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return opts, fmt.Errorf("invalid topIndices: %q", v)
		}
		opts.topIndices = n
	}
	var err error
	if v := params["indicesInclude"]; v != "" {
		if opts.indicesInclude, err = regexp.Compile(v); err != nil {
			return opts, fmt.Errorf("invalid indicesInclude: %w", err)
		}
	}
	exclude, ok := params["indicesExclude"]
	if !ok {
		exclude = defaultIndicesExclude
	}
	if exclude != "" {
		if opts.indicesExclude, err = regexp.Compile(exclude); err != nil {
			return opts, fmt.Errorf("invalid indicesExclude: %w", err)
		}
	}
	return opts, nil
}

func newCollector(client *http.Client, baseURL *url.URL, username, password string, opts options, collectTimeout time.Duration, logger logger.Logger) *Collector {
	ctx, cancel := context.WithCancel(context.Background())
	return &Collector{
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
		client:   client,
		baseURL:  baseURL,
		username: username,
		password: password,
		opts:     opts,
		logger:   logger,
		timeout:  collectTimeout,
	}
}

func (c *Collector) start(scrapeInterval time.Duration) {
	c.started = true
	go func() {
		defer close(c.done)
		ticker := time.NewTicker(scrapeInterval)
		defer ticker.Stop()
		c.snapshot()
		for {
			select {
			case <-ticker.C:
				c.snapshot()
			case <-c.ctx.Done():
				return
			}
		}
	}()
}

// Close stops the snapshot goroutine (aborting an in-flight request) and closes the idle connections.
func (c *Collector) Close() error {
	c.cancel()
	if c.started {
		<-c.done
	}
	c.client.CloseIdleConnections()
	return nil
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {}

// Collect serves the metrics of the last snapshot without touching the network.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.lock.RLock()
	metrics := c.metrics
	c.lock.RUnlock()
	for _, m := range metrics {
		ch <- m
	}
}

func (c *Collector) snapshot() {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("elasticsearch snapshot panic:", r, "\n", string(debug.Stack()))
		}
	}()
	ctx, cancel := context.WithTimeout(c.ctx, c.timeout)
	defer cancel()
	metrics := c.takeSnapshot(ctx)
	c.lock.Lock()
	c.metrics = metrics
	c.lock.Unlock()
}

type rootResponse struct {
	ClusterName string `json:"cluster_name"`
	Version     struct {
		Number       string `json:"number"`
		Distribution string `json:"distribution"` // "opensearch" for OpenSearch, absent for Elasticsearch
	} `json:"version"`
}

func (c *Collector) takeSnapshot(ctx context.Context) []prometheus.Metric {
	var root rootResponse
	if err := c.get(ctx, "/", nil, &root); err != nil {
		c.logger.Warning("failed to query the cluster info:", err)
		return []prometheus.Metric{
			common.Gauge(dUp, 0),
			common.Gauge(dScrapeError, 1, errorReason(err), ""),
		}
	}
	distribution := root.Version.Distribution
	if distribution == "" {
		distribution = "elasticsearch"
	}
	cluster := root.ClusterName
	res := []prometheus.Metric{
		common.Gauge(dUp, 1),
		common.Gauge(dVersionInfo, 1, cluster, root.Version.Number, distribution),
	}

	var warnings []string
	for _, s := range []struct {
		what string
		fn   func(context.Context, string) ([]prometheus.Metric, error)
	}{
		{"cluster_health", c.clusterHealth},
		{"nodes_stats", c.nodesStats},
		{"indices", c.indices},
	} {
		ms, err := s.fn(ctx, cluster)
		if err != nil {
			c.logger.Warning(s.what+":", err)
			warnings = append(warnings, s.what+": "+errorReason(err))
			continue
		}
		res = append(res, ms...)
	}
	if len(warnings) == 0 {
		res = append(res, common.Gauge(dScrapeError, 0, "", ""))
	}
	slices.Sort(warnings)
	for _, w := range slices.Compact(warnings) {
		res = append(res, common.Gauge(dScrapeError, 1, "", w))
	}
	return res
}

// get performs a GET request to path and decodes the JSON response into dst.
// The credentials are sent in the Authorization header only, so they never appear in URLs or errors.
func (c *Collector) get(ctx context.Context, path string, query url.Values, dst any) error {
	u := *c.baseURL
	u.Path = path
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if c.username != "" || c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return &httpStatusError{path: path, code: resp.StatusCode}
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, maxResponseSize)).Decode(dst); err != nil {
		return &decodeError{path: path, err: err}
	}
	return nil
}
