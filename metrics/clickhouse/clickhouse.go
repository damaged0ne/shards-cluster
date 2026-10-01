// Package clickhouse collects ClickHouse server metrics from its system tables.
//
// The system tables are queried by a background goroutine every scrape interval, and Collect serves
// the metrics of the last completed snapshot, so a slow or unreachable server never blocks a scrape.
package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"regexp"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultTopTables  = 100
	defaultTopQueries = 20
	defaultTopErrors  = 100

	// query_log is flushed asynchronously (every 7.5s by default), so the window of the top queries
	// lags behind the current time to not miss the queries that haven't been flushed yet.
	queryLogLag = 10 * time.Second
	// the agent's own queries are excluded from the top queries by the client name
	clientProduct = "coroot-cluster-agent"
)

var (
	dUp          = common.Desc("clickhouse_up", "Whether the last snapshot of the ClickHouse server metrics succeeded (1) or not (0)")
	dScrapeError = common.Desc("clickhouse_scrape_error", "Errors of the last snapshot: error is the reason the server is unavailable, warning is a failed query (what: reason)", "error", "warning")
	dInfo        = common.Desc("clickhouse_info", "ClickHouse server information", "server_version")
)

// querier is the subset of the clickhouse-go connection used by the collector; replaced by a fake in tests.
type querier interface {
	Select(ctx context.Context, dest any, query string, args ...any) error
	Close() error
}

type options struct {
	topTables     int
	topQueries    int
	queryLog      bool
	tablesInclude *regexp.Regexp
	tablesExclude *regexp.Regexp
	excludeDBs    []string
}

type Collector struct {
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	started bool

	q       querier
	opts    options
	logger  logger.Logger
	now     func() time.Time
	timeout time.Duration

	lock    sync.RWMutex
	metrics []prometheus.Metric // the metrics of the last snapshot; never mutated after publishing

	// private to the snapshot goroutine
	queryLogFrom time.Time
}

// New creates a collector for the ClickHouse server at addr and starts taking snapshots every scrapeInterval,
// each bounded by collectTimeout. Supported params:
//   - protocol: "native" (default) or "http"
//   - database: the database to connect to (default "default")
//   - tls: "true", "skip-verify" or "false"; tlsCaFile, tlsCertFile, tlsKeyFile: PEM files
//   - tablesInclude, tablesExclude: regular expressions matched against "database.table"
//   - topTables: the max number of tables to report parts/replica/mutation metrics for (default 100)
//   - queryLog: "false" disables the top queries from system.query_log
//   - topQueries: the max number of top queries (default 20)
func New(addr, username, password string, tlsCreds common.TLSCredentials, params map[string]string,
	scrapeInterval, collectTimeout time.Duration, logger logger.Logger, excludeDatabases []string) (*Collector, error) {

	opts, err := parseOptions(params, excludeDatabases)
	if err != nil {
		return nil, err
	}
	tlsCreds, err = common.TLSCredentialsFromParams(tlsCreds, params)
	if err != nil {
		return nil, err
	}
	var tlsCfg *tls.Config
	if common.TLSEnabled(tlsCreds, params["tls"]) {
		if tlsCfg, err = common.DatabaseTLSConfig(tlsCreds, params["tls"] == "skip-verify"); err != nil {
			return nil, err
		}
	}
	protocol := ch.Native
	switch params["protocol"] {
	case "", "native":
	case "http", "https":
		protocol = ch.HTTP
	default:
		return nil, fmt.Errorf("invalid protocol %q, expected native or http", params["protocol"])
	}
	if username == "" {
		username = "default"
	}
	database := params["database"]
	if database == "" {
		database = "default"
	}
	chOpts := &ch.Options{
		Addr:             []string{addr},
		Protocol:         protocol,
		Auth:             ch.Auth{Database: database, Username: username, Password: password},
		TLS:              tlsCfg,
		DialTimeout:      collectTimeout,
		ReadTimeout:      collectTimeout,
		MaxOpenConns:     2,
		MaxIdleConns:     1,
		ConnMaxLifetime:  10 * time.Minute,
		ConnOpenStrategy: ch.ConnOpenInOrder,
	}
	chOpts.ClientInfo.Products = append(chOpts.ClientInfo.Products, struct{ Name, Version string }{Name: clientProduct})
	conn, err := ch.Open(chOpts)
	if err != nil {
		return nil, err
	}
	c := newCollector(conn, opts, scrapeInterval, collectTimeout, logger)
	c.start(scrapeInterval)
	return c, nil
}

func parseOptions(params map[string]string, excludeDatabases []string) (options, error) {
	opts := options{
		topTables:  defaultTopTables,
		topQueries: defaultTopQueries,
		queryLog:   params["queryLog"] != "false",
		excludeDBs: excludeDatabases,
	}
	var err error
	for _, p := range []struct {
		key string
		dst *int
	}{{"topTables", &opts.topTables}, {"topQueries", &opts.topQueries}} {
		if v := params[p.key]; v != "" {
			if *p.dst, err = strconv.Atoi(v); err != nil || *p.dst < 0 {
				return opts, fmt.Errorf("invalid %s: %q", p.key, v)
			}
		}
	}
	for _, p := range []struct {
		key string
		dst **regexp.Regexp
	}{{"tablesInclude", &opts.tablesInclude}, {"tablesExclude", &opts.tablesExclude}} {
		if v := params[p.key]; v != "" {
			if *p.dst, err = regexp.Compile(v); err != nil {
				return opts, fmt.Errorf("invalid %s: %w", p.key, err)
			}
		}
	}
	return opts, nil
}

func newCollector(q querier, opts options, scrapeInterval, collectTimeout time.Duration, logger logger.Logger) *Collector {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Collector{
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
		q:       q,
		opts:    opts,
		logger:  logger,
		now:     time.Now,
		timeout: collectTimeout,
	}
	c.queryLogFrom = c.now().Add(-queryLogLag - scrapeInterval)
	return c
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

// Close stops the snapshot goroutine and closes the connections to the server.
func (c *Collector) Close() error {
	c.cancel()
	if c.started {
		<-c.done
	}
	return c.q.Close()
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

func (c *Collector) publish(metrics []prometheus.Metric) {
	c.lock.Lock()
	c.metrics = metrics
	c.lock.Unlock()
}

func (c *Collector) snapshot() {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("clickhouse snapshot panic:", r, "\n", string(debug.Stack()))
		}
	}()
	ctx, cancel := context.WithTimeout(c.ctx, c.timeout)
	defer cancel()
	c.publish(c.takeSnapshot(ctx))
}

type versionRow struct {
	Version string `ch:"version"`
}

func (c *Collector) takeSnapshot(ctx context.Context) []prometheus.Metric {
	var version []versionRow
	if err := c.q.Select(ctx, &version, "SELECT version() AS version"); err != nil {
		c.logger.Warning("failed to query the server version:", err)
		return []prometheus.Metric{
			common.Gauge(dUp, 0),
			common.Gauge(dScrapeError, 1, errorReason(err), ""),
		}
	}
	res := []prometheus.Metric{common.Gauge(dUp, 1)}
	if len(version) > 0 {
		res = append(res, common.Gauge(dInfo, 1, version[0].Version))
	}

	var warnings []string
	for _, s := range []struct {
		what string
		fn   func(context.Context) ([]prometheus.Metric, error)
	}{
		{"system.metrics", c.systemMetrics},
		{"system.events", c.systemEvents},
		{"system.asynchronous_metrics", c.systemAsyncMetrics},
		{"system.parts", c.systemParts},
		{"system.replicas", c.systemReplicas},
		{"system.mutations", c.systemMutations},
		{"system.errors", c.systemErrors},
		{"system.query_log", c.topQueries},
	} {
		ms, err := s.fn(ctx)
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
