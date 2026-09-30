package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeQuerier returns the rows (or the error) registered for the first key the query contains.
type fakeQuerier struct {
	mu      sync.Mutex
	rows    map[string]any
	errs    map[string]error
	queries []string
	closed  bool
}

func (f *fakeQuerier) Select(_ context.Context, dest any, query string, _ ...any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, query)
	for k, err := range f.errs {
		if strings.Contains(query, k) {
			return err
		}
	}
	for k, rows := range f.rows {
		if strings.Contains(query, k) {
			reflect.ValueOf(dest).Elem().Set(reflect.ValueOf(rows))
			return nil
		}
	}
	return nil
}

func (f *fakeQuerier) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func testCollector(t *testing.T, q *fakeQuerier, params map[string]string) *Collector {
	opts, err := parseOptions(params, []string{"information_schema"})
	require.NoError(t, err)
	return newCollector(q, opts, 15*time.Second, 5*time.Second, logger.NewKlog("test"))
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

func TestSnapshotRowMapping(t *testing.T) {
	q := &fakeQuerier{rows: map[string]any{
		"version()": []versionRow{{Version: "24.8.4.13"}},
		"FROM system.metrics": []nameValueRow{
			{Name: "Query", Value: 3},
			{Name: "TCPConnection", Value: 7},
			{Name: "HTTPConnection", Value: 2},
			{Name: "PartsActive", Value: 120},
			{Name: "ReadonlyReplica", Value: 1},
			{Name: "SomethingNew", Value: 42}, // not curated: ignored
		},
		"FROM system.events": []nameValueRow{
			{Name: "Query", Value: 1000},
			{Name: "SelectQuery", Value: 800},
			{Name: "FailedQuery", Value: 5},
			{Name: "QueryTimeMicroseconds", Value: 2_500_000},
			{Name: "MergesTimeMilliseconds", Value: 1500},
			{Name: "RejectedInserts", Value: 4},
			{Name: "ZooKeeperHardwareExceptions", Value: 2},
		},
		"FROM system.asynchronous_metrics": []nameValueRow{
			{Name: "Uptime", Value: 3600},
			{Name: "MaxPartCountForPartition", Value: 250},
			{Name: "ReplicasMaxAbsoluteDelay", Value: 12},
		},
		"FROM system.parts": []partsRow{
			{Database: "db", Table: "events", Parts: 30, Rows: 1e6, Bytes: 5e8, Partitions: 3, MaxParts: 15},
			{Database: "db", Table: "small", Parts: 1, Rows: 10, Bytes: 100, Partitions: 1, MaxParts: 1},
			{Database: "information_schema", Table: "x", Parts: 1, Rows: 1, Bytes: 1e9, Partitions: 1, MaxParts: 1}, // excluded database
		},
		"FROM system.replicas": []replicaRow{
			{Database: "db", Table: "events", IsReadonly: 1, AbsoluteDelay: 12, QueueSize: 5, InsertsInQueue: 3, MergesInQueue: 2},
		},
		"FROM system.mutations": []mutationsRow{
			{Database: "db", Table: "events", InProgress: 2, Failing: 1, Stuck: 1},
		},
		"FROM system.errors": []nameValueRow{
			{Name: "UNKNOWN_TABLE", Value: 17},
			{Name: "TOO_MANY_PARTS", Value: 3},
		},
		"FROM system.query_log": []queryLogRow{
			{DB: "db", Query: "SELECT count() FROM events WHERE id = ?", Calls: 30, TotalTime: 15, ReadRows: 3000, ReadBytes: 6000, Errors: 3},
			// the same text after obfuscation as the first row: merged
			{DB: "db", Query: "select count() from events where id = ?", Calls: 30, TotalTime: 15, ReadRows: 3000, ReadBytes: 6000},
			{DB: "db", Query: "INSERT INTO events FORMAT Native", Calls: 15, TotalTime: 3},
		},
	}}
	c := testCollector(t, q, map[string]string{"tablesExclude": `\.small$`})
	now := time.Unix(1_700_000_000, 0)
	c.now = func() time.Time { return now }
	c.queryLogFrom = now.Add(-queryLogLag - 15*time.Second)

	c.snapshot()
	m := gather(t, c)

	assert.Equal(t, 1.0, m[`clickhouse_up{}`])
	assert.Equal(t, 1.0, m[`clickhouse_info{server_version="24.8.4.13"}`])
	assert.Equal(t, 0.0, m[`clickhouse_scrape_error{error="",warning=""}`])

	assert.Equal(t, 3.0, m[`clickhouse_queries_running{}`])
	assert.Equal(t, 7.0, m[`clickhouse_connections{protocol="tcp"}`])
	assert.Equal(t, 2.0, m[`clickhouse_connections{protocol="http"}`])
	assert.Equal(t, 120.0, m[`clickhouse_parts_by_state{state="active"}`])
	assert.Equal(t, 1.0, m[`clickhouse_readonly_replicas{}`])

	assert.Equal(t, 1000.0, m[`clickhouse_queries_total{kind="all"}`])
	assert.Equal(t, 800.0, m[`clickhouse_queries_total{kind="select"}`])
	assert.Equal(t, 5.0, m[`clickhouse_failed_queries_total{kind="all"}`])
	assert.Equal(t, 2.5, m[`clickhouse_query_time_seconds_total{}`])
	assert.Equal(t, 1.5, m[`clickhouse_merge_time_seconds_total{}`])
	assert.Equal(t, 4.0, m[`clickhouse_rejected_inserts_total{}`])
	assert.Equal(t, 2.0, m[`clickhouse_zookeeper_exceptions_total{type="hardware"}`])

	assert.Equal(t, 3600.0, m[`clickhouse_uptime_seconds{}`])
	assert.Equal(t, 250.0, m[`clickhouse_max_part_count_for_partition{}`])
	assert.Equal(t, 12.0, m[`clickhouse_replicas_max_absolute_delay_seconds{}`])

	assert.Equal(t, 30.0, m[`clickhouse_table_parts{db="db",table="events"}`])
	assert.Equal(t, 1e6, m[`clickhouse_table_rows{db="db",table="events"}`])
	assert.Equal(t, 5e8, m[`clickhouse_table_size_bytes{db="db",table="events"}`])
	assert.Equal(t, 15.0, m[`clickhouse_table_max_parts_per_partition{db="db",table="events"}`])
	assert.NotContains(t, m, `clickhouse_table_parts{db="db",table="small"}`)
	assert.NotContains(t, m, `clickhouse_table_parts{db="information_schema",table="x"}`)

	assert.Equal(t, 1.0, m[`clickhouse_replica_readonly{db="db",table="events"}`])
	assert.Equal(t, 12.0, m[`clickhouse_replica_absolute_delay_seconds{db="db",table="events"}`])
	assert.Equal(t, 5.0, m[`clickhouse_replica_queue_size{db="db",table="events"}`])

	assert.Equal(t, 2.0, m[`clickhouse_table_mutations_in_progress{db="db",table="events"}`])
	assert.Equal(t, 1.0, m[`clickhouse_table_mutations_failing{db="db",table="events"}`])
	assert.Equal(t, 1.0, m[`clickhouse_table_mutations_stuck{db="db",table="events"}`])

	assert.Equal(t, 17.0, m[`clickhouse_errors_total{name="UNKNOWN_TABLE"}`])

	sel := `db="db",query="select count ( ? ) from events where id = ?"`
	assert.Equal(t, 4.0, m[`clickhouse_top_query_calls_per_second{`+sel+`}`]) // (30+30)/15s
	assert.Equal(t, 2.0, m[`clickhouse_top_query_time_per_second{`+sel+`}`])
	assert.Equal(t, 0.2, m[`clickhouse_top_query_errors_per_second{`+sel+`}`])
	assert.Equal(t, 1.0, m[`clickhouse_top_query_calls_per_second{db="db",query="insert into events format native"}`])

	// the next query_log window starts where the previous one ended
	assert.Equal(t, now.Add(-queryLogLag), c.queryLogFrom)
}

func TestTopTablesLimit(t *testing.T) {
	var rows []partsRow
	for i := 0; i < 10; i++ {
		rows = append(rows, partsRow{Database: "db", Table: fmt.Sprintf("t%d", i), Bytes: float64(i)})
	}
	q := &fakeQuerier{rows: map[string]any{"version()": []versionRow{{Version: "25.3"}}, "FROM system.parts": rows}}
	c := testCollector(t, q, map[string]string{"topTables": "3", "tablesInclude": `^db\.`, "queryLog": "false"})
	c.snapshot()
	m := gather(t, c)
	var tables []string
	for k := range m {
		if strings.HasPrefix(k, "clickhouse_table_size_bytes{") {
			tables = append(tables, k)
		}
	}
	sort.Strings(tables)
	assert.Equal(t, []string{
		`clickhouse_table_size_bytes{db="db",table="t7"}`,
		`clickhouse_table_size_bytes{db="db",table="t8"}`,
		`clickhouse_table_size_bytes{db="db",table="t9"}`,
	}, tables)
	for _, query := range q.queries {
		assert.NotContains(t, query, "system.query_log")
	}
}

func TestSnapshotErrors(t *testing.T) {
	t.Run("unavailable", func(t *testing.T) {
		q := &fakeQuerier{errs: map[string]error{"version()": &ch.Exception{Code: 516, Message: "default: Authentication failed: password is incorrect"}}}
		c := testCollector(t, q, nil)
		c.snapshot()
		m := gather(t, c)
		assert.Equal(t, map[string]float64{
			`clickhouse_up{}`: 0,
			`clickhouse_scrape_error{error="auth",warning=""}`: 1,
		}, m)
	})
	t.Run("failed queries", func(t *testing.T) {
		q := &fakeQuerier{
			rows: map[string]any{"version()": []versionRow{{Version: "23.8"}}},
			errs: map[string]error{
				"system.query_log": &ch.Exception{Code: 60, Message: "Table system.query_log does not exist"},
				"system.replicas":  &ch.Exception{Code: 497, Message: "Not enough privileges"},
			},
		}
		c := testCollector(t, q, nil)
		c.snapshot()
		m := gather(t, c)
		assert.Equal(t, 1.0, m[`clickhouse_up{}`])
		assert.Equal(t, 1.0, m[`clickhouse_scrape_error{error="",warning="system.query_log: not_found"}`])
		assert.Equal(t, 1.0, m[`clickhouse_scrape_error{error="",warning="system.replicas: permission"}`])
		assert.NotContains(t, m, `clickhouse_scrape_error{error="",warning=""}`)
	})
}

func TestCloseStopsLoopAndClosesConn(t *testing.T) {
	q := &fakeQuerier{rows: map[string]any{"version()": []versionRow{{Version: "24.1"}}}}
	c := testCollector(t, q, nil)
	c.start(time.Hour)
	require.Eventually(t, func() bool { return len(gather(t, c)) > 0 }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, c.Close())
	assert.True(t, q.closed)
}

func TestErrorReason(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{&ch.Exception{Code: 516}, "auth"},
		{fmt.Errorf("wrapped: %w", &ch.Exception{Code: 497}), "permission"},
		{&ch.Exception{Code: 81}, "not_found"},
		{&ch.Exception{Code: 159}, "timeout"},
		{&ch.Exception{Code: 241}, "resource_limit"},
		{&ch.Exception{Code: 1000}, "unknown"},
		{errors.New("sendQuery: [HTTP 403] response body: \"Code: 516. DB::Exception: default: Authentication failed\""), "auth"},
		{ch.ErrAcquireConnTimeout, "timeout"},
		{context.DeadlineExceeded, "timeout"},
		{fmt.Errorf("dial: %w", syscall.ECONNREFUSED), "connection"},
		{io.EOF, "connection"},
		{errors.New("something odd with password=secret"), "unknown"},
	} {
		assert.Equal(t, tc.want, errorReason(tc.err), tc.err.Error())
	}
}

func TestParseOptions(t *testing.T) {
	_, err := parseOptions(map[string]string{"topTables": "x"}, nil)
	assert.Error(t, err)
	_, err = parseOptions(map[string]string{"tablesInclude": "("}, nil)
	assert.Error(t, err)
	opts, err := parseOptions(nil, nil)
	require.NoError(t, err)
	assert.Equal(t, defaultTopTables, opts.topTables)
	assert.True(t, opts.queryLog)
}

func TestQueriesAreStatic(t *testing.T) {
	// the curated names are rendered into the queries as literals: they must not need escaping
	re := regexp.MustCompile(`^[A-Za-z0-9_.]+$`)
	for _, m := range []map[string]mapping{systemMetricsMapping, systemEventsMapping, systemAsyncMetricsMapping} {
		for name := range m {
			assert.Regexp(t, re, name)
		}
	}
	q := queryLogQuery(100, 200, 5)
	assert.Contains(t, q, "event_time > toDateTime(100) AND event_time <= toDateTime(200)")
	assert.Contains(t, q, "LIMIT 5")
}
