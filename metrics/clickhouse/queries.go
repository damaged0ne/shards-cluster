package clickhouse

import (
	"context"
	"fmt"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/obfuscate"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	maxQueryLen       = 1000
	maxQueryLogWindow = 5 * time.Minute
)

var (
	dTopQueryCalls     = common.Desc("clickhouse_top_query_calls_per_second", "Number of executions of the query per second", "db", "query")
	dTopQueryTime      = common.Desc("clickhouse_top_query_time_per_second", "Time spent executing the query per second", "db", "query")
	dTopQueryReadRows  = common.Desc("clickhouse_top_query_read_rows_per_second", "Rows read by the query per second", "db", "query")
	dTopQueryReadBytes = common.Desc("clickhouse_top_query_read_bytes_per_second", "Bytes read by the query per second", "db", "query")
	dTopQueryErrors    = common.Desc("clickhouse_top_query_errors_per_second", "Failed executions of the query per second", "db", "query")
)

type queryLogRow struct {
	DB        string  `ch:"db"`
	Query     string  `ch:"query"`
	Calls     float64 `ch:"calls"`
	TotalTime float64 `ch:"total_time"`
	ReadRows  float64 `ch:"read_rows"`
	ReadBytes float64 `ch:"read_bytes"`
	Errors    float64 `ch:"errors"`
}

// queryLogQuery aggregates the initial (not distributed sub-) queries finished within (from, to] by the normalized query hash.
// The literals are replaced by normalizeQuery() on the server, and the text is obfuscated again by the agent.
// Filtering by event_date (the first column of the primary key) and event_time keeps the query cheap.
func queryLogQuery(from, to int64, limit int) string {
	return fmt.Sprintf(`SELECT current_database AS db, normalizeQuery(any(query)) AS query, toFloat64(count()) AS calls,
  toFloat64(sum(query_duration_ms)) / 1000 AS total_time, toFloat64(sum(read_rows)) AS read_rows, toFloat64(sum(read_bytes)) AS read_bytes,
  toFloat64(countIf(type != 'QueryFinish')) AS errors
FROM system.query_log
WHERE event_date >= toDate(toDateTime(%[1]d)) AND event_time > toDateTime(%[1]d) AND event_time <= toDateTime(%[2]d)
  AND type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing') AND is_initial_query
  AND position(client_name, '%[4]s') = 0 AND position(http_user_agent, '%[4]s') = 0
GROUP BY current_database, normalized_query_hash
ORDER BY total_time DESC
LIMIT %[3]d`, from, to, limit, clientProduct)
}

func (c *Collector) topQueries(ctx context.Context) ([]prometheus.Metric, error) {
	if !c.opts.queryLog || c.opts.topQueries == 0 {
		return nil, nil
	}
	to := c.now().Add(-queryLogLag).Truncate(time.Second)
	from := c.queryLogFrom
	if earliest := to.Add(-maxQueryLogWindow); from.Before(earliest) { // e.g. after the server has been unavailable
		from = earliest
	}
	if !to.After(from) {
		return nil, nil
	}
	var rows []queryLogRow
	if err := c.q.Select(ctx, &rows, queryLogQuery(from.Unix(), to.Unix(), c.opts.topQueries)); err != nil {
		return nil, err
	}
	c.queryLogFrom = to
	return topQueriesMetrics(rows, to.Sub(from).Seconds()), nil
}

// topQueriesMetrics converts the aggregated query_log rows of a window to per-second rates.
// Different hashes can produce the same obfuscated text, so the rows are merged by (db, query).
func topQueriesMetrics(rows []queryLogRow, window float64) []prometheus.Metric {
	if window <= 0 {
		return nil
	}
	type key struct{ db, query string }
	merged := map[key]*queryLogRow{}
	var keys []key
	for _, r := range rows {
		k := key{db: r.DB, query: truncateUTF8(obfuscate.SqlWithDialect(r.Query, obfuscate.DialectMySQL), maxQueryLen)}
		if k.query == "" {
			continue
		}
		m := merged[k]
		if m == nil {
			m = &queryLogRow{}
			merged[k] = m
			keys = append(keys, k)
		}
		m.Calls += r.Calls
		m.TotalTime += r.TotalTime
		m.ReadRows += r.ReadRows
		m.ReadBytes += r.ReadBytes
		m.Errors += r.Errors
	}
	sort.Slice(keys, func(i, j int) bool { return merged[keys[i]].TotalTime > merged[keys[j]].TotalTime })
	var res []prometheus.Metric
	for _, k := range keys {
		m := merged[k]
		res = append(res,
			common.Gauge(dTopQueryCalls, m.Calls/window, k.db, k.query),
			common.Gauge(dTopQueryTime, m.TotalTime/window, k.db, k.query),
			common.Gauge(dTopQueryReadRows, m.ReadRows/window, k.db, k.query),
			common.Gauge(dTopQueryReadBytes, m.ReadBytes/window, k.db, k.query),
			common.Gauge(dTopQueryErrors, m.Errors/window, k.db, k.query),
		)
	}
	return res
}

func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
