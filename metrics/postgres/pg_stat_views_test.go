package postgres

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testMetric struct {
	name   string
	labels string // "k=v,k=v" sorted by name
	value  float64
	gauge  bool
}

func drain(f func(ch chan<- prometheus.Metric)) []testMetric {
	ch := make(chan prometheus.Metric, 1000)
	f(ch)
	close(ch)
	var res []testMetric
	for m := range ch {
		var pb dto.Metric
		if err := m.Write(&pb); err != nil {
			panic(err)
		}
		var labels []string
		for _, l := range pb.GetLabel() {
			labels = append(labels, l.GetName()+"="+l.GetValue())
		}
		sort.Strings(labels)
		tm := testMetric{name: metricName(m.Desc()), labels: strings.Join(labels, ",")}
		if pb.Gauge != nil {
			tm.value, tm.gauge = pb.Gauge.GetValue(), true
		} else {
			tm.value = pb.Counter.GetValue()
		}
		res = append(res, tm)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].name+res[i].labels < res[j].name+res[j].labels
	})
	return res
}

func metricName(d *prometheus.Desc) string {
	s := d.String()
	s = s[strings.Index(s, `fqName: "`)+len(`fqName: "`):]
	return s[:strings.Index(s, `"`)]
}

func selectedColumns(cols []statColumn) []string {
	var res []string
	for _, c := range cols {
		res = append(res, c.expr)
	}
	return res
}

func Test_statView_versionGating(t *testing.T) {
	for _, tc := range []struct {
		major           uint64
		checksums       bool
		sessions        bool
		expectedColumns int
	}{
		{major: 9, expectedColumns: 13},
		{major: 11, expectedColumns: 13},
		{major: 12, checksums: true, expectedColumns: 14},
		{major: 13, checksums: true, expectedColumns: 14},
		{major: 14, checksums: true, sessions: true, expectedColumns: 21},
		{major: 17, checksums: true, sessions: true, expectedColumns: 21},
	} {
		t.Run(fmt.Sprint(tc.major), func(t *testing.T) {
			q, cols := pgStatDatabaseView.query(tc.major)
			assert.Len(t, cols, tc.expectedColumns)
			assert.Equal(t, tc.checksums, strings.Contains(q, "checksum_failures"))
			assert.Equal(t, tc.sessions, strings.Contains(q, "sessions_abandoned"))
			assert.Equal(t, tc.sessions, strings.Contains(q, "idle_in_transaction_time"))
			assert.True(t, strings.HasPrefix(q, "SELECT COALESCE((s.datname)::text, ''), (s.xact_commit)::float8,"), q)
		})
	}

	for _, tc := range []struct {
		view      *statView
		available map[uint64]bool
	}{
		{pgStatDatabaseView, map[uint64]bool{9: true, 10: true, 18: true}},
		{pgStatReplicationView, map[uint64]bool{9: false, 10: true, 17: true}},
		{pgStatSubscriptionView(10), map[uint64]bool{9: false, 10: true}},
		{pgStatSubscriptionStatsView, map[uint64]bool{14: false, 15: true}},
		{pgStatSlruView, map[uint64]bool{12: false, 13: true}},
		{pgStatWalView, map[uint64]bool{13: false, 14: true}},
		{pgStatIOView, map[uint64]bool{15: false, 16: true, 18: true}},
	} {
		for major, expected := range tc.available {
			assert.Equal(t, expected, tc.view.available(major), "%s on PG%d", tc.view.name, major)
		}
	}

	assert.NotContains(t, pgStatSubscriptionView(15).from, "leader_pid")
	assert.Contains(t, pgStatSubscriptionView(16).from, "leader_pid IS NULL")
	assert.Contains(t, pgStatSubscriptionView(16).from, "relid IS NULL")

	// a view without labels (pg_stat_wal)
	q, cols := pgStatWalView.query(16)
	assert.Equal(t, "SELECT (wal_records)::float8, (wal_fpi)::float8, (wal_bytes)::float8, (wal_buffers_full)::float8 FROM pg_stat_wal", q)
	assert.Equal(t, []string{"wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full"}, selectedColumns(cols))

	// the replication lag is 0 (not NULL) when the standby is caught up
	q, _ = pgStatReplicationView.query(16)
	assert.Contains(t, q, "COALESCE(EXTRACT(EPOCH FROM replay_lag), CASE WHEN replay_lsn IS NOT NULL THEN 0 END)")
	assert.Contains(t, q, "LIMIT 50")
}

func Test_statResult_emit(t *testing.T) {
	dCounter := desc("test_total", "", "db")
	dStage := desc("test_lag_seconds", "", "app", "stage")
	nf := func(v float64) sql.NullFloat64 { return sql.NullFloat64{Float64: v, Valid: true} }

	got := drain((&statResult{
		columns: []statColumn{{expr: "a", desc: dCounter}},
		rows: []statRow{
			{labels: []string{"db1"}, values: []sql.NullFloat64{nf(5)}},
			{labels: []string{"db2"}, values: []sql.NullFloat64{{}}}, // NULL: not emitted
		},
	}).emit)
	assert.Equal(t, []testMetric{{name: "test_total", labels: "db=db1", value: 5}}, got)

	got = drain((&statResult{
		columns: []statColumn{
			{expr: "w", desc: dStage, gauge: true, constLabels: []string{"write"}, scale: msToSeconds},
			{expr: "r", desc: dStage, gauge: true, constLabels: []string{"replay"}, scale: msToSeconds},
		},
		rows: []statRow{{labels: []string{"standby1"}, values: []sql.NullFloat64{nf(1500), nf(0)}}},
	}).emit)
	assert.Equal(t, []testMetric{
		{name: "test_lag_seconds", labels: "app=standby1,stage=replay", value: 0, gauge: true},
		{name: "test_lag_seconds", labels: "app=standby1,stage=write", value: 1.5, gauge: true},
	}, got)

	var nilResult *statResult
	assert.Empty(t, drain(nilResult.emit))
}

func Test_statViewDescs_consistentLabels(t *testing.T) {
	// every column must produce exactly the number of labels its desc declares
	for _, v := range statViews(18) {
		for _, c := range v.columns {
			n := len(v.labels) + len(c.constLabels)
			values := make([]sql.NullFloat64, 1)
			values[0] = sql.NullFloat64{Float64: 1, Valid: true}
			labels := make([]string, len(v.labels))
			r := &statResult{columns: []statColumn{c}, rows: []statRow{{labels: labels, values: values}}}
			require.NotPanics(t, func() { drain(r.emit) }, "%s: %s (%d labels)", v.name, c.expr, n)
		}
	}
	assert.NotEmpty(t, statViewDescs())
}

func Test_buildStatStatementsQuery(t *testing.T) {
	set := func(names ...string) map[string]bool {
		m := map[string]bool{}
		for _, n := range names {
			m[n] = true
		}
		return m
	}
	common := []string{"userid", "dbid", "queryid", "query", "calls", "rows",
		"shared_blks_hit", "shared_blks_read", "shared_blks_dirtied", "shared_blks_written",
		"local_blks_hit", "local_blks_read", "local_blks_dirtied", "local_blks_written",
		"temp_blks_read", "temp_blks_written"}
	pg94 := set(append(common, "total_time", "blk_read_time", "blk_write_time")...)
	pg12 := set(append(common, "total_time", "min_time", "max_time", "mean_time", "stddev_time", "blk_read_time", "blk_write_time")...)
	pg13 := set(append(common, "total_plan_time", "total_exec_time", "min_exec_time", "max_exec_time", "mean_exec_time", "blk_read_time", "blk_write_time", "wal_bytes")...)
	pg17 := set(append(common, "total_plan_time", "total_exec_time", "min_exec_time", "max_exec_time", "mean_exec_time",
		"shared_blk_read_time", "shared_blk_write_time", "local_blk_read_time", "local_blk_write_time", "temp_blk_read_time", "temp_blk_write_time", "wal_bytes")...)

	for _, tc := range []struct {
		name     string
		columns  map[string]bool
		contains []string
		absent   []string
	}{
		{
			name:     "9.4 (no min/max/mean)",
			columns:  pg94,
			contains: []string{"s.calls, s.total_time, s.blk_read_time + s.blk_write_time", "s.rows", "s.temp_blks_written, NULL::float8, NULL::float8, NULL::float8, NULL::float8, NULL::float8"},
			absent:   []string{"total_exec_time", "mean_time", "wal_bytes"},
		},
		{
			name:     "12",
			columns:  pg12,
			contains: []string{"s.calls, s.total_time, s.blk_read_time + s.blk_write_time", "NULL::float8, NULL::float8, s.mean_time, s.min_time, s.max_time"},
			absent:   []string{"total_exec_time", "total_plan_time", "wal_bytes"},
		},
		{
			name:     "13",
			columns:  pg13,
			contains: []string{"s.calls, s.total_plan_time + s.total_exec_time, s.blk_read_time + s.blk_write_time", "s.total_plan_time, s.wal_bytes, s.mean_exec_time, s.min_exec_time, s.max_exec_time"},
			absent:   []string{"s.total_time", "s.mean_time"},
		},
		{
			name:    "17",
			columns: pg17,
			contains: []string{"s.shared_blk_read_time + s.shared_blk_write_time + s.local_blk_read_time + s.local_blk_write_time + s.temp_blk_read_time + s.temp_blk_write_time",
				"s.wal_bytes, s.mean_exec_time"},
			absent: []string{"s.blk_read_time", "s.total_time"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, err := buildStatStatementsQuery(tc.columns, 1024)
			require.NoError(t, err)
			assert.Contains(t, q, "LEFT(s.query, 1024), s.queryid")
			for _, s := range tc.contains {
				assert.Contains(t, q, s)
			}
			for _, s := range tc.absent {
				assert.NotContains(t, q, s)
			}
			// 7 fixed columns + counters + mean/min/max (the scan order must not depend on the version)
			selectList := q[len("SELECT "):strings.Index(q, " FROM ")]
			selectList = strings.Replace(selectList, "LEFT(s.query, 1024)", "LEFT(s.query)", 1)
			assert.Len(t, strings.Split(selectList, ", "), 7+ssCountersN+3)
		})
	}

	_, err := buildStatStatementsQuery(set("calls", "query"), 100)
	assert.Error(t, err)
	_, err = buildStatStatementsQuery(set("queryid", "calls"), 100)
	assert.Error(t, err)
}

func Test_QuerySummary_statStatementsCounters(t *testing.T) {
	nf := func(v float64) sql.NullFloat64 { return sql.NullFloat64{Float64: v, Valid: true} }
	row := func(calls int64, totalTime, rows, walBytes float64, mean, min, max sql.NullFloat64) ssRow {
		r := ssRow{calls: sql.NullInt64{Int64: calls, Valid: true}, totalTime: nf(totalTime), ioTime: nf(0),
			meanExecTime: mean, minExecTime: min, maxExecTime: max}
		r.counters[ssRows] = nf(rows)
		r.counters[ssPlanTime] = nf(totalTime / 10)
		if walBytes >= 0 {
			r.counters[ssWalBytes] = nf(walBytes)
		}
		return r
	}

	s := &QuerySummary{}
	// two statements with the same obfuscated text are merged
	s.updateFromStatStatements(row(110, 2000, 1100, -1, nf(10), nf(1), nf(50)), row(100, 1000, 1000, -1, nf(9), nf(1), nf(40)))
	s.updateFromStatStatements(row(30, 900, 300, -1, nf(30), nf(0.5), nf(200)), ssRow{}) // new statement

	assert.Equal(t, 40.0, s.Queries)
	assert.InDelta(t, 1.9, s.TotalTime, 1e-9)
	assert.Equal(t, 400.0, s.Counters[ssRows])
	assert.InDelta(t, 190.0, s.Counters[ssPlanTime], 1e-9)
	assert.True(t, s.hasCounter[ssRows])
	assert.False(t, s.hasCounter[ssWalBytes], "wal_bytes is not available (PG<13)")
	mean, ok := s.ExecTimeMean()
	assert.True(t, ok)
	assert.InDelta(t, (10*110+30*30)/140.0, mean, 1e-9)
	assert.Equal(t, 0.5, s.execMin.Float64)
	assert.Equal(t, 200.0, s.execMax.Float64)

	// a stats reset: negative deltas are ignored
	reset := &QuerySummary{}
	reset.updateFromStatStatements(row(1, 10, 1, 5, nf(10), nf(10), nf(10)), row(100, 1000, 1000, 500, nf(9), nf(1), nf(40)))
	assert.Equal(t, 0.0, reset.Queries)
	assert.Equal(t, 0.0, reset.Counters[ssRows])
	assert.True(t, reset.hasCounter[ssWalBytes])

	got := drain(func(ch chan<- prometheus.Metric) {
		s.extraMetrics(ch, 10e9, QueryKey{DB: "db", User: "u", Query: "select ?"})
	})
	byName := map[string]float64{}
	for _, m := range got {
		assert.True(t, m.gauge)
		assert.Equal(t, "db=db,query=select ?,user=u", m.labels)
		byName[m.name] = m.value
	}
	assert.Equal(t, 40.0, byName["pg_top_query_rows_per_second"])
	assert.InDelta(t, 0.019, byName["pg_top_query_plan_time_per_second"], 1e-9)
	assert.Equal(t, 0.0, byName["pg_top_query_shared_blks_hit_per_second"])
	assert.NotContains(t, byName, "pg_top_query_wal_bytes_per_second")
	assert.InDelta(t, mean/1000, byName["pg_top_query_exec_time_mean_seconds"], 1e-12)
	assert.Equal(t, 0.0005, byName["pg_top_query_exec_time_min_seconds"])
	assert.Equal(t, 0.2, byName["pg_top_query_exec_time_max_seconds"])

	// no mean/min/max on pg_stat_statements 1.2 (PG9.4)
	old := &QuerySummary{}
	old.updateFromStatStatements(row(1, 1, 1, -1, sql.NullFloat64{}, sql.NullFloat64{}, sql.NullFloat64{}), ssRow{})
	for _, m := range drain(func(ch chan<- prometheus.Metric) { old.extraMetrics(ch, 10e9, QueryKey{}) }) {
		assert.NotContains(t, m.name, "exec_time")
	}
}

func Test_countWaitEvent(t *testing.T) {
	ns := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	null := sql.NullString{}
	m := map[waitEventKey]float64{}
	for _, tc := range []struct {
		state, typ, event sql.NullString
	}{
		{ns("active"), ns("Lock"), ns("transactionid")},
		{ns("active"), ns("Lock"), ns("transactionid")},
		{ns("idle in transaction"), ns("Client"), ns("ClientRead")},
		{ns("active"), ns("IO"), ns("DataFileRead")},
		{ns("active"), null, null},                       // on CPU
		{ns("idle"), ns("Client"), ns("ClientRead")},     // idle
		{null, ns("Activity"), ns("AutoVacuumMain")},     // background process main loop
		{ns("active"), ns("Timeout"), ns("VacuumDelay")}, // autovacuum throttling
		{ns("idle in transaction (aborted)"), ns("Client"), ns("ClientRead")},
	} {
		countWaitEvent(m, tc.state, tc.typ, tc.event)
	}
	assert.Equal(t, map[waitEventKey]float64{
		{eventType: "Lock", event: "transactionid"}:  2,
		{eventType: "Client", event: "ClientRead"}:   2,
		{eventType: "IO", event: "DataFileRead"}:     1,
		{eventType: "Timeout", event: "VacuumDelay"}: 1,
	}, m)
}

func Test_trimUnusedIndexes(t *testing.T) {
	valid := func(entries ...bloatEntry) sql.Null[unusedIndexes] {
		var total float64
		for _, e := range entries {
			total += e.Bytes
		}
		return sql.Null[unusedIndexes]{V: unusedIndexes{count: float64(len(entries)), bytes: total, top: entries}, Valid: true}
	}
	stats := map[string]*dbIndexStats{
		"db1": {unused: valid(bloatEntry{"public", "t1", "i1", 100}, bloatEntry{"public", "t1", "i2", 10})},
		"db2": {unused: valid(bloatEntry{"public", "t2", "i3", 50}, bloatEntry{"public", "t2", "i4", 5})},
		"db3": {duplicates: sql.Null[float64]{V: 2, Valid: true}},
		"db4": {unused: valid()},
	}
	trimUnusedIndexes(stats, 3)
	assert.Equal(t, []bloatEntry{{"public", "t1", "i1", 100}, {"public", "t1", "i2", 10}}, stats["db1"].unused.V.top)
	assert.Equal(t, []bloatEntry{{"public", "t2", "i3", 50}}, stats["db2"].unused.V.top)
	assert.Equal(t, 2.0, stats["db2"].unused.V.count, "the per-db count is not trimmed")
	assert.Equal(t, 55.0, stats["db2"].unused.V.bytes)
	assert.False(t, stats["db3"].unused.Valid)
	assert.Empty(t, stats["db4"].unused.V.top)
}

func Test_errorReason_viewNotFound(t *testing.T) {
	assert.Equal(t, dbtracker.ErrorReasonNotFound, errorReason(fmt.Errorf("pg_stat_statements: %w", errViewNotFound)))
}
