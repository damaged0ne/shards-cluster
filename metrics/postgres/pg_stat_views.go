package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// statColumn maps one column (SQL expression) of a statistics view to a metric.
type statColumn struct {
	expr        string
	desc        *prometheus.Desc
	minMajor    uint64   // the column is only queried on servers of this major version or newer
	scale       float64  // multiplier applied to the value (0 means 1), e.g. 0.001 for ms -> s
	gauge       bool     // counter by default
	constLabels []string // appended to the row labels (e.g. a fixed "stage" label)
}

// statView describes a query against a statistics view: every row produces one
// metric per column, labeled by the row's label expressions.
type statView struct {
	name     string   // for logs only
	minMajor uint64   // the view is not queried on older servers
	labels   []string // SQL expressions for the label values (NULLs become "")
	from     string   // FROM ... [WHERE ...] [ORDER BY ... LIMIT ...]
	dbLabel  bool     // the first label is a database name (rows of excluded databases are dropped)
	columns  []statColumn
}

type statRow struct {
	labels []string
	values []sql.NullFloat64
}

// statResult is an immutable result of a statView query.
type statResult struct {
	columns []statColumn
	rows    []statRow
}

// available reports whether the view exists on the server of the given major version.
func (v *statView) available(major uint64) bool {
	return major >= v.minMajor
}

// query builds the SQL query for the given server major version and returns
// the columns it selects (in order, after the label expressions).
func (v *statView) query(major uint64) (string, []statColumn) {
	var cols []statColumn
	exprs := make([]string, 0, len(v.labels)+len(v.columns))
	for _, l := range v.labels {
		exprs = append(exprs, fmt.Sprintf("COALESCE((%s)::text, '')", l))
	}
	for _, c := range v.columns {
		if major < c.minMajor {
			continue
		}
		cols = append(cols, c)
		exprs = append(exprs, fmt.Sprintf("(%s)::float8", c.expr))
	}
	return "SELECT " + strings.Join(exprs, ", ") + " FROM " + v.from, cols
}

func (c *Collector) queryStatView(ctx context.Context, v *statView, major uint64) (*statResult, error) {
	if !v.available(major) {
		return nil, nil
	}
	query, cols := v.query(major)
	if len(cols) == 0 {
		return nil, nil
	}
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", v.name, err)
	}
	defer rows.Close()
	res := &statResult{columns: cols}
	for rows.Next() {
		labels := make([]string, len(v.labels))
		values := make([]sql.NullFloat64, len(cols))
		dest := make([]any, 0, len(labels)+len(values))
		for i := range labels {
			dest = append(dest, &labels[i])
		}
		for i := range values {
			dest = append(dest, &values[i])
		}
		if err := rows.Scan(dest...); err != nil {
			c.logger.Warning("failed to scan", v.name, "row:", err)
			continue
		}
		if v.dbLabel && len(labels) > 0 && c.excludeDatabases[labels[0]] {
			continue
		}
		res.rows = append(res.rows, statRow{labels: labels, values: values})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", v.name, err)
	}
	return res, nil
}

// emit sends one metric per non-NULL value.
func (r *statResult) emit(ch chan<- prometheus.Metric) {
	if r == nil {
		return
	}
	for _, row := range r.rows {
		for i, col := range r.columns {
			v := row.values[i]
			if !v.Valid {
				continue
			}
			value := v.Float64
			if col.scale != 0 {
				value *= col.scale
			}
			labels := row.labels
			if len(col.constLabels) > 0 {
				labels = append(append(make([]string, 0, len(labels)+len(col.constLabels)), labels...), col.constLabels...)
			}
			if col.gauge {
				ch <- gauge(col.desc, value, labels...)
			} else {
				ch <- counter(col.desc, value, labels...)
			}
		}
	}
}

func (v *statView) descs() []*prometheus.Desc {
	seen := map[*prometheus.Desc]bool{}
	var res []*prometheus.Desc
	for _, c := range v.columns {
		if !seen[c.desc] {
			seen[c.desc] = true
			res = append(res, c.desc)
		}
	}
	return res
}

const (
	msToSeconds        = 0.001
	maxStandbys        = 50
	maxSubscriptions   = 100
	notInRecoveryOrLsn = `CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn() ELSE pg_current_wal_lsn() END`
)

// pg_stat_database: per-database counters (pg_db_* scheme: pg_db_<column>_total{db}).
var pgStatDatabaseView = &statView{
	name:     "pg_stat_database",
	minMajor: 9,
	labels:   []string{"s.datname"},
	from:     "pg_stat_database s JOIN pg_database d ON d.oid = s.datid WHERE NOT d.datistemplate",
	dbLabel:  true,
	columns: []statColumn{
		{expr: "s.xact_commit", desc: desc("pg_db_xact_commit_total", "Number of transactions in the database that have been committed", "db")},
		{expr: "s.xact_rollback", desc: desc("pg_db_xact_rollback_total", "Number of transactions in the database that have been rolled back", "db")},
		{expr: "s.blks_read", desc: desc("pg_db_blks_read_total", "Number of disk blocks read in the database (not found in shared buffers)", "db")},
		{expr: "s.blks_hit", desc: desc("pg_db_blks_hit_total", "Number of times disk blocks were found already in shared buffers", "db")},
		{expr: "s.tup_returned", desc: desc("pg_db_tup_returned_total", "Number of live rows fetched by sequential scans and index entries returned by index scans", "db")},
		{expr: "s.tup_fetched", desc: desc("pg_db_tup_fetched_total", "Number of live rows fetched by index scans", "db")},
		{expr: "s.tup_inserted", desc: desc("pg_db_tup_inserted_total", "Number of rows inserted by queries in the database", "db")},
		{expr: "s.tup_updated", desc: desc("pg_db_tup_updated_total", "Number of rows updated by queries in the database", "db")},
		{expr: "s.tup_deleted", desc: desc("pg_db_tup_deleted_total", "Number of rows deleted by queries in the database", "db")},
		{expr: "s.conflicts", desc: desc("pg_db_conflicts_total", "Number of queries canceled due to conflicts with recovery (standbys only)", "db")},
		{expr: "s.temp_files", desc: desc("pg_db_temp_files_total", "Number of temporary files created by queries", "db")},
		{expr: "s.temp_bytes", desc: desc("pg_db_temp_bytes_total", "Total amount of data written to temporary files by queries", "db")},
		{expr: "s.deadlocks", desc: desc("pg_db_deadlocks_total", "Number of deadlocks detected in the database", "db")},
		{expr: "s.checksum_failures", minMajor: 12, desc: desc("pg_db_checksum_failures_total", "Number of data page checksum failures detected (only if data checksums are enabled)", "db")},
		{expr: "s.sessions", minMajor: 14, desc: desc("pg_db_sessions_total", "Number of sessions established to the database", "db")},
		{expr: "s.sessions_abandoned", minMajor: 14, desc: desc("pg_db_sessions_abandoned_total", "Number of sessions terminated because the connection to the client was lost", "db")},
		{expr: "s.sessions_fatal", minMajor: 14, desc: desc("pg_db_sessions_fatal_total", "Number of sessions terminated by fatal errors", "db")},
		{expr: "s.sessions_killed", minMajor: 14, desc: desc("pg_db_sessions_killed_total", "Number of sessions terminated by operator intervention", "db")},
		{expr: "s.session_time", minMajor: 14, scale: msToSeconds, desc: desc("pg_db_session_time_seconds_total", "Time spent by sessions in the database", "db")},
		{expr: "s.active_time", minMajor: 14, scale: msToSeconds, desc: desc("pg_db_active_time_seconds_total", "Time spent executing SQL statements in the database", "db")},
		{expr: "s.idle_in_transaction_time", minMajor: 14, scale: msToSeconds, desc: desc("pg_db_idle_in_transaction_time_seconds_total", "Time spent idling while in a transaction in the database", "db")},
	},
}

var (
	dStandbyLagSeconds = desc("pg_replication_standby_lag_seconds", "Replication lag of a standby as measured by the primary (0 when the standby is caught up)", "application_name", "client_addr", "stage")
	dStandbyLagBytes   = desc("pg_replication_standby_lag_bytes", "Amount of WAL the standby is behind the primary at each stage", "application_name", "client_addr", "stage")
	dStandbyInfo       = desc("pg_replication_standby_info", "Connected standby (WAL sender) with its state and synchronous state", "application_name", "client_addr", "state", "sync_state")
)

// lagSeconds returns the *_lag interval in seconds. The interval is NULL when the
// standby is caught up and idle, in which case the lag is 0 as long as the matching
// LSN is visible (NULL LSNs mean insufficient privileges: report nothing).
func lagSeconds(lag, lsn string) string {
	return fmt.Sprintf("COALESCE(EXTRACT(EPOCH FROM %s), CASE WHEN %s IS NOT NULL THEN 0 END)", lag, lsn)
}

func lsnLagBytes(lsn string) string {
	return fmt.Sprintf("pg_wal_lsn_diff(%s, %s)", notInRecoveryOrLsn, lsn)
}

// pg_stat_replication (primary side, one row per WAL sender).
var pgStatReplicationView = &statView{
	name:     "pg_stat_replication",
	minMajor: 10,
	labels:   []string{"application_name", "host(client_addr)"},
	from:     fmt.Sprintf("pg_stat_replication ORDER BY application_name, client_addr LIMIT %d", maxStandbys),
	columns: []statColumn{
		{expr: lagSeconds("write_lag", "write_lsn"), gauge: true, desc: dStandbyLagSeconds, constLabels: []string{"write"}},
		{expr: lagSeconds("flush_lag", "flush_lsn"), gauge: true, desc: dStandbyLagSeconds, constLabels: []string{"flush"}},
		{expr: lagSeconds("replay_lag", "replay_lsn"), gauge: true, desc: dStandbyLagSeconds, constLabels: []string{"replay"}},
		{expr: lsnLagBytes("sent_lsn"), gauge: true, desc: dStandbyLagBytes, constLabels: []string{"sent"}},
		{expr: lsnLagBytes("write_lsn"), gauge: true, desc: dStandbyLagBytes, constLabels: []string{"write"}},
		{expr: lsnLagBytes("flush_lsn"), gauge: true, desc: dStandbyLagBytes, constLabels: []string{"flush"}},
		{expr: lsnLagBytes("replay_lsn"), gauge: true, desc: dStandbyLagBytes, constLabels: []string{"replay"}},
	},
}

var pgStandbyInfoView = &statView{
	name:     "pg_stat_replication",
	minMajor: 10,
	labels:   []string{"application_name", "host(client_addr)", "state", "sync_state"},
	from:     fmt.Sprintf("pg_stat_replication ORDER BY application_name, client_addr LIMIT %d", maxStandbys),
	columns:  []statColumn{{expr: "1", gauge: true, desc: dStandbyInfo}},
}

var (
	dSubscriptionWorkerUp     = desc("pg_subscription_worker_up", "1 if the apply worker of the logical replication subscription is running", "subscription")
	dSubscriptionLastMsgAge   = desc("pg_subscription_last_msg_receipt_age_seconds", "Time since the last message was received from the publisher", "subscription")
	dSubscriptionTransportLag = desc("pg_subscription_last_msg_delay_seconds", "Delay between sending the last message by the publisher and receiving it (includes clock skew between the servers)", "subscription")
	dSubscriptionLatestEndAge = desc("pg_subscription_latest_end_age_seconds", "Time since the last write-ahead log location was reported to the publisher", "subscription")
	dSubscriptionErrors       = desc("pg_subscription_errors_total", "Number of errors that occurred while applying changes (type=apply) or during the initial table synchronization (type=sync)", "subscription", "type")
	subscriptionColumns       = []statColumn{
		{expr: "CASE WHEN pid IS NULL THEN 0 ELSE 1 END", gauge: true, desc: dSubscriptionWorkerUp},
		{expr: "EXTRACT(EPOCH FROM now() - last_msg_receipt_time)", gauge: true, desc: dSubscriptionLastMsgAge},
		{expr: "EXTRACT(EPOCH FROM last_msg_receipt_time - last_msg_send_time)", gauge: true, desc: dSubscriptionTransportLag},
		{expr: "EXTRACT(EPOCH FROM now() - latest_end_time)", gauge: true, desc: dSubscriptionLatestEndAge},
	}
)

// pgStatSubscriptionView returns the view for the leader apply worker of each subscription
// (table sync workers have relid set; parallel apply workers (PG16+) have leader_pid set).
func pgStatSubscriptionView(major uint64) *statView {
	where := "relid IS NULL"
	if major >= 16 {
		where += " AND leader_pid IS NULL"
	}
	return &statView{
		name:     "pg_stat_subscription",
		minMajor: 10,
		labels:   []string{"subname"},
		from:     fmt.Sprintf("pg_stat_subscription WHERE %s ORDER BY subname LIMIT %d", where, maxSubscriptions),
		columns:  subscriptionColumns,
	}
}

var pgStatSubscriptionStatsView = &statView{
	name:     "pg_stat_subscription_stats",
	minMajor: 15,
	labels:   []string{"subname"},
	from:     fmt.Sprintf("pg_stat_subscription_stats ORDER BY subname LIMIT %d", maxSubscriptions),
	columns: []statColumn{
		{expr: "apply_error_count", desc: dSubscriptionErrors, constLabels: []string{"apply"}},
		{expr: "sync_error_count", desc: dSubscriptionErrors, constLabels: []string{"sync"}},
	},
}

// pg_stat_io (PG16+): the set of (backend_type, object, context) combinations is fixed by the server.
var pgStatIOView = &statView{
	name:     "pg_stat_io",
	minMajor: 16,
	labels:   []string{"backend_type", "object", "context"},
	from:     "pg_stat_io",
	columns: []statColumn{
		{expr: "reads", desc: desc("pg_io_reads_total", "Number of read operations", "backend_type", "object", "context")},
		{expr: "read_time", scale: msToSeconds, desc: desc("pg_io_read_time_seconds_total", "Time spent in read operations (requires track_io_timing)", "backend_type", "object", "context")},
		{expr: "writes", desc: desc("pg_io_writes_total", "Number of write operations", "backend_type", "object", "context")},
		{expr: "write_time", scale: msToSeconds, desc: desc("pg_io_write_time_seconds_total", "Time spent in write operations (requires track_io_timing)", "backend_type", "object", "context")},
		{expr: "writebacks", desc: desc("pg_io_writebacks_total", "Number of units of size BLCKSZ which the process requested the kernel write out to permanent storage", "backend_type", "object", "context")},
		{expr: "extends", desc: desc("pg_io_extends_total", "Number of relation extend operations", "backend_type", "object", "context")},
		{expr: "extend_time", scale: msToSeconds, desc: desc("pg_io_extend_time_seconds_total", "Time spent in extend operations (requires track_io_timing)", "backend_type", "object", "context")},
		{expr: "hits", desc: desc("pg_io_hits_total", "Number of times a desired block was found in a shared buffer", "backend_type", "object", "context")},
		{expr: "evictions", desc: desc("pg_io_evictions_total", "Number of times a block has been written out from a shared or local buffer in order to make it available for another use", "backend_type", "object", "context")},
		{expr: "reuses", desc: desc("pg_io_reuses_total", "Number of times an existing buffer in a size-limited ring buffer was reused", "backend_type", "object", "context")},
		{expr: "fsyncs", desc: desc("pg_io_fsyncs_total", "Number of fsync calls", "backend_type", "object", "context")},
		{expr: "fsync_time", scale: msToSeconds, desc: desc("pg_io_fsync_time_seconds_total", "Time spent in fsync operations (requires track_io_timing)", "backend_type", "object", "context")},
	},
}

// pg_stat_slru (PG13+): one row per SLRU cache (a fixed set).
var pgStatSlruView = &statView{
	name:     "pg_stat_slru",
	minMajor: 13,
	labels:   []string{"name"},
	from:     "pg_stat_slru",
	columns: []statColumn{
		{expr: "blks_zeroed", desc: desc("pg_slru_blks_zeroed_total", "Number of blocks zeroed during initializations", "name")},
		{expr: "blks_hit", desc: desc("pg_slru_blks_hit_total", "Number of times disk blocks were found already in the SLRU", "name")},
		{expr: "blks_read", desc: desc("pg_slru_blks_read_total", "Number of disk blocks read for this SLRU", "name")},
		{expr: "blks_written", desc: desc("pg_slru_blks_written_total", "Number of disk blocks written for this SLRU", "name")},
		{expr: "blks_exists", desc: desc("pg_slru_blks_exists_total", "Number of blocks checked for existence for this SLRU", "name")},
		{expr: "flushes", desc: desc("pg_slru_flushes_total", "Number of flushes of dirty data for this SLRU", "name")},
		{expr: "truncates", desc: desc("pg_slru_truncates_total", "Number of truncates for this SLRU", "name")},
	},
}

// pg_stat_wal (PG14+).
var pgStatWalView = &statView{
	name:     "pg_stat_wal",
	minMajor: 14,
	from:     "pg_stat_wal",
	columns: []statColumn{
		{expr: "wal_records", desc: desc("pg_wal_records_total", "Total number of WAL records generated")},
		{expr: "wal_fpi", desc: desc("pg_wal_fpi_total", "Total number of WAL full page images generated")},
		{expr: "wal_bytes", desc: desc("pg_wal_bytes_total", "Total amount of WAL generated in bytes")},
		{expr: "wal_buffers_full", desc: desc("pg_wal_buffers_full_total", "Number of times WAL data was written to disk because WAL buffers became full")},
	},
}

// statViews returns all the views collected on every snapshot for the given server.
func statViews(major uint64) []*statView {
	return []*statView{
		pgStatDatabaseView,
		pgStatReplicationView,
		pgStandbyInfoView,
		pgStatSubscriptionView(major),
		pgStatSubscriptionStatsView,
		pgStatIOView,
		pgStatSlruView,
		pgStatWalView,
	}
}

func statViewDescs() []*prometheus.Desc {
	var res []*prometheus.Desc
	seen := map[*prometheus.Desc]bool{}
	for _, v := range statViews(0) {
		for _, d := range v.descs() {
			if !seen[d] {
				seen[d] = true
				res = append(res, d)
			}
		}
	}
	return res
}

// getStatViews queries every view available on the server. A failing view (missing on a fork,
// insufficient privileges, ...) is recorded as a scrape error and doesn't affect the others.
func (c *Collector) getStatViews(ctx context.Context, major uint64, st *pgState) {
	views := statViews(major)
	res := make([]*statResult, 0, len(views))
	for _, v := range views {
		r, err := c.queryStatView(ctx, v, major)
		if err != nil {
			c.addScrapeError(st, err)
			continue
		}
		if r != nil {
			res = append(res, r)
		}
	}
	st.statViews = res
}
