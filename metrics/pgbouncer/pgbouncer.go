// Package pgbouncer collects the metrics of PgBouncer from its admin console (the virtual "pgbouncer" database)
// using SHOW STATS, SHOW POOLS and SHOW LISTS. The metric names mirror prometheus-community/pgbouncer_exporter.
//
// The admin console only supports the simple query protocol, so the queries are run without arguments
// (lib/pq then uses the simple protocol instead of preparing statements), and only the startup parameters
// PgBouncer tracks are sent (no statement_timeout: the collection deadline is enforced through the context).
package pgbouncer

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/logger"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	upDesc = common.Desc("pgbouncer_up", "Whether the PgBouncer admin console is reachable (1) or not (0)")
)

type column struct {
	desc  *prometheus.Desc
	vt    prometheus.ValueType
	scale float64 // multiplier applied to the value (e.g. 1e-6 for microseconds)
}

func counter(name, help string, scale float64, labels ...string) column {
	return column{desc: common.Desc(name, help, labels...), vt: prometheus.CounterValue, scale: scale}
}

func gauge(name, help string, scale float64, labels ...string) column {
	return column{desc: common.Desc(name, help, labels...), vt: prometheus.GaugeValue, scale: scale}
}

const us = 1e-6

// SHOW STATS: the totals per database (the averages are derived from them and not exported).
var statsColumns = map[string]column{
	"total_query_count":             counter("pgbouncer_stats_queries_pooled_total", "Total number of SQL queries pooled by pgbouncer", 1, "database"),
	"total_requests":                counter("pgbouncer_stats_queries_pooled_total", "Total number of SQL queries pooled by pgbouncer", 1, "database"), // PgBouncer < 1.8
	"total_query_time":              counter("pgbouncer_stats_queries_duration_seconds_total", "Total number of seconds spent by pgbouncer when actively connected to PostgreSQL, executing queries", us, "database"),
	"total_xact_count":              counter("pgbouncer_stats_sql_transactions_pooled_total", "Total number of SQL transactions pooled by pgbouncer", 1, "database"),
	"total_xact_time":               counter("pgbouncer_stats_server_in_transaction_seconds_total", "Total number of seconds spent by pgbouncer when connected to PostgreSQL in a transaction, either idle in transaction or executing queries", us, "database"),
	"total_wait_time":               counter("pgbouncer_stats_client_wait_seconds_total", "Time spent by clients waiting for a server in seconds", us, "database"),
	"total_received":                counter("pgbouncer_stats_received_bytes_total", "Total volume in bytes of network traffic received by pgbouncer", 1, "database"),
	"total_sent":                    counter("pgbouncer_stats_sent_bytes_total", "Total volume in bytes of network traffic sent by pgbouncer", 1, "database"),
	"total_server_assignment_count": counter("pgbouncer_stats_server_assignments_total", "Total number of times a server was assigned to a client", 1, "database"),
}

// SHOW POOLS: a row per (database, user) pool.
var poolsColumns = map[string]column{
	"cl_active":             gauge("pgbouncer_pools_client_active_connections", "Client connections linked to server connection and able to process queries", 1, "database", "user"),
	"cl_waiting":            gauge("pgbouncer_pools_client_waiting_connections", "Client connections waiting on a server connection", 1, "database", "user"),
	"cl_active_cancel_req":  gauge("pgbouncer_pools_client_active_cancel_connections", "Client connections that have forwarded query cancellations to the server and are waiting for the server response", 1, "database", "user"),
	"cl_waiting_cancel_req": gauge("pgbouncer_pools_client_waiting_cancel_connections", "Client connections that have not forwarded query cancellations to the server yet", 1, "database", "user"),
	"sv_active":             gauge("pgbouncer_pools_server_active_connections", "Server connections linked to a client connection", 1, "database", "user"),
	"sv_active_cancel":      gauge("pgbouncer_pools_server_active_cancel_connections", "Server connections that are currently forwarding a cancel request", 1, "database", "user"),
	"sv_being_canceled":     gauge("pgbouncer_pools_server_being_canceled_connections", "Servers that normally could become idle but are waiting to do so until all in-flight cancel requests have completed", 1, "database", "user"),
	"sv_idle":               gauge("pgbouncer_pools_server_idle_connections", "Server connections idle and ready for a client query", 1, "database", "user"),
	"sv_used":               gauge("pgbouncer_pools_server_used_connections", "Server connections idle more than server_check_delay, needing server_check_query", 1, "database", "user"),
	"sv_tested":             gauge("pgbouncer_pools_server_testing_connections", "Server connections currently running either server_reset_query or server_check_query", 1, "database", "user"),
	"sv_login":              gauge("pgbouncer_pools_server_login_connections", "Server connections currently in the process of logging in", 1, "database", "user"),
}

var poolsMaxWait = gauge("pgbouncer_pools_client_maxwait_seconds", "Age of oldest unserved client connection in seconds", 1, "database", "user")

// SHOW LISTS: a row per internal list (list, items).
var listsColumns = map[string]column{
	"databases":     gauge("pgbouncer_databases", "Count of databases", 1),
	"users":         gauge("pgbouncer_users", "Count of users", 1),
	"pools":         gauge("pgbouncer_pools", "Count of pools", 1),
	"free_clients":  gauge("pgbouncer_free_clients", "Count of free clients", 1),
	"used_clients":  gauge("pgbouncer_used_clients", "Count of used clients", 1),
	"login_clients": gauge("pgbouncer_login_clients", "Count of clients in login state", 1),
	"free_servers":  gauge("pgbouncer_free_servers", "Count of free servers", 1),
	"used_servers":  gauge("pgbouncer_used_servers", "Count of used servers", 1),
	"dns_names":     gauge("pgbouncer_cached_dns_names", "Count of DNS names in the cache", 1),
	"dns_zones":     gauge("pgbouncer_cached_dns_zones", "Count of DNS zones in the cache", 1),
	"dns_queries":   gauge("pgbouncer_in_flight_dns_queries", "Count of in-flight DNS queries", 1),
}

// Row is a result row of an admin console command, by column name.
type Row map[string]string

func (r Row) float(col string) (float64, bool) {
	s, ok := r[col]
	if !ok || s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	return v, err == nil
}

func (r Row) metrics(columns map[string]column, labels ...string) []prometheus.Metric {
	var res []prometheus.Metric
	for name, c := range columns {
		if v, ok := r.float(name); ok {
			res = append(res, prometheus.MustNewConstMetric(c.desc, c.vt, v*c.scale, labels...))
		}
	}
	return res
}

// StatsMetrics maps a SHOW STATS row to the metrics of the database.
func StatsMetrics(r Row) []prometheus.Metric {
	return r.metrics(statsColumns, r["database"])
}

// PoolMetrics maps a SHOW POOLS row to the metrics of the pool.
func PoolMetrics(r Row) []prometheus.Metric {
	res := r.metrics(poolsColumns, r["database"], r["user"])
	if maxwait, ok := r.float("maxwait"); ok {
		maxwaitUs, _ := r.float("maxwait_us") // PgBouncer >= 1.8
		res = append(res, prometheus.MustNewConstMetric(poolsMaxWait.desc, poolsMaxWait.vt, maxwait+maxwaitUs*us, r["database"], r["user"]))
	}
	return res
}

// ListMetrics maps a SHOW LISTS row to the corresponding metric (nil if the list is unknown).
func ListMetrics(r Row) []prometheus.Metric {
	c, ok := listsColumns[r["list"]]
	if !ok {
		return nil
	}
	v, ok := r.float("items")
	if !ok {
		return nil
	}
	return []prometheus.Metric{prometheus.MustNewConstMetric(c.desc, c.vt, v)}
}

type Collector struct {
	db      *sql.DB
	timeout time.Duration
	logger  logger.Logger
}

// New creates a collector for the admin console at dsn (the database must be "pgbouncer").
func New(dsn string, collectTimeout time.Duration, logger logger.Logger) (*Collector, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(10 * time.Minute)
	return &Collector{db: db, timeout: collectTimeout, logger: logger}, nil
}

func (c *Collector) Close() error {
	return c.db.Close()
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- upDesc
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	up := 1.
	for _, q := range []struct {
		cmd     string
		metrics func(Row) []prometheus.Metric
	}{
		{"SHOW STATS", StatsMetrics},
		{"SHOW POOLS", PoolMetrics},
		{"SHOW LISTS", ListMetrics},
	} {
		rows, err := c.query(ctx, q.cmd)
		if err != nil {
			c.logger.Warning(err)
			up = 0
			break
		}
		for _, r := range rows {
			for _, m := range q.metrics(r) {
				ch <- m
			}
		}
	}
	ch <- common.Gauge(upDesc, up)
}

// query runs an admin console command. It has no arguments, so lib/pq sends it with the simple query protocol.
func (c *Collector) query(ctx context.Context, cmd string) ([]Row, error) {
	rows, err := c.db.QueryContext(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", cmd, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", cmd, err)
	}
	var res []Row
	values := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range values {
		dest[i] = &values[i]
	}
	for rows.Next() {
		if err = rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("%s: %w", cmd, err)
		}
		r := make(Row, len(cols))
		for i, col := range cols {
			if values[i].Valid {
				r[col] = values[i].String
			}
		}
		res = append(res, r)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", cmd, err)
	}
	return res, nil
}
