package clickhouse

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
)

// mapping maps a row of system.metrics / system.events / system.asynchronous_metrics to a metric.
type mapping struct {
	desc   *prometheus.Desc
	vt     prometheus.ValueType
	scale  float64
	labels []string
}

func gauge(desc *prometheus.Desc, labels ...string) mapping {
	return mapping{desc: desc, vt: prometheus.GaugeValue, scale: 1, labels: labels}
}

func counter(desc *prometheus.Desc, scale float64, labels ...string) mapping {
	return mapping{desc: desc, vt: prometheus.CounterValue, scale: scale, labels: labels}
}

var (
	dQueriesRunning        = common.Desc("clickhouse_queries_running", "Number of queries being executed")
	dMergesRunning         = common.Desc("clickhouse_merges_running", "Number of background merges being executed")
	dMutationsRunning      = common.Desc("clickhouse_mutations_running", "Number of part mutations being executed")
	dReplFetchesRunning    = common.Desc("clickhouse_replicated_fetches_running", "Number of data parts being fetched from replicas")
	dReplSendsRunning      = common.Desc("clickhouse_replicated_sends_running", "Number of data parts being sent to replicas")
	dConnections           = common.Desc("clickhouse_connections", "Number of client connections by protocol", "protocol")
	dMemoryTracking        = common.Desc("clickhouse_memory_tracking_bytes", "Total amount of memory allocated by the server")
	dInsertsDelayed        = common.Desc("clickhouse_inserts_delayed", "Number of INSERT queries throttled due to a high number of active data parts in a partition")
	dReadonlyReplicas      = common.Desc("clickhouse_readonly_replicas", "Number of replicated tables in the readonly state (e.g. after a ZooKeeper session loss)")
	dZooKeeperSessions     = common.Desc("clickhouse_zookeeper_sessions", "Number of sessions to ZooKeeper/Keeper")
	dZooKeeperWatches      = common.Desc("clickhouse_zookeeper_watches", "Number of watches in ZooKeeper/Keeper")
	dZooKeeperRequests     = common.Desc("clickhouse_zookeeper_requests_in_flight", "Number of requests to ZooKeeper/Keeper in flight")
	dBgMergesTasks         = common.Desc("clickhouse_background_merges_mutations_tasks", "Number of active merges and mutations in the background pool")
	dBgFetchesTasks        = common.Desc("clickhouse_background_fetches_tasks", "Number of active fetches in the background pool")
	dPartsByState          = common.Desc("clickhouse_parts_by_state", "Number of data parts of MergeTree tables by state", "state")
	dDistributedFiles      = common.Desc("clickhouse_distributed_files_to_insert", "Number of pending files to send to remote servers by Distributed tables")
	dQueriesTotal          = common.Desc("clickhouse_queries_total", "Number of queries by kind", "kind")
	dFailedQueriesTotal    = common.Desc("clickhouse_failed_queries_total", "Number of failed queries by kind", "kind")
	dQueryTimeTotal        = common.Desc("clickhouse_query_time_seconds_total", "Total time of queries")
	dInsertedRows          = common.Desc("clickhouse_inserted_rows_total", "Number of rows inserted into all tables")
	dInsertedBytes         = common.Desc("clickhouse_inserted_bytes_total", "Number of uncompressed bytes inserted into all tables")
	dSelectedRows          = common.Desc("clickhouse_selected_rows_total", "Number of rows selected from all tables")
	dSelectedBytes         = common.Desc("clickhouse_selected_bytes_total", "Number of uncompressed bytes selected from all tables")
	dMergesTotal           = common.Desc("clickhouse_merges_total", "Number of launched background merges")
	dMergedRows            = common.Desc("clickhouse_merged_rows_total", "Number of rows read for background merges")
	dMergeTime             = common.Desc("clickhouse_merge_time_seconds_total", "Total time spent on background merges")
	dDelayedInserts        = common.Desc("clickhouse_delayed_inserts_total", "Number of INSERTs throttled due to a high number of active parts in a partition")
	dDelayedInsertsTime    = common.Desc("clickhouse_delayed_inserts_time_seconds_total", "Total time INSERTs were throttled")
	dRejectedInserts       = common.Desc("clickhouse_rejected_inserts_total", "Number of INSERTs rejected with 'Too many parts'")
	dZooKeeperExceptions   = common.Desc("clickhouse_zookeeper_exceptions_total", "Number of ZooKeeper/Keeper exceptions by type", "type")
	dReplFailedFetches     = common.Desc("clickhouse_replicated_part_failed_fetches_total", "Number of failed data part fetches from replicas")
	dReplDataLoss          = common.Desc("clickhouse_replicated_data_loss_total", "Number of data parts that were not found on any replica")
	dDistributedConnFails  = common.Desc("clickhouse_distributed_connection_fail_try_total", "Number of failed connection retries of Distributed tables")
	dQueryMemLimitExceeded = common.Desc("clickhouse_query_memory_limit_exceeded_total", "Number of queries that exceeded the memory limit")
	dUptime                = common.Desc("clickhouse_uptime_seconds", "Server uptime")
	dMaxPartsPerPartition  = common.Desc("clickhouse_max_part_count_for_partition", "Max number of active parts in a partition across all MergeTree tables")
	dReplMaxDelay          = common.Desc("clickhouse_replicas_max_absolute_delay_seconds", "Max replication delay across all replicated tables")
	dReplMaxQueue          = common.Desc("clickhouse_replicas_max_queue_size", "Max replication queue size across all replicated tables")
	dReplSumQueue          = common.Desc("clickhouse_replicas_sum_queue_size", "Total replication queue size across all replicated tables")
	dMergeTreeParts        = common.Desc("clickhouse_mergetree_parts", "Total number of active parts of all MergeTree tables")
	dMergeTreeRows         = common.Desc("clickhouse_mergetree_rows", "Total number of rows of all MergeTree tables")
	dMergeTreeBytes        = common.Desc("clickhouse_mergetree_bytes", "Total size of all MergeTree tables")
	dDatabases             = common.Desc("clickhouse_databases", "Number of databases")
	dTables                = common.Desc("clickhouse_tables", "Number of tables")
	dMemoryResident        = common.Desc("clickhouse_memory_resident_bytes", "Resident memory of the server process")
	dOSMemoryTotal         = common.Desc("clickhouse_os_memory_total_bytes", "Total memory of the host")
	dOSMemoryAvailable     = common.Desc("clickhouse_os_memory_available_bytes", "Memory available to programs on the host")

	dTableParts          = common.Desc("clickhouse_table_parts", "Number of active data parts of the table", "db", "table")
	dTableRows           = common.Desc("clickhouse_table_rows", "Number of rows in the active parts of the table", "db", "table")
	dTableBytes          = common.Desc("clickhouse_table_size_bytes", "Size of the active parts of the table on disk", "db", "table")
	dTablePartitions     = common.Desc("clickhouse_table_partitions", "Number of partitions of the table", "db", "table")
	dTableMaxPartsPerPtn = common.Desc("clickhouse_table_max_parts_per_partition", "Max number of active parts in a partition of the table", "db", "table")

	dReplicaReadonly   = common.Desc("clickhouse_replica_readonly", "Whether the replicated table is in the readonly state", "db", "table")
	dReplicaExpired    = common.Desc("clickhouse_replica_session_expired", "Whether the ZooKeeper/Keeper session of the replicated table has expired", "db", "table")
	dReplicaDelay      = common.Desc("clickhouse_replica_absolute_delay_seconds", "Replication delay of the table", "db", "table")
	dReplicaQueue      = common.Desc("clickhouse_replica_queue_size", "Size of the replication queue of the table", "db", "table")
	dReplicaQueueIns   = common.Desc("clickhouse_replica_inserts_in_queue", "Number of inserts of data blocks in the replication queue of the table", "db", "table")
	dReplicaQueueMerge = common.Desc("clickhouse_replica_merges_in_queue", "Number of merges in the replication queue of the table", "db", "table")

	dMutationsInProgress = common.Desc("clickhouse_table_mutations_in_progress", "Number of unfinished mutations of the table", "db", "table")
	dMutationsFailing    = common.Desc("clickhouse_table_mutations_failing", "Number of unfinished mutations of the table whose last attempt failed", "db", "table")
	dMutationsStuck      = common.Desc("clickhouse_table_mutations_stuck", "Number of mutations of the table unfinished for more than an hour", "db", "table")

	dErrors = common.Desc("clickhouse_errors_total", "Number of errors since the server start by error name (top by count)", "name")
)

var systemMetricsMapping = map[string]mapping{
	"Query":                                gauge(dQueriesRunning),
	"Merge":                                gauge(dMergesRunning),
	"PartMutation":                         gauge(dMutationsRunning),
	"ReplicatedFetch":                      gauge(dReplFetchesRunning),
	"ReplicatedSend":                       gauge(dReplSendsRunning),
	"TCPConnection":                        gauge(dConnections, "tcp"),
	"HTTPConnection":                       gauge(dConnections, "http"),
	"MySQLConnection":                      gauge(dConnections, "mysql"),
	"PostgreSQLConnection":                 gauge(dConnections, "postgresql"),
	"InterserverConnection":                gauge(dConnections, "interserver"),
	"MemoryTracking":                       gauge(dMemoryTracking),
	"DelayedInserts":                       gauge(dInsertsDelayed),
	"ReadonlyReplica":                      gauge(dReadonlyReplicas),
	"ZooKeeperSession":                     gauge(dZooKeeperSessions),
	"ZooKeeperWatch":                       gauge(dZooKeeperWatches),
	"ZooKeeperRequest":                     gauge(dZooKeeperRequests),
	"BackgroundMergesAndMutationsPoolTask": gauge(dBgMergesTasks),
	"BackgroundFetchesPoolTask":            gauge(dBgFetchesTasks),
	"PartsActive":                          gauge(dPartsByState, "active"),
	"PartsPreActive":                       gauge(dPartsByState, "pre_active"),
	"PartsOutdated":                        gauge(dPartsByState, "outdated"),
	"PartsDeleting":                        gauge(dPartsByState, "deleting"),
	"PartsTemporary":                       gauge(dPartsByState, "temporary"),
	"DistributedFilesToInsert":             gauge(dDistributedFiles),
}

var systemEventsMapping = map[string]mapping{
	"Query":                        counter(dQueriesTotal, 1, "all"),
	"SelectQuery":                  counter(dQueriesTotal, 1, "select"),
	"InsertQuery":                  counter(dQueriesTotal, 1, "insert"),
	"FailedQuery":                  counter(dFailedQueriesTotal, 1, "all"),
	"FailedSelectQuery":            counter(dFailedQueriesTotal, 1, "select"),
	"FailedInsertQuery":            counter(dFailedQueriesTotal, 1, "insert"),
	"QueryTimeMicroseconds":        counter(dQueryTimeTotal, 1e-6),
	"InsertedRows":                 counter(dInsertedRows, 1),
	"InsertedBytes":                counter(dInsertedBytes, 1),
	"SelectedRows":                 counter(dSelectedRows, 1),
	"SelectedBytes":                counter(dSelectedBytes, 1),
	"Merge":                        counter(dMergesTotal, 1),
	"MergedRows":                   counter(dMergedRows, 1),
	"MergesTimeMilliseconds":       counter(dMergeTime, 1e-3),
	"DelayedInserts":               counter(dDelayedInserts, 1),
	"DelayedInsertsMilliseconds":   counter(dDelayedInsertsTime, 1e-3),
	"RejectedInserts":              counter(dRejectedInserts, 1),
	"ZooKeeperHardwareExceptions":  counter(dZooKeeperExceptions, 1, "hardware"),
	"ZooKeeperUserExceptions":      counter(dZooKeeperExceptions, 1, "user"),
	"ZooKeeperOtherExceptions":     counter(dZooKeeperExceptions, 1, "other"),
	"ReplicatedPartFailedFetches":  counter(dReplFailedFetches, 1),
	"ReplicatedDataLoss":           counter(dReplDataLoss, 1),
	"DistributedConnectionFailTry": counter(dDistributedConnFails, 1),
	"QueryMemoryLimitExceeded":     counter(dQueryMemLimitExceeded, 1),
}

var systemAsyncMetricsMapping = map[string]mapping{
	"Uptime":                      gauge(dUptime),
	"MaxPartCountForPartition":    gauge(dMaxPartsPerPartition),
	"ReplicasMaxAbsoluteDelay":    gauge(dReplMaxDelay),
	"ReplicasMaxQueueSize":        gauge(dReplMaxQueue),
	"ReplicasSumQueueSize":        gauge(dReplSumQueue),
	"TotalPartsOfMergeTreeTables": gauge(dMergeTreeParts),
	"TotalRowsOfMergeTreeTables":  gauge(dMergeTreeRows),
	"TotalBytesOfMergeTreeTables": gauge(dMergeTreeBytes),
	"NumberOfDatabases":           gauge(dDatabases),
	"NumberOfTables":              gauge(dTables),
	"MemoryResident":              gauge(dMemoryResident),
	"OSMemoryTotal":               gauge(dOSMemoryTotal),
	"OSMemoryAvailable":           gauge(dOSMemoryAvailable),
}

type nameValueRow struct {
	Name  string  `ch:"name"`
	Value float64 `ch:"value"`
}

// inList renders the (constant) names of a mapping as a sorted SQL list of string literals.
func inList(m map[string]mapping) string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, "'"+n+"'")
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

var (
	qSystemMetrics      = "SELECT metric AS name, toFloat64(value) AS value FROM system.metrics WHERE metric IN (" + inList(systemMetricsMapping) + ")"
	qSystemEvents       = "SELECT event AS name, toFloat64(value) AS value FROM system.events WHERE event IN (" + inList(systemEventsMapping) + ")"
	qSystemAsyncMetrics = "SELECT metric AS name, toFloat64(value) AS value FROM system.asynchronous_metrics WHERE metric IN (" + inList(systemAsyncMetricsMapping) + ")"
)

func mapNameValues(rows []nameValueRow, m map[string]mapping) []prometheus.Metric {
	seen := map[string]bool{}
	var res []prometheus.Metric
	for _, r := range rows {
		mp, ok := m[r.Name]
		if !ok || seen[r.Name] {
			continue
		}
		seen[r.Name] = true
		res = append(res, prometheus.MustNewConstMetric(mp.desc, mp.vt, r.Value*mp.scale, mp.labels...))
	}
	return res
}

func (c *Collector) selectNameValues(ctx context.Context, query string, m map[string]mapping) ([]prometheus.Metric, error) {
	var rows []nameValueRow
	if err := c.q.Select(ctx, &rows, query); err != nil {
		return nil, err
	}
	return mapNameValues(rows, m), nil
}

func (c *Collector) systemMetrics(ctx context.Context) ([]prometheus.Metric, error) {
	return c.selectNameValues(ctx, qSystemMetrics, systemMetricsMapping)
}

func (c *Collector) systemEvents(ctx context.Context) ([]prometheus.Metric, error) {
	return c.selectNameValues(ctx, qSystemEvents, systemEventsMapping)
}

func (c *Collector) systemAsyncMetrics(ctx context.Context) ([]prometheus.Metric, error) {
	return c.selectNameValues(ctx, qSystemAsyncMetrics, systemAsyncMetricsMapping)
}

// tableAllowed applies the excluded databases and the include/exclude regular expressions (matched against "db.table").
func (c *Collector) tableAllowed(db, table string) bool {
	if slices.Contains(c.opts.excludeDBs, db) {
		return false
	}
	name := db + "." + table
	if c.opts.tablesInclude != nil && !c.opts.tablesInclude.MatchString(name) {
		return false
	}
	if c.opts.tablesExclude != nil && c.opts.tablesExclude.MatchString(name) {
		return false
	}
	return true
}

// filterTables keeps the allowed tables and at most topTables of them (the ones with the largest key).
func filterTables[T any](c *Collector, rows []T, table func(T) (string, string), key func(T) float64) []T {
	res := rows[:0:0]
	for _, r := range rows {
		if db, t := table(r); c.tableAllowed(db, t) {
			res = append(res, r)
		}
	}
	sort.SliceStable(res, func(i, j int) bool { return key(res[i]) > key(res[j]) })
	if len(res) > c.opts.topTables {
		res = res[:c.opts.topTables]
	}
	return res
}

type partsRow struct {
	Database   string  `ch:"database"`
	Table      string  `ch:"table"`
	Parts      float64 `ch:"parts"`
	Rows       float64 `ch:"rows"`
	Bytes      float64 `ch:"bytes"`
	Partitions float64 `ch:"partitions"`
	MaxParts   float64 `ch:"max_parts_per_partition"`
}

const qSystemParts = `SELECT database, table, toFloat64(sum(p)) AS parts, toFloat64(sum(r)) AS rows, toFloat64(sum(b)) AS bytes,
  toFloat64(count()) AS partitions, toFloat64(max(p)) AS max_parts_per_partition
FROM (
  SELECT database, table, partition_id, count() AS p, sum(rows) AS r, sum(bytes_on_disk) AS b
  FROM system.parts WHERE active GROUP BY database, table, partition_id
)
GROUP BY database, table`

func (c *Collector) systemParts(ctx context.Context) ([]prometheus.Metric, error) {
	var rows []partsRow
	if err := c.q.Select(ctx, &rows, qSystemParts); err != nil {
		return nil, err
	}
	rows = filterTables(c, rows, func(r partsRow) (string, string) { return r.Database, r.Table }, func(r partsRow) float64 { return r.Bytes })
	var res []prometheus.Metric
	for _, r := range rows {
		res = append(res,
			common.Gauge(dTableParts, r.Parts, r.Database, r.Table),
			common.Gauge(dTableRows, r.Rows, r.Database, r.Table),
			common.Gauge(dTableBytes, r.Bytes, r.Database, r.Table),
			common.Gauge(dTablePartitions, r.Partitions, r.Database, r.Table),
			common.Gauge(dTableMaxPartsPerPtn, r.MaxParts, r.Database, r.Table),
		)
	}
	return res, nil
}

type replicaRow struct {
	Database       string  `ch:"database"`
	Table          string  `ch:"table"`
	IsReadonly     float64 `ch:"is_readonly"`
	SessionExpired float64 `ch:"is_session_expired"`
	AbsoluteDelay  float64 `ch:"absolute_delay"`
	QueueSize      float64 `ch:"queue_size"`
	InsertsInQueue float64 `ch:"inserts_in_queue"`
	MergesInQueue  float64 `ch:"merges_in_queue"`
}

// The columns that require ZooKeeper requests (total_replicas, active_replicas, log_*) are not selected, so the query is cheap.
const qSystemReplicas = `SELECT database, table, toFloat64(is_readonly) AS is_readonly, toFloat64(is_session_expired) AS is_session_expired,
  toFloat64(absolute_delay) AS absolute_delay, toFloat64(queue_size) AS queue_size,
  toFloat64(inserts_in_queue) AS inserts_in_queue, toFloat64(merges_in_queue) AS merges_in_queue
FROM system.replicas`

func (c *Collector) systemReplicas(ctx context.Context) ([]prometheus.Metric, error) {
	var rows []replicaRow
	if err := c.q.Select(ctx, &rows, qSystemReplicas); err != nil {
		return nil, err
	}
	rows = filterTables(c, rows, func(r replicaRow) (string, string) { return r.Database, r.Table },
		func(r replicaRow) float64 {
			return r.IsReadonly*1e12 + r.SessionExpired*1e12 + r.AbsoluteDelay*1e3 + r.QueueSize
		})
	var res []prometheus.Metric
	for _, r := range rows {
		res = append(res,
			common.Gauge(dReplicaReadonly, r.IsReadonly, r.Database, r.Table),
			common.Gauge(dReplicaExpired, r.SessionExpired, r.Database, r.Table),
			common.Gauge(dReplicaDelay, r.AbsoluteDelay, r.Database, r.Table),
			common.Gauge(dReplicaQueue, r.QueueSize, r.Database, r.Table),
			common.Gauge(dReplicaQueueIns, r.InsertsInQueue, r.Database, r.Table),
			common.Gauge(dReplicaQueueMerge, r.MergesInQueue, r.Database, r.Table),
		)
	}
	return res, nil
}

type mutationsRow struct {
	Database   string  `ch:"database"`
	Table      string  `ch:"table"`
	InProgress float64 `ch:"in_progress"`
	Failing    float64 `ch:"failing"`
	Stuck      float64 `ch:"stuck"`
}

const qSystemMutations = `SELECT database, table, toFloat64(count()) AS in_progress,
  toFloat64(countIf(latest_fail_reason != '')) AS failing,
  toFloat64(countIf(create_time < now() - INTERVAL 1 HOUR)) AS stuck
FROM system.mutations WHERE NOT is_done
GROUP BY database, table`

func (c *Collector) systemMutations(ctx context.Context) ([]prometheus.Metric, error) {
	var rows []mutationsRow
	if err := c.q.Select(ctx, &rows, qSystemMutations); err != nil {
		return nil, err
	}
	rows = filterTables(c, rows, func(r mutationsRow) (string, string) { return r.Database, r.Table },
		func(r mutationsRow) float64 { return r.Failing*1e6 + r.Stuck*1e3 + r.InProgress })
	var res []prometheus.Metric
	for _, r := range rows {
		res = append(res,
			common.Gauge(dMutationsInProgress, r.InProgress, r.Database, r.Table),
			common.Gauge(dMutationsFailing, r.Failing, r.Database, r.Table),
			common.Gauge(dMutationsStuck, r.Stuck, r.Database, r.Table),
		)
	}
	return res, nil
}

// system.errors has a row per error code (and per local/remote origin in newer versions), so it's aggregated by name.
var qSystemErrors = fmt.Sprintf(`SELECT name, toFloat64(sum(value)) AS value FROM system.errors GROUP BY name ORDER BY value DESC LIMIT %d`, defaultTopErrors)

func (c *Collector) systemErrors(ctx context.Context) ([]prometheus.Metric, error) {
	var rows []nameValueRow
	if err := c.q.Select(ctx, &rows, qSystemErrors); err != nil {
		return nil, err
	}
	var res []prometheus.Metric
	seen := map[string]bool{}
	for _, r := range rows {
		if seen[r.Name] || len(res) >= defaultTopErrors {
			continue
		}
		seen[r.Name] = true
		res = append(res, common.Counter(dErrors, r.Value, r.Name))
	}
	return res, nil
}
