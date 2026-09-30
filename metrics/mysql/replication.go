package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/coroot/coroot-cluster-agent/common"

	"github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
)

type ReplicaStatus struct {
	vals map[string]string
}

func (rs *ReplicaStatus) Get(keys ...string) string {
	for _, key := range keys {
		if val, ok := rs.vals[key]; ok {
			return val
		}
	}
	return ""
}

func (c *Collector) updateReplicationStatus(ctx context.Context, st *state) error {
	st.replicaStatuses = nil // never reuse the published slice
	for _, q := range []string{"SHOW REPLICA STATUS", "SHOW SLAVE STATUS"} {
		if c.invalidQueries[q] {
			continue
		}
		rows, err := c.db.QueryContext(ctx, q)
		if err != nil {
			if mysqlErr, ok := err.(*mysql.MySQLError); ok && mysqlErr.Number == 1064 {
				c.invalidQueries[q] = true
				continue
			}
			return err
		}
		defer rows.Close()
		for rows.Next() {
			cols, err := rows.Columns()
			if err != nil {
				return err
			}
			scanArgs := make([]interface{}, len(cols))
			for i := range scanArgs {
				scanArgs[i] = &sql.RawBytes{}
			}
			if err = rows.Scan(scanArgs...); err != nil {
				return err
			}
			rs := &ReplicaStatus{vals: map[string]string{}}
			for i, col := range cols {
				raw, ok := scanArgs[i].(*sql.RawBytes)
				if !ok {
					continue
				}
				rs.vals[col] = string(*raw)
			}
			st.replicaStatuses = append(st.replicaStatuses, rs)
		}
		break
	}
	return nil
}

func (st *state) replicationMetrics(ch chan<- prometheus.Metric) {
	for _, rs := range st.replicaStatuses {
		sourceServerId := rs.Get("Source_Server_Id", "Master_Server_Id")
		sourceServerUUID := rs.Get("Source_UUID", "Master_UUID")

		if ioRunning := rs.Get("Replica_IO_Running", "Slave_IO_Running"); ioRunning != "" {
			status := 0.
			if ioRunning == "Yes" {
				status = 1.
			}
			ch <- common.Gauge(
				dReplicationIORunning,
				status,
				sourceServerId,
				sourceServerUUID,
				rs.Get("Replica_IO_State", "Slave_IO_State"),
				rs.Get("Last_IO_Error"),
			)
		}
		if sqlRunning := rs.Get("Replica_SQL_Running", "Slave_SQL_Running"); sqlRunning != "" {
			status := 0.
			if sqlRunning == "Yes" {
				status = 1.
			}
			ch <- common.Gauge(
				dReplicationSQLRunning,
				status,
				sourceServerId,
				sourceServerUUID,
				rs.Get("Replica_SQL_Running_State", "Slave_SQL_Running_State"),
				rs.Get("Last_SQL_Error"),
			)
		}
		if lag, err := strconv.ParseUint(rs.Get("Seconds_Behind_Source", "Seconds_Behind_Master"), 10, 64); err == nil {
			ch <- common.Gauge(dReplicationLag, float64(lag), sourceServerId, sourceServerUUID)
		}
	}
}

// applierWorkerRow is a row of performance_schema.replication_applier_status_by_worker (8.0+).
type applierWorkerRow struct {
	channel        string
	lastAppliedLag sql.NullFloat64 // µs between the original commit and the end of applying of the last applied transaction
	applyingAge    sql.NullFloat64 // µs since the original commit of the transaction being applied now
}

type applierLag struct {
	lastApplied sql.NullFloat64 // seconds
	current     float64         // seconds
}

// Zero timestamps ('0000-00-00 00:00:00') mean "no transaction", hence the comparisons.
// Both timestamps are taken from the source (original commit) and the replica, so clock skew
// between the servers affects the values.
const applierWorkersQuery = `
	SELECT CHANNEL_NAME,
		IF(LAST_APPLIED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP > '1970-01-02' AND LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP > '1970-01-02',
			TIMESTAMPDIFF(MICROSECOND, LAST_APPLIED_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP, LAST_APPLIED_TRANSACTION_END_APPLY_TIMESTAMP), NULL),
		IF(APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP > '1970-01-02',
			TIMESTAMPDIFF(MICROSECOND, APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP, NOW(6)), NULL)
	FROM performance_schema.replication_applier_status_by_worker`

// aggregateApplierLag computes per-channel lags: the worst of the workers of the channel.
// An idle channel (no transaction being applied) has current lag 0.
func aggregateApplierLag(rows []applierWorkerRow) map[string]*applierLag {
	res := map[string]*applierLag{}
	for _, r := range rows {
		l := res[r.channel]
		if l == nil {
			l = &applierLag{}
			res[r.channel] = l
		}
		if r.lastAppliedLag.Valid {
			v := max(r.lastAppliedLag.Float64, 0) / 1e6
			if !l.lastApplied.Valid || v > l.lastApplied.Float64 {
				l.lastApplied = sql.NullFloat64{Float64: v, Valid: true}
			}
		}
		if r.applyingAge.Valid {
			l.current = max(l.current, r.applyingAge.Float64/1e6)
		}
	}
	return res
}

// applierLagSupported: the applier lag is collected on MySQL 8.0+ replicas only
// (the columns don't exist in 5.7 and MariaDB).
func applierLagSupported(st *state) bool {
	return len(st.replicaStatuses) > 0 && !st.isMariaDB && versionAtLeast(st.globalVariables["version"], 8, 0) && perfschemaEnabled(st.globalVariables)
}

func (c *Collector) applierLagSnapshot(ctx context.Context, st *state) {
	st.applierLag = nil
	if !applierLagSupported(st) {
		return
	}

	rows, err := c.db.QueryContext(ctx, applierWorkersQuery)
	if err != nil {
		c.addScrapeError(st, fmt.Errorf("replication applier status: %w", err))
		return
	}
	defer rows.Close()
	var res []applierWorkerRow
	for rows.Next() {
		var r applierWorkerRow
		if err := rows.Scan(&r.channel, &r.lastAppliedLag, &r.applyingAge); err != nil {
			c.logger.Warning(err)
			continue
		}
		res = append(res, r)
	}
	if err := rows.Err(); err != nil {
		c.addScrapeError(st, fmt.Errorf("replication applier status: %w", err))
		return
	}
	st.applierLag = aggregateApplierLag(res)
}

func (st *state) applierLagMetrics(ch chan<- prometheus.Metric) {
	for channel, l := range st.applierLag {
		ch <- common.Gauge(dReplicationApplierCurrentLag, l.current, channel)
		if l.lastApplied.Valid {
			ch <- common.Gauge(dReplicationApplierLastLag, l.lastApplied.Float64, channel)
		}
	}
}
