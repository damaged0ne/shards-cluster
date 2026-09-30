package mysql

import (
	"context"
	"database/sql"
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
