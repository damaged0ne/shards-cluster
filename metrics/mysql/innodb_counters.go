package mysql

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

type innodbCounters struct {
	values map[string]float64
}

func (c *Collector) innodbCountersSnapshot(ctx context.Context, st *state) {
	counters, err := c.queryInnodbCounters(ctx, st.isMariaDB)
	if err != nil {
		c.addScrapeError(st, err)
		return
	}
	st.innodbCounters = counters
}

func (c *Collector) queryInnodbCounters(ctx context.Context, isMariaDB bool) (*innodbCounters, error) {
	res := &innodbCounters{values: map[string]float64{}}
	enabled := "STATUS = 'enabled'"
	if isMariaDB {
		enabled = "ENABLED = 1"
	}
	rows, err := c.db.QueryContext(ctx, `
		SELECT NAME, COUNT
		FROM information_schema.INNODB_METRICS
		WHERE `+enabled+`
		    AND NAME IN ('lock_deadlocks', 'lock_timeouts', 'trx_rseg_history_len')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var v float64
		if err := rows.Scan(&name, &v); err != nil {
			c.logger.Warning(err)
			continue
		}
		res.values[name] = v
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func (st *state) innodbCountersMetrics(ch chan<- prometheus.Metric) {
	if st.innodbCounters == nil {
		return
	}
	emit := func(desc *prometheus.Desc, name string, typ prometheus.ValueType) {
		v, ok := st.innodbCounters.values[name]
		if !ok {
			return
		}
		ch <- prometheus.MustNewConstMetric(desc, typ, v)
	}
	emit(dInnodbDeadlocks, "lock_deadlocks", prometheus.CounterValue)
	emit(dInnodbLockWaitTimeouts, "lock_timeouts", prometheus.CounterValue)
	emit(dInnodbHistoryListLength, "trx_rseg_history_len", prometheus.GaugeValue)
}
