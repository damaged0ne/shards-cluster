package mysql

import (
	"context"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"

	"github.com/prometheus/client_golang/prometheus"
)

type ioTableKey struct {
	schema string
	table  string
}

type ioSummary struct {
	readTotalTime  uint64
	writeTotalTime uint64
}

type ioByTableSnapshot struct {
	ts   time.Time
	rows map[ioTableKey]ioSummary
}

func (c *Collector) queryTableIOWaits(ctx context.Context) (*ioByTableSnapshot, error) {
	snapshot := &ioByTableSnapshot{ts: time.Now(), rows: map[ioTableKey]ioSummary{}}
	q := `
	SELECT
    	OBJECT_SCHEMA, 
    	OBJECT_NAME, 
    	SUM_TIMER_READ, 
    	SUM_TIMER_WRITE 
	FROM performance_schema.table_io_waits_summary_by_table 
	WHERE 
	    OBJECT_SCHEMA is not null AND 
	    OBJECT_NAME is not null`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var k ioTableKey
		var r ioSummary
		if err := rows.Scan(&k.schema, &k.table, &r.readTotalTime, &r.writeTotalTime); err != nil {
			c.logger.Warning(err)
			continue
		}
		if c.excludeDatabases[k.schema] {
			continue
		}
		snapshot.rows[k] = r
	}
	return snapshot, rows.Err()
}

type ioStats struct {
	readTimePerSecond  float64
	writeTimePerSecond float64
	totalTimePerSecond float64
}

type ioStatsWithKey struct {
	k ioTableKey
	s *ioStats
}

func (st *state) ioMetrics(ch chan<- prometheus.Metric, n int) {
	if st.ioByTablePrev == nil || st.ioByTableCurr == nil {
		return
	}
	res := map[ioTableKey]*ioStats{}

	withKeys := make([]ioStatsWithKey, 0, len(res))

	interval := st.ioByTableCurr.ts.Sub(st.ioByTablePrev.ts).Seconds()
	for k, s := range st.ioByTableCurr.rows {
		prev := st.ioByTablePrev.rows[k]
		stats := &ioStats{}
		if v := s.readTotalTime - prev.readTotalTime; v > 0 {
			stats.readTimePerSecond = float64(v) / picoSeconds / interval
		}
		if v := s.writeTotalTime - prev.writeTotalTime; v > 0 {
			stats.writeTimePerSecond = float64(v) / picoSeconds / interval
		}
		stats.totalTimePerSecond = stats.readTimePerSecond + stats.writeTimePerSecond
		if stats.totalTimePerSecond > 0 {
			withKeys = append(withKeys, ioStatsWithKey{k: k, s: stats})
		}
	}
	withKeys = common.TopN(withKeys, n, func(a, b ioStatsWithKey) bool { return a.s.totalTimePerSecond > b.s.totalTimePerSecond })
	for _, i := range withKeys {
		ch <- common.Gauge(dIOTime, i.s.readTimePerSecond, i.k.schema, i.k.table, "read")
		ch <- common.Gauge(dIOTime, i.s.writeTimePerSecond, i.k.schema, i.k.table, "write")
	}

}
