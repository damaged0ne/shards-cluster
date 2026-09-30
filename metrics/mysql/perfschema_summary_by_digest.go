package mysql

import (
	"context"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"

	"github.com/coroot/coroot-cluster-agent/obfuscate"
	"github.com/prometheus/client_golang/prometheus"
)

type queryKey struct {
	schema string
	query  string
}

type statementsSummaryRow struct {
	obfuscatedQueryText string
	calls               uint64
	totalTime           uint64
	lockTime            uint64
	rowsExamined        uint64
	rowsSent            uint64
}

type digestKey struct {
	schema string
	digest string
}

type statementsSummarySnapshot struct {
	ts   time.Time
	rows map[digestKey]statementsSummaryRow
}

func (c *Collector) queryStatementsSummary(ctx context.Context, prev *statementsSummarySnapshot) (*statementsSummarySnapshot, error) {
	snapshot := &statementsSummarySnapshot{ts: time.Now(), rows: map[digestKey]statementsSummaryRow{}}
	q := `
	SELECT
	    ifnull(SCHEMA_NAME, ''),
	    DIGEST,
	    DIGEST_TEXT,
	    COUNT_STAR,
	    SUM_TIMER_WAIT,
	    SUM_LOCK_TIME,
	    SUM_ROWS_EXAMINED,
	    SUM_ROWS_SENT
	FROM
		performance_schema.events_statements_summary_by_digest
	WHERE
	    DIGEST IS NOT NULL AND
	    DIGEST_TEXT IS NOT NULL`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var digestText string
	for rows.Next() {
		var k digestKey
		var r statementsSummaryRow
		if err := rows.Scan(&k.schema, &k.digest, &digestText, &r.calls, &r.totalTime, &r.lockTime, &r.rowsExamined, &r.rowsSent); err != nil {
			c.logger.Warning(err)
			continue
		}
		if prev != nil {
			if p, ok := prev.rows[k]; ok {
				r.obfuscatedQueryText = p.obfuscatedQueryText
			}
		}
		if r.obfuscatedQueryText == "" {
			r.obfuscatedQueryText = obfuscate.Sql(digestText)
		}
		snapshot.rows[k] = r
	}
	return snapshot, rows.Err()
}

type queryStats struct {
	calls        float64
	totalTime    float64 // seconds
	lockTime     float64 // seconds
	rowsExamined float64
	rowsSent     float64
	inflight     bool // has an in-flight contribution (keep even with zero completed calls)
}

type statsWithKey struct {
	k queryKey
	s *queryStats
}

func (st *state) queryMetrics(ch chan<- prometheus.Metric, n int) {
	if st.perfschemaPrev == nil || st.perfschemaCurr == nil {
		return
	}
	interval := st.perfschemaCurr.ts.Sub(st.perfschemaPrev.ts).Seconds()
	if interval <= 0 {
		return
	}
	res := map[queryKey]*queryStats{}
	getOrCreate := func(qk queryKey) *queryStats {
		r := res[qk]
		if r == nil {
			r = &queryStats{}
			res[qk] = r
		}
		return r
	}

	for k, s := range st.perfschemaCurr.rows {
		prev := st.perfschemaPrev.rows[k]
		r := getOrCreate(queryKey{schema: k.schema, query: s.obfuscatedQueryText})
		if calls := s.calls - prev.calls; calls > 0 {
			r.calls += float64(calls)
		}
		if totalTime := s.totalTime - prev.totalTime; totalTime > 0 {
			r.totalTime += float64(totalTime) / picoSeconds
		}
		if lockTime := s.lockTime - prev.lockTime; lockTime > 0 {
			r.lockTime += float64(lockTime) / picoSeconds
		}
		if rowsExamined := s.rowsExamined - prev.rowsExamined; rowsExamined > 0 {
			r.rowsExamined += float64(rowsExamined)
		}
		if rowsSent := s.rowsSent - prev.rowsSent; rowsSent > 0 {
			r.rowsSent += float64(rowsSent)
		}
	}

	if st.activeCurr != nil {
		for _, stmt := range st.activeCurr.stmts {
			d := stmt.elapsedSec
			if d > interval {
				d = interval
			}
			if d <= 0 {
				continue
			}
			r := getOrCreate(stmt.qk)
			r.totalTime += d
			r.inflight = true
		}
	}

	if st.activePrev != nil && st.activeCurr != nil {
		for k, stmt := range st.activePrev.stmts {
			if _, still := st.activeCurr.stmts[k]; still {
				continue
			}
			r := res[stmt.qk]
			if r == nil {
				continue
			}
			if r.totalTime -= stmt.elapsedSec; r.totalTime < 0 {
				r.totalTime = 0
			}
		}
	}

	withKeys := make([]statsWithKey, 0, len(res))
	for k, s := range res {
		if s.calls == 0 && !s.inflight {
			continue
		}
		withKeys = append(withKeys, statsWithKey{k: k, s: s})
	}
	withKeys = common.TopN(withKeys, n, func(a, b statsWithKey) bool { return a.s.totalTime > b.s.totalTime })
	for _, i := range withKeys {
		s := i.s
		ch <- common.Gauge(dQueryCalls, s.calls/interval, i.k.schema, i.k.query)
		ch <- common.Gauge(dQueryTotalTime, s.totalTime/interval, i.k.schema, i.k.query)
		ch <- common.Gauge(dQueryLockTime, s.lockTime/interval, i.k.schema, i.k.query)
		ch <- common.Gauge(dQueryRowsExamined, s.rowsExamined/interval, i.k.schema, i.k.query)
		ch <- common.Gauge(dQueryRowsSent, s.rowsSent/interval, i.k.schema, i.k.query)
	}
}
