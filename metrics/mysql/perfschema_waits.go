package mysql

import (
	"context"
	"fmt"
	"strings"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
)

const topWaitEventsN = 30

type waitEvent struct {
	name     string
	count    float64
	sumTimer float64 // picoseconds
}

// The top-N set is selected by the cumulative SUM_TIMER_WAIT, which changes slowly, so the
// reported set is stable and every reported series is a monotonic counter.
// "idle" is the time spent by sessions waiting for the client and is not a server-side wait.
var waitEventsQuery = fmt.Sprintf(`
	SELECT EVENT_NAME, COUNT_STAR, SUM_TIMER_WAIT
	FROM performance_schema.events_waits_summary_global_by_event_name
	WHERE EVENT_NAME <> 'idle' AND COUNT_STAR > 0
	ORDER BY SUM_TIMER_WAIT DESC
	LIMIT %d`, topWaitEventsN)

// perfschemaEnabled reports whether performance_schema is enabled (performance_schema=ON).
func perfschemaEnabled(variables map[string]string) bool {
	v := strings.ToUpper(variables["performance_schema"])
	return v == "ON" || v == "1"
}

func (c *Collector) waitEventsSnapshot(ctx context.Context, st *state) {
	st.waitEvents = nil
	if !perfschemaEnabled(st.globalVariables) {
		return
	}
	rows, err := c.db.QueryContext(ctx, waitEventsQuery)
	if err != nil {
		c.addScrapeError(st, fmt.Errorf("wait events: %w", err))
		return
	}
	defer rows.Close()
	var res []waitEvent
	for rows.Next() {
		var e waitEvent
		if err := rows.Scan(&e.name, &e.count, &e.sumTimer); err != nil {
			c.logger.Warning(err)
			continue
		}
		res = append(res, e)
	}
	if err := rows.Err(); err != nil {
		c.addScrapeError(st, fmt.Errorf("wait events: %w", err))
		return
	}
	st.waitEvents = res
}

func (st *state) waitEventsMetrics(ch chan<- prometheus.Metric) {
	// the query already limits the set to the top N (the published state must not be sorted in place)
	for _, e := range st.waitEvents {
		ch <- common.Counter(dWaitEventSeconds, e.sumTimer/picoSeconds, e.name)
		ch <- common.Counter(dWaitEventCount, e.count, e.name)
	}
}
