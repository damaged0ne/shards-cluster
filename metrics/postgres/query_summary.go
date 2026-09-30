package postgres

import (
	"database/sql"
	"time"
)

type QuerySummary struct {
	Queries   float64
	TotalTime float64
	IOTime    float64

	// deltas of the additional pg_stat_statements counters (see ssRows...ssWalBytes);
	// hasCounter is set when the column is available on the server
	Counters   [ssCountersN]float64
	hasCounter [ssCountersN]bool

	// execution time statistics since the last pg_stat_statements reset (ms);
	// several statements with the same key are merged (weighted mean, min of mins, max of maxes)
	execMeanSum   float64 // sum(mean * calls)
	execMeanCalls float64 // sum(calls) of the rows with a valid mean
	execMin       sql.NullFloat64
	execMax       sql.NullFloat64
}

// ExecTimeMean returns the mean execution time in ms since the last stats reset.
func (s *QuerySummary) ExecTimeMean() (float64, bool) {
	if s.execMeanCalls <= 0 {
		return 0, false
	}
	return s.execMeanSum / s.execMeanCalls, true
}

func (s *QuerySummary) updateFromStatActivity(prevTs, ts time.Time, conn Connection) {
	if conn.State.String != "active" {
		return
	}
	if !conn.QueryStart.Valid {
		return
	}
	duration := ts.Sub(conn.QueryStart.Time)
	if duration < 0 {
		return
	}
	interval := ts.Sub(prevTs)
	if duration > interval {
		duration = interval
	}
	if conn.IsClientBackend() {
		s.Queries += 1
		s.TotalTime += duration.Seconds()
	}
	if conn.WaitEventType.String == "IO" {
		s.IOTime += duration.Seconds()
	}
}

func (s *QuerySummary) correctFromPrevStatActivity(ts time.Time, conn Connection) {
	if !conn.QueryStart.Valid {
		return
	}
	duration := ts.Sub(conn.QueryStart.Time).Seconds()
	if duration < 0 {
		return
	}
	if conn.IsClientBackend() && s.Queries > 0 && s.TotalTime > duration {
		s.Queries -= 1
		s.TotalTime -= duration
	}
	if conn.WaitEventType.String == "IO" && s.IOTime > duration {
		s.IOTime -= duration
	}
}

func (s *QuerySummary) updateFromStatStatements(cur, prev ssRow) {
	if cur.meanExecTime.Valid && cur.calls.Int64 > 0 {
		s.execMeanSum += cur.meanExecTime.Float64 * float64(cur.calls.Int64)
		s.execMeanCalls += float64(cur.calls.Int64)
	}
	if cur.minExecTime.Valid && cur.calls.Int64 > 0 && (!s.execMin.Valid || cur.minExecTime.Float64 < s.execMin.Float64) {
		s.execMin = cur.minExecTime
	}
	if cur.maxExecTime.Valid && (!s.execMax.Valid || cur.maxExecTime.Float64 > s.execMax.Float64) {
		s.execMax = cur.maxExecTime
	}
	for i, c := range cur.counters {
		if c.Valid {
			s.hasCounter[i] = true
		}
	}

	callsDelta := float64(cur.calls.Int64 - prev.calls.Int64)
	totalTimeDelta := (cur.totalTime.Float64 - prev.totalTime.Float64) / 1000
	ioTimeDelta := (cur.ioTime.Float64 - prev.ioTime.Float64) / 1000
	if totalTimeDelta < 0 || callsDelta < 0 || ioTimeDelta < 0 {
		return
	}
	s.Queries += callsDelta
	s.TotalTime += totalTimeDelta
	s.IOTime += ioTimeDelta
	for i, c := range cur.counters {
		// a missing previous value means the statement is new: its counters are the delta
		if d := c.Float64 - prev.counters[i].Float64; c.Valid && d > 0 {
			s.Counters[i] += d
		}
	}
}
