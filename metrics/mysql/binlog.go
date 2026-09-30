package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
)

type binlogStats struct {
	binlogBytes float64
	binlogFiles float64
	hasBinlog   bool

	undoBytes float64
	hasUndo   bool
}

func (c *Collector) binlogSnapshot(ctx context.Context, st *state) {
	stats := &binlogStats{}
	if v := st.globalVariables["log_bin"]; v == "ON" || v == "1" {
		if err := c.queryBinlogSize(ctx, stats); err != nil {
			c.addScrapeError(st, err)
		} else {
			stats.hasBinlog = true
		}
	} else {
		stats.hasBinlog = true
	}
	if st.hasUndoTablespaces {
		if err := c.queryUndoSize(ctx, stats); err != nil {
			c.addScrapeError(st, err)
		} else {
			stats.hasUndo = true
		}
	}
	st.binlogStats = stats
}

func (c *Collector) queryBinlogSize(ctx context.Context, stats *binlogStats) error {
	rows, err := c.db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	sizeIdx := -1
	for i, name := range cols {
		if name == "File_size" {
			sizeIdx = i
			break
		}
	}
	if sizeIdx < 0 {
		return fmt.Errorf("no File_size column in SHOW BINARY LOGS output: %v", cols)
	}
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		size, err := strconv.ParseFloat(vals[sizeIdx].String, 64)
		if err != nil {
			return fmt.Errorf("unexpected binlog File_size %q: %w", vals[sizeIdx].String, err)
		}
		stats.binlogFiles++
		stats.binlogBytes += size
	}
	return rows.Err()
}

func (c *Collector) queryUndoSize(ctx context.Context, stats *binlogStats) error {
	query := `
		SELECT IFNULL(SUM(FILE_SIZE), 0)
		FROM information_schema.INNODB_TABLESPACES
		WHERE NAME LIKE 'innodb_undo%'`
	return c.db.QueryRowContext(ctx, query).Scan(&stats.undoBytes)
}

func (st *state) binlogMetrics(ch chan<- prometheus.Metric) {
	if st.binlogStats == nil {
		return
	}
	if st.binlogStats.hasBinlog {
		ch <- common.Gauge(dBinlogSize, st.binlogStats.binlogBytes)
		ch <- common.Gauge(dBinlogFiles, st.binlogStats.binlogFiles)
	}
	if st.binlogStats.hasUndo {
		ch <- common.Gauge(dUndoSize, st.binlogStats.undoBytes)
	}
	if !metricFromVariable(ch, dBinlogExpireSeconds, "binlog_expire_logs_seconds", prometheus.GaugeValue, st.globalVariables) {
		metricFromVariable(ch, dBinlogExpireSeconds, "expire_logs_days", prometheus.GaugeValue, st.globalVariables, daysToSeconds)
	}
}

func daysToSeconds(v float64) float64 { return v * 86400 }
