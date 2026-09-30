package mysql

import (
	"github.com/prometheus/client_golang/prometheus"
)

func msToSeconds(v float64) float64 { return v / 1000 }

func (st *state) innodbMetrics(ch chan<- prometheus.Metric) {
	s := st.globalStatus

	metricFromVariable(ch, dInnodbBufferPoolReadRequests, "Innodb_buffer_pool_read_requests", prometheus.CounterValue, s)
	metricFromVariable(ch, dInnodbBufferPoolReads, "Innodb_buffer_pool_reads", prometheus.CounterValue, s)
	metricFromVariable(ch, dInnodbBufferPoolWriteRequests, "Innodb_buffer_pool_write_requests", prometheus.CounterValue, s)
	metricFromVariable(ch, dInnodbBufferPoolPagesTotal, "Innodb_buffer_pool_pages_total", prometheus.GaugeValue, s)
	metricFromVariable(ch, dInnodbBufferPoolPagesFree, "Innodb_buffer_pool_pages_free", prometheus.GaugeValue, s)
	metricFromVariable(ch, dInnodbBufferPoolPagesDirty, "Innodb_buffer_pool_pages_dirty", prometheus.GaugeValue, s)
	metricFromVariable(ch, dInnodbBufferPoolPagesData, "Innodb_buffer_pool_pages_data", prometheus.GaugeValue, s)
	metricFromVariable(ch, dInnodbBufferPoolWaitFree, "Innodb_buffer_pool_wait_free", prometheus.CounterValue, s)
	metricFromVariable(ch, dInnodbBufferPoolPagesFlushed, "Innodb_buffer_pool_pages_flushed", prometheus.CounterValue, s)
	metricFromVariable(ch, dInnodbPageSize, "innodb_page_size", prometheus.GaugeValue, st.globalVariables)
	metricFromVariable(ch, dInnodbRowsRead, "Innodb_rows_read", prometheus.CounterValue, s)
	metricFromVariable(ch, dInnodbRowsInserted, "Innodb_rows_inserted", prometheus.CounterValue, s)
	metricFromVariable(ch, dInnodbRowsUpdated, "Innodb_rows_updated", prometheus.CounterValue, s)
	metricFromVariable(ch, dInnodbRowsDeleted, "Innodb_rows_deleted", prometheus.CounterValue, s)
	metricFromVariable(ch, dInnodbRowLockWaits, "Innodb_row_lock_waits", prometheus.CounterValue, s)
	metricFromVariable(ch, dInnodbRowLockTime, "Innodb_row_lock_time", prometheus.CounterValue, s, msToSeconds)
	metricFromVariable(ch, dInnodbRowLockCurrentWaits, "Innodb_row_lock_current_waits", prometheus.GaugeValue, s)

	metricFromVariable(ch, dInnodbDataReads, "Innodb_data_reads", prometheus.CounterValue, s)
	metricFromVariable(ch, dInnodbDataWrites, "Innodb_data_writes", prometheus.CounterValue, s)
	metricFromVariable(ch, dInnodbDataRead, "Innodb_data_read", prometheus.CounterValue, s)
	metricFromVariable(ch, dInnodbDataWritten, "Innodb_data_written", prometheus.CounterValue, s)
	metricFromVariable(ch, dInnodbDataFsyncs, "Innodb_data_fsyncs", prometheus.CounterValue, s)

	metricFromVariable(ch, dInnodbLogWaits, "Innodb_log_waits", prometheus.CounterValue, s)
	metricFromVariable(ch, dInnodbOsLogWritten, "Innodb_os_log_written", prometheus.CounterValue, s)

	metricFromVariable(ch, dSortMergePasses, "Sort_merge_passes", prometheus.CounterValue, s)

	metricFromVariable(ch, dComCommit, "Com_commit", prometheus.CounterValue, s)
	metricFromVariable(ch, dComRollback, "Com_rollback", prometheus.CounterValue, s)
}
