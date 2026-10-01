package pgbouncer

import (
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sample struct {
	labels map[string]string
	value  float64
	typ    string
}

func collect(t *testing.T, ms []prometheus.Metric) map[string]sample {
	t.Helper()
	res := map[string]sample{}
	for _, m := range ms {
		var pb dto.Metric
		require.NoError(t, m.Write(&pb))
		s := sample{labels: map[string]string{}}
		for _, l := range pb.GetLabel() {
			s.labels[l.GetName()] = l.GetValue()
		}
		switch {
		case pb.Counter != nil:
			s.typ, s.value = "counter", pb.Counter.GetValue()
		case pb.Gauge != nil:
			s.typ, s.value = "gauge", pb.Gauge.GetValue()
		}
		name := m.Desc().String()
		// fqName: "<name>"
		start := len(`Desc{fqName: "`)
		end := start
		for end < len(name) && name[end] != '"' {
			end++
		}
		res[name[start:end]] = s
	}
	return res
}

func TestStatsMetrics(t *testing.T) {
	row := Row{ // PgBouncer 1.23
		"database":                      "app",
		"total_server_assignment_count": "12",
		"total_xact_count":              "100",
		"total_query_count":             "250",
		"total_received":                "4096",
		"total_sent":                    "8192",
		"total_xact_time":               "2500000",
		"total_query_time":              "1500000",
		"total_wait_time":               "500000",
		"avg_xact_count":                "1",
		"avg_query_time":                "10",
	}
	ms := collect(t, StatsMetrics(row))
	assert.Len(t, ms, 8)
	exp := map[string]float64{
		"pgbouncer_stats_queries_pooled_total":                250,
		"pgbouncer_stats_sql_transactions_pooled_total":       100,
		"pgbouncer_stats_received_bytes_total":                4096,
		"pgbouncer_stats_sent_bytes_total":                    8192,
		"pgbouncer_stats_server_in_transaction_seconds_total": 2.5,
		"pgbouncer_stats_queries_duration_seconds_total":      1.5,
		"pgbouncer_stats_client_wait_seconds_total":           0.5,
		"pgbouncer_stats_server_assignments_total":            12,
	}
	for name, v := range exp {
		s, ok := ms[name]
		require.True(t, ok, name)
		assert.InDelta(t, v, s.value, 1e-9, name)
		assert.Equal(t, "counter", s.typ, name)
		assert.Equal(t, map[string]string{"database": "app"}, s.labels, name)
	}
}

func TestStatsMetricsOldVersion(t *testing.T) {
	ms := collect(t, StatsMetrics(Row{"database": "app", "total_requests": "7", "total_query_time": ""}))
	require.Len(t, ms, 1)
	assert.Equal(t, 7., ms["pgbouncer_stats_queries_pooled_total"].value)
}

func TestPoolMetrics(t *testing.T) {
	row := Row{
		"database":   "app",
		"user":       "svc",
		"cl_active":  "5",
		"cl_waiting": "2",
		"sv_active":  "3",
		"sv_idle":    "4",
		"sv_used":    "1",
		"sv_tested":  "0",
		"sv_login":   "0",
		"maxwait":    "1",
		"maxwait_us": "250000",
		"pool_mode":  "transaction",
	}
	ms := collect(t, PoolMetrics(row))
	exp := map[string]float64{
		"pgbouncer_pools_client_active_connections":  5,
		"pgbouncer_pools_client_waiting_connections": 2,
		"pgbouncer_pools_server_active_connections":  3,
		"pgbouncer_pools_server_idle_connections":    4,
		"pgbouncer_pools_server_used_connections":    1,
		"pgbouncer_pools_server_testing_connections": 0,
		"pgbouncer_pools_server_login_connections":   0,
		"pgbouncer_pools_client_maxwait_seconds":     1.25,
	}
	var names []string
	for n := range ms {
		names = append(names, n)
	}
	sort.Strings(names)
	require.Len(t, ms, len(exp), names)
	for name, v := range exp {
		s, ok := ms[name]
		require.True(t, ok, name)
		assert.InDelta(t, v, s.value, 1e-9, name)
		assert.Equal(t, "gauge", s.typ, name)
		assert.Equal(t, map[string]string{"database": "app", "user": "svc"}, s.labels, name)
	}
}

func TestListMetrics(t *testing.T) {
	ms := collect(t, ListMetrics(Row{"list": "used_clients", "items": "17"}))
	require.Len(t, ms, 1)
	assert.Equal(t, 17., ms["pgbouncer_used_clients"].value)
	assert.Equal(t, 3., collect(t, ListMetrics(Row{"list": "dns_queries", "items": "3"}))["pgbouncer_in_flight_dns_queries"].value)
	assert.Empty(t, ListMetrics(Row{"list": "peers", "items": "1"}))
	assert.Empty(t, ListMetrics(Row{"list": "users", "items": "x"}))
}
