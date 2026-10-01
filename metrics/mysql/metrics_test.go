package mysql

import (
	"database/sql"
	"sort"
	"strconv"

	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

func drainMetrics(f func(ch chan<- prometheus.Metric)) []string {
	ch := make(chan prometheus.Metric, 1000)
	f(ch)
	close(ch)
	var res []string
	for m := range ch {
		var pb dto.Metric
		if err := m.Write(&pb); err != nil {
			panic(err)
		}
		s := m.Desc().String()
		s = s[strings.Index(s, `fqName: "`)+len(`fqName: "`):]
		name := s[:strings.Index(s, `"`)]
		var labels []string
		for _, l := range pb.GetLabel() {
			labels = append(labels, l.GetName()+"="+l.GetValue())
		}
		v := pb.GetGauge().GetValue()
		if pb.Counter != nil {
			v = pb.GetCounter().GetValue()
		}
		res = append(res, name+"{"+strings.Join(labels, ",")+"} "+strconv.FormatFloat(v, 'g', -1, 64))
	}
	sort.Strings(res)
	return res
}

func Test_waitEventsMetrics(t *testing.T) {
	st := &state{waitEvents: []waitEvent{
		{name: "wait/io/file/innodb/innodb_data_file", count: 10, sumTimer: 2.5e12},
		{name: "wait/lock/table/sql/handler", count: 3, sumTimer: 1e9},
	}}
	assert.Equal(t, []string{
		"mysql_wait_event_count_total{event=wait/io/file/innodb/innodb_data_file} 10",
		"mysql_wait_event_count_total{event=wait/lock/table/sql/handler} 3",
		"mysql_wait_event_seconds_total{event=wait/io/file/innodb/innodb_data_file} 2.5",
		"mysql_wait_event_seconds_total{event=wait/lock/table/sql/handler} 0.001",
	}, drainMetrics(st.waitEventsMetrics))
	assert.Empty(t, drainMetrics((&state{}).waitEventsMetrics))
	assert.Contains(t, waitEventsQuery, "EVENT_NAME <> 'idle'")
	assert.Contains(t, waitEventsQuery, "LIMIT 30")
}

func Test_perfschemaEnabled(t *testing.T) {
	for v, expected := range map[string]bool{"ON": true, "on": true, "1": true, "OFF": false, "": false} {
		assert.Equal(t, expected, perfschemaEnabled(map[string]string{"performance_schema": v}), v)
	}
}

func Test_applierLag(t *testing.T) {
	nf := func(v float64) sql.NullFloat64 { return sql.NullFloat64{Float64: v, Valid: true} }
	lags := aggregateApplierLag([]applierWorkerRow{
		{channel: "", lastAppliedLag: nf(1_500_000), applyingAge: sql.NullFloat64{}},
		{channel: "", lastAppliedLag: nf(500_000), applyingAge: nf(3_000_000)},
		{channel: "", lastAppliedLag: sql.NullFloat64{}, applyingAge: sql.NullFloat64{}}, // an idle worker
		{channel: "src2", lastAppliedLag: sql.NullFloat64{}, applyingAge: sql.NullFloat64{}},
		{channel: "src3", lastAppliedLag: nf(-100), applyingAge: sql.NullFloat64{}}, // clock skew
	})
	assert.Equal(t, map[string]*applierLag{
		"":     {lastApplied: nf(1.5), current: 3},
		"src2": {current: 0},
		"src3": {lastApplied: nf(0), current: 0},
	}, lags)

	st := &state{applierLag: lags}
	assert.Equal(t, []string{
		"mysql_replication_applier_current_lag_seconds{channel=src2} 0",
		"mysql_replication_applier_current_lag_seconds{channel=src3} 0",
		"mysql_replication_applier_current_lag_seconds{channel=} 3",
		"mysql_replication_applier_last_transaction_lag_seconds{channel=src3} 0",
		"mysql_replication_applier_last_transaction_lag_seconds{channel=} 1.5",
	}, drainMetrics(st.applierLagMetrics))
}

func Test_applierLagSupported(t *testing.T) {
	replica := []*ReplicaStatus{{vals: map[string]string{}}}
	for _, tc := range []struct {
		name     string
		version  string
		mariaDB  bool
		ps       string
		replicas []*ReplicaStatus
		expected bool
	}{
		{name: "8.0 replica", version: "8.0.36", ps: "ON", replicas: replica, expected: true},
		{name: "8.4 replica", version: "8.4.0", ps: "ON", replicas: replica, expected: true},
		{name: "5.7 replica", version: "5.7.44-log", ps: "ON", replicas: replica},
		{name: "mariadb", version: "10.11.6-MariaDB", mariaDB: true, ps: "ON", replicas: replica},
		{name: "perfschema off", version: "8.0.36", ps: "OFF", replicas: replica},
		{name: "not a replica", version: "8.0.36", ps: "ON"},
	} {
		st := &state{
			globalVariables: map[string]string{"version": tc.version, "performance_schema": tc.ps},
			isMariaDB:       tc.mariaDB,
			replicaStatuses: tc.replicas,
		}
		assert.Equal(t, tc.expected, applierLagSupported(st), tc.name)
	}
}

func Test_buildUnusedIndexes(t *testing.T) {
	k := func(s, t, i string) indexKey { return indexKey{schema: s, table: t, index: i} }
	unused := []indexKey{
		k("app", "users", "idx_name"),
		k("app", "users", "uq_email"), // unique: excluded
		k("app", "orders", "idx_created"),
		k("app", "orders", "idx_status"),
		k("other", "t", "idx_a"),
		k("skipme", "t", "idx_b"), // excluded schema
	}
	unique := map[indexKey]bool{k("app", "users", "uq_email"): true}
	sizes := map[indexKey]float64{
		k("app", "users", "idx_name"):     16384,
		k("app", "orders", "idx_created"): 1 << 20,
		k("other", "t", "idx_a"):          16384,
	}
	exclude := map[string]bool{"skipme": true}

	res := buildUnusedIndexes(unused, unique, sizes, exclude, 3)
	assert.True(t, res.sizeKnown)
	assert.Equal(t, map[string]float64{"app": 3, "other": 1}, res.bySchema)
	assert.Equal(t, []unusedIndex{
		{indexKey: k("app", "orders", "idx_created"), size: sql.NullFloat64{Float64: 1 << 20, Valid: true}},
		{indexKey: k("app", "users", "idx_name"), size: sql.NullFloat64{Float64: 16384, Valid: true}},
		{indexKey: k("other", "t", "idx_a"), size: sql.NullFloat64{Float64: 16384, Valid: true}},
	}, res.top)

	// no access to mysql.innodb_index_stats: sorted by name, no sizes
	res = buildUnusedIndexes(unused, unique, nil, exclude, 2)
	assert.False(t, res.sizeKnown)
	assert.Equal(t, []unusedIndex{
		{indexKey: k("app", "orders", "idx_created")},
		{indexKey: k("app", "orders", "idx_status")},
	}, res.top)
	assert.Equal(t, map[string]float64{"app": 3, "other": 1}, res.bySchema)
}

func Test_rePartitionSuffix(t *testing.T) {
	for in, out := range map[string]string{
		"orders":             "orders",
		"orders#p#p0":        "orders",
		"orders#P#p2023":     "orders",
		"orders#p#p0#sp#sp1": "orders",
	} {
		assert.Equal(t, out, rePartitionSuffix.ReplaceAllString(in, ""))
	}
}
