package azure

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	dDBCpuUsage      = common.Desc("azure_db_cpu_usage_percent", "CPU utilization, percent")
	dDBMemoryUsage   = common.Desc("azure_db_memory_usage_percent", "Memory utilization, percent")
	dDBStorageUsage  = common.Desc("azure_db_storage_usage_percent", "Storage utilization, percent")
	dDBStorageUsed   = common.Desc("azure_db_storage_used_bytes", "Storage used")
	dDBIOPS          = common.Desc("azure_db_iops", "Disk I/O operations per second (PostgreSQL only)")
	dDBIOOps         = common.Desc("azure_db_io_ops_per_second", "Disk I/O operations per second by operation (PostgreSQL only)", "operation")
	dDBIOConsumption = common.Desc("azure_db_io_consumption_percent", "I/O utilization relative to the provisioned IOPS, percent (MySQL only)")
	dDBNetworkBytes  = common.Desc("azure_db_network_bytes_per_second", "Network throughput", "direction")
	dDBConnections   = common.Desc("azure_db_connections_active", "Number of active connections")
	dDBReplicaLag    = common.Desc("azure_db_replication_lag_seconds", "Replication lag of the read replica")

	dRedisCpuUsage     = common.Desc("azure_redis_cpu_usage_percent", "CPU utilization, percent")
	dRedisMemoryUsage  = common.Desc("azure_redis_memory_usage_percent", "Memory utilization, percent")
	dRedisMemoryUsed   = common.Desc("azure_redis_memory_used_bytes", "Memory used by the cache")
	dRedisServerLoad   = common.Desc("azure_redis_server_load_percent", "Server load, percent")
	dRedisConnections  = common.Desc("azure_redis_connected_clients", "Number of client connections")
	dRedisNetworkBytes = common.Desc("azure_redis_network_bytes_per_second", "Data read from (tx) and written to (rx) the cache", "direction")
)

type aggregation string

const (
	average aggregation = "Average"
	maximum aggregation = "Maximum"
	total   aggregation = "Total"

	metricsInterval   = time.Minute // the time grain (PT1M)
	metricsLookback   = 10 * time.Minute
	metricsConcurrent = 4
)

type metricDef struct {
	name        string
	aggregation aggregation
	desc        *prometheus.Desc
	label       string
	perSecond   bool // a Total over the time grain divided by its duration
}

// metricGroup is a set of metrics requested with a single call. The metrics are split into groups so that a metric
// missing for a resource (e.g. a new metric not available in a region) doesn't fail the others.
type metricGroup struct {
	namespace   string
	metrics     []metricDef
	replicaOnly bool
}

var (
	postgresMetricGroups = []metricGroup{
		{namespace: "Microsoft.DBforPostgreSQL/flexibleServers", metrics: []metricDef{
			{name: "cpu_percent", aggregation: average, desc: dDBCpuUsage},
			{name: "memory_percent", aggregation: average, desc: dDBMemoryUsage},
			{name: "storage_percent", aggregation: average, desc: dDBStorageUsage},
			{name: "storage_used", aggregation: average, desc: dDBStorageUsed},
			{name: "iops", aggregation: average, desc: dDBIOPS},
			{name: "network_bytes_ingress", aggregation: total, desc: dDBNetworkBytes, label: "rx", perSecond: true},
			{name: "network_bytes_egress", aggregation: total, desc: dDBNetworkBytes, label: "tx", perSecond: true},
			{name: "active_connections", aggregation: average, desc: dDBConnections},
		}},
		{namespace: "Microsoft.DBforPostgreSQL/flexibleServers", metrics: []metricDef{
			{name: "read_iops", aggregation: average, desc: dDBIOOps, label: "read"},
			{name: "write_iops", aggregation: average, desc: dDBIOOps, label: "write"},
		}},
		{namespace: "Microsoft.DBforPostgreSQL/flexibleServers", replicaOnly: true, metrics: []metricDef{
			{name: "read_replica_lag", aggregation: maximum, desc: dDBReplicaLag},
		}},
	}
	mysqlMetricGroups = []metricGroup{
		{namespace: "Microsoft.DBforMySQL/flexibleServers", metrics: []metricDef{
			{name: "cpu_percent", aggregation: average, desc: dDBCpuUsage},
			{name: "memory_percent", aggregation: average, desc: dDBMemoryUsage},
			{name: "storage_percent", aggregation: average, desc: dDBStorageUsage},
			{name: "storage_used", aggregation: average, desc: dDBStorageUsed},
			{name: "io_consumption_percent", aggregation: average, desc: dDBIOConsumption},
			{name: "network_bytes_ingress", aggregation: total, desc: dDBNetworkBytes, label: "rx", perSecond: true},
			{name: "network_bytes_egress", aggregation: total, desc: dDBNetworkBytes, label: "tx", perSecond: true},
			{name: "active_connections", aggregation: average, desc: dDBConnections},
		}},
		{namespace: "Microsoft.DBforMySQL/flexibleServers", replicaOnly: true, metrics: []metricDef{
			{name: "replication_lag", aggregation: maximum, desc: dDBReplicaLag},
		}},
	}
	redisMetricGroups = []metricGroup{
		{namespace: "Microsoft.Cache/redis", metrics: []metricDef{
			{name: "percentProcessorTime", aggregation: maximum, desc: dRedisCpuUsage},
			{name: "usedmemorypercentage", aggregation: maximum, desc: dRedisMemoryUsage},
			{name: "usedmemory", aggregation: maximum, desc: dRedisMemoryUsed},
			{name: "serverLoad", aggregation: maximum, desc: dRedisServerLoad},
			{name: "connectedclients", aggregation: maximum, desc: dRedisConnections},
			{name: "cacheRead", aggregation: maximum, desc: dRedisNetworkBytes, label: "tx"},
			{name: "cacheWrite", aggregation: maximum, desc: dRedisNetworkBytes, label: "rx"},
		}},
	}
)

type monitoringTarget struct {
	resourceID string
	group      metricGroup
}

type monitoringValue struct {
	desc  *prometheus.Desc
	value float64
	label string
}

// Monitoring holds the Azure Monitor metrics fetched by the discovery goroutine, served by the collectors.
type Monitoring struct {
	lock   sync.RWMutex
	values map[string][]monitoringValue // by resource id (lower-cased)
}

func NewMonitoring() *Monitoring {
	return &Monitoring{values: map[string][]monitoringValue{}}
}

func (m *Monitoring) refresh(d *Discoverer, targets []monitoringTarget) {
	end := time.Now().UTC().Truncate(time.Minute)
	timespan := end.Add(-metricsLookback).Format(time.RFC3339) + "/" + end.Format(time.RFC3339)
	values := map[string][]monitoringValue{}
	var lock sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, metricsConcurrent)
	for _, t := range targets {
		if d.ctx.Err() != nil { // stopped
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			var names []string
			for _, metric := range t.group.metrics {
				names = append(names, metric.name)
			}
			resp, err := d.api.metrics(t.resourceID, &armmonitor.MetricsClientListOptions{
				Metricnames:     to.Ptr(strings.Join(names, ",")),
				Metricnamespace: to.Ptr(t.group.namespace),
				Aggregation:     to.Ptr(string(average) + "," + string(maximum) + "," + string(total)),
				Interval:        to.Ptr("PT1M"),
				Timespan:        to.Ptr(timespan),
			})
			if err != nil {
				d.registerError(fmt.Errorf("Azure Monitor (%s): %w", strings.Join(names, ","), err))
				return
			}
			vs := metricValues(t.group, resp.Response)
			lock.Lock()
			values[t.resourceID] = append(values[t.resourceID], vs...)
			lock.Unlock()
		}()
	}
	wg.Wait()
	m.lock.Lock()
	m.values = values
	m.lock.Unlock()
}

// metricValues maps the response to the values: the latest data point having the aggregation of each metric
// (the latest time grains are often empty: the data is published with a delay), summed across the time series.
func metricValues(group metricGroup, resp armmonitor.Response) []monitoringValue {
	defs := map[string]metricDef{}
	for _, m := range group.metrics {
		defs[strings.ToLower(m.name)] = m
	}
	var res []monitoringValue
	for _, metric := range resp.Value {
		if metric == nil || metric.Name == nil || metric.Name.Value == nil {
			continue
		}
		def, ok := defs[strings.ToLower(*metric.Name.Value)]
		if !ok {
			continue
		}
		found := false
		sum := 0.0
		for _, ts := range metric.Timeseries {
			if ts == nil {
				continue
			}
			if v, ok := latest(ts.Data, def.aggregation); ok {
				sum += v
				found = true
			}
		}
		if !found {
			continue
		}
		if def.perSecond {
			sum /= metricsInterval.Seconds()
		}
		res = append(res, monitoringValue{desc: def.desc, value: sum, label: def.label})
	}
	return res
}

func latest(data []*armmonitor.MetricValue, a aggregation) (float64, bool) {
	var res *float64
	var ts time.Time
	for _, d := range data {
		if d == nil {
			continue
		}
		var v *float64
		switch a {
		case average:
			v = d.Average
		case maximum:
			v = d.Maximum
		case total:
			v = d.Total
		}
		if v == nil {
			continue
		}
		var t time.Time
		if d.TimeStamp != nil {
			t = *d.TimeStamp
		}
		if res == nil || !t.Before(ts) {
			res, ts = v, t
		}
	}
	if res == nil {
		return 0, false
	}
	return *res, true
}

func (m *Monitoring) collect(id string, ch chan<- prometheus.Metric) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, v := range m.values[id] {
		if v.label != "" {
			ch <- common.Gauge(v.desc, v.value, v.label)
		} else {
			ch <- common.Gauge(v.desc, v.value)
		}
	}
}
