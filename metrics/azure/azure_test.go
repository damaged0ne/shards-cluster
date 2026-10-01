package azure

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/mysql/armmysqlflexibleservers"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/postgresql/armpostgresqlflexibleservers/v4"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/redis/armredis/v3"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const sub = "00000000-0000-0000-0000-000000000001"

func pgID(rg, name string) string {
	return "/subscriptions/" + sub + "/resourceGroups/" + rg + "/providers/Microsoft.DBforPostgreSQL/flexibleServers/" + name
}

func mysqlID(rg, name string) string {
	return "/subscriptions/" + sub + "/resourceGroups/" + rg + "/providers/Microsoft.DBforMySQL/flexibleServers/" + name
}

func redisID(rg, name string) string {
	return "/subscriptions/" + sub + "/resourceGroups/" + rg + "/providers/Microsoft.Cache/Redis/" + name
}

type fakeAPI struct {
	lock     sync.Mutex
	postgres []*armpostgresqlflexibleservers.Server
	mysql    []*armmysqlflexibleservers.Server
	redis    []*armredis.ResourceInfo
	err      error
	lists    []string // subscription/resource group of the listings
	metricsF func(resourceID string, options *armmonitor.MetricsClientListOptions) (armmonitor.MetricsClientListResponse, error)
	requests []string
}

func (f *fakeAPI) listPostgres(subscription, resourceGroup string) ([]*armpostgresqlflexibleservers.Server, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.lists = append(f.lists, subscription+"/"+resourceGroup)
	return f.postgres, f.err
}

func (f *fakeAPI) listMySQL(string, string) ([]*armmysqlflexibleservers.Server, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.mysql, f.err
}

func (f *fakeAPI) listRedis(string, string) ([]*armredis.ResourceInfo, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.redis, f.err
}

func (f *fakeAPI) metrics(resourceID string, options *armmonitor.MetricsClientListOptions) (armmonitor.MetricsClientListResponse, error) {
	f.lock.Lock()
	f.requests = append(f.requests, resourceID+" "+*options.Metricnames)
	f.lock.Unlock()
	if f.metricsF == nil {
		return armmonitor.MetricsClientListResponse{}, nil
	}
	return f.metricsF(resourceID, options)
}

func newTestDiscoverer(reg prometheus.Registerer, cfg *config.AzureConfig, api azureAPI) *Discoverer {
	ctx, cancel := context.WithCancel(context.Background())
	if cfg == nil {
		cfg = &config.AzureConfig{}
	}
	d := newDiscoverer(ctx, cancel, cfg, []string{sub}, reg)
	d.api = api
	return d
}

func postgresServer(rg, name string, tags map[string]*string, props *armpostgresqlflexibleservers.ServerProperties) *armpostgresqlflexibleservers.Server {
	return &armpostgresqlflexibleservers.Server{
		ID:         to.Ptr(pgID(rg, name)),
		Name:       to.Ptr(name),
		Location:   to.Ptr("westeurope"),
		Tags:       tags,
		SKU:        &armpostgresqlflexibleservers.SKU{Name: to.Ptr("Standard_D2ds_v5"), Tier: to.Ptr(armpostgresqlflexibleservers.SKUTierGeneralPurpose)},
		Properties: props,
	}
}

func TestPostgresInfo(t *testing.T) {
	i, ok := postgresInfo(postgresServer("RG1", "pg1", map[string]*string{"env": to.Ptr("prod")}, &armpostgresqlflexibleservers.ServerProperties{
		FullyQualifiedDomainName: to.Ptr("pg1.postgres.database.azure.com"),
		Version:                  to.Ptr(armpostgresqlflexibleservers.ServerVersionSixteen),
		MinorVersion:             to.Ptr("4"),
		State:                    to.Ptr(armpostgresqlflexibleservers.ServerStateReady),
		AvailabilityZone:         to.Ptr("1"),
		HighAvailability:         &armpostgresqlflexibleservers.HighAvailability{Mode: to.Ptr(armpostgresqlflexibleservers.HighAvailabilityModeZoneRedundant)},
		Storage:                  &armpostgresqlflexibleservers.Storage{StorageSizeGB: to.Ptr[int32](128)},
		ReplicationRole:          to.Ptr(armpostgresqlflexibleservers.ReplicationRolePrimary),
	}))
	if !ok {
		t.Fatal("not mapped")
	}
	expected := dbInfo{
		id: strings.ToLower(pgID("RG1", "pg1")), key: sub + "/rg1/postgres/pg1", name: "pg1", subscription: sub, resourceGroup: "RG1",
		location: "westeurope", zone: "1", engine: "postgres", version: "16.4", fqdn: "pg1.postgres.database.azure.com", port: "5432",
		sku: "Standard_D2ds_v5", tier: "GeneralPurpose", highAvailability: "ZoneRedundant", role: "Primary", state: "Ready",
		storageBytes: 128 * gib, tags: map[string]string{"env": "prod"},
	}
	if !reflect.DeepEqual(i, expected) {
		t.Fatalf("unexpected info:\n%+v\nwant:\n%+v", i, expected)
	}

	// a replica
	i, _ = postgresInfo(postgresServer("rg1", "pg1-replica", nil, &armpostgresqlflexibleservers.ServerProperties{
		ReplicationRole:        to.Ptr(armpostgresqlflexibleservers.ReplicationRoleAsyncReplica),
		SourceServerResourceID: to.Ptr(pgID("rg1", "pg1")),
	}))
	if i.primary != "pg1" || i.primaryKey != sub+"/rg1/postgres/pg1" {
		t.Fatalf("unexpected primary: %+v", i)
	}
	// a server restored from a backup of another one is not its replica
	i, _ = postgresInfo(postgresServer("rg1", "pg1-restored", nil, &armpostgresqlflexibleservers.ServerProperties{
		ReplicationRole:        to.Ptr(armpostgresqlflexibleservers.ReplicationRoleNone),
		SourceServerResourceID: to.Ptr(pgID("rg1", "pg1")),
	}))
	if i.primary != "" {
		t.Fatalf("unexpected primary: %+v", i)
	}
	if _, ok := postgresInfo(&armpostgresqlflexibleservers.Server{ID: to.Ptr("bogus")}); ok {
		t.Fatal("an invalid id was mapped")
	}
	if _, ok := postgresInfo(nil); ok {
		t.Fatal("nil was mapped")
	}
}

func TestMySQLInfo(t *testing.T) {
	i, ok := mysqlInfo(&armmysqlflexibleservers.Server{
		ID:       to.Ptr(mysqlID("rg2", "my1-replica")),
		Location: to.Ptr("eastus"),
		SKU:      &armmysqlflexibleservers.SKU{Name: to.Ptr("Standard_B1ms"), Tier: to.Ptr(armmysqlflexibleservers.SKUTierBurstable)},
		Properties: &armmysqlflexibleservers.ServerProperties{
			FullyQualifiedDomainName: to.Ptr("my1-replica.mysql.database.azure.com"),
			Version:                  to.Ptr(armmysqlflexibleservers.ServerVersionEight021),
			State:                    to.Ptr(armmysqlflexibleservers.ServerStateReady),
			ReplicationRole:          to.Ptr(armmysqlflexibleservers.ReplicationRoleReplica),
			SourceServerResourceID:   to.Ptr(mysqlID("rg2", "my1")),
			Storage:                  &armmysqlflexibleservers.Storage{StorageSizeGB: to.Ptr[int32](20)},
		},
	})
	if !ok || i.engine != "mysql" || i.port != "3306" || i.version != "8.0.21" || i.primary != "my1" || i.role != "Replica" ||
		i.storageBytes != 20*gib || i.fqdn != "my1-replica.mysql.database.azure.com" || i.tier != "Burstable" {
		t.Fatalf("unexpected info: %+v", i)
	}
}

func TestRedisInfo(t *testing.T) {
	i, ok := redisCacheInfo(&armredis.ResourceInfo{
		ID:       to.Ptr(redisID("rg3", "cache1")),
		Location: to.Ptr("West Europe"),
		Tags:     map[string]*string{"team": to.Ptr("web")},
		Properties: &armredis.Properties{
			HostName:          to.Ptr("cache1.redis.cache.windows.net"),
			Port:              to.Ptr[int32](6379),
			SSLPort:           to.Ptr[int32](6380),
			EnableNonSSLPort:  to.Ptr(false),
			RedisVersion:      to.Ptr("6.0.14"),
			ProvisioningState: to.Ptr(armredis.ProvisioningStateSucceeded),
			SKU:               &armredis.SKU{Name: to.Ptr(armredis.SKUNamePremium), Family: to.Ptr(armredis.SKUFamilyP), Capacity: to.Ptr[int32](1)},
			ShardCount:        to.Ptr[int32](2),
		},
	})
	if !ok || i.host != "cache1.redis.cache.windows.net" || i.sslPort != 6380 || i.nonSSLPortEnabled || i.sku != "Premium" || i.family != "P" ||
		i.capacity != 1 || i.shards != 2 || i.state != "Succeeded" || i.key != sub+"/rg3/redis/cache1" || i.tags["team"] != "web" {
		t.Fatalf("unexpected info: %+v", i)
	}
}

func TestDiscover(t *testing.T) {
	reg := prometheus.NewRegistry()
	api := &fakeAPI{
		postgres: []*armpostgresqlflexibleservers.Server{
			postgresServer("rg1", "pg1", map[string]*string{"env": to.Ptr("prod")}, &armpostgresqlflexibleservers.ServerProperties{
				FullyQualifiedDomainName: to.Ptr("pg1.postgres.database.azure.com"),
				ReplicationRole:          to.Ptr(armpostgresqlflexibleservers.ReplicationRolePrimary),
			}),
			// untagged replica of a matched primary: discovered
			postgresServer("rg1", "pg1-r", nil, &armpostgresqlflexibleservers.ServerProperties{
				FullyQualifiedDomainName: to.Ptr("pg1-r.postgres.database.azure.com"),
				ReplicationRole:          to.Ptr(armpostgresqlflexibleservers.ReplicationRoleAsyncReplica),
				SourceServerResourceID:   to.Ptr(pgID("rg1", "pg1")),
			}),
			postgresServer("rg1", "pg2", map[string]*string{"env": to.Ptr("dev")}, &armpostgresqlflexibleservers.ServerProperties{
				FullyQualifiedDomainName: to.Ptr("pg2.postgres.database.azure.com"),
			}),
		},
		mysql: []*armmysqlflexibleservers.Server{
			{ID: to.Ptr(mysqlID("rg1", "pg1")), Location: to.Ptr("westeurope"), // the same name as a PostgreSQL server
				Properties: &armmysqlflexibleservers.ServerProperties{FullyQualifiedDomainName: to.Ptr("pg1.mysql.database.azure.com")}},
			{ID: to.Ptr(mysqlID("rg1", "far")), Location: to.Ptr("eastus"),
				Properties: &armmysqlflexibleservers.ServerProperties{FullyQualifiedDomainName: to.Ptr("far.mysql.database.azure.com")}},
		},
		redis: []*armredis.ResourceInfo{
			{ID: to.Ptr(redisID("rg1", "tls")), Location: to.Ptr("westeurope"), Properties: &armredis.Properties{
				HostName: to.Ptr("tls.redis.cache.windows.net"), Port: to.Ptr[int32](6379), SSLPort: to.Ptr[int32](6380), EnableNonSSLPort: to.Ptr(false)}},
			{ID: to.Ptr(redisID("rg1", "plain")), Location: to.Ptr("westeurope"), Properties: &armredis.Properties{
				HostName: to.Ptr("plain.redis.cache.windows.net"), Port: to.Ptr[int32](6379), SSLPort: to.Ptr[int32](6380), EnableNonSSLPort: to.Ptr(true)}},
		},
	}
	cfg := &config.AzureConfig{
		ResourceGroups:     []string{"rg1", "rg2"},
		Locations:          []string{"West Europe"},
		PostgresTagFilters: map[string]string{"env": "prod"},
	}
	d := newTestDiscoverer(reg, cfg, api)
	reg.MustRegister(d)
	api.metricsF = func(resourceID string, options *armmonitor.MetricsClientListOptions) (armmonitor.MetricsClientListResponse, error) {
		if strings.HasSuffix(resourceID, "/pg1") && strings.Contains(*options.Metricnames, "cpu_percent") {
			return metricsResponse("cpu_percent", &armmonitor.MetricValue{TimeStamp: to.Ptr(time.Now()), Average: to.Ptr(12.5)}), nil
		}
		return armmonitor.MetricsClientListResponse{}, nil
	}
	d.discover()

	if strings.Join(api.lists, ",") != sub+"/rg1,"+sub+"/rg2" {
		t.Fatalf("unexpected listings: %v", api.lists)
	}
	if len(d.dbCollectors) != 3 { // pg1, pg1-r, mysql pg1
		t.Fatalf("unexpected DB collectors: %v", keys(d.dbCollectors))
	}
	if len(d.redisCollectors) != 2 {
		t.Fatalf("unexpected Redis collectors: %v", keys(d.redisCollectors))
	}
	if e, ok := d.DBEndpoint("postgres", "pg1"); !ok || e != (common.Endpoint{Host: "pg1.postgres.database.azure.com", Port: "5432"}) {
		t.Fatalf("unexpected endpoint: %v", e)
	}
	if e, ok := d.DBEndpoint("mysql", "pg1"); !ok || e != (common.Endpoint{Host: "pg1.mysql.database.azure.com", Port: "3306"}) {
		t.Fatalf("unexpected endpoint: %v", e)
	}
	if _, ok := d.DBEndpoint("postgres", "pg2"); ok {
		t.Fatal("pg2 must be filtered out by the tags")
	}
	if _, ok := d.DBEndpoint("mysql", "far"); ok {
		t.Fatal("far must be filtered out by the location")
	}
	if r := d.DBReplicas("postgres", "pg1"); len(r) != 1 || r[0] != "pg1-r" {
		t.Fatalf("unexpected replicas: %v", r)
	}
	if e, tls, ok := d.RedisEndpoint("tls"); !ok || !tls || e.Port != "6380" {
		t.Fatalf("unexpected endpoint: %v %v", e, tls)
	}
	if e, tls, ok := d.RedisEndpoint("plain"); !ok || tls || e.Port != "6379" {
		t.Fatalf("unexpected endpoint: %v %v", e, tls)
	}

	// the replica lag is requested only for the replica
	replicaLag := 0
	for _, r := range api.requests {
		if strings.Contains(r, "read_replica_lag") {
			replicaLag++
			if !strings.HasSuffix(strings.Fields(r)[0], "/pg1-r") {
				t.Errorf("unexpected replica lag request: %s", r)
			}
		}
	}
	if replicaLag != 1 {
		t.Errorf("unexpected requests: %v", api.requests)
	}

	metrics := gatherByID(t, reg, "azure_db_id", sub+"/rg1/postgres/pg1")
	if m := metrics["azure_db_cpu_usage_percent"]; m == nil || m.GetGauge().GetValue() != 12.5 {
		t.Fatalf("unexpected CPU usage: %v", m)
	}
	if m := metrics["azure_db_info"]; m == nil || label(m, "engine") != "postgres" {
		t.Fatalf("unexpected info: %v", m)
	}

	// a failed listing keeps the collectors
	api.err = errors.New("boom")
	d.discover()
	if len(d.dbCollectors) != 3 || len(d.redisCollectors) != 2 {
		t.Fatal("the collectors were dropped on an error")
	}
	// deleted resources
	api.err = nil
	api.postgres, api.mysql, api.redis = nil, nil, nil
	d.discover()
	if len(d.dbCollectors) != 0 || len(d.redisCollectors) != 0 {
		t.Fatal("the collectors of the deleted resources were not dropped")
	}
	if len(gatherByID(t, reg, "azure_db_id", sub+"/rg1/postgres/pg1")) != 0 {
		t.Fatal("the collector was not unregistered")
	}
}

func keys[V any](m map[string]V) []string {
	var res []string
	for k := range m {
		res = append(res, k)
	}
	return res
}

func label(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

func gatherByID(t *testing.T, reg *prometheus.Registry, labelName, id string) map[string]*dto.Metric {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	res := map[string]*dto.Metric{}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			if label(m, labelName) == id {
				res[mf.GetName()] = m
			}
		}
	}
	return res
}

func metricsResponse(name string, data ...*armmonitor.MetricValue) armmonitor.MetricsClientListResponse {
	return armmonitor.MetricsClientListResponse{Response: armmonitor.Response{Value: []*armmonitor.Metric{
		{Name: &armmonitor.LocalizableString{Value: to.Ptr(name)}, Timeseries: []*armmonitor.TimeSeriesElement{{Data: data}}},
	}}}
}

func TestMetricValues(t *testing.T) {
	now := time.Now()
	group := metricGroup{metrics: []metricDef{
		{name: "cpu_percent", aggregation: average, desc: dDBCpuUsage},
		{name: "network_bytes_egress", aggregation: total, desc: dDBNetworkBytes, label: "tx", perSecond: true},
		{name: "usedmemory", aggregation: maximum, desc: dRedisMemoryUsed},
		{name: "active_connections", aggregation: average, desc: dDBConnections},
	}}
	resp := armmonitor.Response{Value: []*armmonitor.Metric{
		{Name: &armmonitor.LocalizableString{Value: to.Ptr("cpu_percent")}, Timeseries: []*armmonitor.TimeSeriesElement{{Data: []*armmonitor.MetricValue{
			{TimeStamp: to.Ptr(now.Add(-3 * time.Minute)), Average: to.Ptr(10.0)},
			{TimeStamp: to.Ptr(now.Add(-2 * time.Minute)), Average: to.Ptr(20.0)},
			{TimeStamp: to.Ptr(now.Add(-time.Minute))}, // not published yet
		}}}},
		{Name: &armmonitor.LocalizableString{Value: to.Ptr("Network_Bytes_Egress")}, Timeseries: []*armmonitor.TimeSeriesElement{{Data: []*armmonitor.MetricValue{
			{TimeStamp: to.Ptr(now), Total: to.Ptr(6000.0)},
		}}}},
		// several time series (e.g. per shard): summed
		{Name: &armmonitor.LocalizableString{Value: to.Ptr("usedmemory")}, Timeseries: []*armmonitor.TimeSeriesElement{
			{Data: []*armmonitor.MetricValue{{TimeStamp: to.Ptr(now), Maximum: to.Ptr(100.0), Average: to.Ptr(1.0)}}},
			{Data: []*armmonitor.MetricValue{{TimeStamp: to.Ptr(now), Maximum: to.Ptr(50.0)}}},
		}},
		// no data points
		{Name: &armmonitor.LocalizableString{Value: to.Ptr("active_connections")}, Timeseries: []*armmonitor.TimeSeriesElement{{}}},
		// not requested
		{Name: &armmonitor.LocalizableString{Value: to.Ptr("other")}, Timeseries: []*armmonitor.TimeSeriesElement{{Data: []*armmonitor.MetricValue{{Average: to.Ptr(1.0)}}}}},
		nil,
	}}
	values := metricValues(group, resp)
	got := map[*prometheus.Desc]monitoringValue{}
	for _, v := range values {
		got[v.desc] = v
	}
	if len(values) != 3 {
		t.Fatalf("unexpected values: %v", values)
	}
	if v := got[dDBCpuUsage]; v.value != 20 {
		t.Errorf("unexpected CPU usage: %v", v)
	}
	if v := got[dDBNetworkBytes]; v.value != 100 || v.label != "tx" {
		t.Errorf("unexpected network: %v", v)
	}
	if v := got[dRedisMemoryUsed]; v.value != 150 {
		t.Errorf("unexpected memory: %v", v)
	}
}

func TestMonitoringRefreshErrors(t *testing.T) {
	api := &fakeAPI{metricsF: func(resourceID string, options *armmonitor.MetricsClientListOptions) (armmonitor.MetricsClientListResponse, error) {
		if strings.Contains(*options.Metricnames, "read_iops") {
			return armmonitor.MetricsClientListResponse{}, errors.New("metric not supported")
		}
		if *options.Interval != "PT1M" || !strings.Contains(*options.Timespan, "/") || *options.Metricnamespace != "Microsoft.DBforPostgreSQL/flexibleServers" {
			t.Errorf("unexpected options: %+v", options)
		}
		return metricsResponse("memory_percent", &armmonitor.MetricValue{Average: to.Ptr(55.0)}), nil
	}}
	d := newTestDiscoverer(nil, nil, api)
	targets := []monitoringTarget{{resourceID: "id1", group: postgresMetricGroups[0]}, {resourceID: "id1", group: postgresMetricGroups[1]}}
	d.monitoring.refresh(d, targets)
	if len(d.errors) != 1 {
		t.Fatalf("unexpected errors: %v", d.errors)
	}
	ch := make(chan prometheus.Metric, 10)
	d.monitoring.collect("id1", ch)
	close(ch)
	n := 0
	for range ch {
		n++
	}
	if n != 1 { // the failed group doesn't affect the other one
		t.Fatalf("unexpected number of metrics: %d", n)
	}
}

func TestDiscovererStop(t *testing.T) {
	reg := prometheus.NewRegistry()
	d := newTestDiscoverer(reg, nil, &fakeAPI{})
	reg.MustRegister(d)
	c := &DBCollector{discoverer: d, info: dbInfo{key: "k1", name: "db1"}}
	prometheus.WrapRegistererWith(dbLabels("k1"), reg).MustRegister(c)
	d.dbCollectors["k1"] = c
	r := &RedisCollector{discoverer: d, info: redisInfo{key: "k2", name: "cache1"}}
	prometheus.WrapRegistererWith(redisLabels("k2"), reg).MustRegister(r)
	d.redisCollectors["k2"] = r

	started := make(chan struct{})
	go d.run(func() {
		close(started)
		<-d.ctx.Done() // an in-flight API call: returns only when the context is cancelled
	})
	<-started
	stopped := make(chan struct{})
	go func() {
		d.Stop()
		d.Stop() // idempotent
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop blocked")
	}
	if mfs, _ := reg.Gather(); len(mfs) != 0 {
		t.Fatalf("collectors left registered: %v", mfs)
	}
	// the errors caused by the cancellation are not reported
	d.registerError(context.Canceled)
	if len(d.errors) != 0 {
		t.Fatalf("unexpected errors: %v", d.errors)
	}
}

func TestCollectorsUpdateConcurrentlyWithCollect(t *testing.T) {
	api := &fakeAPI{metricsF: func(string, *armmonitor.MetricsClientListOptions) (armmonitor.MetricsClientListResponse, error) {
		return metricsResponse("cpu_percent", &armmonitor.MetricValue{Average: to.Ptr(1.0)}), nil
	}}
	d := newTestDiscoverer(nil, nil, api)
	db := &DBCollector{discoverer: d, info: dbInfo{id: "id1", key: "k1", engine: "postgres"}}
	rd := &RedisCollector{discoverer: d, info: redisInfo{id: "id2", key: "k2"}}
	d.dbCollectors["k1"] = db
	d.redisCollectors["k2"] = rd
	reg := prometheus.NewRegistry()
	reg.MustRegister(db, rd)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			db.setInfo(dbInfo{id: "id1", key: "k1", engine: "postgres", state: "Ready", fqdn: "h"})
			rd.setInfo(redisInfo{id: "id2", key: "k2", state: "Succeeded", host: "h", sslPort: 6380})
			d.monitoring.refresh(d, d.monitoringTargets())
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if _, err := reg.Gather(); err != nil {
				t.Error(err)
				return
			}
			d.publishEndpoints()
		}
	}()
	wg.Wait()
}
