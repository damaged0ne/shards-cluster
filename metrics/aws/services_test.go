package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	ectypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/aws/aws-sdk-go-v2/service/memorydb"
	mdbtypes "github.com/aws/aws-sdk-go-v2/service/memorydb/types"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/smithy-go"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type fakeCloudWatch struct {
	lock  sync.Mutex
	calls []*cloudwatch.GetMetricDataInput
	fail  func(call int) error
	// values returns the data points of a query, split into two pages if paged
	values func(q cwtypes.MetricDataQuery) ([]float64, []time.Time)
	paged  bool
}

func (f *fakeCloudWatch) GetMetricData(_ context.Context, in *cloudwatch.GetMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error) {
	f.lock.Lock()
	call := len(f.calls)
	f.calls = append(f.calls, in)
	f.lock.Unlock()
	if f.fail != nil {
		if err := f.fail(call); err != nil {
			return nil, err
		}
	}
	out := &cloudwatch.GetMetricDataOutput{}
	secondPage := aws.ToString(in.NextToken) == "page2"
	for _, q := range in.MetricDataQueries {
		values, timestamps := f.values(q)
		if f.paged && len(values) > 1 {
			if secondPage {
				values, timestamps = values[1:], timestamps[1:]
			} else {
				values, timestamps = values[:1], timestamps[:1]
			}
		} else if secondPage {
			continue
		}
		out.MetricDataResults = append(out.MetricDataResults, cwtypes.MetricDataResult{Id: q.Id, Values: values, Timestamps: timestamps})
	}
	if f.paged && !secondPage {
		out.NextToken = aws.String("page2")
	}
	return out, nil
}

func cwValues(values []cwValue, desc *prometheus.Desc) []float64 {
	var res []float64
	for _, v := range values {
		if v.desc == desc {
			res = append(res, v.value)
		}
	}
	return res
}

func TestMapMetricDataResults(t *testing.T) {
	now := time.Now()
	queries := append(
		auroraQueries("r/db1", "db1", "db.serverless", auroraInstance{}, false),
		serverlessCacheQueries("r/cache1", "cache1")...,
	)
	if len(queries) != 6 {
		t.Fatalf("unexpected queries: %v", queries)
	}
	results := []cwtypes.MetricDataResult{
		// ascending order: the newest value is picked by the timestamps
		{Id: aws.String("q0"), Values: []float64{100, 250}, Timestamps: []time.Time{now.Add(-2 * time.Minute), now.Add(-time.Minute)}},
		{Id: aws.String("q1"), Values: []float64{4.5}, Timestamps: []time.Time{now}},
		{Id: aws.String("q2"), Values: nil}, // no data: not served
		{Id: aws.String("q1"), Values: []float64{1}, Timestamps: []time.Time{now.Add(-time.Hour)}}, // a later page: ignored
		{Id: aws.String("q3"), Values: []float64{6000}, Timestamps: []time.Time{now}},
		{Id: aws.String("q4"), Values: []float64{1024}, Timestamps: []time.Time{now}},
		{Id: aws.String("q99"), Values: []float64{1}},
		{Id: aws.String("bogus"), Values: []float64{1}},
	}
	values := map[string][]cwValue{}
	mapMetricDataResults(queries, results, time.Minute, values)
	if v := cwValues(values["r/db1"], dRDSAuroraReplicaLag); len(v) != 1 || v[0] != 0.25 {
		t.Errorf("unexpected replica lag: %v", v)
	}
	if v := cwValues(values["r/db1"], dRDSServerlessCapacity); len(v) != 1 || v[0] != 4.5 {
		t.Errorf("unexpected capacity: %v", v)
	}
	if v := cwValues(values["r/db1"], dRDSServerlessUtilization); len(v) != 0 {
		t.Errorf("unexpected utilization: %v", v)
	}
	if v := cwValues(values["r/cache1"], dECServerlessECPU); len(v) != 1 || v[0] != 100 { // a Sum over 60s
		t.Errorf("unexpected ECPUs: %v", v)
	}
	if v := cwValues(values["r/cache1"], dECServerlessBytesUsed); len(v) != 1 || v[0] != 1024 {
		t.Errorf("unexpected bytes used: %v", v)
	}
	if len(values) != 2 {
		t.Errorf("unexpected keys: %v", values)
	}
}

func TestAuroraQueries(t *testing.T) {
	names := func(qs []cwQuery) []string {
		var res []string
		for _, q := range qs {
			res = append(res, q.metric)
		}
		return res
	}
	if got := names(auroraQueries("k", "db1", "db.r6g.large", auroraInstance{writer: true}, true)); len(got) != 0 {
		t.Errorf("writer: %v", got)
	}
	if got := names(auroraQueries("k", "db1", "db.r6g.large", auroraInstance{}, true)); fmt.Sprint(got) != "[AuroraReplicaLag]" {
		t.Errorf("reader: %v", got)
	}
	if got := names(auroraQueries("k", "db1", "db.serverless", auroraInstance{writer: true}, true)); fmt.Sprint(got) != "[ServerlessDatabaseCapacity ACUUtilization]" {
		t.Errorf("serverless writer: %v", got)
	}
	q := auroraQueries("k", "db1", "db.r6g.large", auroraInstance{}, false)[0]
	if q.String() != "AWS/RDS/AuroraReplicaLag{DBInstanceIdentifier=db1}" {
		t.Errorf("unexpected query: %s", q)
	}
}

func TestCloudWatchPeriod(t *testing.T) {
	for _, c := range []struct {
		seconds  int
		expected time.Duration
	}{{0, time.Minute}, {-5, time.Minute}, {30, time.Minute}, {60, time.Minute}, {61, 2 * time.Minute}, {300, 5 * time.Minute}} {
		if got := cloudWatchPeriod(&config.AWSConfig{CloudWatchPeriodSeconds: c.seconds}); got != c.expected {
			t.Errorf("cloudWatchPeriod(%d) = %s, want %s", c.seconds, got, c.expected)
		}
	}
	if cloudWatchPeriod(nil) != time.Minute {
		t.Error("unexpected default")
	}
}

func TestFetchCloudWatchBatches(t *testing.T) {
	d := newTestDiscoverer(prometheus.NewRegistry())
	var queries []cwQuery
	for i := 0; i < 600; i++ {
		queries = append(queries, serverlessCacheQueries(fmt.Sprintf("r/c%d", i), fmt.Sprintf("c%d", i))[1]) // BytesUsedForCache
	}
	now := time.Now()
	f := &fakeCloudWatch{paged: true, values: func(q cwtypes.MetricDataQuery) ([]float64, []time.Time) {
		// newest first, the second value comes with the second page
		return []float64{7, 3}, []time.Time{now, now.Add(-time.Minute)}
	}}
	values := d.fetchCloudWatch(f, queries, time.Minute, now)
	if len(f.calls) != 4 { // 2 batches (500 + 100) x 2 pages
		t.Fatalf("unexpected number of calls: %d", len(f.calls))
	}
	if n := len(f.calls[0].MetricDataQueries); n != 500 {
		t.Fatalf("unexpected batch size: %d", n)
	}
	if n := len(f.calls[2].MetricDataQueries); n != 100 {
		t.Fatalf("unexpected batch size: %d", n)
	}
	in := f.calls[0]
	if in.ScanBy != cwtypes.ScanByTimestampDescending || in.EndTime.Sub(*in.StartTime) != cloudWatchLookback*time.Minute {
		t.Fatalf("unexpected input: %+v", in)
	}
	if p := aws.ToInt32(in.MetricDataQueries[0].MetricStat.Period); p != 60 {
		t.Fatalf("unexpected period: %d", p)
	}
	if len(values) != 600 {
		t.Fatalf("unexpected values: %d", len(values))
	}
	if v := cwValues(values["r/c599"], dECServerlessBytesUsed); len(v) != 1 || v[0] != 7 {
		t.Fatalf("unexpected value: %v", v)
	}

	// a failed batch: its values are not served, the error is reported
	f = &fakeCloudWatch{values: f.values, fail: func(call int) error {
		if call == 1 {
			return errors.New("throttled")
		}
		return nil
	}}
	values = d.fetchCloudWatch(f, queries, time.Minute, now)
	if len(values) != 500 {
		t.Fatalf("unexpected values: %d", len(values))
	}
	if !d.errors["throttled"] {
		t.Fatalf("the error wasn't reported: %v", d.errors)
	}
}

func TestOptionalAPIAccessDenied(t *testing.T) {
	d := newTestDiscoverer(prometheus.NewRegistry())
	denied := &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "not authorized to perform: cloudwatch:GetMetricData"}
	f := &fakeCloudWatch{fail: func(int) error { return denied }}
	values := d.fetchCloudWatch(f, serverlessCacheQueries("r/c", "c"), time.Minute, time.Now())
	if len(values) != 0 {
		t.Fatalf("unexpected values: %v", values)
	}
	if len(d.errors) != 0 {
		t.Fatalf("access denied to an optional API must not be reported as a discovery error: %v", d.errors)
	}
	if !d.deniedAPIs["cloudwatch:GetMetricData"] {
		t.Fatal("the denied API wasn't recorded")
	}
	d.registerOptionalAPIError("memorydb:DescribeClusters", errors.New("boom"))
	if !d.errors["boom"] {
		t.Fatalf("other errors must be reported: %v", d.errors)
	}
}

type fakeRDSClusters struct {
	input    *rds.DescribeDBClustersInput
	clusters []rdstypes.DBCluster
}

func (f *fakeRDSClusters) DescribeDBClusters(_ context.Context, in *rds.DescribeDBClustersInput, _ ...func(*rds.Options)) (*rds.DescribeDBClustersOutput, error) {
	f.input = in
	return &rds.DescribeDBClustersOutput{DBClusters: f.clusters}, nil
}

func TestDiscoverAurora(t *testing.T) {
	d := newTestDiscoverer(prometheus.NewRegistry())
	f := &fakeRDSClusters{clusters: []rdstypes.DBCluster{
		{
			DBClusterIdentifier: aws.String("c1"),
			DBClusterMembers: []rdstypes.DBClusterMember{
				{DBInstanceIdentifier: aws.String("w"), IsClusterWriter: aws.Bool(true)},
				{DBInstanceIdentifier: aws.String("r"), IsClusterWriter: aws.Bool(false)},
			},
			ServerlessV2ScalingConfiguration: &rdstypes.ServerlessV2ScalingConfigurationInfo{MinCapacity: aws.Float64(0.5), MaxCapacity: aws.Float64(16)},
		},
	}}

	// no Aurora instances: DescribeDBClusters isn't called
	d.rdsCollectors["us-east-1/pg"] = &RDSCollector{instance: &rdstypes.DBInstance{DBInstanceIdentifier: aws.String("pg"), Engine: aws.String("postgres")}}
	d.discoverAurora("us-east-1", f)
	if f.input != nil {
		t.Fatal("DescribeDBClusters called without Aurora instances")
	}

	writer := &rdstypes.DBInstance{DBInstanceIdentifier: aws.String("w"), Engine: aws.String("aurora-postgresql"), DBInstanceClass: aws.String("db.serverless")}
	reader := &rdstypes.DBInstance{DBInstanceIdentifier: aws.String("r"), Engine: aws.String("aurora-postgresql"), DBInstanceClass: aws.String("db.r6g.large")}
	d.rdsCollectors["us-east-1/w"] = &RDSCollector{discoverer: d, region: "us-east-1", instance: writer}
	d.rdsCollectors["us-east-1/r"] = &RDSCollector{discoverer: d, region: "us-east-1", instance: reader}
	d.discoverAurora("us-east-1", f)
	if f.input == nil || len(f.input.Filters) != 1 || aws.ToString(f.input.Filters[0].Name) != "engine" {
		t.Fatalf("unexpected input: %+v", f.input)
	}
	if a, ok := d.auroraInstance("us-east-1/w"); !ok || !a.writer || a.maxACU != 16 || a.minACU != 0.5 {
		t.Fatalf("unexpected writer info: %+v %v", a, ok)
	}
	if a, ok := d.auroraInstance("us-east-1/r"); !ok || a.writer {
		t.Fatalf("unexpected reader info: %+v %v", a, ok)
	}

	queries := d.buildCloudWatchQueries()
	byKey := map[string][]string{}
	for _, q := range queries {
		byKey[q.key] = append(byKey[q.key], q.metric)
	}
	if fmt.Sprint(byKey["us-east-1/w"]) != "[ServerlessDatabaseCapacity ACUUtilization]" || fmt.Sprint(byKey["us-east-1/r"]) != "[AuroraReplicaLag]" || len(byKey) != 2 {
		t.Fatalf("unexpected queries: %v", byKey)
	}

	d.cloudwatch.set(map[string][]cwValue{"us-east-1/r": {{desc: dRDSAuroraReplicaLag, value: 0.02}}})
	m := gather(t, d.rdsCollectors["us-east-1/r"])
	if v := m["aws_rds_aurora_replica_lag_seconds"]; len(v) != 1 || v[0].GetGauge().GetValue() != 0.02 {
		t.Fatalf("unexpected replica lag: %v", v)
	}
	if v := m["aws_rds_cluster_role"]; len(v) != 1 || labelValue(v[0], "role") != "reader" {
		t.Fatalf("unexpected role: %v", v)
	}
	m = gather(t, d.rdsCollectors["us-east-1/w"])
	if v := m["aws_rds_serverless_max_capacity_acu"]; len(v) != 1 || v[0].GetGauge().GetValue() != 16 {
		t.Fatalf("unexpected max capacity: %v", v)
	}
}

func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

type fakeElastiCache struct {
	lock      sync.Mutex
	caches    []ectypes.ServerlessCache
	tags      map[string]map[string]string
	tagsCalls int
	err       error
}

func (f *fakeElastiCache) DescribeServerlessCaches(_ context.Context, in *elasticache.DescribeServerlessCachesInput, _ ...func(*elasticache.Options)) (*elasticache.DescribeServerlessCachesOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	// two pages
	if in.NextToken == nil && len(f.caches) > 1 {
		return &elasticache.DescribeServerlessCachesOutput{ServerlessCaches: f.caches[:1], NextToken: aws.String("next")}, nil
	}
	if in.NextToken != nil {
		return &elasticache.DescribeServerlessCachesOutput{ServerlessCaches: f.caches[1:]}, nil
	}
	return &elasticache.DescribeServerlessCachesOutput{ServerlessCaches: f.caches}, nil
}

func (f *fakeElastiCache) ListTagsForResource(_ context.Context, in *elasticache.ListTagsForResourceInput, _ ...func(*elasticache.Options)) (*elasticache.ListTagsForResourceOutput, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.tagsCalls++
	out := &elasticache.ListTagsForResourceOutput{}
	for k, v := range f.tags[aws.ToString(in.ResourceName)] {
		out.TagList = append(out.TagList, ectypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	return out, nil
}

func serverlessCache(name string) ectypes.ServerlessCache {
	return ectypes.ServerlessCache{
		ServerlessCacheName: aws.String(name),
		ARN:                 aws.String("arn:aws:elasticache:us-east-1:1:serverlesscache:" + name),
		Engine:              aws.String("valkey"),
		FullEngineVersion:   aws.String("8.0"),
		Status:              aws.String("available"),
		Endpoint:            &ectypes.Endpoint{Address: aws.String(name + ".serverless.use1.cache.amazonaws.com"), Port: aws.Int32(6379)},
		ReaderEndpoint:      &ectypes.Endpoint{Address: aws.String(name + ".reader"), Port: aws.Int32(6380)},
		CacheUsageLimits: &ectypes.CacheUsageLimits{
			DataStorage:   &ectypes.DataStorage{Maximum: aws.Int32(10), Unit: ectypes.DataStorageUnitGb},
			ECPUPerSecond: &ectypes.ECPUPerSecond{Maximum: aws.Int32(5000)},
		},
	}
}

func TestDiscoverECServerless(t *testing.T) {
	reg := prometheus.NewRegistry()
	d := newTestDiscoverer(reg)
	f := &fakeElastiCache{
		caches: []ectypes.ServerlessCache{serverlessCache("prod"), serverlessCache("dev")},
		tags: map[string]map[string]string{
			"arn:aws:elasticache:us-east-1:1:serverlesscache:prod": {"env": "production"},
			"arn:aws:elasticache:us-east-1:1:serverlesscache:dev":  {"env": "dev"},
		},
	}
	cfg := &config.AWSConfig{ElasticacheTagFilters: map[string]string{"env": "prod*"}}
	d.discoverECServerless(cfg, "us-east-1", f)
	if len(d.ecServerlessCollectors) != 1 || d.ecServerlessCollectors["us-east-1/prod"] == nil {
		t.Fatalf("unexpected collectors: %v", d.ecServerlessCollectors)
	}
	d.discoverECServerless(cfg, "us-east-1", f)
	if f.tagsCalls != 2 {
		t.Fatalf("the tags must be cached, got %d calls", f.tagsCalls)
	}

	d.publishEndpoints()
	if e := d.ElastiCacheEndpoints("prod"); len(e) != 1 || e[0] != (common.Endpoint{Host: "prod.serverless.use1.cache.amazonaws.com", Port: "6379"}) {
		t.Fatalf("unexpected endpoints: %v", e)
	}
	if !d.ElastiCacheRequiresTLS("prod") || d.ElastiCacheRequiresTLS("dev") {
		t.Fatal("unexpected TLS flags")
	}

	d.cloudwatch.set(map[string][]cwValue{"us-east-1/prod": {{desc: dECServerlessECPU, value: 42}}})
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]*dto.Metric{}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			if labelValue(m, "ec_serverless_id") == "us-east-1/prod" {
				found[mf.GetName()] = m
			}
		}
	}
	for name, value := range map[string]float64{
		"aws_elasticache_serverless_info":                     1,
		"aws_elasticache_serverless_status":                   1,
		"aws_elasticache_serverless_data_storage_limit_bytes": 10 << 30,
		"aws_elasticache_serverless_ecpu_limit_per_second":    5000,
		"aws_elasticache_serverless_ecpu_per_second":          42,
	} {
		if m := found[name]; m == nil || m.GetGauge().GetValue() != value {
			t.Errorf("unexpected %s: %v", name, m)
		}
	}
	if m := found["aws_elasticache_serverless_info"]; labelValue(m, "reader_endpoint") != "prod.reader" || labelValue(m, "engine") != "valkey" {
		t.Errorf("unexpected info: %v", m)
	}

	// a failed listing keeps the collectors
	f.err = errors.New("boom")
	d.discoverECServerless(cfg, "us-east-1", f)
	if len(d.ecServerlessCollectors) != 1 {
		t.Fatal("the collectors were dropped on an error")
	}
	// a deleted cache
	f.err = nil
	f.caches = []ectypes.ServerlessCache{serverlessCache("dev")}
	d.discoverECServerless(cfg, "us-east-1", f)
	if len(d.ecServerlessCollectors) != 0 {
		t.Fatalf("unexpected collectors: %v", d.ecServerlessCollectors)
	}
}

type fakeMemoryDB struct {
	clusters []mdbtypes.Cluster
	tags     map[string]map[string]string
	input    *memorydb.DescribeClustersInput
}

func (f *fakeMemoryDB) DescribeClusters(_ context.Context, in *memorydb.DescribeClustersInput, _ ...func(*memorydb.Options)) (*memorydb.DescribeClustersOutput, error) {
	f.input = in
	return &memorydb.DescribeClustersOutput{Clusters: f.clusters}, nil
}

func (f *fakeMemoryDB) ListTags(_ context.Context, in *memorydb.ListTagsInput, _ ...func(*memorydb.Options)) (*memorydb.ListTagsOutput, error) {
	out := &memorydb.ListTagsOutput{}
	for k, v := range f.tags[aws.ToString(in.ResourceArn)] {
		out.TagList = append(out.TagList, mdbtypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	return out, nil
}

func TestDiscoverMemoryDB(t *testing.T) {
	reg := prometheus.NewRegistry()
	d := newTestDiscoverer(reg)
	f := &fakeMemoryDB{
		clusters: []mdbtypes.Cluster{
			{
				Name: aws.String("orders"), ARN: aws.String("arn:orders"), Status: aws.String("available"), Engine: aws.String("valkey"),
				EngineVersion: aws.String("7.2"), EnginePatchVersion: aws.String("7.2.6"), NodeType: aws.String("db.r7g.large"),
				NumberOfShards: aws.Int32(1), TLSEnabled: aws.Bool(true),
				ClusterEndpoint: &mdbtypes.Endpoint{Address: aws.String("clustercfg.orders.memorydb.amazonaws.com"), Port: 6379},
				Shards: []mdbtypes.Shard{{Name: aws.String("0001"), Nodes: []mdbtypes.Node{
					{Name: aws.String("orders-0001-001"), AvailabilityZone: aws.String("us-east-1a"), Status: aws.String("available"),
						Endpoint: &mdbtypes.Endpoint{Address: aws.String("orders-0001-001.memorydb.amazonaws.com"), Port: 6379}},
					{Name: aws.String("orders-0001-002"), AvailabilityZone: aws.String("us-east-1b"), Status: aws.String("available"),
						Endpoint: &mdbtypes.Endpoint{Address: aws.String("orders-0001-002.memorydb.amazonaws.com"), Port: 6379}},
				}}},
			},
			{
				Name: aws.String("sessions"), ARN: aws.String("arn:sessions"), Status: aws.String("creating"),
				ClusterEndpoint: &mdbtypes.Endpoint{Address: aws.String("clustercfg.sessions.memorydb.amazonaws.com"), Port: 6379},
			},
			{Name: aws.String("skipped"), ARN: aws.String("arn:skipped")},
		},
		tags: map[string]map[string]string{"arn:orders": {"team": "shop"}, "arn:sessions": {"team": "shop"}, "arn:skipped": {"team": "other"}},
	}
	d.discoverMemoryDB(&config.AWSConfig{MemoryDBTagFilters: map[string]string{"team": "shop"}}, "us-east-1", f)
	if !aws.ToBool(f.input.ShowShardDetails) {
		t.Fatal("the shard details must be requested")
	}
	if len(d.memoryDBCollectors) != 2 {
		t.Fatalf("unexpected collectors: %v", d.memoryDBCollectors)
	}
	d.publishEndpoints()
	e, tls := d.MemoryDBEndpoints("orders")
	if len(e) != 2 || e[1].Host != "orders-0001-002.memorydb.amazonaws.com" || e[1].Port != "6379" || !tls {
		t.Fatalf("unexpected endpoints: %v %v", e, tls)
	}
	e, tls = d.MemoryDBEndpoints("sessions") // no shard details: the cluster endpoint
	if len(e) != 1 || e[0].Host != "clustercfg.sessions.memorydb.amazonaws.com" || tls {
		t.Fatalf("unexpected endpoints: %v %v", e, tls)
	}
	m := gather(t, d.memoryDBCollectors["us-east-1/orders"])
	if v := m["aws_memorydb_node_info"]; len(v) != 2 {
		t.Fatalf("unexpected node info: %v", v)
	}
	if v := m["aws_memorydb_info"]; len(v) != 1 || labelValue(v[0], "engine_version") != "7.2.6" || labelValue(v[0], "tls") != "true" {
		t.Fatalf("unexpected info: %v", v)
	}
}

func TestDiscovererStopWithCloudWatchLoop(t *testing.T) {
	reg := prometheus.NewRegistry()
	d := newTestDiscoverer(reg)
	reg.MustRegister(d)
	d.cwDone = make(chan struct{})
	go d.cloudWatchLoop()
	c := &ECServerlessCollector{discoverer: d, key: "r/c", cache: &ectypes.ServerlessCache{ServerlessCacheName: aws.String("c")}}
	prometheus.WrapRegistererWith(ecServerlessLabels("r/c"), reg).MustRegister(c)
	d.ecServerlessCollectors["r/c"] = c
	m := &MemoryDBCollector{cluster: &mdbtypes.Cluster{Name: aws.String("m")}}
	prometheus.WrapRegistererWith(memoryDBLabels("r/m"), reg).MustRegister(m)
	d.memoryDBCollectors["r/m"] = m

	started := make(chan struct{})
	go d.run(func() {
		close(started)
		<-d.ctx.Done()
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
	select {
	case <-d.cwDone:
	default:
		t.Fatal("the CloudWatch goroutine is still running")
	}
	if mfs, _ := reg.Gather(); len(mfs) != 0 {
		t.Fatalf("collectors left registered: %v", mfs)
	}
}

func TestNewCollectorsConcurrentlyWithCollect(t *testing.T) {
	d := newTestDiscoverer(nil)
	instance := &rdstypes.DBInstance{DBInstanceIdentifier: aws.String("db1"), Engine: aws.String("aurora-mysql"), DBInstanceClass: aws.String("db.serverless")}
	rc := &RDSCollector{discoverer: d, ctx: context.Background(), region: "r", instance: instance}
	sc := &ECServerlessCollector{discoverer: d, key: "r/c"}
	mc := &MemoryDBCollector{}
	d.rdsCollectors["r/db1"] = rc
	d.ecServerlessCollectors["r/c"] = sc
	d.memoryDBCollectors["r/m"] = mc
	reg := prometheus.NewRegistry()
	reg.MustRegister(rc, sc, mc)
	f := &fakeRDSClusters{clusters: []rdstypes.DBCluster{{DBClusterMembers: []rdstypes.DBClusterMember{{DBInstanceIdentifier: aws.String("db1"), IsClusterWriter: aws.Bool(true)}}}}}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			d.discoverAurora("r", f)
			sc.update("r", &ectypes.ServerlessCache{ServerlessCacheName: aws.String("c"), Status: aws.String("available")})
			mc.update("r", &mdbtypes.Cluster{Name: aws.String("m"), Status: aws.String("available")})
			d.setCloudWatchQueries(d.buildCloudWatchQueries())
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			d.cloudwatch.set(map[string][]cwValue{"r/db1": {{desc: dRDSServerlessCapacity, value: float64(i)}}})
			_ = d.cloudWatchQueries()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if _, err := reg.Gather(); err != nil {
				t.Error(err)
				return
			}
			d.publishEndpoints()
		}
	}()
	wg.Wait()
}
