package aws

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ectypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/coroot/logparser"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func newTestDiscoverer(reg prometheus.Registerer) *Discoverer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Discoverer{
		ctx:           ctx,
		cancel:        cancel,
		reg:           reg,
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
		errors:        map[string]bool{},
		rdsCollectors: map[string]*RDSCollector{},
		ecCollectors:  map[string]*ECCollector{},
		ecTags:        newTagCache(ecTagsTTL),
	}
}

func gather(t *testing.T, c prometheus.Collector) map[string][]*dto.Metric {
	t.Helper()
	reg := prometheus.NewRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatal(err)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	res := map[string][]*dto.Metric{}
	for _, mf := range mfs {
		res[mf.GetName()] = mf.GetMetric()
	}
	return res
}

func TestRDSTagFiltering(t *testing.T) {
	tags := rdsTags([]rdstypes.Tag{
		{Key: aws.String("env"), Value: aws.String("production")},
		{Key: aws.String("team"), Value: aws.String("db")},
	})
	cases := []struct {
		filters map[string]string
		matched bool
	}{
		{nil, true},
		{map[string]string{"env": "production"}, true},
		{map[string]string{"env": "prod*"}, true},
		{map[string]string{"env": "staging"}, false},
		{map[string]string{"env": "production", "team": "db"}, true},
		{map[string]string{"env": "production", "team": "web"}, false},
		{map[string]string{"missing": "*"}, true},
		{map[string]string{"missing": "x"}, false},
	}
	for _, c := range cases {
		if got := tagsMatched(c.filters, tags); got != c.matched {
			t.Errorf("tagsMatched(%v) = %v, want %v", c.filters, got, c.matched)
		}
	}
	if tags := rdsTags(nil); len(tags) != 0 {
		t.Errorf("unexpected tags: %v", tags)
	}
}

func TestTagCache(t *testing.T) {
	now := time.Now()
	c := newTagCache(10 * time.Minute)
	c.now = func() time.Time { return now }
	if _, ok := c.get("arn1"); ok {
		t.Fatal("unexpected hit")
	}
	c.set("arn1", map[string]string{"k": "v"})
	if tags, ok := c.get("arn1"); !ok || tags["k"] != "v" {
		t.Fatalf("expected a hit, got %v %v", tags, ok)
	}
	now = now.Add(9 * time.Minute)
	if _, ok := c.get("arn1"); !ok {
		t.Fatal("expected a hit before the TTL")
	}
	now = now.Add(time.Minute)
	if _, ok := c.get("arn1"); ok {
		t.Fatal("expected a miss after the TTL")
	}
	c.prune()
	if len(c.entries) != 0 {
		t.Fatalf("expected the expired entry to be pruned, got %v", c.entries)
	}
}

func TestDiscovererStopDoesNotWaitForUntimedCalls(t *testing.T) {
	reg := prometheus.NewRegistry()
	d := newTestDiscoverer(reg)
	if err := reg.Register(d); err != nil {
		t.Fatal(err)
	}
	c := NewRDSCollector(d, "us-east-1", &rdstypes.DBInstance{DBInstanceIdentifier: aws.String("db1")})
	if err := prometheus.WrapRegistererWith(rdsLabels("us-east-1/db1"), reg).Register(c); err != nil {
		t.Fatal(err)
	}
	d.rdsCollectors["us-east-1/db1"] = c

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
	select {
	case <-c.stop:
	default:
		t.Fatal("the RDS collector wasn't stopped")
	}
	if c.ctx.Err() == nil {
		t.Fatal("the RDS collector's context wasn't cancelled")
	}
	if mfs, _ := reg.Gather(); len(mfs) != 0 {
		t.Fatalf("collectors left registered: %v", mfs)
	}
}

func TestRDSCollectorServesCachedOsMetrics(t *testing.T) {
	// no discoverer: Collect must not call any API
	c := &RDSCollector{
		region: "us-east-1",
		instance: &rdstypes.DBInstance{
			DBInstanceIdentifier: aws.String("db1"),
			MonitoringInterval:   aws.Int32(60),
			DbiResourceId:        aws.String("db-ABC"),
		},
		osMetrics: &RDSOSMetrics{
			NumVCPUs: 4,
			Cpu:      RDSCPUUtilization{User: 12.5},
			Memory:   RDSMemory{Total: 1024},
		},
	}
	metrics := gather(t, c)
	if m := metrics["aws_rds_cpu_cores"]; len(m) != 1 || m[0].GetGauge().GetValue() != 4 {
		t.Fatalf("unexpected aws_rds_cpu_cores: %v", m)
	}
	if m := metrics["aws_rds_memory_total_bytes"]; len(m) != 1 || m[0].GetGauge().GetValue() != 1024000 {
		t.Fatalf("unexpected aws_rds_memory_total_bytes: %v", m)
	}
	if len(metrics["aws_rds_info"]) != 1 {
		t.Fatal("aws_rds_info is missing")
	}

	// Enhanced Monitoring disabled: the cached values are not served
	c.instance = &rdstypes.DBInstance{DBInstanceIdentifier: aws.String("db1")}
	if m := gather(t, c)["aws_rds_cpu_cores"]; len(m) != 0 {
		t.Fatalf("unexpected aws_rds_cpu_cores: %v", m)
	}
}

func TestOsMetricsInterval(t *testing.T) {
	for _, c := range []struct {
		monitoringInterval int32
		expected           time.Duration
	}{
		{0, osMetricsMinInterval},
		{1, osMetricsMinInterval},
		{30, 30 * time.Second},
		{60, time.Minute},
		{600, osMetricsMaxInterval},
	} {
		if got := osMetricsInterval(&rdstypes.DBInstance{MonitoringInterval: aws.Int32(c.monitoringInterval)}); got != c.expected {
			t.Errorf("osMetricsInterval(%d) = %s, want %s", c.monitoringInterval, got, c.expected)
		}
	}
}

func TestLogReaderStop(t *testing.T) {
	ch := make(chan logparser.LogEntry) // nobody reads it: the parser is stopped
	r := &LogReader{ctx: context.Background(), ch: ch, stop: make(chan struct{})}
	r.Stop()
	r.Stop() // idempotent
	done := make(chan struct{})
	go func() {
		r.write(aws.String("line1\nline2\n"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("write blocked after Stop")
	}
	if r.refresh(false) {
		t.Fatal("refresh after Stop")
	}
}

func TestCollectorsUpdateConcurrentlyWithCollect(t *testing.T) {
	rc := &RDSCollector{ctx: context.Background(), instance: &rdstypes.DBInstance{DBInstanceIdentifier: aws.String("db1")}}
	ec := NewECCollector("us-east-1", &ectypes.CacheCluster{CacheClusterId: aws.String("c1")}, &ectypes.CacheNode{CacheNodeId: aws.String("0001")})
	reg := prometheus.NewRegistry()
	reg.MustRegister(rc, ec)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			rc.update("us-east-1", &rdstypes.DBInstance{DBInstanceIdentifier: aws.String("db1"), DBInstanceStatus: aws.String("available")})
			ec.update(context.Background(), "us-east-1", &ectypes.CacheCluster{CacheClusterId: aws.String("c1")}, &ectypes.CacheNode{CacheNodeId: aws.String("0001")})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if _, err := reg.Gather(); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
}
