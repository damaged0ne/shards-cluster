package metrics

import (
	"crypto/tls"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/coroot/logger"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
)

var fakeDesc = prometheus.NewDesc("fake_metric", "", nil, nil)

type fakeCollector struct {
	block chan struct{} // if not nil, Collect blocks until it's closed
}

func (c *fakeCollector) Describe(ch chan<- *prometheus.Desc) { ch <- fakeDesc }

func (c *fakeCollector) Collect(ch chan<- prometheus.Metric) {
	if c.block != nil {
		<-c.block
	}
	ch <- prometheus.MustNewConstMetric(fakeDesc, prometheus.GaugeValue, 1)
}

type stopCounter struct{ n atomic.Int32 }

func (s *stopCounter) stop() { s.n.Add(1) }

func newTestTarget(addr string) *Target {
	t := &Target{Type: TargetTypeRedis, Addr: addr}
	t.logger = logger.NewKlog(t.String())
	return t
}

func newTestMetrics() *Metrics {
	return &Metrics{
		reg:     prometheus.NewRegistry(),
		targets: map[string]*Target{},
		stopCh:  make(chan struct{}),
	}
}

func gatherFake(t *testing.T, reg prometheus.Gatherer) []*dto.Metric {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "fake_metric" {
			return mf.Metric
		}
	}
	return nil
}

func TestActivateRegisterFailure(t *testing.T) {
	reg := prometheus.NewRegistry()
	a := newTestTarget("10.0.0.1:6379")
	var stopA stopCounter
	if err := a.activate(reg, &fakeCollector{}, stopA.stop, time.Second); err != nil {
		t.Fatal(err)
	}

	b := newTestTarget("10.0.0.1:6379") // same address: the registration must fail
	var stopB stopCounter
	if err := b.activate(reg, &fakeCollector{}, stopB.stop, time.Second); err == nil {
		t.Fatal("expected a duplicate registration error")
	}
	if stopB.n.Load() != 1 {
		t.Fatalf("the collector of the target that failed to register must be stopped, got %d stops", stopB.n.Load())
	}
	if b.IsExporterStarted() {
		t.Fatal("a target that failed to register must not be marked as started")
	}

	// stopping the failed target must not unregister the other target with the same address
	b.StopExporter(reg)
	if len(gatherFake(t, reg)) != 1 {
		t.Fatal("the registered target must still be collected")
	}
	if stopA.n.Load() != 0 {
		t.Fatal("the registered target must not be stopped")
	}

	a.StopExporter(reg)
	a.StopExporter(reg) // idempotent
	if stopA.n.Load() != 1 {
		t.Fatalf("expected exactly one stop, got %d", stopA.n.Load())
	}
	if len(gatherFake(t, reg)) != 0 {
		t.Fatal("the target must be unregistered")
	}
	// now the address is free
	if err := b.activate(reg, &fakeCollector{}, stopB.stop, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestStartExportersReplacedTarget(t *testing.T) {
	ms := newTestMetrics()
	a := newTestTarget("10.0.0.2:6379")
	a2 := newTestTarget("10.0.0.2:6379")
	a2.Params = map[string]string{"tls": "true"} // a changed target with the same address
	ms.addTarget(a)

	stops := map[*Target]*stopCounter{a: {}, a2: {}}
	ms.startTarget = func(tg *Target, _ Credentials, _ common.TLSCredentials) error {
		if tg == a {
			// discovery replaces the target while its exporter is starting (e.g. connecting to the database):
			// delTarget can't stop it yet as it has no collector
			ms.delTarget(a)
			ms.addTarget(a2)
		}
		return tg.activate(ms.reg, &fakeCollector{}, stops[tg].stop, time.Second)
	}

	ms.startPendingExporters()
	if a.IsExporterStarted() {
		t.Fatal("the replaced target must not be left running")
	}
	if stops[a].n.Load() != 1 {
		t.Fatalf("the collector of the replaced target must be stopped, got %d stops", stops[a].n.Load())
	}
	if len(gatherFake(t, ms.reg)) != 0 {
		t.Fatal("the replaced target must be unregistered")
	}

	ms.startPendingExporters()
	if !a2.IsExporterStarted() {
		t.Fatal("the new target must be started")
	}
	if len(gatherFake(t, ms.reg)) != 1 {
		t.Fatal("the new target must be collected")
	}

	ms.delTarget(a2)
	if stops[a2].n.Load() != 1 || len(gatherFake(t, ms.reg)) != 0 {
		t.Fatal("the new target must be stopped and unregistered")
	}
}

func TestStartExportersSkipsRemovedTarget(t *testing.T) {
	ms := newTestMetrics()
	a := newTestTarget("10.0.0.3:6379")
	ms.addTarget(a)
	started := 0
	ms.startTarget = func(tg *Target, _ Credentials, _ common.TLSCredentials) error {
		started++
		return tg.activate(ms.reg, &fakeCollector{}, func() {}, time.Second)
	}
	ms.delTarget(a)
	ms.startTargetIfCurrent(a, Credentials{}, common.TLSCredentials{})
	if started != 0 || a.IsExporterStarted() {
		t.Fatal("a removed target must not be started")
	}
}

func TestCollectDeadline(t *testing.T) {
	reg := prometheus.NewRegistry()
	slow := newTestTarget("10.0.0.4:6379")
	block := make(chan struct{})
	if err := slow.activate(reg, &fakeCollector{block: block}, func() {}, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	fast := newTestTarget("10.0.0.5:6379")
	if err := fast.activate(reg, &fakeCollector{}, func() {}, time.Second); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("the slow target blocked the collection for %s", d)
	}
	ms := gatherFake(t, prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) { return mfs, nil }))
	if len(ms) != 1 || ms[0].Label[0].GetValue() != fast.Addr {
		t.Fatalf("only the metrics of the fast target are expected, got %v", ms)
	}
	checkSelfMetrics(t, mfs, slow.Addr, 0, 1)
	checkSelfMetrics(t, mfs, fast.Addr, 1, 0)

	// the abandoned collection is still running: no new one is started, the scrape isn't blocked
	start = time.Now()
	if mfs, err = reg.Gather(); err != nil {
		t.Fatal(err)
	}
	checkSelfMetrics(t, mfs, slow.Addr, 0, 2)
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("a target with an in-flight collection must be skipped, took %s", d)
	}

	close(block) // the abandoned collection completes, its metrics are discarded
	deadline := time.Now().Add(5 * time.Second)
	for slow.collecting.Load() {
		if time.Now().After(deadline) {
			t.Fatal("the abandoned collection hasn't finished")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(gatherFake(t, reg)) != 2 {
		t.Fatal("the slow target must be collected again once it has recovered")
	}
}

func checkSelfMetrics(t *testing.T, mfs []*dto.MetricFamily, addr string, success, timeouts float64) {
	t.Helper()
	found := map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			ls := map[string]string{}
			for _, l := range m.Label {
				ls[l.GetName()] = l.GetValue()
			}
			if ls["address"] != addr || ls["target_type"] != string(TargetTypeRedis) {
				continue
			}
			switch {
			case m.Gauge != nil:
				found[mf.GetName()] = m.Gauge.GetValue()
			case m.Counter != nil:
				found[mf.GetName()] = m.Counter.GetValue()
			}
		}
	}
	if _, ok := found["coroot_cluster_agent_target_collect_duration_seconds"]; !ok {
		t.Fatalf("%s: no duration metric: %v", addr, found)
	}
	if v := found["coroot_cluster_agent_target_collect_success"]; v != success {
		t.Fatalf("%s: success=%v, expected %v", addr, v, success)
	}
	if v := found["coroot_cluster_agent_target_collect_timeouts_total"]; v != timeouts {
		t.Fatalf("%s: timeouts=%v, expected %v", addr, v, timeouts)
	}
}

func TestConcurrentStartStop(t *testing.T) {
	reg := prometheus.NewRegistry()
	for i := 0; i < 100; i++ {
		tg := newTestTarget("10.0.0.6:6379")
		var stops stopCounter
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); _ = tg.activate(reg, &fakeCollector{}, stops.stop, time.Second) }()
		go func() { defer wg.Done(); tg.StopExporter(reg) }()
		go func() { defer wg.Done(); tg.Collect(make(chan prometheus.Metric, 10)) }()
		wg.Wait()
		tg.StopExporter(reg)
		if stops.n.Load() != 1 {
			t.Fatalf("expected exactly one stop, got %d", stops.n.Load())
		}
	}
}

func TestMysqlDSN(t *testing.T) {
	creds := Credentials{Username: "user@corp", Password: "p@ss:w/rd?&="}
	if err := gomysql.RegisterTLSConfig("coroot-10.0.0.1:3306-1", &tls.Config{}); err != nil {
		t.Fatal(err)
	}
	defer gomysql.DeregisterTLSConfig("coroot-10.0.0.1:3306-1")
	dsn, err := mysqlDSN(creds, "10.0.0.1:3306", 9*time.Second, "coroot-10.0.0.1:3306-1")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := gomysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("%s: %s", dsn, err)
	}
	if cfg.User != creds.Username || cfg.Passwd != creds.Password || cfg.Addr != "10.0.0.1:3306" || cfg.Net != "tcp" ||
		cfg.Timeout != 9*time.Second || cfg.TLSConfig != "coroot-10.0.0.1:3306-1" {
		t.Fatalf("unexpected config parsed from %s: %+v", dsn, cfg)
	}

	dsn, err = mysqlDSN(Credentials{Username: "u"}, "10.0.0.1:3306", time.Second, "false")
	if err != nil {
		t.Fatal(err)
	}
	if cfg, err = gomysql.ParseDSN(dsn); err != nil || cfg.User != "u" || cfg.Passwd != "" || cfg.TLSConfig != "false" {
		t.Fatalf("%s: %+v, %v", dsn, cfg, err)
	}

	if _, err = mysqlDSN(Credentials{Username: "a:b"}, "10.0.0.1:3306", time.Second, "false"); err == nil {
		t.Fatal("a username with ':' can't be represented in a DSN")
	}
}

func TestSecretKeys(t *testing.T) {
	a := newTestTarget("10.0.0.1:5432")
	a.CredentialsSecret = CredentialsSecret{Namespace: "ns", Name: "db", UsernameKey: "user", PasswordKey: "pass"}
	b := newTestTarget("10.0.0.2:5432")
	b.TLSSecret = TLSSecret{Namespace: "ns", Name: "db", CAKey: "ca", CertKey: "cert", KeyKey: "key"}
	c := newTestTarget("10.0.0.3:5432")
	c.CredentialsSecret = CredentialsSecret{Namespace: "ns", Name: "db", UsernameKey: "user", PasswordKey: "pass"}

	for _, order := range [][]*Target{{a, b, c}, {b, a, c}, {c, b, a}} {
		keys := secretKeys(order)
		got := strings.Join(keys[secretId{namespace: "ns", name: "db"}], ",")
		if got != "ca,cert,key,pass,user" {
			t.Fatalf("unexpected keys: %s", got)
		}
	}
}

func testPod(uid, name, ip string) *k8s.Pod {
	return &k8s.Pod{
		Id:          k8s.PodId{Namespace: "ns", Name: name},
		UID:         uid,
		Phase:       "Running",
		IP:          ip,
		Annotations: map[string]string{"coroot.com/redis-scrape": "true"},
	}
}

func TestPodIPReuse(t *testing.T) {
	ms := newTestMetrics()
	p1 := testPod("uid-1", "redis-1", "10.1.0.1")
	p2 := testPod("uid-2", "redis-2", "10.1.0.1") // gets the IP of p1 before p1's deletion is observed
	addr := "10.1.0.1:6379"

	ms.handlePodEvent(k8s.PodEvent{Type: k8s.PodEventTypeAdd, Pod: p1})
	ms.handlePodEvent(k8s.PodEvent{Type: k8s.PodEventTypeAdd, Pod: p2})
	if tg := ms.targets[addr]; tg == nil || tg.podKey != "uid-2" {
		t.Fatalf("the target must belong to the new pod: %v", tg)
	}
	ms.handlePodEvent(k8s.PodEvent{Type: k8s.PodEventTypeDelete, Pod: p1})
	if tg := ms.targets[addr]; tg == nil || tg.podKey != "uid-2" {
		t.Fatal("the deletion of the old pod must not remove the target of the new pod")
	}
	ms.handlePodEvent(k8s.PodEvent{Type: k8s.PodEventTypeDelete, Pod: p2})
	if len(ms.targets) != 0 {
		t.Fatal("the target must be removed with its pod")
	}

	// a pod's deletion doesn't remove a configured target with the same address
	cfgTarget := newTestTarget(addr)
	ms.addTarget(cfgTarget)
	ms.handlePodEvent(k8s.PodEvent{Type: k8s.PodEventTypeDelete, Pod: p1})
	if ms.targets[addr] != cfgTarget {
		t.Fatal("a configured target must not be removed by a pod deletion")
	}
}

func TestHttpHandlerServesSelfMetrics(t *testing.T) {
	ms := newTestMetrics()
	tg := newTestTarget("10.0.0.7:6379")
	if err := tg.activate(ms.reg, &fakeCollector{}, func() {}, time.Second); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	ms.HttpHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, name := range []string{"fake_metric", "go_goroutines", "process_", "coroot_cluster_agent_target_collect_success"} {
		if !strings.Contains(string(body), name) {
			t.Fatalf("%s is missing in the /metrics response", name)
		}
	}
}

func TestK8sDiscoveryPodAnnotations(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.96.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	cfg := k8sDiscovery()
	for _, rc := range cfg.RelabelConfigs {
		for _, l := range rc.SourceLabels {
			if strings.HasPrefix(string(l), "__meta_kubernetes_service_") {
				t.Fatalf("the pod role doesn't provide service labels: %s", l)
			}
		}
	}
	lbls := labels.FromMap(map[string]string{
		model.AddressLabel: "10.1.0.1:8080",
		"__meta_kubernetes_pod_annotation_coroot_com_scrape_metrics": "true",
		"__meta_kubernetes_pod_annotation_coroot_com_metrics_scheme": "https",
		"__meta_kubernetes_pod_annotation_coroot_com_metrics_port":   "9090",
		"__meta_kubernetes_pod_phase":                                "Running",
		model.SchemeLabel:                                            "http",
	})
	res, keep := relabel.Process(lbls, cfg.RelabelConfigs...)
	if !keep {
		t.Fatal("the target must be kept")
	}
	if s := res.Get(model.SchemeLabel); s != "https" {
		t.Fatalf("scheme=%q, expected https", s)
	}
	if a := res.Get(model.AddressLabel); a != "10.1.0.1:9090" {
		t.Fatalf("address=%q", a)
	}
}
