package metrics

import (
	"testing"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/coroot/coroot-cluster-agent/metrics/kafka"
)

func TestKafkaTargetFromPod(t *testing.T) {
	pod := &k8s.Pod{
		Id:    k8s.PodId{Namespace: "ns", Name: "kafka-0"},
		UID:   "uid-1",
		Phase: "Running",
		IP:    "10.1.0.5",
		Annotations: map[string]string{
			"coroot.com/kafka-scrape":                         "true",
			"coroot.com/kafka-scrape-param-sasl":              "scram-sha-512",
			"coroot.com/kafka-scrape-credentials-secret-name": "kafka-monitor",
			"coroot.com/kafka-scrape-tls-secret-name":         "kafka-tls",
			"coroot.com/kafka-scrape-tls-secret-ca-key":       "ca.crt",
		},
	}
	tg := TargetFromPod(pod)
	if tg == nil || tg.Type != TargetTypeKafka || tg.Addr != "10.1.0.5:9092" {
		t.Fatalf("unexpected target: %+v", tg)
	}
	if tg.Params["sasl"] != "scram-sha-512" || tg.Params["clusterMetrics"] != kafka.ClusterMetricsLowestBroker {
		t.Fatalf("unexpected params: %v", tg.Params)
	}
	if tg.CredentialsSecret.Name != "kafka-monitor" || tg.TLSSecret.Name != "kafka-tls" || tg.TLSSecret.CAKey != "ca.crt" {
		t.Fatalf("unexpected secrets: %+v %+v", tg.CredentialsSecret, tg.TLSSecret)
	}

	pod.Annotations["coroot.com/kafka-scrape-port"] = "9093"
	pod.Annotations["coroot.com/kafka-scrape-param-cluster-metrics"] = kafka.ClusterMetricsAll
	tg = TargetFromPod(pod)
	if tg.Addr != "10.1.0.5:9093" || tg.Params["clusterMetrics"] != kafka.ClusterMetricsAll {
		t.Fatalf("unexpected target: %+v", tg)
	}
}

func TestKafkaNewCollector(t *testing.T) {
	tg := TargetFromConfig(config.ApplicationInstrumentation{
		Type:   "kafka",
		Host:   "127.0.0.1",
		Port:   "1",
		Params: map[string]string{"brokers": "kafka-2:9092"},
	})
	coll, stop, err := tg.newCollector(Credentials{}, common.TLSCredentials{}, time.Hour, time.Second, nil, 0, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := coll.(*kafka.Collector); !ok {
		t.Fatalf("unexpected collector: %T", coll)
	}
	stop()

	tg.Params = map[string]string{"sasl": "unknown"}
	if _, _, err = tg.newCollector(Credentials{}, common.TLSCredentials{}, time.Hour, time.Second, nil, 0, false, false, nil); err == nil {
		t.Fatal("an invalid configuration must fail")
	}
}
