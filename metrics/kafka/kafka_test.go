package kafka

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// gather returns the metrics of the collector as "name{label="value",...}" -> value.
func gather(t *testing.T, c prometheus.Collector) map[string]float64 {
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(c))
	mfs, err := reg.Gather()
	require.NoError(t, err)
	res := map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			var labels []string
			for _, l := range m.GetLabel() {
				labels = append(labels, fmt.Sprintf("%s=%q", l.GetName(), l.GetValue()))
			}
			sort.Strings(labels)
			res[mf.GetName()+"{"+strings.Join(labels, ",")+"}"] = m.GetGauge().GetValue()
		}
	}
	return res
}

func startCollector(t *testing.T, addr, username, password string, params map[string]string) *Collector {
	c, err := New(addr, username, password, common.TLSCredentials{}, params, time.Hour, 5*time.Second, logger.NewKlog("test"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	require.Eventually(t, func() bool { return c.getState() != nil }, 10*time.Second, 10*time.Millisecond)
	return c
}

func TestCollectorKfake(t *testing.T) {
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(3),
		kfake.ClusterID("test-cluster"),
		kfake.SeedTopics(3, "orders"),
		kfake.SeedTopics(1, "payments", "tmp-scratch"),
	)
	require.NoError(t, err)
	defer cluster.Close()
	addrs := cluster.ListenAddrs()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	producer, err := kgo.NewClient(kgo.SeedBrokers(addrs...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	defer producer.Close()
	var records []*kgo.Record
	for topic, perPartition := range map[string][]int{"orders": {10, 5, 0}, "payments": {3}} {
		for p, n := range perPartition {
			for i := 0; i < n; i++ {
				records = append(records, &kgo.Record{Topic: topic, Partition: int32(p), Value: []byte("x")})
			}
		}
	}
	require.NoError(t, producer.ProduceSync(ctx, records...).FirstErr())

	adm := kadm.NewClient(producer)
	offsets := kadm.Offsets{}
	offsets.AddOffset("orders", 0, 4, -1)
	offsets.AddOffset("orders", 1, 5, -1)
	offsets.AddOffset("payments", 0, 1, -1)
	_, err = adm.CommitOffsets(ctx, "billing", offsets)
	require.NoError(t, err)
	tmpOffsets := kadm.Offsets{}
	tmpOffsets.AddOffset("orders", 0, 1, -1)
	_, err = adm.CommitOffsets(ctx, "tmp-debug", tmpOffsets)
	require.NoError(t, err)
	internalOffsets := kadm.Offsets{}
	internalOffsets.AddOffset("tmp-scratch", 0, 0, -1)
	_, err = adm.CommitOffsets(ctx, "scratch-reader", internalOffsets)
	require.NoError(t, err)

	c := startCollector(t, addrs[0], "", "", map[string]string{
		"excludeTopics":         "tmp-.*",
		"excludeConsumerGroups": "tmp-.*",
	})
	m := gather(t, c)

	assert.Equal(t, 1.0, m[`kafka_up{}`])
	assert.Equal(t, 0.0, m[`kafka_scrape_error{error="",warning=""}`])
	assert.Equal(t, 3.0, m[`kafka_brokers{}`])
	assert.Equal(t, 1.0, m[`kafka_cluster_info{cluster_id="test-cluster"}`])
	_, ok := m[`kafka_controller_id{}`]
	assert.True(t, ok)
	assert.ElementsMatch(t, addrs, gatherBrokerAddrs(m))

	assert.Equal(t, 3.0, m[`kafka_topic_partitions{topic="orders"}`])
	assert.Equal(t, 1.0, m[`kafka_topic_partitions{topic="payments"}`])
	assert.Equal(t, 0.0, m[`kafka_topic_under_replicated_partitions{topic="orders"}`])
	assert.Equal(t, 0.0, m[`kafka_topic_offline_partitions{topic="orders"}`])
	assert.Equal(t, 15.0, m[`kafka_topic_current_offset_sum{topic="orders"}`])
	assert.Equal(t, 3.0, m[`kafka_topic_current_offset_sum{topic="payments"}`])

	assert.Equal(t, 9.0, m[`kafka_consumergroup_current_offset_sum{consumergroup="billing",topic="orders"}`])
	assert.Equal(t, 6.0, m[`kafka_consumergroup_lag_sum{consumergroup="billing",topic="orders"}`]) // p0: 10-4, p1: 5-5; p2 has no commit
	assert.Equal(t, 2.0, m[`kafka_consumergroup_lag_sum{consumergroup="billing",topic="payments"}`])
	assert.Equal(t, 0.0, m[`kafka_consumergroup_members{consumergroup="billing"}`])
	assert.Equal(t, 1.0, m[`kafka_consumergroup_state{consumergroup="billing",state="Empty"}`])
	assert.Equal(t, 0.0, m[`kafka_consumergroup_members{consumergroup="scratch-reader"}`])

	for k := range m {
		assert.NotContains(t, k, "tmp-", "excluded topics and groups must not be reported")
		assert.NotContains(t, k, "__consumer_offsets", "internal topics are skipped by default")
		assert.NotContains(t, k, "partition=", "per-partition metrics are off by default")
	}

	t.Run("per-partition", func(t *testing.T) {
		c := startCollector(t, addrs[1], "", "", map[string]string{"perPartitionMetrics": "true", "topics": "orders"})
		m := gather(t, c)
		assert.Equal(t, 10.0, m[`kafka_topic_partition_current_offset{partition="0",topic="orders"}`])
		assert.Equal(t, 0.0, m[`kafka_topic_partition_current_offset{partition="2",topic="orders"}`])
		assert.Equal(t, 3.0, m[`kafka_topic_partition_replicas{partition="0",topic="orders"}`])
		assert.Equal(t, 3.0, m[`kafka_topic_partition_in_sync_replica{partition="0",topic="orders"}`])
		assert.Equal(t, 0.0, m[`kafka_topic_partition_under_replicated_partition{partition="0",topic="orders"}`])
		assert.Equal(t, 6.0, m[`kafka_consumergroup_lag{consumergroup="billing",partition="0",topic="orders"}`])
		assert.Equal(t, 4.0, m[`kafka_consumergroup_current_offset{consumergroup="billing",partition="0",topic="orders"}`])
		_, ok := m[`kafka_topic_partitions{topic="payments"}`]
		assert.False(t, ok, "topics filtered out")
		_, ok = m[`kafka_consumergroup_lag_sum{consumergroup="billing",topic="payments"}`]
		assert.False(t, ok, "the lag of filtered-out topics is not reported")
	})

	t.Run("lowest-broker", func(t *testing.T) {
		brokers := gatherBrokerAddrs(m)
		lowest := brokers[0]
		for _, a := range addrs {
			c := startCollector(t, a, "", "", map[string]string{"clusterMetrics": ClusterMetricsLowestBroker})
			m := gather(t, c)
			assert.Equal(t, 1.0, m[`kafka_up{}`])
			_, reported := m[`kafka_brokers{}`]
			assert.Equal(t, a == lowest, reported, a)
		}
	})

	t.Run("live consumer", func(t *testing.T) {
		consumer, err := kgo.NewClient(kgo.SeedBrokers(addrs...), kgo.ConsumerGroup("live"), kgo.ConsumeTopics("payments"),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		require.NoError(t, err)
		defer consumer.Close()
		fetches := consumer.PollFetches(ctx)
		require.NoError(t, fetches.Err())
		require.NoError(t, consumer.CommitUncommittedOffsets(ctx))
		c := startCollector(t, addrs[0], "", "", nil)
		m := gather(t, c)
		assert.Equal(t, 1.0, m[`kafka_consumergroup_members{consumergroup="live"}`])
		assert.Equal(t, 1.0, m[`kafka_consumergroup_state{consumergroup="live",state="Stable"}`])
		assert.Equal(t, 0.0, m[`kafka_consumergroup_lag_sum{consumergroup="live",topic="payments"}`])
	})
}

// gatherBrokerAddrs returns the broker addresses ordered by node ID.
func gatherBrokerAddrs(m map[string]float64) []string {
	type b struct {
		id   int
		addr string
	}
	var bs []b
	for k := range m {
		var id int
		var addr string
		if !strings.HasPrefix(k, "kafka_broker_info{") {
			continue
		}
		for _, l := range strings.Split(strings.TrimSuffix(strings.TrimPrefix(k, "kafka_broker_info{"), "}"), ",") {
			name, value, _ := strings.Cut(l, "=")
			value = strings.Trim(value, `"`)
			switch name {
			case "id":
				_, _ = fmt.Sscan(value, &id)
			case "address":
				addr = value
			}
		}
		bs = append(bs, b{id: id, addr: addr})
	}
	sort.Slice(bs, func(i, j int) bool { return bs[i].id < bs[j].id })
	var res []string
	for _, x := range bs {
		res = append(res, x.addr)
	}
	return res
}

func TestCollectorSASL(t *testing.T) {
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.EnableSASL(),
		kfake.Superuser("SCRAM-SHA-256", "monitor", "correct-password"),
		kfake.SeedTopics(1, "orders"),
	)
	require.NoError(t, err)
	defer cluster.Close()
	addr := cluster.ListenAddrs()[0]

	c := startCollector(t, addr, "monitor", "correct-password", map[string]string{"sasl": "scram-sha-256"})
	m := gather(t, c)
	assert.Equal(t, 1.0, m[`kafka_up{}`])
	assert.Equal(t, 1.0, m[`kafka_topic_partitions{topic="orders"}`])

	// Kafka answers a failed authentication with SASL_AUTHENTICATION_FAILED (reason "auth", see TestErrorReason);
	// kfake just closes the connection
	c = startCollector(t, addr, "monitor", "wrong-password", map[string]string{"sasl": "scram-sha-256"})
	m = gather(t, c)
	assert.Equal(t, 0.0, m[`kafka_up{}`])
	require.Len(t, m, 2)
	_, ok := m[`kafka_scrape_error{error="connection",warning=""}`]
	_, okAuth := m[`kafka_scrape_error{error="auth",warning=""}`]
	assert.True(t, ok || okAuth, m)
}

func TestCollectorUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close()) // nothing listens on addr anymore

	c, err := New(addr, "", "", common.TLSCredentials{}, nil, time.Hour, 500*time.Millisecond, logger.NewKlog("test"))
	require.NoError(t, err)
	defer c.Close()
	require.Eventually(t, func() bool { return c.getState() != nil }, 10*time.Second, 10*time.Millisecond)

	start := time.Now()
	m := gather(t, c)
	assert.Less(t, time.Since(start), 100*time.Millisecond, "Collect serves the last snapshot")
	assert.Equal(t, 0.0, m[`kafka_up{}`])
	require.Len(t, m, 2)
	for k := range m {
		if strings.HasPrefix(k, "kafka_scrape_error") {
			assert.Contains(t, []string{`kafka_scrape_error{error="connection",warning=""}`, `kafka_scrape_error{error="timeout",warning=""}`}, k)
		}
	}
	require.NoError(t, c.Close())
	require.NoError(t, c.Close(), "Close is idempotent")
}
