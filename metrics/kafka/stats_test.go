package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestFilter(t *testing.T) {
	f, err := NewFilter("orders.*|payments", "orders-dlq")
	require.NoError(t, err)
	assert.True(t, f.Match("orders"))
	assert.True(t, f.Match("orders-v2"))
	assert.True(t, f.Match("payments"))
	assert.False(t, f.Match("payments-v2"), "the include regexp is anchored")
	assert.False(t, f.Match("orders-dlq"))
	assert.False(t, f.Match("audit"))

	f, err = NewFilter("", "tmp-.*")
	require.NoError(t, err)
	assert.True(t, f.Match("audit"))
	assert.False(t, f.Match("tmp-1"))

	var nilFilter *Filter
	assert.True(t, nilFilter.Match("anything"))

	_, err = NewFilter("(", "")
	assert.Error(t, err)
}

func TestParseOptions(t *testing.T) {
	o, err := ParseOptions("10.0.0.1:9092", "", common.TLSCredentials{}, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.1:9092"}, o.Seeds)
	assert.Equal(t, "", o.SASLMechanism)
	assert.Nil(t, o.TLS)
	assert.False(t, o.PerPartition)
	assert.False(t, o.IncludeInternalTopics)
	assert.Equal(t, defaultMaxTopics, o.MaxTopics)
	assert.Equal(t, ClusterMetricsAll, o.ClusterMetrics)

	o, err = ParseOptions("10.0.0.1:9092", "user", common.TLSCredentials{}, map[string]string{
		"brokers":             "kafka-2:9092, kafka-3:9092,",
		"tls":                 "skip-verify",
		"perPartitionMetrics": "true",
		"maxTopics":           "10",
		"clusterMetrics":      "lowest-broker",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.1:9092", "kafka-2:9092", "kafka-3:9092"}, o.Seeds)
	assert.Equal(t, "plain", o.SASLMechanism, "plain is the default mechanism when credentials are set")
	require.NotNil(t, o.TLS)
	assert.True(t, o.TLS.InsecureSkipVerify)
	assert.Nil(t, o.TLS.VerifyConnection)
	assert.True(t, o.PerPartition)
	assert.Equal(t, 10, o.MaxTopics)
	assert.Equal(t, ClusterMetricsLowestBroker, o.ClusterMetrics)

	o, err = ParseOptions("10.0.0.1:9092", "user", common.TLSCredentials{}, map[string]string{"sasl": "SCRAM-SHA-512", "tls": "true"})
	require.NoError(t, err)
	assert.Equal(t, "scram-sha-512", o.SASLMechanism)
	require.NotNil(t, o.TLS)
	assert.NotNil(t, o.TLS.VerifyConnection, "the chain must be verified against the system roots")

	for _, params := range []map[string]string{
		{"sasl": "gssapi"},
		{"tls": "yes"},
		{"brokers": "kafka-2"},
		{"topics": "("},
		{"excludeConsumerGroups": "("},
		{"perPartitionMetrics": "maybe"},
		{"maxConsumerGroups": "-1"},
		{"clusterMetrics": "some"},
		{"tlsCaFile": "/nonexistent/ca.pem"},
	} {
		_, err = ParseOptions("10.0.0.1:9092", "user", common.TLSCredentials{}, params)
		assert.Error(t, err, params)
	}
	_, err = ParseOptions("10.0.0.1:9092", "", common.TLSCredentials{}, map[string]string{"sasl": "scram-sha-256"})
	assert.Error(t, err, "scram requires credentials")

	_, err = ParseOptions("10.0.0.1:9092", "user", common.TLSCredentials{}, map[string]string{"sasl": "plain", "password": "s3cret"})
	require.NoError(t, err)
}

func TestParseOptionsErrorsDontLeakCredentials(t *testing.T) {
	_, err := ParseOptions("10.0.0.1:9092", "admin-user", common.TLSCredentials{}, map[string]string{"sasl": "kerberos"})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "admin-user")
}

func topicDetail(name string, internal bool, partitions ...kadm.PartitionDetail) kadm.TopicDetail {
	td := kadm.TopicDetail{Topic: name, IsInternal: internal, Partitions: kadm.PartitionDetails{}}
	for i, p := range partitions {
		p.Topic = name
		p.Partition = int32(i)
		td.Partitions[int32(i)] = p
	}
	return td
}

func TestSelectTopics(t *testing.T) {
	topics := kadm.TopicDetails{
		"__consumer_offsets":  topicDetail("__consumer_offsets", true),
		"__amazon_msk_canary": topicDetail("__amazon_msk_canary", false),
		"orders":              topicDetail("orders", false),
		"payments":            topicDetail("payments", false),
		"audit":               topicDetail("audit", false),
		"broken":              {Topic: "broken", Err: kerr.UnknownTopicOrPartition},
	}
	names, limited := selectTopics(topics, nil, false, 0)
	assert.Equal(t, []string{"audit", "orders", "payments"}, names)
	assert.False(t, limited)

	names, _ = selectTopics(topics, nil, true, 0)
	assert.Equal(t, []string{"__amazon_msk_canary", "__consumer_offsets", "audit", "orders", "payments"}, names)

	f, _ := NewFilter("", "audit")
	names, limited = selectTopics(topics, f, false, 1)
	assert.Equal(t, []string{"orders"}, names)
	assert.True(t, limited)
}

func TestSelectGroups(t *testing.T) {
	f, _ := NewFilter("", "console-consumer-.*")
	names, limited := selectGroups([]string{"b", "console-consumer-123", "a"}, f, 0)
	assert.Equal(t, []string{"a", "b"}, names)
	assert.False(t, limited)
}

func TestComputeTopicStats(t *testing.T) {
	td := topicDetail("orders", false,
		kadm.PartitionDetail{Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}},
		kadm.PartitionDetail{Leader: 2, Replicas: []int32{1, 2, 3}, ISR: []int32{2}},
		kadm.PartitionDetail{Leader: -1, Replicas: []int32{1, 2, 3}, ISR: []int32{}},
	)
	ends := endOffsets{"orders": {0: 100, 1: 50}} // the end offset of the offline partition is unknown

	ts := computeTopicStats(td, ends, false)
	assert.Equal(t, 3, ts.partitions)
	assert.Equal(t, 2, ts.underReplicated)
	assert.Equal(t, 1, ts.offline)
	assert.Equal(t, int64(150), ts.currentOffsetSum)
	assert.True(t, ts.hasOffsets)
	assert.Empty(t, ts.parts, "no per-partition stats by default")

	ts = computeTopicStats(td, ends, true)
	require.Len(t, ts.parts, 3)
	assert.Equal(t, partitionStats{partition: 1, leader: 2, replicas: 3, isr: 1, currentOffset: 50, underReplicated: true}, ts.parts[1])
	assert.Equal(t, int64(-1), ts.parts[2].currentOffset)

	ts = computeTopicStats(td, endOffsets{}, false)
	assert.False(t, ts.hasOffsets)
}

func TestEndAndCommittedOffsets(t *testing.T) {
	ends := endOffsetsFrom(kadm.ListedOffsets{
		"orders":  {0: {Offset: 10}, 1: {Offset: 20, Err: kerr.NotLeaderForPartition}},
		"missing": {-1: {Offset: -1, Err: kerr.UnknownTopicOrPartition}},
	})
	assert.Equal(t, endOffsets{"orders": {0: 10}}, ends)

	committed := committedOffsetsFrom(kadm.OffsetResponses{
		"orders": {
			0: {Offset: kadm.Offset{At: 5}},
			1: {Offset: kadm.Offset{At: -1}}, // no commit
			2: {Offset: kadm.Offset{At: 3}, Err: kerr.UnstableOffsetCommit},
		},
	})
	assert.Equal(t, committedOffsets{"orders": {0: 5}}, committed)
}

func TestComputeGroupLag(t *testing.T) {
	ends := endOffsets{
		"orders":   {0: 100, 1: 200, 2: 300},
		"payments": {0: 10},
	}
	committed := committedOffsets{
		"orders":   {0: 90, 1: 200, 2: 310, 3: 5}, // p2: committed ahead of the (stale) end offset; p3: unknown partition
		"payments": {0: 4},
		"excluded": {0: 1}, // not monitored: no end offsets
	}
	lags := computeGroupLag(committed, ends, false)
	assert.Equal(t, []groupTopicLag{
		{topic: "orders", currentOffsetSum: 90 + 200 + 310, lagSum: 10 + 0 + 0},
		{topic: "payments", currentOffsetSum: 4, lagSum: 6},
	}, lags)

	lags = computeGroupLag(committed, ends, true)
	require.Len(t, lags, 2)
	assert.Equal(t, []partitionLag{
		{partition: 0, currentOffset: 90, lag: 10},
		{partition: 1, currentOffset: 200, lag: 0},
		{partition: 2, currentOffset: 310, lag: 0},
	}, lags[0].parts)

	assert.Empty(t, computeGroupLag(committedOffsets{}, ends, false))
	assert.Empty(t, computeGroupLag(committedOffsets{"orders": {7: 1}}, ends, false))
}

func TestErrorReason(t *testing.T) {
	assert.Equal(t, "", errorReason(nil))
	assert.Equal(t, dbtracker.ErrorReasonAuth, errorReason(fmt.Errorf("sasl: %w", kerr.SaslAuthenticationFailed)))
	assert.Equal(t, dbtracker.ErrorReasonPermission, errorReason(&kadm.AuthError{Err: kerr.ClusterAuthorizationFailed}))
	assert.Equal(t, dbtracker.ErrorReasonPermission, errorReason(kerr.GroupAuthorizationFailed))
	assert.Equal(t, dbtracker.ErrorReasonNotFound, errorReason(kerr.UnknownTopicOrPartition))
	assert.Equal(t, dbtracker.ErrorReasonConnection, errorReason(kerr.CoordinatorNotAvailable))
	assert.Equal(t, dbtracker.ErrorReasonUnknown, errorReason(kerr.InvalidReplicationFactor))
	assert.Equal(t, dbtracker.ErrorReasonTimeout, errorReason(context.DeadlineExceeded))
	assert.Equal(t, dbtracker.ErrorReasonConnection, errorReason(&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}))
	assert.Equal(t, dbtracker.ErrorReasonUnknown, errorReason(errors.New("something")))
}

func TestIsLowestBroker(t *testing.T) {
	c := &Collector{addr: "10.0.0.1:9092", resolve: func(_ context.Context, host string) ([]string, error) {
		switch host {
		case "kafka-0.kafka":
			return []string{"10.0.0.1"}, nil
		case "kafka-1.kafka":
			return []string{"10.0.0.2"}, nil
		}
		return nil, errors.New("not found")
	}}
	ctx := context.Background()
	assert.True(t, c.isLowestBroker(ctx, []broker{{id: 0, address: "kafka-0.kafka:9092"}, {id: 1, address: "kafka-1.kafka:9092"}}))
	assert.False(t, c.isLowestBroker(ctx, []broker{{id: 1, address: "kafka-1.kafka:9092"}}))
	assert.False(t, c.isLowestBroker(ctx, []broker{{id: 0, address: "kafka-0.kafka:9093"}}))
	assert.True(t, c.isLowestBroker(ctx, []broker{{id: 0, address: "10.0.0.1:9092"}}))
	assert.False(t, c.isLowestBroker(ctx, []broker{{id: 0, address: "unknown:9092"}}))
	assert.False(t, c.isLowestBroker(ctx, nil))
}
