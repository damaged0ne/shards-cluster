// Package kafka collects cluster, topic and consumer group metrics from Apache Kafka
// (and Kafka API compatible systems such as Redpanda) using the franz-go admin client.
//
// The metric names follow danielqsj/kafka_exporter where possible, so that existing
// dashboards and alerts translate, but topic and consumer group metrics are aggregated
// per topic by default to keep the cardinality bounded.
package kafka

import (
	"context"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	awssasl "github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var (
	dUp          = common.Desc("kafka_up", "Whether the Kafka cluster is reachable through the target (1) or not (0)")
	dScrapeError = common.Desc("kafka_scrape_error", "Scrape error or warning (partial failure), by reason", "error", "warning")

	dClusterInfo  = common.Desc("kafka_cluster_info", "Kafka cluster ID", "cluster_id")
	dControllerID = common.Desc("kafka_controller_id", "Node ID of the controller broker reported by the cluster (-1 if unknown)")
	dBrokers      = common.Desc("kafka_brokers", "Number of brokers in the cluster")
	dBrokerInfo   = common.Desc("kafka_broker_info", "Broker information", "id", "address", "rack")

	dTopicPartitions      = common.Desc("kafka_topic_partitions", "Number of partitions of the topic", "topic")
	dTopicUnderReplicated = common.Desc("kafka_topic_under_replicated_partitions", "Number of partitions of the topic with fewer in-sync replicas than replicas", "topic")
	dTopicOffline         = common.Desc("kafka_topic_offline_partitions", "Number of partitions of the topic without a leader", "topic")
	dTopicCurrentOffset   = common.Desc("kafka_topic_current_offset_sum", "Sum of the current (end) offsets of the partitions of the topic", "topic")

	dPartitionCurrentOffset   = common.Desc("kafka_topic_partition_current_offset", "Current (end) offset of the partition", "topic", "partition")
	dPartitionLeader          = common.Desc("kafka_topic_partition_leader", "Node ID of the leader of the partition (-1 if offline)", "topic", "partition")
	dPartitionReplicas        = common.Desc("kafka_topic_partition_replicas", "Number of replicas of the partition", "topic", "partition")
	dPartitionInSyncReplicas  = common.Desc("kafka_topic_partition_in_sync_replica", "Number of in-sync replicas of the partition", "topic", "partition")
	dPartitionUnderReplicated = common.Desc("kafka_topic_partition_under_replicated_partition", "Whether the partition is under-replicated (1) or not (0)", "topic", "partition")

	dGroupMembers          = common.Desc("kafka_consumergroup_members", "Number of members of the consumer group", "consumergroup")
	dGroupState            = common.Desc("kafka_consumergroup_state", "State of the consumer group", "consumergroup", "state")
	dGroupCurrentOffsetSum = common.Desc("kafka_consumergroup_current_offset_sum", "Sum of the offsets committed by the consumer group for the partitions of the topic", "consumergroup", "topic")
	dGroupLagSum           = common.Desc("kafka_consumergroup_lag_sum", "Sum of the lag of the consumer group over the partitions of the topic", "consumergroup", "topic")
	dGroupCurrentOffset    = common.Desc("kafka_consumergroup_current_offset", "Offset committed by the consumer group for the partition", "consumergroup", "topic", "partition")
	dGroupLag              = common.Desc("kafka_consumergroup_lag", "Lag of the consumer group for the partition", "consumergroup", "topic", "partition")
)

// knownGroupStates bounds the values of the state label.
var knownGroupStates = map[string]bool{
	"Stable": true, "Empty": true, "PreparingRebalance": true, "CompletingRebalance": true, "Dead": true,
	"Assigning": true, "Reconciling": true, // KIP-848 consumer groups
}

// state is the result of a snapshot. Once published it is never modified.
type state struct {
	up       bool
	errors   map[string]bool // reason -> true: the cluster is unreachable
	warnings map[string]bool // reason -> true: partial failures

	clusterMetrics bool // false if another target reports the cluster-wide metrics (see Options.ClusterMetrics)
	clusterID      string
	controller     int32
	brokers        []broker
	topics         []topicStats
	groups         []groupStats
}

type Collector struct {
	addr           string
	opts           *Options
	client         *kgo.Client
	adm            *kadm.Client
	logger         logger.Logger
	collectTimeout time.Duration

	// resolve returns the IP addresses of a host; replaced in tests
	resolve func(ctx context.Context, host string) ([]string, error)

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once

	lock  sync.RWMutex
	state *state

	// accessed only by the snapshot goroutine
	topicsLimitLogged, groupsLimitLogged bool
}

// New creates the collector of a Kafka target and starts taking snapshots of the cluster state
// every scrapeInterval in the background; Collect serves the latest snapshot and never blocks on the network.
func New(addr, username, password string, tlsCreds common.TLSCredentials, params map[string]string,
	scrapeInterval, collectTimeout time.Duration, logger logger.Logger) (*Collector, error) {
	opts, err := ParseOptions(addr, username, tlsCreds, params)
	if err != nil {
		return nil, err
	}
	c, err := newCollector(addr, opts, username, password, scrapeInterval, collectTimeout, logger)
	if err != nil {
		return nil, err
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(scrapeInterval)
		defer ticker.Stop()
		c.snapshot()
		for {
			select {
			case <-ticker.C:
				c.snapshot()
			case <-c.ctx.Done():
				c.logger.Info("stopping kafka collector")
				return
			}
		}
	}()
	return c, nil
}

func newCollector(addr string, opts *Options, username, password string, scrapeInterval, collectTimeout time.Duration, logger logger.Logger) (*Collector, error) {
	kopts := []kgo.Opt{
		kgo.SeedBrokers(opts.Seeds...),
		kgo.ClientID("coroot-cluster-agent"),
		kgo.DialTimeout(collectTimeout),
		kgo.RetryTimeout(collectTimeout),
		// keep the connections between snapshots instead of re-dialing (and re-authenticating) every time
		// (franz-go allows at most 15m)
		kgo.ConnIdleTimeout(min(max(2*scrapeInterval, 20*time.Second), 15*time.Minute)),
	}
	if opts.TLS != nil {
		kopts = append(kopts, kgo.DialTLSConfig(opts.TLS))
	}
	if m := saslMechanism(opts.SASLMechanism, username, password); m != nil {
		kopts = append(kopts, kgo.SASL(m))
	}
	client, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Collector{
		addr:           addr,
		opts:           opts,
		client:         client,
		adm:            kadm.NewClient(client),
		logger:         logger,
		collectTimeout: collectTimeout,
		resolve:        net.DefaultResolver.LookupHost,
		ctx:            ctx,
		cancel:         cancel,
	}, nil
}

func saslMechanism(name, username, password string) sasl.Mechanism {
	switch name {
	case "plain":
		return plain.Auth{User: username, Pass: password}.AsMechanism()
	case "scram-sha-256":
		return scram.Auth{User: username, Pass: password}.AsSha256Mechanism()
	case "scram-sha-512":
		return scram.Auth{User: username, Pass: password}.AsSha512Mechanism()
	case "aws-msk-iam":
		return awssasl.ManagedStreamingIAM(func(ctx context.Context) (awssasl.Auth, error) {
			if username != "" { // static access key ID / secret access key
				return awssasl.Auth{AccessKey: username, SecretKey: password, UserAgent: "coroot-cluster-agent"}, nil
			}
			cfg, err := awsconfig.LoadDefaultConfig(ctx) // environment, shared config, instance/task role, ...
			if err != nil {
				return awssasl.Auth{}, err
			}
			creds, err := cfg.Credentials.Retrieve(ctx)
			if err != nil {
				return awssasl.Auth{}, err
			}
			return awssasl.Auth{
				AccessKey:    creds.AccessKeyID,
				SecretKey:    creds.SecretAccessKey,
				SessionToken: creds.SessionToken,
				UserAgent:    "coroot-cluster-agent",
			}, nil
		})
	}
	return nil
}

func (c *Collector) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		c.wg.Wait()
		c.client.Close()
	})
	return nil
}

func (c *Collector) getState() *state {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.state
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		dUp, dScrapeError, dClusterInfo, dControllerID, dBrokers, dBrokerInfo,
		dTopicPartitions, dTopicUnderReplicated, dTopicOffline, dTopicCurrentOffset,
		dPartitionCurrentOffset, dPartitionLeader, dPartitionReplicas, dPartitionInSyncReplicas, dPartitionUnderReplicated,
		dGroupMembers, dGroupState, dGroupCurrentOffsetSum, dGroupLagSum, dGroupCurrentOffset, dGroupLag,
	} {
		ch <- d
	}
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	st := c.getState()
	if st == nil { // no snapshot yet
		return
	}
	if !st.up {
		ch <- common.Gauge(dUp, 0)
		for e := range st.errors {
			ch <- common.Gauge(dScrapeError, 1, e, "")
		}
		return
	}
	ch <- common.Gauge(dUp, 1)
	if len(st.warnings) > 0 {
		for w := range st.warnings {
			ch <- common.Gauge(dScrapeError, 1, "", w)
		}
	} else {
		ch <- common.Gauge(dScrapeError, 0, "", "")
	}
	if !st.clusterMetrics {
		return
	}
	if st.clusterID != "" {
		ch <- common.Gauge(dClusterInfo, 1, st.clusterID)
	}
	ch <- common.Gauge(dControllerID, float64(st.controller))
	ch <- common.Gauge(dBrokers, float64(len(st.brokers)))
	for _, b := range st.brokers {
		ch <- common.Gauge(dBrokerInfo, 1, strconv.Itoa(int(b.id)), b.address, b.rack)
	}
	for _, t := range st.topics {
		ch <- common.Gauge(dTopicPartitions, float64(t.partitions), t.name)
		ch <- common.Gauge(dTopicUnderReplicated, float64(t.underReplicated), t.name)
		ch <- common.Gauge(dTopicOffline, float64(t.offline), t.name)
		if t.hasOffsets {
			ch <- common.Gauge(dTopicCurrentOffset, float64(t.currentOffsetSum), t.name)
		}
		for _, p := range t.parts {
			partition := strconv.Itoa(int(p.partition))
			if p.currentOffset >= 0 {
				ch <- common.Gauge(dPartitionCurrentOffset, float64(p.currentOffset), t.name, partition)
			}
			ch <- common.Gauge(dPartitionLeader, float64(p.leader), t.name, partition)
			ch <- common.Gauge(dPartitionReplicas, float64(p.replicas), t.name, partition)
			ch <- common.Gauge(dPartitionInSyncReplicas, float64(p.isr), t.name, partition)
			ch <- common.Gauge(dPartitionUnderReplicated, boolToFloat(p.underReplicated), t.name, partition)
		}
	}
	for _, g := range st.groups {
		if g.hasInfo {
			ch <- common.Gauge(dGroupMembers, float64(g.members), g.name)
			ch <- common.Gauge(dGroupState, 1, g.name, g.state)
		}
		for _, t := range g.topics {
			ch <- common.Gauge(dGroupCurrentOffsetSum, float64(t.currentOffsetSum), g.name, t.topic)
			ch <- common.Gauge(dGroupLagSum, float64(t.lagSum), g.name, t.topic)
			for _, p := range t.parts {
				partition := strconv.Itoa(int(p.partition))
				ch <- common.Gauge(dGroupCurrentOffset, float64(p.currentOffset), g.name, t.topic, partition)
				ch <- common.Gauge(dGroupLag, float64(p.lag), g.name, t.topic, partition)
			}
		}
	}
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// snapshot queries the cluster without holding the lock and publishes the new state at the end,
// so a slow or unreachable cluster never blocks Collect. All requests share the target's collect deadline.
func (c *Collector) snapshot() {
	ctx, cancel := context.WithTimeout(c.ctx, c.collectTimeout)
	defer cancel()
	st := &state{errors: map[string]bool{}, warnings: map[string]bool{}}
	defer func() {
		c.lock.Lock()
		c.state = st
		c.lock.Unlock()
	}()

	md, err := c.adm.Metadata(ctx)
	if err != nil {
		c.logger.Warning("failed to get cluster metadata:", err)
		st.errors[errorReason(err)] = true
		return
	}
	st.up = true
	st.clusterID = md.Cluster
	st.controller = md.Controller
	for _, b := range md.Brokers {
		rack := ""
		if b.Rack != nil {
			rack = *b.Rack
		}
		st.brokers = append(st.brokers, broker{id: b.NodeID, address: net.JoinHostPort(b.Host, strconv.Itoa(int(b.Port))), rack: rack})
	}
	sort.Slice(st.brokers, func(i, j int) bool { return st.brokers[i].id < st.brokers[j].id })

	st.clusterMetrics = c.opts.ClusterMetrics != ClusterMetricsLowestBroker || c.isLowestBroker(ctx, st.brokers)
	if !st.clusterMetrics {
		return
	}

	topics, limited := selectTopics(md.Topics, c.opts.Topics, c.opts.IncludeInternalTopics, c.opts.MaxTopics)
	if limited && !c.topicsLimitLogged {
		c.logger.Warningf("the number of topics exceeds maxTopics=%d, only the first %d topics (sorted by name) are monitored", c.opts.MaxTopics, c.opts.MaxTopics)
	}
	c.topicsLimitLogged = limited
	ends := endOffsets{}
	if len(topics) > 0 { // no topics would mean "all topics"
		listed, err := c.adm.ListEndOffsets(ctx, topics...)
		if err != nil { // may be partial (*kadm.ShardErrors): use what has been listed
			c.warn(st, "failed to list end offsets:", err)
		}
		ends = endOffsetsFrom(listed)
	}
	for _, name := range topics {
		st.topics = append(st.topics, computeTopicStats(md.Topics[name], ends, c.opts.PerPartition))
	}

	listed, err := c.adm.ListGroups(ctx)
	if err != nil {
		c.warn(st, "failed to list consumer groups:", err)
	}
	groups, limited := selectGroups(listed.Groups(), c.opts.ConsumerGroups, c.opts.MaxConsumerGroups)
	if limited && !c.groupsLimitLogged {
		c.logger.Warningf("the number of consumer groups exceeds maxConsumerGroups=%d, only the first %d groups (sorted by name) are monitored", c.opts.MaxConsumerGroups, c.opts.MaxConsumerGroups)
	}
	c.groupsLimitLogged = limited
	if len(groups) == 0 { // no groups would mean "all groups"
		return
	}
	described, err := c.adm.DescribeGroups(ctx, groups...)
	if err != nil {
		c.warn(st, "failed to describe consumer groups:", err)
	}
	fetched := c.adm.FetchManyOffsets(ctx, groups...)
	for _, g := range groups {
		gs := groupStats{name: g}
		if d, ok := described[g]; ok && d.Err == nil {
			gs.hasInfo = true
			gs.members = len(d.Members)
			gs.state = d.State
			if !knownGroupStates[gs.state] {
				gs.state = "Unknown"
			}
		}
		if f, ok := fetched[g]; ok {
			if f.Err != nil {
				c.warn(st, "failed to fetch the offsets of consumer group "+g+":", f.Err)
			} else {
				gs.topics = computeGroupLag(committedOffsetsFrom(f.Fetched), ends, c.opts.PerPartition)
			}
		}
		st.groups = append(st.groups, gs)
	}
}

// warn records a partial failure; each reason is logged once per snapshot.
func (c *Collector) warn(st *state, msg string, err error) {
	r := errorReason(err)
	if st.warnings[r] {
		return
	}
	st.warnings[r] = true
	c.logger.Warning(msg, err)
}

// isLowestBroker reports whether the target address is the broker with the lowest node ID,
// so that when every broker is a target, only one of them reports the cluster-wide metrics.
func (c *Collector) isLowestBroker(ctx context.Context, brokers []broker) bool {
	if len(brokers) == 0 {
		return false
	}
	b := brokers[0]
	if b.address == c.addr {
		return true
	}
	host, port, err := net.SplitHostPort(b.address)
	if err != nil {
		return false
	}
	ips, err := c.resolve(ctx, host)
	if err != nil {
		return false
	}
	for _, ip := range ips {
		if net.JoinHostPort(ip, port) == c.addr {
			return true
		}
	}
	return false
}
