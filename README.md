Coroot Cluster Agent.
## Kafka

The agent can monitor Apache Kafka clusters, as well as Kafka API compatible systems such as Redpanda
(and managed services like Amazon MSK), through the Kafka admin API ([franz-go](https://github.com/twmb/franz-go)).
Metric names follow [kafka_exporter](https://github.com/danielqsj/kafka_exporter) where possible,
so existing dashboards and alerts translate.

A Kafka target describes a whole cluster: add one entry per cluster to the static configuration
(`--config-file`); the brokers are discovered from the cluster metadata.

```yaml
databases:
  - type: kafka
    host: kafka-1              # a bootstrap broker (resolved to its IPs: one target per IP)
    port: "9092"
    credentials:               # optional: enables SASL
      username: coroot
      password: ${KAFKA_PASSWORD}
    params:
      brokers: kafka-2:9092,kafka-3:9092   # additional bootstrap brokers (optional)
      sasl: scram-sha-512                  # plain (default with credentials), scram-sha-256, scram-sha-512, aws-msk-iam
      tls: "true"                          # "true", "skip-verify" or "false"
      # tlsCaFile: /etc/coroot/kafka-ca.pem  # custom CA (the chain is verified, the hostname is not)
      # tlsCertFile: /etc/coroot/kafka-client.pem
      # tlsKeyFile: /etc/coroot/kafka-client-key.pem
      excludeTopics: tmp-.*|test-.*
      excludeConsumerGroups: console-consumer-.*
```

Parameters (all optional):

| Param | Default | Description |
|---|---|---|
| `brokers` | | Comma-separated `host:port` list of additional bootstrap brokers |
| `sasl` | `plain` if credentials are set | `plain`, `scram-sha-256`, `scram-sha-512` or `aws-msk-iam` (credentials are optional for MSK IAM: username/password are used as the access key ID/secret access key, otherwise the default AWS credential chain; set `AWS_REGION` when the bootstrap address isn't an MSK hostname) |
| `tls` | enabled if TLS files/secrets are set | `true` (system roots; the hostname is verified when a hostname is dialed), `skip-verify`, `false` |
| `tlsCaFile`, `tlsCertFile`, `tlsKeyFile` | | PEM files with the CA and the client certificate/key |
| `topics`, `excludeTopics` | all | Regular expressions (anchored, `^`/`$` are not needed) selecting the monitored topics; the consumer group lag is reported only for the monitored topics |
| `consumerGroups`, `excludeConsumerGroups` | all | Regular expressions selecting the monitored consumer groups |
| `includeInternalTopics` | `false` | Also monitor internal topics (`__consumer_offsets`, `__transaction_state` and other `__*` topics) |
| `perPartitionMetrics` | `false` | Also report the per-partition metrics (high cardinality) |
| `maxTopics`, `maxConsumerGroups` | `1000` | Monitor at most this many topics / consumer groups (the first ones by name) |
| `clusterMetrics` | `all` | `lowest-broker`: report the cluster metrics only if the target is the broker with the lowest node ID (for setups where every broker is a target) |

The configuration file is expanded with environment variables, so don't use `$` in the regular expressions (they are anchored anyway).

On Kubernetes, a Kafka pod can be annotated instead:

```yaml
coroot.com/kafka-scrape: "true"
coroot.com/kafka-scrape-port: "9092"
coroot.com/kafka-scrape-credentials-secret-name: kafka-monitor
coroot.com/kafka-scrape-credentials-secret-username-key: username
coroot.com/kafka-scrape-credentials-secret-password-key: password
coroot.com/kafka-scrape-param-sasl: scram-sha-512
coroot.com/kafka-scrape-param-tls: "true"
coroot.com/kafka-scrape-tls-secret-name: kafka-cluster-ca-cert
coroot.com/kafka-scrape-tls-secret-ca-key: ca.crt
coroot.com/kafka-scrape-param-exclude-topics: tmp-.*
coroot.com/kafka-scrape-param-exclude-consumer-groups: console-consumer-.*
```

Since every annotated broker pod becomes a target, pod targets default to `clusterMetrics: lowest-broker`
(override with `coroot.com/kafka-scrape-param-cluster-metrics: all`); the other brokers report only `kafka_up`.

Metrics (all labeled with the target `address`):

| Metric | Labels | Description |
|---|---|---|
| `kafka_up` | | Whether the cluster is reachable |
| `kafka_scrape_error` | `error`, `warning` | Failure / partial failure reason: `auth`, `permission`, `connection`, `timeout`, `tls`, `not_found`, `unknown` |
| `kafka_cluster_info` | `cluster_id` | |
| `kafka_controller_id` | | Controller node ID (KRaft clusters report an arbitrary broker) |
| `kafka_brokers` | | Number of brokers |
| `kafka_broker_info` | `id`, `address`, `rack` | |
| `kafka_topic_partitions` | `topic` | |
| `kafka_topic_under_replicated_partitions` | `topic` | Partitions with fewer in-sync replicas than replicas |
| `kafka_topic_offline_partitions` | `topic` | Partitions without a leader |
| `kafka_topic_current_offset_sum` | `topic` | Sum of the partition end offsets (`rate()` gives the produce rate in messages/s) |
| `kafka_consumergroup_members` | `consumergroup` | |
| `kafka_consumergroup_state` | `consumergroup`, `state` | `Stable`, `Empty`, `PreparingRebalance`, `CompletingRebalance`, `Dead`, `Assigning`, `Reconciling`, `Unknown` |
| `kafka_consumergroup_current_offset_sum` | `consumergroup`, `topic` | Sum of the committed offsets |
| `kafka_consumergroup_lag_sum` | `consumergroup`, `topic` | Sum of the partition lags (end offset - committed offset) |

With `perPartitionMetrics: "true"`: `kafka_topic_partition_current_offset`, `kafka_topic_partition_leader`,
`kafka_topic_partition_replicas`, `kafka_topic_partition_in_sync_replica`, `kafka_topic_partition_under_replicated_partition`
(`topic`, `partition`), `kafka_consumergroup_current_offset` and `kafka_consumergroup_lag` (`consumergroup`, `topic`, `partition`).

The user needs the `Describe` permission on the cluster, the topics and the consumer groups.
The cluster is queried in the background every scrape interval, with the target's collect deadline (the scrape timeout minus 1s)
for all requests; `/metrics` serves the latest snapshot and never waits for the cluster.
