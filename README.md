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


## ClickHouse and Elasticsearch/OpenSearch targets

The agent can monitor ClickHouse servers and Elasticsearch/OpenSearch clusters. Both are configured like the
other database targets: in the static configuration file (`--config-file`, the main path for Docker Compose
deployments) or with pod annotations in Kubernetes. Metrics are collected in the background every scrape interval
(each snapshot is bounded by the target deadline, i.e. the scrape timeout minus 1s), and scrapes serve the last
snapshot, so a slow or unavailable server never delays the metrics of other targets.

### Static configuration

```yaml
databases:
  - type: clickhouse
    host: clickhouse          # resolved to IPs; one target per address
    port: "9000"              # native protocol; use 8123 with protocol: http
    credentials:
      username: monitoring    # "default" if empty
      password: ${CLICKHOUSE_PASSWORD}   # environment variables are expanded
    params:
      protocol: native        # native (default) or http
      tls: "false"            # "true", "skip-verify" or "false"
      # tlsCaFile: /etc/coroot/clickhouse-ca.pem   # PEM files: CA (chain verified, hostname not checked), client cert/key
      # tlsCertFile: /etc/coroot/client.pem
      # tlsKeyFile: /etc/coroot/client-key.pem
      tablesExclude: '^system\.'  # regexes matched against "database.table" (also tablesInclude)
      topTables: "100"        # max tables with per-table metrics (the largest ones)
      queryLog: "true"        # top queries from system.query_log; "false" disables
      topQueries: "20"

  - type: elasticsearch       # or "opensearch": the same collector and metrics
    host: es
    port: "9200"
    credentials:              # basic auth, optional
      username: monitoring
      password: ${ES_PASSWORD}
    params:
      tls: "true"             # https; "skip-verify" disables certificate verification
      tlsCaFile: /etc/coroot/es-ca.pem
      nodes: _local           # node stats of the node at the address (default) or "_all"
      indicesExclude: '^\.'   # default: hidden and system indices; set to "" to include them (also indicesInclude)
      topIndices: "100"       # max indices with per-index metrics (the largest ones)
```

All params are optional and must be strings (quote `"true"` and numbers). As hosts are resolved to IP addresses,
`tls: "true"` without `tlsCaFile` only works with certificates issued for the IP; use `tlsCaFile` (verifies the
chain against the CA without the hostname check) or `skip-verify`.

### Kubernetes pod annotations

```yaml
coroot.com/clickhouse-scrape: "true"
coroot.com/clickhouse-scrape-port: "9000"
coroot.com/clickhouse-scrape-credentials-secret-name: clickhouse-monitoring
coroot.com/clickhouse-scrape-credentials-secret-username-key: username
coroot.com/clickhouse-scrape-credentials-secret-password-key: password
coroot.com/clickhouse-scrape-param-protocol: native
coroot.com/clickhouse-scrape-param-tls: "true"
coroot.com/clickhouse-scrape-tls-secret-name: clickhouse-tls   # plus -tls-secret-ca-key/-cert-key/-key-key as for other targets

coroot.com/elasticsearch-scrape: "true"
coroot.com/elasticsearch-scrape-port: "9200"
coroot.com/elasticsearch-scrape-credentials-username: monitoring
coroot.com/elasticsearch-scrape-credentials-password: changeme
coroot.com/elasticsearch-scrape-param-tls: skip-verify
coroot.com/elasticsearch-scrape-param-nodes: _local
```

### Required privileges

- ClickHouse: `SELECT` on `system.metrics`, `system.events`, `system.asynchronous_metrics`, `system.parts`,
  `system.replicas`, `system.mutations`, `system.errors` and `system.query_log`
  (e.g. `GRANT SELECT ON system.* TO monitoring`). A failing table is reported as a warning; the rest is still collected.
- Elasticsearch/OpenSearch: the `monitor` cluster privilege and `monitor` on the indices to report.

### Metrics

Every target reports `<prefix>_up` and `<prefix>_scrape_error{error, warning}`, where `error` is the reason the
server is unavailable and `warning` is `"<what>: <reason>"` for a failed query/request. Reasons are a small fixed
set (`auth`, `permission`, `not_found`, `timeout`, `connection`, `tls`, `resource_limit`, `server_error`,
`bad_response`, `unknown`); the full errors are logged (credentials are never logged).

ClickHouse (`clickhouse_`):
- `clickhouse_info{server_version}`
- from `system.metrics`: `queries_running`, `merges_running`, `mutations_running`, `replicated_fetches_running`,
  `replicated_sends_running`, `connections{protocol}`, `memory_tracking_bytes`, `inserts_delayed`, `readonly_replicas`,
  `zookeeper_sessions`, `zookeeper_watches`, `zookeeper_requests_in_flight`, `background_merges_mutations_tasks`,
  `background_fetches_tasks`, `parts_by_state{state}`, `distributed_files_to_insert`
- from `system.events`: `queries_total{kind}`, `failed_queries_total{kind}`, `query_time_seconds_total`,
  `inserted_rows_total`, `inserted_bytes_total`, `selected_rows_total`, `selected_bytes_total`, `merges_total`,
  `merged_rows_total`, `merge_time_seconds_total`, `delayed_inserts_total`, `delayed_inserts_time_seconds_total`,
  `rejected_inserts_total`, `zookeeper_exceptions_total{type}`, `replicated_part_failed_fetches_total`,
  `replicated_data_loss_total`, `distributed_connection_fail_try_total`, `query_memory_limit_exceeded_total`
- from `system.asynchronous_metrics`: `uptime_seconds`, `max_part_count_for_partition`,
  `replicas_max_absolute_delay_seconds`, `replicas_max_queue_size`, `replicas_sum_queue_size`, `mergetree_parts`,
  `mergetree_rows`, `mergetree_bytes`, `databases`, `tables`, `memory_resident_bytes`, `os_memory_total_bytes`,
  `os_memory_available_bytes`
- per table `{db, table}` (bounded by `topTables`, the include/exclude regexes and `--exclude-databases`):
  `table_parts`, `table_rows`, `table_size_bytes`, `table_partitions`, `table_max_parts_per_partition` (active parts);
  `replica_readonly`, `replica_session_expired`, `replica_absolute_delay_seconds`, `replica_queue_size`,
  `replica_inserts_in_queue`, `replica_merges_in_queue`; `table_mutations_in_progress`, `table_mutations_failing`,
  `table_mutations_stuck` (unfinished for more than an hour)
- `clickhouse_errors_total{name}` (the top 100 error names from `system.errors`)
- top queries `{db, query}` from `system.query_log` (initial queries grouped by `normalized_query_hash`, the text
  normalized with `normalizeQuery()` and obfuscated again by the agent; the agent's own queries are excluded):
  `top_query_calls_per_second`, `top_query_time_per_second`, `top_query_read_rows_per_second`,
  `top_query_read_bytes_per_second`, `top_query_errors_per_second`. The window lags 10s behind the current time
  because `query_log` is flushed asynchronously.

Elasticsearch/OpenSearch (`elasticsearch_`, names follow
[elasticsearch_exporter](https://github.com/prometheus-community/elasticsearch_exporter) where possible):
- `elasticsearch_clusterinfo_version_info{cluster, version, distribution}`
- `_cluster/health` `{cluster}`: `cluster_health_status{color}`, `cluster_health_number_of_nodes`,
  `cluster_health_number_of_data_nodes`, `cluster_health_active_primary_shards`, `cluster_health_active_shards`,
  `cluster_health_relocating_shards`, `cluster_health_initializing_shards`, `cluster_health_unassigned_shards`,
  `cluster_health_delayed_unassigned_shards`, `cluster_health_number_of_pending_tasks`,
  `cluster_health_number_of_in_flight_fetch`, `cluster_health_task_max_waiting_in_queue_millis`, `cluster_health_timed_out`
- `_nodes/<nodes>/stats` `{cluster, host, name}`: `jvm_memory_used_bytes{area}`, `jvm_memory_max_bytes{area}`,
  `jvm_memory_committed_bytes{area}`, `jvm_gc_collection_seconds_count{gc}`, `jvm_gc_collection_seconds_sum{gc}`,
  `filesystem_data_{available,free,size}_bytes{mount, path}`, `indices_docs`, `indices_docs_deleted`,
  `indices_store_size_bytes`, `indices_segments_count`, `indices_indexing_index_total`,
  `indices_indexing_index_time_seconds_total`, `indices_indexing_index_failed_total`, `indices_search_query_total`,
  `indices_search_query_time_seconds`, `indices_search_fetch_total`, `indices_search_fetch_time_seconds`,
  `indices_merges_total`, `indices_merges_total_time_seconds_total`, `indices_refresh_total`,
  `indices_refresh_time_seconds_total`, `indices_flush_total`, `indices_flush_time_seconds`,
  `thread_pool_{rejected,active,queue,threads}_count{type}`, `breakers_tripped{breaker}`,
  `breakers_estimated_size_bytes{breaker}`, `breakers_limit_size_bytes{breaker}`, `process_cpu_percent`,
  `process_open_files_count`, `process_max_files_descriptors`
- `_cat/indices` `{cluster, index}` (bounded by `topIndices` and the include/exclude regexes): `indices_docs_primary`,
  `indices_deleted_docs_primary`, `indices_store_size_bytes_primary`, `indices_store_size_bytes_total`,
  `indices_shards_primary`, `indices_replicas`, `indices_health_status{color}`


## Cloud integrations

The AWS, GCP, OCI and Azure integrations discover managed databases, expose their metadata and system metrics,
and publish their endpoints so that the database exporters (PostgreSQL, MySQL, Redis) can be attached to them
through the `databases` section of the static configuration file (`--config-file` / `CONFIG_FILE`).
Environment variables in the file are expanded (`${VAR}`), so passwords can come from Kubernetes secrets.

Every discoverer runs in the background (discovery once a minute; cloud metrics are fetched in the background and
served from a cache on scrape), bounds each API call with a timeout, and reports API failures as
`<cloud>_discovery_error`.

### AWS

```yaml
aws:
  region: us-east-1                  # optional: AWS_REGION, the region of the Kubernetes nodes, or IMDS
  rdsTagFilters: {env: prod*}        # glob patterns; also applied to Aurora instances
  elasticacheTagFilters: {team: web} # ElastiCache clusters and ElastiCache Serverless caches
  memorydbTagFilters: {team: web}
  cloudwatchPeriodSeconds: 60        # period (and refresh interval) of the CloudWatch metrics, rounded up to a multiple of 60

databases:
  - {type: postgres, rds: orders-db, credentials: {username: coroot, password: "${PG_PASSWORD}"}}
  - {type: redis, elasticache: sessions} # an ElastiCache cluster id or an ElastiCache Serverless cache name
  - {type: redis, memorydb: cart}        # a MemoryDB cluster name: the exporter is attached to each node
```

Credentials come from the default AWS credential chain (IRSA / EKS Pod Identity, instance profile, environment),
or from `accessKeyId`/`secretAccessKey`.

**Aurora.** In addition to the RDS metrics, Aurora instances get:

| Metric | Source |
|---|---|
| `aws_rds_cluster_role{role="writer"\|"reader"}` | `rds:DescribeDBClusters` (cluster members) |
| `aws_rds_aurora_replica_lag_seconds` (readers) | CloudWatch `AuroraReplicaLag` |
| `aws_rds_serverless_capacity_acu`, `aws_rds_serverless_acu_utilization_percent` (`db.serverless` instances) | CloudWatch `ServerlessDatabaseCapacity`, `ACUUtilization` |
| `aws_rds_serverless_min_capacity_acu`, `aws_rds_serverless_max_capacity_acu` | `rds:DescribeDBClusters` (Serverless v2 scaling configuration) |

**ElastiCache Serverless** (Valkey / Redis OSS / Memcached): `aws_elasticache_serverless_info`, `_status`,
`_data_storage_limit_bytes` and `_ecpu_limit_per_second` from `DescribeServerlessCaches`;
`aws_elasticache_serverless_ecpu_per_second`, `_used_bytes` and `_connections` from CloudWatch
(`ElastiCacheProcessingUnits`, `BytesUsedForCache`, `CurrConnections`). The endpoint of a cache is published under its
name, like the ElastiCache clusters (`elasticache: <name>`). Serverless caches only accept TLS: `tls: skip-verify` is
set on the target unless `params.tls` is configured (the endpoint is resolved to IP addresses, so the certificate can't
be verified against the host name).

**MemoryDB** (Valkey / Redis OSS): `aws_memorydb_info`, `aws_memorydb_status` and `aws_memorydb_node_info` from
`DescribeClusters`. The endpoints of the nodes are published under the cluster name (`memorydb: <name>`);
`tls: skip-verify` is set on the targets of the clusters with in-transit encryption unless `params.tls` is configured.

The CloudWatch metrics are fetched with `GetMetricData` (up to 500 metrics per request) once per
`cloudwatchPeriodSeconds` by a background goroutine, never on scrape. CloudWatch charges `GetMetricData` per metric
requested: 1-3 metrics per Aurora instance and 3 per Serverless cache per period.

Required IAM actions:

```
rds:DescribeDBInstances
rds:DescribeDBClusters              # Aurora roles and Serverless v2 capacity range
rds:DescribeDBLogFiles              # RDS logs
rds:DownloadDBLogFilePortion        # RDS logs
logs:GetLogEvents                   # RDS Enhanced Monitoring (RDSOSMetrics log group)
elasticache:DescribeCacheClusters
elasticache:DescribeServerlessCaches
elasticache:ListTagsForResource     # only with elasticacheTagFilters
memorydb:DescribeClusters
memorydb:ListTags                   # only with memorydbTagFilters
cloudwatch:GetMetricData            # Aurora and ElastiCache Serverless metrics
sts:GetCallerIdentity               # logs the identity in use
```

The actions used only by the Aurora/Serverless/MemoryDB integrations (`rds:DescribeDBClusters`,
`elasticache:DescribeServerlessCaches`, `memorydb:*`, `cloudwatch:GetMetricData`) are optional: if the policy doesn't
allow them, a warning is logged once and the corresponding resources/metrics are skipped, without reporting a
discovery error every minute.

### Azure

Discovers Azure Database for PostgreSQL flexible servers, Azure Database for MySQL flexible servers and
Azure Cache for Redis in the configured subscriptions and resource groups.

```yaml
azure:
  subscriptionIds: [00000000-0000-0000-0000-000000000000] # AZURE_SUBSCRIPTION_ID by default
  tenantId: ""                     # optional
  resourceGroups: [prod-db]        # optional: all the resource groups of the subscriptions by default
  locations: [westeurope]          # optional: all the locations by default
  postgresTagFilters: {env: prod*} # glob patterns; read replicas follow their primary
  mysqlTagFilters: {env: prod*}
  redisTagFilters: {team: web}

databases:
  - {type: postgres, azuredb: orders-pg, credentials: {username: coroot, password: "${PG_PASSWORD}"}}
  - {type: mysql, azuredb: billing-mysql, credentials: {username: coroot, password: "${MYSQL_PASSWORD}"}}
  - {type: redis, azureredis: sessions-cache, credentials: {password: "${REDIS_ACCESS_KEY}"}}
```

Authentication uses `DefaultAzureCredential`: an environment service principal (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`,
and `AZURE_CLIENT_SECRET` or `AZURE_CLIENT_CERTIFICATE_PATH`), AKS workload identity (`AZURE_FEDERATED_TOKEN_FILE`,
injected by the workload identity webhook), or a managed identity. No secret is part of the configuration and none is
logged; the agent doesn't read the Redis access keys (pass them through `credentials`).

`azuredb` targets are resolved by `type` (`postgres` or `mysql`) and server name; the read replicas of a server are
monitored with the same credentials. The flexible servers require TLS by default, so `sslmode: require` (PostgreSQL)
and `tls: skip-verify` (MySQL) are set unless configured in `params`. For Redis, the TLS port (6380) is used with
`tls: skip-verify` unless the non-TLS port is enabled.

Metrics come from Azure Monitor (`PT1M` grain), fetched once a minute in the background with one request per resource
and metric group:

| Metric | PostgreSQL | MySQL |
|---|---|---|
| `azure_db_info`, `azure_db_status`, `azure_db_storage_total_bytes` | ARM | ARM |
| `azure_db_cpu_usage_percent` | `cpu_percent` | `cpu_percent` |
| `azure_db_memory_usage_percent` | `memory_percent` | `memory_percent` |
| `azure_db_storage_usage_percent`, `azure_db_storage_used_bytes` | `storage_percent`, `storage_used` | `storage_percent`, `storage_used` |
| `azure_db_iops`, `azure_db_io_ops_per_second{operation}` | `iops`, `read_iops`, `write_iops` | - |
| `azure_db_io_consumption_percent` | - | `io_consumption_percent` |
| `azure_db_network_bytes_per_second{direction}` | `network_bytes_ingress`, `network_bytes_egress` | same |
| `azure_db_connections_active` | `active_connections` | `active_connections` |
| `azure_db_replication_lag_seconds` (replicas) | `read_replica_lag` | `replication_lag` |

Redis: `azure_redis_info`, `azure_redis_status`, `azure_redis_cpu_usage_percent` (`percentProcessorTime`),
`azure_redis_memory_usage_percent` (`usedmemorypercentage`), `azure_redis_memory_used_bytes` (`usedmemory`),
`azure_redis_server_load_percent` (`serverLoad`), `azure_redis_connected_clients` (`connectedclients`),
`azure_redis_network_bytes_per_second{direction}` (`cacheRead`/`cacheWrite`).

Required RBAC roles on the subscriptions or resource groups: **Reader** (listing the servers and caches) and
**Monitoring Reader** (Azure Monitor metrics). The database logs are not collected by the Azure integration: Azure
exports them through diagnostic settings (Log Analytics, Event Hubs, storage), which needs a separate pipeline.
