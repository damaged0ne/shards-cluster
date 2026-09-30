Coroot Cluster Agent.

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
