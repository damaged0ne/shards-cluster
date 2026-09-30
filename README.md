Coroot Cluster Agent.

## Native Prometheus endpoints: RabbitMQ and etcd

RabbitMQ (the `rabbitmq_prometheus` plugin) and etcd expose their metrics in the Prometheus format, so no exporter
runs in the agent: their `/metrics` endpoints are scraped by the agent's Prometheus scrape manager and sent to Coroot
along with the other metrics (via the WAL and remote write).

Each target gets the `job` label `rabbitmq` or `etcd` and the `instance` label `<host>:<port>`.

To keep the cardinality bounded, only an allowlist of metrics is kept (`metric_relabel_configs` with a `keep` action on
`__name__`), and a scrape returning more than 50000 samples after relabeling is rejected as a whole (`up` becomes `0`):

* RabbitMQ: the node-level and aggregated metrics of `/metrics` (`rabbitmq_build_info`, `rabbitmq_identity_info`,
  alarms, memory and disk limits, file descriptors and sockets, connections, channels, consumers, queues,
  `rabbitmq_queue_messages{,_ready,_unacked}`, `rabbitmq_global_messages_*_total`, ...). The per-object endpoints
  (`/metrics/per-object`, `/metrics/detailed`) grow with the number of queues/connections and are not recommended.
* etcd: leadership, proposals, `etcd_mvcc_db_total_size_in{,_use_in}_bytes`, the WAL fsync and backend commit latency
  histograms, peer round-trip time and traffic, gRPC traffic and `grpc_server_handled_total`, process resources.

The allowlist can be replaced for a statically configured target with the `metrics_allowlist` param (a regular
expression matching the whole metric name).

### Static configuration

```yaml
databases:
  - type: rabbitmq
    host: rabbitmq.example.internal
    port: "15692"                     # optional, the default for rabbitmq
    credentials:                      # optional, HTTP basic auth
      username: monitoring
      password: ${RABBITMQ_MONITORING_PASSWORD}
  - type: etcd
    host: etcd-0.example.internal
    port: "2379"                      # optional, defaults to 2381 (--listen-metrics-urls, plain HTTP)
    params:
      tls_ca_file: /etc/etcd/pki/ca.crt
      tls_cert_file: /etc/etcd/pki/client.crt
      tls_key_file: /etc/etcd/pki/client.key
```

Supported `params`:

| param                           | description                                                        |
|---------------------------------|--------------------------------------------------------------------|
| `scheme`                        | `http` or `https`; defaults to `https` if any `tls_*` param is set |
| `metrics_path`                  | defaults to `/metrics`                                             |
| `tls_ca_file`                   | CA certificate to verify the server certificate                    |
| `tls_cert_file`, `tls_key_file` | client certificate and key (must be set together)                  |
| `tls_server_name`               | server name to verify the certificate against                      |
| `tls_insecure_skip_verify`      | `true` to skip the verification of the server certificate          |
| `metrics_allowlist`             | regular expression replacing the default metric allowlist          |

The host is resolved by the scrape manager; the cloud sources (`rds`, `cloudsql`, ...) aren't supported for these types.

### Kubernetes pod annotations

```yaml
metadata:
  annotations:
    coroot.com/rabbitmq-scrape: "true"
    coroot.com/rabbitmq-scrape-port: "15692"            # optional, default 15692 (etcd: 2381)
    coroot.com/rabbitmq-scrape-scheme: "http"           # optional, http or https
    coroot.com/rabbitmq-scrape-metrics-path: "/metrics" # optional
```

Use `coroot.com/etcd-scrape` (and the `coroot.com/etcd-scrape-*` annotations) for etcd. The discovered targets also get
the `namespace` and `pod` labels. Annotation-based targets can't use client certificates or basic auth: use the static
configuration for those.

## PgBouncer

The agent collects the metrics of PgBouncer from its admin console (the virtual `pgbouncer` database) with
`SHOW STATS`, `SHOW POOLS` and `SHOW LISTS`, using the metric names of
[pgbouncer_exporter](https://github.com/prometheus-community/pgbouncer_exporter):

* `pgbouncer_up`
* `pgbouncer_stats_*{database}`: `queries_pooled_total`, `queries_duration_seconds_total`,
  `sql_transactions_pooled_total`, `server_in_transaction_seconds_total`, `client_wait_seconds_total`,
  `received_bytes_total`, `sent_bytes_total`, `server_assignments_total`
* `pgbouncer_pools_*{database,user}`: `client_active_connections`, `client_waiting_connections`,
  `server_active_connections`, `server_idle_connections`, `server_used_connections`, `server_testing_connections`,
  `server_login_connections`, `client_maxwait_seconds`, ...
* `pgbouncer_databases`, `pgbouncer_users`, `pgbouncer_pools`, `pgbouncer_{free,used,login}_clients`,
  `pgbouncer_{free,used}_servers`, `pgbouncer_cached_dns_{names,zones}`, `pgbouncer_in_flight_dns_queries`

The user must be allowed to use the admin console (listed in `stats_users` or `admin_users` of `pgbouncer.ini`).
The admin console only supports the simple query protocol: the agent never prepares statements and doesn't send
startup parameters PgBouncer doesn't track, so `ignore_startup_parameters` doesn't need to be changed.

```yaml
databases:
  - type: pgbouncer
    host: pgbouncer.example.internal
    port: "6432"
    credentials:
      username: stats
      password: ${PGBOUNCER_STATS_PASSWORD}
    params:
      sslmode: disable                 # optional, as for postgres
```

Pod annotations follow the same pattern as for Postgres:

```yaml
metadata:
  annotations:
    coroot.com/pgbouncer-scrape: "true"
    coroot.com/pgbouncer-scrape-port: "6432"   # optional, default 6432
    coroot.com/pgbouncer-scrape-credentials-secret-name: pgbouncer-stats
    coroot.com/pgbouncer-scrape-credentials-secret-username-key: username
    coroot.com/pgbouncer-scrape-credentials-secret-password-key: password
    # coroot.com/pgbouncer-scrape-param-sslmode and coroot.com/pgbouncer-scrape-tls-secret-* work as for postgres
```
