# Postgres and MySQL metrics

This document lists the Postgres and MySQL metrics added on top of the original coroot-cluster-agent set,
the server versions they require, and the privileges the monitoring user needs.

General rules:

* Every optional query is gated by the server version (and, for `pg_stat_statements`, by the columns
  of the installed extension version). A view that is missing or not readable does not break the rest
  of the snapshot: the error is logged and reported as `pg_scrape_error{warning="<reason>"}` /
  `mysql_scrape_error{warning="<reason>"}` with a bounded reason
  (`timeout`, `auth`, `connection`, `permission`, `not_found`, `unknown`).
* Label cardinality is bounded: per-query/per-index series are limited to top N, per-standby and
  per-subscription series are capped, other label sets are fixed by the server (wait events, I/O contexts, SLRUs).
* "Tracker interval" metrics are collected by the database tracker (every 60s, per database for Postgres)
  and only when `--track-database-sizes` is enabled (default).

## Postgres

### Required privileges

Grant the `pg_monitor` role (PG10+): `GRANT pg_monitor TO coroot;`. It includes `pg_read_all_stats`
(needed to see other users' `pg_stat_activity`, `pg_stat_statements` and `pg_stat_replication` rows;
without it the lag/LSN columns of other sessions are NULL and are not reported) and `pg_read_all_settings`.
The `pg_stat_statements` extension must be created in the database the agent connects to
(`CREATE EXTENSION pg_stat_statements;`, plus `shared_preload_libraries = 'pg_stat_statements'`).
Per-database index metrics connect to every database, so the user needs `CONNECT` on them.

### pg_stat_database (every scrape)

Per-database counters, `pg_db_<column>_total{db}` (time columns are converted to seconds):

| Metric | Version |
|---|---|
| `pg_db_xact_commit_total`, `pg_db_xact_rollback_total` | all |
| `pg_db_blks_read_total`, `pg_db_blks_hit_total` | all |
| `pg_db_tup_returned_total`, `pg_db_tup_fetched_total`, `pg_db_tup_inserted_total`, `pg_db_tup_updated_total`, `pg_db_tup_deleted_total` | all |
| `pg_db_conflicts_total`, `pg_db_temp_files_total`, `pg_db_temp_bytes_total`, `pg_db_deadlocks_total` | all |
| `pg_db_checksum_failures_total` (only when data checksums are enabled) | 12+ |
| `pg_db_sessions_total`, `pg_db_sessions_abandoned_total`, `pg_db_sessions_fatal_total`, `pg_db_sessions_killed_total` | 14+ |
| `pg_db_session_time_seconds_total`, `pg_db_active_time_seconds_total`, `pg_db_idle_in_transaction_time_seconds_total` | 14+ |

Template databases and `--exclude-databases` are skipped.

### Replication (every scrape)

Primary side (also on cascading standbys), `pg_stat_replication`, PG10+, at most 50 standbys:

| Metric | Labels |
|---|---|
| `pg_replication_standby_lag_seconds` — `write_lag`/`flush_lag`/`replay_lag`; 0 when the standby is caught up (the view reports NULL then) | `application_name`, `client_addr`, `stage` = `write`\|`flush`\|`replay` |
| `pg_replication_standby_lag_bytes` — current WAL LSN minus the standby's `sent`/`write`/`flush`/`replay` LSN | `application_name`, `client_addr`, `stage` = `sent`\|`write`\|`flush`\|`replay` |
| `pg_replication_standby_info` = 1 | `application_name`, `client_addr`, `state`, `sync_state` |

Logical replication, subscriber side (at most 100 subscriptions):

| Metric | Labels | Version |
|---|---|---|
| `pg_subscription_worker_up` — 1 if the apply worker is running | `subscription` | 10+ |
| `pg_subscription_last_msg_receipt_age_seconds` | `subscription` | 10+ |
| `pg_subscription_last_msg_delay_seconds` — receipt time minus publisher send time (includes clock skew) | `subscription` | 10+ |
| `pg_subscription_latest_end_age_seconds` | `subscription` | 10+ |
| `pg_subscription_errors_total` | `subscription`, `type` = `apply`\|`sync` | 15+ (`pg_stat_subscription_stats`) |

### pg_stat_statements (every scrape, top 20 queries)

The existing top-N selection (by total time) and label set (`db`, `user`, `query`) are unchanged.
New series for each top query:

| Metric | Source column | Version |
|---|---|---|
| `pg_top_query_rows_per_second` | `rows` | all |
| `pg_top_query_shared_blks_{hit,read,dirtied,written}_per_second` | `shared_blks_*` | all |
| `pg_top_query_local_blks_{hit,read,dirtied,written}_per_second` | `local_blks_*` | all |
| `pg_top_query_temp_blks_{read,written}_per_second` | `temp_blks_*` | all |
| `pg_top_query_plan_time_per_second` (needs `pg_stat_statements.track_planning`) | `total_plan_time` | 13+ |
| `pg_top_query_wal_bytes_per_second` | `wal_bytes` | 13+ |
| `pg_top_query_exec_time_{mean,min,max}_seconds` — since the last `pg_stat_statements_reset()` | `mean/min/max_exec_time` (13+), `mean/min/max_time` (9.5–12) | 9.5+ |

The column names are detected from the installed extension (`total_time` vs `total_exec_time`,
`blk_read_time` vs `shared_blk_read_time` in PG17), so an extension that wasn't upgraded after
`pg_upgrade` keeps working. If `pg_stat_statements` is not available, `pg_stat_activity`-based metrics
are still collected and `pg_scrape_error{warning="not_found"}` is reported.

### Wait events (every scrape)

| Metric | Labels |
|---|---|
| `pg_wait_event_sessions` — number of non-idle backends waiting on the event at snapshot time (`Activity` main-loop waits of background processes are excluded) | `wait_event_type`, `wait_event` |

PG9.6+ (earlier versions have no `wait_event`).

### I/O, SLRU and WAL statistics (every scrape)

| Metric | Labels | Version |
|---|---|---|
| `pg_io_{reads,writes,writebacks,extends,hits,evictions,reuses,fsyncs}_total` | `backend_type`, `object`, `context` | 16+ |
| `pg_io_{read,write,extend,fsync}_time_seconds_total` (need `track_io_timing`) | `backend_type`, `object`, `context` | 16+ |
| `pg_slru_{blks_zeroed,blks_hit,blks_read,blks_written,blks_exists,flushes,truncates}_total` | `name` | 13+ |
| `pg_wal_records_total`, `pg_wal_fpi_total`, `pg_wal_bytes_total`, `pg_wal_buffers_full_total` | | 14+ |

### Indexes (tracker interval, per database)

| Metric | Labels |
|---|---|
| `pg_index_unused_bytes` — size of a never-scanned (`idx_scan = 0`) index; unique, primary key and constraint-backing indexes are excluded; top 20 by size across all databases | `db`, `schema`, `table`, `index` |
| `pg_db_unused_indexes`, `pg_db_unused_indexes_bytes` — count and total size of such indexes | `db` |
| `pg_db_duplicate_indexes` — number of redundant indexes (same table, access method, columns, operator classes, collations, expressions and predicate as another index) | `db` |

`idx_scan` is counted since the last statistics reset and only on the server being monitored:
an index used only by queries on a standby looks unused on the primary.

## MySQL

### Required privileges

```sql
GRANT PROCESS, REPLICATION CLIENT ON *.* TO 'coroot'@'%';
GRANT SELECT ON performance_schema.* TO 'coroot'@'%';
GRANT SELECT ON sys.* TO 'coroot'@'%';
-- optional, for mysql_index_unused_bytes:
GRANT SELECT ON mysql.innodb_index_stats TO 'coroot'@'%';
```

`performance_schema` must be enabled (`performance_schema=ON`, the default in MySQL 5.7+/8.0).

### Wait events (every scrape)

`performance_schema.events_waits_summary_global_by_event_name`, top 30 events by total wait time
(the set is selected by the cumulative time, so it is stable), `idle` excluded:

| Metric | Labels |
|---|---|
| `mysql_wait_event_seconds_total` | `event` |
| `mysql_wait_event_count_total` | `event` |

Only the enabled instruments are counted (in MySQL 8.0 by default: file and table I/O, table locks).

### Replication (every scrape)

`mysql_replication_lag_seconds` (`Seconds_Behind_Source`/`Seconds_Behind_Master`) was already collected.
New, MySQL 8.0+ replicas only (not 5.7/MariaDB), from `performance_schema.replication_applier_status_by_worker`:

| Metric | Labels |
|---|---|
| `mysql_replication_applier_last_transaction_lag_seconds` — end of applying minus original commit time of the last applied transaction, worst worker | `channel` |
| `mysql_replication_applier_current_lag_seconds` — time since the original commit of the transaction being applied now (0 when idle), worst worker | `channel` |

Both compare timestamps of the source and the replica, so clock skew affects them.

### Unused indexes (tracker interval)

From `performance_schema.table_io_waits_summary_by_index_usage` (`COUNT_STAR = 0`, not `PRIMARY`),
excluding unique indexes (`information_schema.STATISTICS`) and system schemas:

| Metric | Labels |
|---|---|
| `mysql_index_unused` = 1 — top 20 by size (by name if sizes are unknown) | `schema`, `table`, `index` |
| `mysql_index_unused_bytes` — from `mysql.innodb_index_stats` (partitions summed); needs `SELECT` on it | `schema`, `table`, `index` |
| `mysql_schema_unused_indexes` — number of unused indexes | `schema` |

The counters are reset on server restart (and on `TRUNCATE` of the P_S table), so right after a restart
every index looks unused.

### InnoDB

Buffer pool hit ratio inputs (`mysql_innodb_buffer_pool_read_requests_total`, `mysql_innodb_buffer_pool_reads_total`),
row lock waits (`mysql_innodb_row_lock_waits_total`, `mysql_innodb_row_lock_time_seconds_total`,
`mysql_innodb_row_lock_current_waits`) and the history list length (`mysql_innodb_history_list_length`)
were already collected; nothing was added.
