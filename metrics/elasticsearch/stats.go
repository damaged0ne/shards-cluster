package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"sort"
	"strconv"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
)

var clusterLabels = []string{"cluster"}

func clusterDesc(name, help string, labels ...string) *prometheus.Desc {
	return common.Desc(name, help, append(append([]string{}, clusterLabels...), labels...)...)
}

var (
	dHealthStatus            = clusterDesc("elasticsearch_cluster_health_status", "Whether all primary and replica shards are allocated (1 for the current color)", "color")
	dHealthNodes             = clusterDesc("elasticsearch_cluster_health_number_of_nodes", "Number of nodes in the cluster")
	dHealthDataNodes         = clusterDesc("elasticsearch_cluster_health_number_of_data_nodes", "Number of data nodes in the cluster")
	dHealthActivePrimary     = clusterDesc("elasticsearch_cluster_health_active_primary_shards", "Number of active primary shards")
	dHealthActiveShards      = clusterDesc("elasticsearch_cluster_health_active_shards", "Number of active primary and replica shards")
	dHealthRelocating        = clusterDesc("elasticsearch_cluster_health_relocating_shards", "Number of shards that are relocating")
	dHealthInitializing      = clusterDesc("elasticsearch_cluster_health_initializing_shards", "Number of shards that are initializing")
	dHealthUnassigned        = clusterDesc("elasticsearch_cluster_health_unassigned_shards", "Number of shards that are not allocated")
	dHealthDelayedUnassigned = clusterDesc("elasticsearch_cluster_health_delayed_unassigned_shards", "Number of shards whose allocation has been delayed")
	dHealthPendingTasks      = clusterDesc("elasticsearch_cluster_health_number_of_pending_tasks", "Number of cluster-level changes which have not yet been executed")
	dHealthInFlightFetch     = clusterDesc("elasticsearch_cluster_health_number_of_in_flight_fetch", "Number of unfinished shard fetches")
	dHealthTaskMaxWaiting    = clusterDesc("elasticsearch_cluster_health_task_max_waiting_in_queue_millis", "Time the oldest pending task has been waiting, in milliseconds")
	dHealthTimedOut          = clusterDesc("elasticsearch_cluster_health_timed_out", "Whether the cluster health request timed out")
)

var healthColors = []string{"green", "yellow", "red"}

type clusterHealthResponse struct {
	Status                      string  `json:"status"`
	TimedOut                    bool    `json:"timed_out"`
	NumberOfNodes               float64 `json:"number_of_nodes"`
	NumberOfDataNodes           float64 `json:"number_of_data_nodes"`
	ActivePrimaryShards         float64 `json:"active_primary_shards"`
	ActiveShards                float64 `json:"active_shards"`
	RelocatingShards            float64 `json:"relocating_shards"`
	InitializingShards          float64 `json:"initializing_shards"`
	UnassignedShards            float64 `json:"unassigned_shards"`
	DelayedUnassignedShards     float64 `json:"delayed_unassigned_shards"`
	NumberOfPendingTasks        float64 `json:"number_of_pending_tasks"`
	NumberOfInFlightFetch       float64 `json:"number_of_in_flight_fetch"`
	TaskMaxWaitingInQueueMillis float64 `json:"task_max_waiting_in_queue_millis"`
}

func (c *Collector) clusterHealth(ctx context.Context, cluster string) ([]prometheus.Metric, error) {
	var h clusterHealthResponse
	if err := c.get(ctx, "/_cluster/health", nil, &h); err != nil {
		return nil, err
	}
	return clusterHealthMetrics(cluster, &h), nil
}

func clusterHealthMetrics(cluster string, h *clusterHealthResponse) []prometheus.Metric {
	res := []prometheus.Metric{
		common.Gauge(dHealthNodes, h.NumberOfNodes, cluster),
		common.Gauge(dHealthDataNodes, h.NumberOfDataNodes, cluster),
		common.Gauge(dHealthActivePrimary, h.ActivePrimaryShards, cluster),
		common.Gauge(dHealthActiveShards, h.ActiveShards, cluster),
		common.Gauge(dHealthRelocating, h.RelocatingShards, cluster),
		common.Gauge(dHealthInitializing, h.InitializingShards, cluster),
		common.Gauge(dHealthUnassigned, h.UnassignedShards, cluster),
		common.Gauge(dHealthDelayedUnassigned, h.DelayedUnassignedShards, cluster),
		common.Gauge(dHealthPendingTasks, h.NumberOfPendingTasks, cluster),
		common.Gauge(dHealthInFlightFetch, h.NumberOfInFlightFetch, cluster),
		common.Gauge(dHealthTaskMaxWaiting, h.TaskMaxWaitingInQueueMillis, cluster),
		common.Gauge(dHealthTimedOut, b2f(h.TimedOut), cluster),
	}
	for _, color := range healthColors {
		res = append(res, common.Gauge(dHealthStatus, b2f(h.Status == color), cluster, color))
	}
	return res
}

var nodeLabels = []string{"cluster", "host", "name"}

func nodeDesc(name, help string, labels ...string) *prometheus.Desc {
	return common.Desc(name, help, append(append([]string{}, nodeLabels...), labels...)...)
}

var (
	dJvmMemoryUsed      = nodeDesc("elasticsearch_jvm_memory_used_bytes", "JVM memory currently used by area", "area")
	dJvmMemoryMax       = nodeDesc("elasticsearch_jvm_memory_max_bytes", "JVM memory max by area", "area")
	dJvmMemoryCommitted = nodeDesc("elasticsearch_jvm_memory_committed_bytes", "JVM memory currently committed by area", "area")
	dJvmGcCount         = nodeDesc("elasticsearch_jvm_gc_collection_seconds_count", "Count of JVM GC runs", "gc")
	dJvmGcTime          = nodeDesc("elasticsearch_jvm_gc_collection_seconds_sum", "GC run time in seconds", "gc")

	dFsAvailable = nodeDesc("elasticsearch_filesystem_data_available_bytes", "Available space on block device in bytes", "mount", "path")
	dFsFree      = nodeDesc("elasticsearch_filesystem_data_free_bytes", "Free space on block device in bytes", "mount", "path")
	dFsSize      = nodeDesc("elasticsearch_filesystem_data_size_bytes", "Size of block device in bytes", "mount", "path")

	dIdxDocs            = nodeDesc("elasticsearch_indices_docs", "Count of documents on this node")
	dIdxDocsDeleted     = nodeDesc("elasticsearch_indices_docs_deleted", "Count of deleted documents on this node")
	dIdxStoreSize       = nodeDesc("elasticsearch_indices_store_size_bytes", "Current size of stored index data in bytes")
	dIdxSegments        = nodeDesc("elasticsearch_indices_segments_count", "Count of index segments on this node")
	dIdxIndexingTotal   = nodeDesc("elasticsearch_indices_indexing_index_total", "Total index calls")
	dIdxIndexingTime    = nodeDesc("elasticsearch_indices_indexing_index_time_seconds_total", "Cumulative index time in seconds")
	dIdxIndexingFailed  = nodeDesc("elasticsearch_indices_indexing_index_failed_total", "Total failed index calls")
	dIdxSearchQuery     = nodeDesc("elasticsearch_indices_search_query_total", "Total number of queries")
	dIdxSearchQueryTime = nodeDesc("elasticsearch_indices_search_query_time_seconds", "Total search query time in seconds")
	dIdxSearchFetch     = nodeDesc("elasticsearch_indices_search_fetch_total", "Total number of fetches")
	dIdxSearchFetchTime = nodeDesc("elasticsearch_indices_search_fetch_time_seconds", "Total search fetch time in seconds")
	dIdxMerges          = nodeDesc("elasticsearch_indices_merges_total", "Total merges")
	dIdxMergesTime      = nodeDesc("elasticsearch_indices_merges_total_time_seconds_total", "Total time spent merging in seconds")
	dIdxRefresh         = nodeDesc("elasticsearch_indices_refresh_total", "Total refreshes")
	dIdxRefreshTime     = nodeDesc("elasticsearch_indices_refresh_time_seconds_total", "Total time spent refreshing in seconds")
	dIdxFlush           = nodeDesc("elasticsearch_indices_flush_total", "Total flushes")
	dIdxFlushTime       = nodeDesc("elasticsearch_indices_flush_time_seconds", "Cumulative flush time in seconds")

	dThreadPoolRejected = nodeDesc("elasticsearch_thread_pool_rejected_count", "Thread pool rejected tasks count", "type")
	dThreadPoolActive   = nodeDesc("elasticsearch_thread_pool_active_count", "Thread pool active threads count", "type")
	dThreadPoolQueue    = nodeDesc("elasticsearch_thread_pool_queue_count", "Thread pool queued tasks count", "type")
	dThreadPoolThreads  = nodeDesc("elasticsearch_thread_pool_threads_count", "Thread pool threads count", "type")

	dBreakerTripped   = nodeDesc("elasticsearch_breakers_tripped", "Number of times the circuit breaker has been tripped", "breaker")
	dBreakerEstimated = nodeDesc("elasticsearch_breakers_estimated_size_bytes", "Estimated size in bytes of the breaker", "breaker")
	dBreakerLimit     = nodeDesc("elasticsearch_breakers_limit_size_bytes", "Limit size in bytes for the breaker", "breaker")

	dProcessCPU       = nodeDesc("elasticsearch_process_cpu_percent", "Percent CPU used by the process")
	dProcessOpenFiles = nodeDesc("elasticsearch_process_open_files_count", "Open file descriptors")
	dProcessMaxFiles  = nodeDesc("elasticsearch_process_max_files_descriptors", "Max file descriptors")
)

type timedStats struct {
	Total       float64 `json:"total"`
	TotalTimeMs float64 `json:"total_time_in_millis"`
	IndexTotal  float64 `json:"index_total"`
	IndexTimeMs float64 `json:"index_time_in_millis"`
	IndexFailed float64 `json:"index_failed"`
	QueryTotal  float64 `json:"query_total"`
	QueryTimeMs float64 `json:"query_time_in_millis"`
	FetchTotal  float64 `json:"fetch_total"`
	FetchTimeMs float64 `json:"fetch_time_in_millis"`
	Count       float64 `json:"count"`
	Deleted     float64 `json:"deleted"`
	SizeInBytes float64 `json:"size_in_bytes"`
}

type nodeStats struct {
	Name    string `json:"name"`
	Host    string `json:"host"`
	Indices struct {
		Docs     timedStats `json:"docs"`
		Store    timedStats `json:"store"`
		Indexing timedStats `json:"indexing"`
		Search   timedStats `json:"search"`
		Merges   timedStats `json:"merges"`
		Refresh  timedStats `json:"refresh"`
		Flush    timedStats `json:"flush"`
		Segments timedStats `json:"segments"`
	} `json:"indices"`
	JVM *struct {
		Mem struct {
			HeapUsed         float64 `json:"heap_used_in_bytes"`
			HeapMax          float64 `json:"heap_max_in_bytes"`
			HeapCommitted    float64 `json:"heap_committed_in_bytes"`
			NonHeapUsed      float64 `json:"non_heap_used_in_bytes"`
			NonHeapCommitted float64 `json:"non_heap_committed_in_bytes"`
		} `json:"mem"`
		GC struct {
			Collectors map[string]struct {
				Count  float64 `json:"collection_count"`
				TimeMs float64 `json:"collection_time_in_millis"`
			} `json:"collectors"`
		} `json:"gc"`
	} `json:"jvm"`
	FS struct {
		Data []struct {
			Path      string  `json:"path"`
			Mount     string  `json:"mount"`
			Total     float64 `json:"total_in_bytes"`
			Free      float64 `json:"free_in_bytes"`
			Available float64 `json:"available_in_bytes"`
		} `json:"data"`
	} `json:"fs"`
	ThreadPool map[string]struct {
		Threads  float64 `json:"threads"`
		Queue    float64 `json:"queue"`
		Active   float64 `json:"active"`
		Rejected float64 `json:"rejected"`
	} `json:"thread_pool"`
	Breakers map[string]struct {
		Limit     float64 `json:"limit_size_in_bytes"`
		Estimated float64 `json:"estimated_size_in_bytes"`
		Tripped   float64 `json:"tripped"`
	} `json:"breakers"`
	Process *struct {
		CPU struct {
			Percent float64 `json:"percent"`
		} `json:"cpu"`
		OpenFDs float64 `json:"open_file_descriptors"`
		MaxFDs  float64 `json:"max_file_descriptors"`
	} `json:"process"`
}

type nodesStatsResponse struct {
	ClusterName string                `json:"cluster_name"`
	Nodes       map[string]*nodeStats `json:"nodes"`
}

func (c *Collector) nodesStats(ctx context.Context, cluster string) ([]prometheus.Metric, error) {
	var r nodesStatsResponse
	path := "/_nodes/" + c.opts.nodes + "/stats/jvm,fs,indices,thread_pool,breaker,process"
	q := url.Values{"filter_path": {"cluster_name,nodes.*.name,nodes.*.host,nodes.*.jvm,nodes.*.fs.data,nodes.*.indices,nodes.*.thread_pool,nodes.*.breakers,nodes.*.process"}}
	if err := c.get(ctx, path, q, &r); err != nil {
		return nil, err
	}
	if r.ClusterName != "" {
		cluster = r.ClusterName
	}
	return nodesStatsMetrics(cluster, &r), nil
}

func nodesStatsMetrics(cluster string, r *nodesStatsResponse) []prometheus.Metric {
	ids := make([]string, 0, len(r.Nodes))
	for id := range r.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var res []prometheus.Metric
	seen := map[[2]string]bool{}
	for _, id := range ids {
		n := r.Nodes[id]
		if n == nil {
			continue
		}
		key := [2]string{n.Host, n.Name}
		if seen[key] { // two nodes with the same host and name would produce duplicate series
			continue
		}
		seen[key] = true
		l := []string{cluster, n.Host, n.Name}
		g := func(d *prometheus.Desc, v float64, extra ...string) prometheus.Metric {
			return common.Gauge(d, v, append(l[:3:3], extra...)...)
		}
		cnt := func(d *prometheus.Desc, v float64, extra ...string) prometheus.Metric {
			return common.Counter(d, v, append(l[:3:3], extra...)...)
		}
		ix := &n.Indices
		res = append(res,
			g(dIdxDocs, ix.Docs.Count),
			g(dIdxDocsDeleted, ix.Docs.Deleted),
			g(dIdxStoreSize, ix.Store.SizeInBytes),
			g(dIdxSegments, ix.Segments.Count),
			cnt(dIdxIndexingTotal, ix.Indexing.IndexTotal),
			cnt(dIdxIndexingTime, ix.Indexing.IndexTimeMs/1000),
			cnt(dIdxIndexingFailed, ix.Indexing.IndexFailed),
			cnt(dIdxSearchQuery, ix.Search.QueryTotal),
			cnt(dIdxSearchQueryTime, ix.Search.QueryTimeMs/1000),
			cnt(dIdxSearchFetch, ix.Search.FetchTotal),
			cnt(dIdxSearchFetchTime, ix.Search.FetchTimeMs/1000),
			cnt(dIdxMerges, ix.Merges.Total),
			cnt(dIdxMergesTime, ix.Merges.TotalTimeMs/1000),
			cnt(dIdxRefresh, ix.Refresh.Total),
			cnt(dIdxRefreshTime, ix.Refresh.TotalTimeMs/1000),
			cnt(dIdxFlush, ix.Flush.Total),
			cnt(dIdxFlushTime, ix.Flush.TotalTimeMs/1000),
		)
		if jvm := n.JVM; jvm != nil {
			res = append(res,
				g(dJvmMemoryUsed, jvm.Mem.HeapUsed, "heap"),
				g(dJvmMemoryUsed, jvm.Mem.NonHeapUsed, "non-heap"),
				g(dJvmMemoryMax, jvm.Mem.HeapMax, "heap"),
				g(dJvmMemoryCommitted, jvm.Mem.HeapCommitted, "heap"),
				g(dJvmMemoryCommitted, jvm.Mem.NonHeapCommitted, "non-heap"),
			)
			for _, name := range sortedKeys(jvm.GC.Collectors) {
				gc := jvm.GC.Collectors[name]
				res = append(res, cnt(dJvmGcCount, gc.Count, name), cnt(dJvmGcTime, gc.TimeMs/1000, name))
			}
		}
		seenFS := map[[2]string]bool{}
		for _, d := range n.FS.Data {
			k := [2]string{d.Mount, d.Path}
			if seenFS[k] {
				continue
			}
			seenFS[k] = true
			res = append(res,
				g(dFsAvailable, d.Available, d.Mount, d.Path),
				g(dFsFree, d.Free, d.Mount, d.Path),
				g(dFsSize, d.Total, d.Mount, d.Path),
			)
		}
		for _, name := range sortedKeys(n.ThreadPool) {
			tp := n.ThreadPool[name]
			res = append(res,
				cnt(dThreadPoolRejected, tp.Rejected, name),
				g(dThreadPoolActive, tp.Active, name),
				g(dThreadPoolQueue, tp.Queue, name),
				g(dThreadPoolThreads, tp.Threads, name),
			)
		}
		for _, name := range sortedKeys(n.Breakers) {
			b := n.Breakers[name]
			res = append(res,
				cnt(dBreakerTripped, b.Tripped, name),
				g(dBreakerEstimated, b.Estimated, name),
				g(dBreakerLimit, b.Limit, name),
			)
		}
		if p := n.Process; p != nil {
			res = append(res,
				g(dProcessCPU, p.CPU.Percent),
				g(dProcessOpenFiles, p.OpenFDs),
				g(dProcessMaxFiles, p.MaxFDs),
			)
		}
	}
	return res
}

var (
	dIndexDocsPrimary        = clusterDesc("elasticsearch_indices_docs_primary", "Count of documents in the primary shards of the index", "index")
	dIndexDeletedDocsPrimary = clusterDesc("elasticsearch_indices_deleted_docs_primary", "Count of deleted documents in the primary shards of the index", "index")
	dIndexStorePrimary       = clusterDesc("elasticsearch_indices_store_size_bytes_primary", "Size of the primary shards of the index in bytes", "index")
	dIndexStoreTotal         = clusterDesc("elasticsearch_indices_store_size_bytes_total", "Size of all shards (primary and replica) of the index in bytes", "index")
	dIndexShardsPrimary      = clusterDesc("elasticsearch_indices_shards_primary", "Number of primary shards of the index", "index")
	dIndexReplicas           = clusterDesc("elasticsearch_indices_replicas", "Number of replicas of each primary shard of the index", "index")
	dIndexHealth             = clusterDesc("elasticsearch_indices_health_status", "Health of the index (1 for the current color)", "index", "color")
)

// catNumber is a number in the _cat API JSON output: a string, a number or null (e.g. for a closed index).
type catNumber struct {
	value float64
	ok    bool
}

func (n *catNumber) UnmarshalJSON(b []byte) error {
	b = bytes.Trim(b, `"`)
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	v, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return nil // not a number (e.g. a human-readable size): treated as absent
	}
	n.value, n.ok = v, true
	return nil
}

type catIndex struct {
	Index        string    `json:"index"`
	Health       string    `json:"health"`
	Status       string    `json:"status"`
	Pri          catNumber `json:"pri"`
	Rep          catNumber `json:"rep"`
	DocsCount    catNumber `json:"docs.count"`
	DocsDeleted  catNumber `json:"docs.deleted"`
	StoreSize    catNumber `json:"store.size"`
	PriStoreSize catNumber `json:"pri.store.size"`
}

func (c *Collector) indices(ctx context.Context, cluster string) ([]prometheus.Metric, error) {
	var rows []catIndex
	q := url.Values{
		"format": {"json"},
		"bytes":  {"b"},
		"h":      {"index,health,status,pri,rep,docs.count,docs.deleted,store.size,pri.store.size"},
	}
	if err := c.get(ctx, "/_cat/indices", q, &rows); err != nil {
		return nil, err
	}
	return c.indicesMetrics(cluster, rows), nil
}

func (c *Collector) indicesMetrics(cluster string, rows []catIndex) []prometheus.Metric {
	var filtered []catIndex
	seen := map[string]bool{}
	for _, r := range rows {
		if r.Index == "" || seen[r.Index] {
			continue
		}
		if c.opts.indicesInclude != nil && !c.opts.indicesInclude.MatchString(r.Index) {
			continue
		}
		if c.opts.indicesExclude != nil && c.opts.indicesExclude.MatchString(r.Index) {
			continue
		}
		seen[r.Index] = true
		filtered = append(filtered, r)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		if filtered[i].StoreSize.value != filtered[j].StoreSize.value {
			return filtered[i].StoreSize.value > filtered[j].StoreSize.value
		}
		return filtered[i].Index < filtered[j].Index
	})
	if len(filtered) > c.opts.topIndices {
		filtered = filtered[:c.opts.topIndices]
	}
	var res []prometheus.Metric
	for _, r := range filtered {
		for _, m := range []struct {
			d *prometheus.Desc
			v catNumber
		}{
			{dIndexDocsPrimary, r.DocsCount},
			{dIndexDeletedDocsPrimary, r.DocsDeleted},
			{dIndexStorePrimary, r.PriStoreSize},
			{dIndexStoreTotal, r.StoreSize},
			{dIndexShardsPrimary, r.Pri},
			{dIndexReplicas, r.Rep},
		} {
			if m.v.ok {
				res = append(res, common.Gauge(m.d, m.v.value, cluster, r.Index))
			}
		}
		if r.Health != "" {
			for _, color := range healthColors {
				res = append(res, common.Gauge(dIndexHealth, b2f(r.Health == color), cluster, r.Index, color))
			}
		}
	}
	return res
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

var _ json.Unmarshaler = (*catNumber)(nil)
