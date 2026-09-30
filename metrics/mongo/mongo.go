package mongo

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/coroot/coroot-cluster-agent/schema"
	"github.com/coroot/logger"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	dUp            = common.Desc("mongo_up", "")
	dScrapeError   = common.Desc("mongo_scrape_error", "", "error", "warning")
	dInfo          = common.Desc("mongo_info", "", "server_version", "flavor")
	dRsStatus      = common.Desc("mongo_rs_status", "", "rs", "role")
	dRsLastApplied = common.Desc("mongo_rs_last_applied_timestamp_ms", "")

	dRsMemberConfig = common.Desc("mongo_rs_member_config_info", "Replica set member configuration", "rs", "member", "arbiter", "votes")
	dRsConfigInfo   = common.Desc("mongo_rs_config_info", "Replica set configuration", "rs", "write_concern_majority_journal_default")

	dProfilingLevel = common.Desc("mongo_profiling_level", "Database profiling level (0 - off, 1 - slow operations, 2 - all operations)", "db")

	dOplogWindow  = common.Desc("mongo_oplog_window_seconds", "Time span between the oldest and the newest oplog entries")
	dOplogSize    = common.Desc("mongo_oplog_size_bytes", "Current size of the oplog data")
	dOplogMaxSize = common.Desc("mongo_oplog_max_size_bytes", "Configured maximum oplog size")

	dConnectionsCurrent   = common.Desc("mongo_connections_current", "Number of client connections")
	dConnectionsActive    = common.Desc("mongo_connections_active", "Number of connections currently executing operations")
	dConnectionsMax       = common.Desc("mongo_connections_max", "Connection limit (net.maxIncomingConnections)")
	dConnectionsCreated   = common.Desc("mongo_connections_created_total", "Total number of connections created")
	dOpcounters           = common.Desc("mongo_opcounters_total", "Total number of operations by type", "op")
	dDocumentsReturned    = common.Desc("mongo_documents_returned_total", "Total number of documents returned by queries")
	dOpLatencyTotal       = common.Desc("mongo_op_latency_seconds_total", "Total operation latency by type", "type")
	dOpLatencyOps         = common.Desc("mongo_op_latency_ops_total", "Total number of operations by latency type", "type")
	dQueuedOps            = common.Desc("mongo_queued_operations", "Number of operations queued waiting for a lock or a ticket", "type")
	dTicketsAvailable     = common.Desc("mongo_wt_tickets_available", "Number of available WiredTiger concurrency tickets", "type")
	dWtCacheUsed          = common.Desc("mongo_wt_cache_used_bytes", "Bytes currently in the WiredTiger cache")
	dWtCacheDirty         = common.Desc("mongo_wt_cache_dirty_bytes", "Dirty bytes in the WiredTiger cache")
	dWtCacheMaxBytes      = common.Desc("mongo_wt_cache_max_bytes", "Maximum WiredTiger cache size")
	dWtEvictedApp         = common.Desc("mongo_wt_pages_evicted_by_app_threads_total", "Pages evicted from the WiredTiger cache by application threads")
	dWtAppEvictTime       = common.Desc("mongo_wt_app_threads_evicting_seconds_total", "Total time application threads spent evicting pages from the WiredTiger cache")
	dWtCheckpoints        = common.Desc("mongo_wt_checkpoints_total", "Total number of WiredTiger checkpoints completed")
	dWtCheckpointTime     = common.Desc("mongo_wt_checkpoint_seconds_total", "Total time spent in WiredTiger checkpoints")
	dWtJournalBytes       = common.Desc("mongo_wt_journal_bytes_written_total", "Total bytes written to the WiredTiger journal")
	dWtJournalSinceCkpt   = common.Desc("mongo_wt_journal_bytes_since_checkpoint", "Bytes written to the journal since the last checkpoint (to be replayed on crash recovery)")
	dTimeSinceCkpt        = common.Desc("mongo_time_since_last_checkpoint_seconds", "Time since the last WiredTiger checkpoint observed by the agent")
	dCollectionScans      = common.Desc("mongo_collection_scans_total", "Total number of queries that performed a collection scan")
	dConnectionsRejected  = common.Desc("mongo_connections_rejected_total", "Total number of rejected connections")
	dScannedKeys          = common.Desc("mongo_scanned_keys_total", "Total number of index keys scanned")
	dScannedObjects       = common.Desc("mongo_scanned_documents_total", "Total number of documents scanned")
	dScanAndOrder         = common.Desc("mongo_scan_and_order_total", "Total number of queries that performed an in-memory sort")
	dTtlDeleted           = common.Desc("mongo_ttl_deleted_documents_total", "Total number of documents deleted by TTL indexes")
	dFlowControlTime      = common.Desc("mongo_flow_control_time_acquiring_seconds_total", "Total time write operations spent acquiring flow control tickets")
	dWriteConflicts       = common.Desc("mongo_write_conflicts_total", "Total number of write conflicts (WiredTiger optimistic-concurrency retries)")
	dWtCacheReadInto      = common.Desc("mongo_wt_cache_bytes_read_into_total", "Total bytes read from disk into the WiredTiger cache (cache misses)")
	dReplApplyOps         = common.Desc("mongo_repl_apply_ops_total", "Total number of oplog operations applied by this member")
	dReplBufferCount      = common.Desc("mongo_repl_buffer_operations", "Number of oplog operations buffered waiting to be applied")
	dReplBufferBytes      = common.Desc("mongo_repl_buffer_bytes", "Size of the oplog apply buffer in bytes")
	dCursorsOpen          = common.Desc("mongo_cursors_open", "Number of open cursors")
	dCursorsNoTimeout     = common.Desc("mongo_cursors_open_no_timeout", "Number of open cursors created with DBQuery.Option.noTimeout")
	dCursorsTimedOut      = common.Desc("mongo_cursors_timed_out_total", "Total number of cursors that timed out")
	dPreparedTransactions = common.Desc("mongo_prepared_transactions", "Number of transactions currently in the prepared state (they hold locks until commit/abort replicates)")

	dTopQueryCalls        = common.Desc("mongo_top_query_calls_per_second", "Number of executions per second", "db", "collection", "query")
	dTopQueryTime         = common.Desc("mongo_top_query_time_per_second", "Time spent executing the query per second", "db", "collection", "query")
	dTopQueryDocsReturned = common.Desc("mongo_top_query_docs_returned_per_second", "Documents returned by the query per second", "db", "collection", "query")
	dTopQueryDocsExamined = common.Desc("mongo_top_query_docs_examined_per_second", "Documents examined by the query per second", "db", "collection", "query")
	dTopQueryKeysExamined = common.Desc("mongo_top_query_keys_examined_per_second", "Index keys examined by the query per second", "db", "collection", "query")

	dOpsWaitingLock        = common.Desc("mongo_operations_waiting_for_lock", "Number of operations waiting for a lock", "db")
	dConnectionsByApp      = common.Desc("mongo_connections_by_app", "Number of connections by client application", "app")
	dOpenTransactionsByApp = common.Desc("mongo_open_transactions", "Number of open (incl. prepared) transactions by client application - a long/prepared one holds locks and can block oplog apply", "app")
	dLongRunningOps        = common.Desc("mongo_long_running_operations", "Number of operations of this shape that have been running for at least 10s", "db", "collection", "query", "plan")
	dFsyncLocked           = common.Desc("mongo_fsync_locked", "1 if db.fsyncLock() is holding a global lock on this member (e.g. a backup), which blocks oplog apply on a secondary")

	dDbSize               = common.Desc("mongo_database_size_bytes", "Total size of the database in bytes", "db")
	dCollectionSize       = common.Desc("mongo_collection_size_bytes", "Total size of the collection in bytes", "db", "collection")
	dCollectionSizeGrowth = common.Desc("mongo_collection_size_growth_bytes_per_second", "Collection size growth rate in bytes per second", "db", "collection")
	dCollStorageSize      = common.Desc("mongo_collection_storage_size_bytes", "Bytes allocated on disk for documents of the collection", "db", "collection")
	dCollFreeStorage      = common.Desc("mongo_collection_free_storage_bytes", "Reusable (fragmented) bytes within the allocated collection storage", "db", "collection")
	dCollectionDocuments  = common.Desc("mongo_collection_documents", "Number of documents in the collection", "db", "collection")
)

const (
	StatusReplicationNotEnabled        = 76
	StatusReplicationNotYetInitialized = 94
)

type Collector struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	host       string
	clientOpts *options.ClientOptions
	clientLock sync.Mutex
	client     *clientRef // guarded by clientLock
	closed     bool       // guarded by clientLock
	done       chan struct{}
	logger     logger.Logger

	scrapeInterval time.Duration
	collectTimeout time.Duration

	// The fields below are the published state read by Collect. They are guarded by lock
	// and written only by the snapshot goroutine, which builds new values without holding
	// the lock and swaps them in at the end of each snapshot.
	lock         sync.RWMutex
	scrapeErrors map[string]bool

	serverVersion string
	isPercona     bool
	rsStatus      *ReplStatus

	rsConfig   *ReplConfig
	oplog      *OplogStats
	ss         *serverStatus
	ssCounters serverStatusCounters

	journalBytesAtCheckpoint float64
	lastCheckpointAt         time.Time

	currentOp  *CurrentOpStats
	topQueries []TopQuery

	profilingLevels map[string]int64

	dbSizes     map[string]*dbtracker.DBSizeSnapshot
	tableGrowth []dbtracker.TableGrowthEntry

	// The fields below are private to the snapshot goroutine.
	ssPrev           *serverStatus
	ssTotals         serverStatusCounters
	ckptJournalBytes float64
	ckptAt           time.Time

	profilerLastTs map[string]time.Time
	profilerPrevAt time.Time
	profilerWindow []profilerInterval

	dbTracker        *databaseTracker
	emitter          dbtracker.ChangeEmitter
	targetAddr       string
	prevSettingsText string
}

// clientRef is a reference-counted mongo client. A client that failed a ping is retired
// (replaced by a new one on the next use) and disconnected only after its last user releases it,
// so a snapshot running concurrently with Collect never operates on a nil or disconnected client.
type clientRef struct {
	client  *mongo.Client
	refs    int  // guarded by Collector.clientLock
	retired bool // guarded by Collector.clientLock
}

var errCollectorClosed = errors.New("mongo collector is closed")

func New(host, username, password string, tlsCreds common.TLSCredentials, params map[string]string, scrapeInterval, collectTimeout time.Duration,
	logger logger.Logger, emitter dbtracker.ChangeEmitter, targetAddr string,
	maxTablesPerDB int, trackSizes bool) *Collector {

	ctx, cancelFunc := context.WithCancel(context.Background())
	c := &Collector{
		ctx:            ctx,
		cancelFunc:     cancelFunc,
		host:           host,
		logger:         logger,
		scrapeInterval: scrapeInterval,
		collectTimeout: collectTimeout,
		scrapeErrors:   map[string]bool{},
		done:           make(chan struct{}),
		emitter:        emitter,
		targetAddr:     targetAddr,
		ssCounters:     newServerStatusCounters(),
		ssTotals:       newServerStatusCounters(),
		profilerLastTs: map[string]time.Time{},
	}
	c.clientOpts = options.Client().
		SetHosts([]string{host}).
		SetDirect(true).
		SetAppName("coroot-cluster-agent").
		SetServerSelectionTimeout(collectTimeout).
		SetConnectTimeout(collectTimeout)
	if username != "" {
		authSource := params["authSource"]
		if authSource == "" {
			authSource = "admin"
		}
		c.clientOpts.SetAuth(options.Credential{
			AuthSource: authSource,
			Username:   username,
			Password:   password,
		})
	}
	tlsEnabled := params["tls"] == "true" || params["tls"] == "skip-verify" ||
		tlsCreds.CA != "" || (tlsCreds.Cert != "" && tlsCreds.Key != "")
	if tlsEnabled {
		if cfg, err := common.DatabaseTLSConfig(tlsCreds, params["tls"] == "skip-verify"); err != nil {
			logger.Error("invalid TLS configuration:", err)
		} else {
			c.clientOpts.SetTLSConfig(cfg)
		}
	}
	trackSchema := c.emitter != nil
	if trackSchema || trackSizes {
		c.dbTracker = newDatabaseTracker(maxTablesPerDB, trackSchema, trackSizes, logger)
	}
	go func() {
		defer close(c.done)
		ticker := time.NewTicker(scrapeInterval)
		defer ticker.Stop()
		c.snapshot()
		for {
			select {
			case <-ticker.C:
				c.snapshot()
			case <-ctx.Done():
				c.logger.Info("stopping mongo collector")
				return
			}
		}
	}()
	return c
}

func (c *Collector) acquireClient(ctx context.Context) (*clientRef, error) {
	c.clientLock.Lock()
	defer c.clientLock.Unlock()
	if c.closed {
		return nil, errCollectorClosed
	}
	if c.client == nil {
		c.logger.Info("connecting to mongodb")
		client, err := mongo.Connect(ctx, c.clientOpts)
		if err != nil {
			return nil, err
		}
		c.client = &clientRef{client: client}
	}
	c.client.refs++
	return c.client, nil
}

// retireClientLocked must be called with clientLock held. It reports whether the client
// is no longer used and must be disconnected by the caller.
func (c *Collector) retireClientLocked(ref *clientRef) bool {
	if c.client == ref {
		c.client = nil
	}
	ref.retired = true
	return ref.refs == 0
}

func (c *Collector) releaseClient(ref *clientRef) {
	c.clientLock.Lock()
	ref.refs--
	disconnect := ref.retired && ref.refs == 0
	c.clientLock.Unlock()
	if disconnect {
		c.disconnect(ref)
	}
}

func (c *Collector) disconnect(ref *clientRef) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.collectTimeout)
	defer cancel()
	return ref.client.Disconnect(ctx)
}

// connectAndPing returns a pinged client reference that the caller must release with releaseClient.
func (c *Collector) connectAndPing(ctx context.Context) (*clientRef, error) {
	ref, err := c.acquireClient(ctx)
	if err != nil {
		return nil, err
	}
	if err = ref.client.Ping(ctx, nil); err != nil {
		c.clientLock.Lock()
		c.retireClientLocked(ref)
		c.clientLock.Unlock()
		c.releaseClient(ref)
		return nil, err
	}
	return ref, nil
}

func (c *Collector) Close() error {
	c.cancelFunc()
	<-c.done
	c.clientLock.Lock()
	c.closed = true
	ref := c.client
	disconnect := ref != nil && c.retireClientLocked(ref)
	c.clientLock.Unlock()
	if disconnect {
		return c.disconnect(ref)
	}
	return nil
}

func (c *Collector) snapshot() {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("mongo snapshot panic:", r, "\n", string(debug.Stack()))
		}
	}()

	timeout := c.scrapeInterval - time.Second
	if timeout <= 0 {
		timeout = time.Second
	}

	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()

	ref, err := c.connectAndPing(ctx)
	if err != nil {
		c.logger.Warning(err)
		c.lock.Lock()
		c.scrapeErrors = map[string]bool{errorReason(err): true}
		c.serverVersion = ""
		c.rsStatus = nil
		c.rsConfig = nil
		c.oplog = nil
		c.ss = nil
		c.currentOp = nil
		c.topQueries = nil
		c.lock.Unlock()
		return
	}
	defer c.releaseClient(ref)
	client := ref.client

	scrapeErrors := map[string]bool{}
	scrapeError := func(what string, err error) {
		c.logger.Warning(what+":", err)
		scrapeErrors[what+": "+errorReason(err)] = true
	}

	bi, err := c.collectBuildInfo(ctx, client)
	if err != nil {
		scrapeError("buildInfo", err)
	}

	ss, err := c.collectServerStatus(ctx, client)
	if err != nil {
		scrapeError("serverStatus", err)
	}

	currentOp, err := c.collectCurrentOp(ctx, client)
	if err != nil {
		scrapeError("$currentOp", err)
	}

	profilingLevels, topQueries, profilerErr := c.collectProfiler(ctx, client)
	if profilerErr != nil {
		scrapeError("profiler", profilerErr)
	}

	rsStatus := c.rsStatus // only the snapshot goroutine writes it, so reading without the lock is safe
	if rs, err := c.collectReplStatus(ctx, client); err != nil {
		scrapeError("replSetGetStatus", err)
	} else {
		rsStatus = rs
	}

	rsConfig := c.rsConfig
	if cfg, err := c.collectReplConfig(ctx, client); err != nil {
		scrapeError("replSetGetConfig", err)
	} else {
		rsConfig = cfg
	}

	var oplog *OplogStats
	if rsStatus != nil && rsStatus.ReplicaSet != "" {
		oplog = c.oplog
		if o, err := c.collectOplog(ctx, client); err != nil {
			scrapeError("oplog", err)
		} else {
			oplog = o
		}
	}

	if c.emitter != nil {
		c.trackSettingsChanges(ctx, client)
	}
	dbSizes, tableGrowth := c.dbSizes, c.tableGrowth
	if c.dbTracker != nil {
		c.dbTracker.client = client
		c.dbTracker.Track(ctx, c.emitter, c.targetAddr)
		// Track replaces (never mutates) these after it returns, so they can be published as is.
		dbSizes, tableGrowth = c.dbTracker.DBSizes, c.dbTracker.TableGrowth
		c.dbTracker.client = nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	c.scrapeErrors = scrapeErrors
	if bi != nil {
		c.serverVersion = bi.Version
		c.isPercona = bi.PsmdbVersion != ""
	}
	c.ss = ss
	c.ssCounters = c.ssTotals.clone()
	c.journalBytesAtCheckpoint = c.ckptJournalBytes
	c.lastCheckpointAt = c.ckptAt
	c.currentOp = currentOp
	if profilerErr != nil {
		c.topQueries = nil
	} else {
		c.profilingLevels = profilingLevels
		c.topQueries = topQueries
	}
	c.rsStatus = rsStatus
	c.rsConfig = rsConfig
	c.oplog = oplog
	c.dbSizes = dbSizes
	c.tableGrowth = tableGrowth
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancelFunc := context.WithTimeout(c.ctx, c.collectTimeout)
	defer cancelFunc()

	ref, err := c.connectAndPing(ctx)
	if err != nil {
		c.logger.Warning(err)
		ch <- common.Gauge(dUp, 0)
		ch <- common.Gauge(dScrapeError, 1, errorReason(err), "")
		return
	}
	c.releaseClient(ref)
	ch <- common.Gauge(dUp, 1)

	c.lock.RLock()
	defer c.lock.RUnlock()

	if c.serverVersion != "" {
		flavor := "mongodb"
		if c.isPercona {
			flavor = "percona"
		}
		ch <- common.Gauge(dInfo, 1, c.serverVersion, flavor)
	}

	c.replicationMetrics(ch)
	c.serverStatusMetrics(ch)
	c.currentOpMetrics(ch)
	c.topQueriesMetrics(ch)
	c.sizesMetrics(ch)

	if len(c.scrapeErrors) > 0 {
		for e := range c.scrapeErrors {
			ch <- common.Gauge(dScrapeError, 1, "", e)
		}
	} else {
		ch <- common.Gauge(dScrapeError, 0, "", "")
	}
}

type BuildInfo struct {
	Version      string `bson:"version"`
	PsmdbVersion string `bson:"psmdbVersion"`
}

func (c *Collector) collectBuildInfo(ctx context.Context, client *mongo.Client) (*BuildInfo, error) {
	res := client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: "1"}})
	var bi BuildInfo
	if err := res.Decode(&bi); err != nil {
		return nil, err
	}
	return &bi, nil
}

type Member struct {
	Name        string             `bson:"name"`
	State       string             `bson:"stateStr"`
	Self        bool               `bson:"self"`
	OptimeDate  primitive.DateTime `bson:"optimeDate"`
	LastApplied primitive.DateTime `bson:"lastAppliedWallTime"`
}

type ReplStatus struct {
	ReplicaSet string   `bson:"set"`
	Members    []Member `bson:"members"`
}

func (c *Collector) collectReplStatus(ctx context.Context, client *mongo.Client) (*ReplStatus, error) {
	res := client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: "1"}})
	var s ReplStatus
	if err := res.Decode(&s); err != nil {
		if e, ok := err.(mongo.CommandError); ok {
			switch e.Code {
			case StatusReplicationNotEnabled, StatusReplicationNotYetInitialized:
				return nil, nil
			}
		}
		return nil, err
	}
	return &s, nil
}

var settingsMetadataKeys = map[string]bool{
	"ok":            true,
	"$clusterTime":  true,
	"operationTime": true,
}

func (c *Collector) trackSettingsChanges(ctx context.Context, client *mongo.Client) {
	res := client.Database("admin").RunCommand(ctx, bson.D{{Key: "getParameter", Value: "*"}})
	var params bson.M
	if err := res.Decode(&params); err != nil {
		c.logger.Warning("getParameter:", err)
		return
	}

	keys := make([]string, 0, len(params))
	for k := range params {
		if !settingsMetadataKeys[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var buf strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&buf, "%s = %v\n", k, params[k])
	}
	curr := buf.String()

	if c.prevSettingsText != "" && curr != c.prevSettingsText {
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A:        difflib.SplitLines(c.prevSettingsText),
			B:        difflib.SplitLines(curr),
			FromFile: "parameters",
			ToFile:   "parameters",
			Context:  3,
		})
		c.emitter.Emit(schema.Change{
			Object: "parameters",
			Type:   schema.ChangeTypeChanged,
			Diff:   diff,
		}, "mongodb", c.targetAddr)
	}
	c.prevSettingsText = curr
}

func (c *Collector) sizesMetrics(ch chan<- prometheus.Metric) {
	if c.dbTracker == nil || !c.dbTracker.trackSizes {
		return
	}
	for dbName, snap := range c.dbSizes {
		ch <- common.Gauge(dDbSize, snap.DatabaseSize, dbName)
		for _, t := range snap.Tables {
			ch <- common.Gauge(dCollectionSize, t.Size, dbName, t.Table)
			ch <- common.Gauge(dCollStorageSize, t.StorageSize, dbName, t.Table)
			ch <- common.Gauge(dCollFreeStorage, t.FreeStorage, dbName, t.Table)
			ch <- common.Gauge(dCollectionDocuments, t.Documents, dbName, t.Table)
		}
	}
	for _, g := range c.tableGrowth {
		ch <- common.Gauge(dCollectionSizeGrowth, g.Growth, g.DB, g.Table)
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- dUp
	ch <- dScrapeError
	ch <- dInfo
	ch <- dRsStatus
	ch <- dRsLastApplied
	ch <- dDbSize
	ch <- dCollectionSize
	ch <- dCollectionSizeGrowth
}
