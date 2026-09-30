package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/coroot/coroot-cluster-agent/schema"

	"github.com/coroot/logger"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	picoSeconds = 1e12
	topN        = 20 // top queries / tables reported by queryMetrics and ioMetrics
)

var reVersion = regexp.MustCompile(`^(\d+)\.(\d+)`)

// state is the result of a snapshot. Once published via Collector.state it is never
// modified, so Collect can read it without holding the lock.
type state struct {
	scrapeErrors map[string]bool // error reason -> true
	isUp         bool

	globalVariables  map[string]string
	globalStatus     map[string]string
	perfschemaPrev   *statementsSummarySnapshot
	perfschemaCurr   *statementsSummarySnapshot
	activePrev       *activeStatementsSnapshot
	activeCurr       *activeStatementsSnapshot
	lockWaits        *lockWaits
	innodbTrx        *innodbTrx
	innodbCounters   *innodbCounters
	binlogStats      *binlogStats
	groupReplication *groupReplication
	replicaStatuses  []*ReplicaStatus
	ioByTablePrev    *ioByTableSnapshot
	ioByTableCurr    *ioByTableSnapshot
	waitEvents       []waitEvent
	applierLag       map[string]*applierLag

	isMariaDB          bool
	isGalera           bool
	hasUndoTablespaces bool
}

type Collector struct {
	ctx        context.Context
	db         *sql.DB
	logger     logger.Logger
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	errorLogLock sync.Mutex
	errorLog     *ErrorLogReader

	lock  sync.RWMutex // guards state
	state *state

	scrapeInterval time.Duration
	collectTimeout time.Duration

	excludeDatabases map[string]bool

	// accessed only by the snapshot goroutine
	invalidQueries    map[string]bool
	prevSettingsText  string
	writableVariables map[string]bool

	dbTracker  *databaseTracker
	emitter    dbtracker.ChangeEmitter
	targetAddr string
}

func New(dsn string, logger logger.Logger, scrapeInterval, collectTimeout time.Duration,
	emitter dbtracker.ChangeEmitter, targetAddr string, maxTablesPerDB int,
	trackSizes bool, excludeDatabases []string) (*Collector, error) {

	ctx, cancelFunc := context.WithCancel(context.Background())
	exclude := make(map[string]bool, len(excludeDatabases))
	for _, db := range excludeDatabases {
		exclude[db] = true
	}
	c := &Collector{
		ctx:            ctx,
		logger:         logger,
		cancelFunc:     cancelFunc,
		scrapeInterval: scrapeInterval,
		collectTimeout: collectTimeout,
		emitter:        emitter,
		targetAddr:     targetAddr,

		state:            &state{globalStatus: map[string]string{}, globalVariables: map[string]string{}},
		invalidQueries:   map[string]bool{},
		excludeDatabases: exclude,
	}
	var err error
	c.db, err = sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	c.db.SetMaxOpenConns(1)
	trackSchema := c.emitter != nil
	if trackSchema || trackSizes {
		// the tracker runs concurrently with the snapshot: give it its own connection
		// so that neither of them waits for the other one
		c.db.SetMaxOpenConns(2)
		c.dbTracker = newDatabaseTracker(c.db, maxTablesPerDB, trackSchema, trackSizes, excludeDatabases, logger)
	}
	pingCtx, pingCancelFunc := context.WithTimeout(ctx, collectTimeout)
	defer pingCancelFunc()
	if err := c.db.PingContext(pingCtx); err != nil {
		c.logger.Warning("probe failed:", err)
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
			case <-ctx.Done():
				c.logger.Info("stopping mysql collector")
				return
			}
		}
	}()
	if c.dbTracker != nil {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.dbTracker.Run(ctx, dbtracker.TrackMinInterval, c.emitter, c.targetAddr)
		}()
	}

	return c, nil
}

func (c *Collector) Close() error {
	c.cancelFunc()
	c.wg.Wait()
	c.errorLogLock.Lock()
	if c.errorLog != nil {
		c.errorLog.Stop()
	}
	c.errorLogLock.Unlock()
	return c.db.Close()
}

func (c *Collector) getState() *state {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.state
}

func (c *Collector) addScrapeError(st *state, err error) {
	c.logger.Warning(err)
	st.scrapeErrors[errorReason(err)] = true
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	st := c.getState()

	if !st.isUp {
		ch <- common.Gauge(dUp, 0)
		for e := range st.scrapeErrors {
			ch <- common.Gauge(dScrapeError, 1, e, "")
		}
		return
	}
	ch <- common.Gauge(dUp, 1)
	if version := st.globalVariables["version"]; version != "" {
		ch <- common.Gauge(dInfo, 1, version, st.globalVariables["server_id"], st.globalVariables["server_uuid"])
	}

	warnings := st.scrapeErrors
	if c.dbTracker != nil {
		if trackerErrors := c.dbTracker.errorReasons(); len(trackerErrors) > 0 {
			warnings = maps.Clone(warnings)
			if warnings == nil {
				warnings = map[string]bool{}
			}
			maps.Copy(warnings, trackerErrors)
		}
	}
	if len(warnings) > 0 {
		for e := range warnings {
			ch <- common.Gauge(dScrapeError, 1, "", e)
		}
	} else {
		ch <- common.Gauge(dScrapeError, 0, "", "")
	}
	st.queryMetrics(ch, topN)
	st.ioMetrics(ch, topN)
	if st.lockWaits != nil {
		for _, q := range st.lockWaits.locked {
			ch <- common.Gauge(dLockedQueries, q.count, q.schema, q.query)
		}
		for _, q := range st.lockWaits.awaiting {
			ch <- common.Gauge(dLockAwaitingQueries, q.count, q.schema, q.query)
		}
	}
	st.innodbTrxMetrics(ch)
	st.replicationMetrics(ch)
	st.applierLagMetrics(ch)
	st.waitEventsMetrics(ch)
	c.tableSizeMetrics(ch)
	c.unusedIndexMetrics(ch)
	metricFromVariable(ch, dConnectionsMax, "max_connections", prometheus.GaugeValue, st.globalVariables)
	metricFromVariable(ch, dConnectionsCurrent, "Threads_connected", prometheus.GaugeValue, st.globalStatus)
	metricFromVariable(ch, dConnectionsTotal, "Connections", prometheus.CounterValue, st.globalStatus)
	metricFromVariable(ch, dConnectionsAborted, "Aborted_connects", prometheus.CounterValue, st.globalStatus)
	metricFromVariable(ch, dConnectionErrorsMaxConnections, "Connection_errors_max_connections", prometheus.CounterValue, st.globalStatus)
	metricFromVariable(ch, dThreadsRunning, "Threads_running", prometheus.GaugeValue, st.globalStatus)
	metricFromVariable(ch, dTmpDiskTables, "Created_tmp_disk_tables", prometheus.CounterValue, st.globalStatus)
	st.galeraMetrics(ch)
	st.groupReplicationMetrics(ch)
	st.innodbMetrics(ch)
	st.innodbCountersMetrics(ch)
	st.binlogMetrics(ch)
	metricFromVariable(ch, dTableLocksWaited, "Table_locks_waited", prometheus.CounterValue, st.globalStatus)
	metricFromVariable(ch, dTableLocksImmediate, "Table_locks_immediate", prometheus.CounterValue, st.globalStatus)
	metricFromVariable(ch, dBytesReceived, "Bytes_received", prometheus.CounterValue, st.globalStatus)
	metricFromVariable(ch, dBytesSent, "Bytes_sent", prometheus.CounterValue, st.globalStatus)
	metricFromVariable(ch, dQueries, "Questions", prometheus.CounterValue, st.globalStatus)
	metricFromVariable(ch, dSlowQueries, "Slow_queries", prometheus.CounterValue, st.globalStatus)
}

// snapshot queries the server without holding the lock: the new state is built from a
// copy of the current one and published at the end, so a slow server never blocks Collect.
func (c *Collector) snapshot() {
	timeout := c.scrapeInterval - time.Second
	if timeout <= 0 {
		timeout = time.Second
	}

	ctx, cancelFunc := context.WithTimeout(c.ctx, timeout)
	defer cancelFunc()

	st := *c.getState() // shallow copy: fields are replaced, never mutated in place
	st.scrapeErrors = map[string]bool{}
	defer func() {
		c.lock.Lock()
		c.state = &st
		c.lock.Unlock()
	}()

	variables, err := c.queryVariables(ctx, "SHOW GLOBAL VARIABLES")
	if err != nil {
		c.addScrapeError(&st, err)
		st.isUp = false
		return
	}
	st.globalVariables = variables
	st.isUp = true
	st.isMariaDB = strings.Contains(strings.ToLower(st.globalVariables["version"]), "mariadb")
	st.hasUndoTablespaces = !st.isMariaDB && versionAtLeast(st.globalVariables["version"], 8, 0)
	status, err := c.queryVariables(ctx, "SHOW GLOBAL STATUS")
	if err != nil {
		c.addScrapeError(&st, err)
		return
	}
	st.globalStatus = status
	st.isGalera = false
	if _, ok := st.globalStatus["wsrep_cluster_size"]; ok {
		st.isGalera = wsrepEnabled(st.globalVariables)
	}
	if err := c.updateReplicationStatus(ctx, &st); err != nil {
		c.addScrapeError(&st, err)
		return
	}
	c.applierLagSnapshot(ctx, &st)
	c.waitEventsSnapshot(ctx, &st)
	st.perfschemaPrev = st.perfschemaCurr
	st.perfschemaCurr, err = c.queryStatementsSummary(ctx, st.perfschemaPrev)
	if err != nil {
		c.addScrapeError(&st, err)
		return
	}
	st.activePrev = st.activeCurr
	if st.activeCurr, err = c.queryActiveStatements(ctx, st.activePrev); err != nil {
		c.addScrapeError(&st, err)
		st.activeCurr = nil
	}
	st.ioByTablePrev = st.ioByTableCurr
	st.ioByTableCurr, err = c.queryTableIOWaits(ctx)
	if err != nil {
		c.addScrapeError(&st, err)
		return
	}

	c.lockWaitsSnapshot(ctx, &st)
	c.innodbTrxSnapshot(ctx, &st)
	c.innodbCountersSnapshot(ctx, &st)
	c.binlogSnapshot(ctx, &st)

	if st.globalVariables["group_replication_group_name"] != "" {
		c.groupReplicationSnapshot(ctx, &st)
	} else {
		st.groupReplication = nil
	}

	if c.emitter != nil {
		c.trackSettingsChanges(ctx, &st)
	}
}

func (c *Collector) loadWritableVariableNames(ctx context.Context, isMariaDB bool) error {
	var query string
	if isMariaDB {
		query = "SELECT VARIABLE_NAME FROM information_schema.SYSTEM_VARIABLES WHERE READ_ONLY = 'NO'"
	} else {
		query = "SELECT VARIABLE_NAME FROM performance_schema.variables_info WHERE VARIABLE_SOURCE != 'COMPILED'"
	}
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	c.writableVariables = map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			c.logger.Warning(err)
			continue
		}
		c.writableVariables[name] = true
	}
	return nil
}

func (c *Collector) trackSettingsChanges(ctx context.Context, st *state) {
	if err := c.loadWritableVariableNames(ctx, st.isMariaDB); err != nil {
		c.logger.Warning("failed to load writable variable names:", err)
		return
	}

	names := make([]string, 0, len(c.writableVariables))
	for name := range c.writableVariables {
		names = append(names, name)
	}
	curr := variablesToText(names, st.globalVariables)
	if c.prevSettingsText != "" && curr != c.prevSettingsText {
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A:        difflib.SplitLines(c.prevSettingsText),
			B:        difflib.SplitLines(curr),
			FromFile: "global_variables",
			ToFile:   "global_variables",
			Context:  3,
		})
		c.emitter.Emit(schema.Change{
			Object: "global_variables",
			Type:   schema.ChangeTypeChanged,
			Diff:   diff,
		}, "mysql", c.targetAddr)
	}
	c.prevSettingsText = curr
}

// variablesToText renders the given variables in a stable order, redacting values
// of the variables that may contain secrets.
func variablesToText(names []string, values map[string]string) string {
	names = slices.Clone(names)
	sort.Strings(names)
	var buf strings.Builder
	for _, name := range names {
		v := values[name]
		if v != "" && dbtracker.IsSensitiveSettingName(name) {
			v = dbtracker.RedactedValue
		}
		fmt.Fprintf(&buf, "%s = %s\n", name, v)
	}
	return buf.String()
}

func (c *Collector) tableSizeMetrics(ch chan<- prometheus.Metric) {
	if c.dbTracker == nil || !c.dbTracker.trackSizes {
		return
	}
	dbSizes, tableGrowth := c.dbTracker.Sizes()
	for dbName, snap := range dbSizes {
		ch <- common.Gauge(dDbSize, snap.DatabaseSize, dbName)
		for _, t := range snap.Tables {
			ch <- common.Gauge(dTableSize, t.Size, dbName, t.Table)
		}
	}
	for _, g := range tableGrowth {
		ch <- common.Gauge(dTableSizeGrowth, g.Growth, g.DB, g.Table)
	}
}

func (c *Collector) unusedIndexMetrics(ch chan<- prometheus.Metric) {
	if c.dbTracker == nil {
		return
	}
	u := c.dbTracker.indexResults()
	if u == nil {
		return
	}
	for schemaName, count := range u.bySchema {
		ch <- common.Gauge(dSchemaUnusedIndexes, count, schemaName)
	}
	for _, ix := range u.top {
		ch <- common.Gauge(dIndexUnused, 1, ix.schema, ix.table, ix.index)
		if ix.size.Valid {
			ch <- common.Gauge(dIndexUnusedBytes, ix.size.Float64, ix.schema, ix.table, ix.index)
		}
	}
}

func versionAtLeast(version string, major, minor int) bool {
	m := reVersion.FindStringSubmatch(version)
	if m == nil {
		return false
	}
	maj, err := strconv.Atoi(m[1])
	if err != nil {
		return false
	}
	min, err := strconv.Atoi(m[2])
	if err != nil {
		return false
	}
	return maj > major || (maj == major && min >= minor)
}

func metricFromVariable(ch chan<- prometheus.Metric, desc *prometheus.Desc, name string, typ prometheus.ValueType, variables map[string]string, convert ...func(float64) float64) bool {
	v, ok := variables[name]
	if !ok {
		return false
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return false
	}
	for _, c := range convert {
		if c != nil {
			f = c(f)
		}
	}
	ch <- prometheus.MustNewConstMetric(desc, typ, f)
	return true
}
