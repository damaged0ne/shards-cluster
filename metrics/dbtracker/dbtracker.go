package dbtracker

import (
	"context"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/schema"
	"github.com/coroot/logger"
)

const (
	TrackMinInterval = 60 * time.Second
	TopTablesN       = 20
)

type ChangeEmitter interface {
	Emit(change schema.Change, dbSystem, targetAddr string)
}

type TableSizeEntry struct {
	schema.TableKey
	Size        float64
	StorageSize float64
	FreeStorage float64
	Documents   float64
}

type TableGrowthEntry struct {
	schema.TableKey
	Growth float64
}

type DBSizeSnapshot struct {
	DatabaseSize float64
	Tables       []TableSizeEntry
}

type CollectFunc func(ctx context.Context) (schema.Snapshot, map[string]*DBSizeSnapshot, error)

type Tracker struct {
	dbSystem    string
	trackSchema bool
	trackSizes  bool
	logger      logger.Logger
	collect     CollectFunc

	prev           schema.Snapshot
	lastTracked    time.Time
	prevTableSizes map[schema.TableKey]float64

	// DBSizes and TableGrowth are written by Track under mu. Callers that read them
	// concurrently with Track must use Sizes().
	mu          sync.RWMutex
	DBSizes     map[string]*DBSizeSnapshot
	TableGrowth []TableGrowthEntry
}

func NewTracker(dbSystem string, trackSchema, trackSizes bool, collect CollectFunc, logger logger.Logger) *Tracker {
	return &Tracker{
		dbSystem:    dbSystem,
		trackSchema: trackSchema,
		trackSizes:  trackSizes,
		collect:     collect,
		logger:      logger,
	}
}

func (t *Tracker) Track(ctx context.Context, emitter ChangeEmitter, targetAddr string) {
	if time.Since(t.lastTracked) < TrackMinInterval {
		return
	}
	prevTracked := t.lastTracked
	t.lastTracked = time.Now()

	curr, dbSizes, err := t.collect(ctx)
	if err != nil {
		t.logger.Warning("database tracking:", err)
		return
	}

	growth := t.TableGrowth
	if t.trackSizes {
		growth = t.computeTableGrowth(dbSizes, t.lastTracked.Sub(prevTracked))
		trimTopTables(dbSizes, TopTablesN)
	}
	t.mu.Lock()
	t.DBSizes = dbSizes
	t.TableGrowth = growth
	t.mu.Unlock()

	if t.trackSchema {
		for _, c := range schema.Diff(t.prev, curr) {
			emitter.Emit(c, t.dbSystem, targetAddr)
		}
		t.prev = curr
	}
}

// Sizes returns the latest published database sizes and table growth rates.
// The returned values must not be modified.
func (t *Tracker) Sizes() (map[string]*DBSizeSnapshot, []TableGrowthEntry) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.DBSizes, t.TableGrowth
}

// Run calls Track immediately and then every TrackMinInterval until ctx is canceled.
// Each Track call gets its own deadline of timeout (no deadline if timeout <= 0),
// so a slow tracking run never shares a deadline with the collector's main snapshot.
func (t *Tracker) Run(ctx context.Context, timeout time.Duration, emitter ChangeEmitter, targetAddr string) {
	track := func() {
		tctx, cancel := ctx, context.CancelFunc(func() {})
		if timeout > 0 {
			tctx, cancel = context.WithTimeout(ctx, timeout)
		}
		defer cancel()
		t.Track(tctx, emitter, targetAddr)
	}
	ticker := time.NewTicker(TrackMinInterval)
	defer ticker.Stop()
	track()
	for {
		select {
		case <-ticker.C:
			track()
		case <-ctx.Done():
			return
		}
	}
}

func (t *Tracker) computeTableGrowth(dbSizes map[string]*DBSizeSnapshot, elapsed time.Duration) []TableGrowthEntry {
	currSizes := map[schema.TableKey]float64{}
	for _, snap := range dbSizes {
		for _, te := range snap.Tables {
			currSizes[te.TableKey] = te.Size
		}
	}

	var res []TableGrowthEntry
	if t.prevTableSizes != nil && elapsed > 0 {
		var all []TableGrowthEntry
		for key, currSize := range currSizes {
			if prevSize, ok := t.prevTableSizes[key]; ok {
				growth := (currSize - prevSize) / elapsed.Seconds()
				if growth > 0 {
					all = append(all, TableGrowthEntry{TableKey: key, Growth: growth})
				}
			}
		}
		res = common.TopN(all, TopTablesN, func(a, b TableGrowthEntry) bool { return a.Growth > b.Growth })
	}
	t.prevTableSizes = currSizes
	return res
}

func trimTopTables(dbSizes map[string]*DBSizeSnapshot, n int) {
	var all []TableSizeEntry
	for _, snap := range dbSizes {
		all = append(all, snap.Tables...)
	}
	all = common.TopN(all, n, func(a, b TableSizeEntry) bool { return a.Size > b.Size })
	byDB := make(map[string][]TableSizeEntry, len(dbSizes))
	for _, te := range all {
		byDB[te.DB] = append(byDB[te.DB], te)
	}
	for db, snap := range dbSizes {
		snap.Tables = byDB[db]
	}
}
