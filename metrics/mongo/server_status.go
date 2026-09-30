package mongo

import (
	"context"
	"maps"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type ticketStats struct {
	Out          int64 `bson:"out"`
	Available    int64 `bson:"available"`
	TotalTickets int64 `bson:"totalTickets"`
	QueueLength  int64 `bson:"queueLength"`
}

type opLatencyStats struct {
	LatencyMicros int64 `bson:"latency"`
	Ops           int64 `bson:"ops"`
}

type serverStatus struct {
	Connections struct {
		Current      int64 `bson:"current"`
		Available    int64 `bson:"available"`
		Active       int64 `bson:"active"`
		TotalCreated int64 `bson:"totalCreated"`
		Rejected     int64 `bson:"rejected"`
	} `bson:"connections"`
	Opcounters struct {
		Insert  int64 `bson:"insert"`
		Query   int64 `bson:"query"`
		Update  int64 `bson:"update"`
		Delete  int64 `bson:"delete"`
		Getmore int64 `bson:"getmore"`
		Command int64 `bson:"command"`
	} `bson:"opcounters"`
	GlobalLock struct {
		CurrentQueue struct {
			Readers int64 `bson:"readers"`
			Writers int64 `bson:"writers"`
		} `bson:"currentQueue"`
	} `bson:"globalLock"`
	// MongoDB 7.0+: execution control queues replace wiredTiger.concurrentTransactions
	Queues *struct {
		Execution struct {
			Read  ticketStats `bson:"read"`
			Write ticketStats `bson:"write"`
		} `bson:"execution"`
	} `bson:"queues"`
	WiredTiger *struct {
		Cache struct {
			BytesInCache float64 `bson:"bytes currently in the cache"`
			MaxBytes     float64 `bson:"maximum bytes configured"`
			DirtyBytes   float64 `bson:"tracked dirty bytes in the cache"`
			// pre-8.0
			PagesEvictedByAppThreads float64 `bson:"pages evicted by application threads"`
			// 8.0+
			PageEvictAttemptsByAppThreads         float64 `bson:"page evict attempts by application threads"`
			ModifiedPageEvictAttemptsByAppThreads float64 `bson:"modified page evict attempts by application threads"`
			AppThreadTimeEvictingUs               float64 `bson:"application thread time evicting (usecs)"`
			BytesReadInto                         float64 `bson:"bytes read into cache"`
		} `bson:"cache"`
		// pre-7.0; replaced by queues.execution
		ConcurrentTransactions *struct {
			Read  ticketStats `bson:"read"`
			Write ticketStats `bson:"write"`
		} `bson:"concurrentTransactions"`
		// pre-8.0 location of checkpoint stats
		Transaction struct {
			Checkpoints           float64 `bson:"transaction checkpoints"`
			CheckpointTotalTimeMs float64 `bson:"transaction checkpoint total time (msecs)"`
		} `bson:"transaction"`
		// 8.0+
		Checkpoint *struct {
			Checkpoints float64 `bson:"checkpoints"`
			TotalTimeMs float64 `bson:"total time (msecs)"`
		} `bson:"checkpoint"`
		Log struct {
			BytesWritten float64 `bson:"log bytes written"`
		} `bson:"log"`
	} `bson:"wiredTiger"`
	OpLatencies *struct {
		Reads    opLatencyStats `bson:"reads"`
		Writes   opLatencyStats `bson:"writes"`
		Commands opLatencyStats `bson:"commands"`
	} `bson:"opLatencies"`
	Metrics struct {
		Document struct {
			Returned int64 `bson:"returned"`
		} `bson:"document"`
		QueryExecutor struct {
			Scanned         int64 `bson:"scanned"`
			ScannedObjects  int64 `bson:"scannedObjects"`
			CollectionScans struct {
				Total int64 `bson:"total"`
			} `bson:"collectionScans"`
		} `bson:"queryExecutor"`
		Operation struct {
			ScanAndOrder   int64 `bson:"scanAndOrder"`
			WriteConflicts int64 `bson:"writeConflicts"`
		} `bson:"operation"`
		TTL struct {
			DeletedDocuments int64 `bson:"deletedDocuments"`
		} `bson:"ttl"`
		Repl struct {
			Apply struct {
				Ops int64 `bson:"ops"`
			} `bson:"apply"`
			Buffer struct {
				Count     int64 `bson:"count"`
				SizeBytes int64 `bson:"sizeBytes"`
			} `bson:"buffer"`
		} `bson:"repl"`
		Cursor struct {
			TimedOut int64 `bson:"timedOut"`
			Open     struct {
				Total     int64 `bson:"total"`
				NoTimeout int64 `bson:"noTimeout"`
			} `bson:"open"`
		} `bson:"cursor"`
	} `bson:"metrics"`
	FlowControl *struct {
		TimeAcquiringMicros int64 `bson:"timeAcquiringMicros"`
	} `bson:"flowControl"`
	Transactions *struct {
		CurrentPrepared int64 `bson:"currentPrepared"`
	} `bson:"transactions"`
}

func (s *serverStatus) preparedTransactions() float64 {
	if s.Transactions == nil {
		return 0
	}
	return float64(s.Transactions.CurrentPrepared)
}

func (ss *serverStatus) tickets() *struct {
	Read  ticketStats `bson:"read"`
	Write ticketStats `bson:"write"`
} {
	// 8.0+ exposes queues.execution; 7.0 and earlier only wiredTiger.concurrentTransactions.
	// On 7.0 queues.execution may be present but empty, so pick whichever is populated.
	if ss.Queues != nil && (ss.Queues.Execution.Read.TotalTickets > 0 || ss.Queues.Execution.Write.TotalTickets > 0) {
		return &ss.Queues.Execution
	}
	if ss.WiredTiger != nil && ss.WiredTiger.ConcurrentTransactions != nil {
		return ss.WiredTiger.ConcurrentTransactions
	}
	if ss.Queues != nil {
		return &ss.Queues.Execution
	}
	return nil
}

func (s *serverStatus) appEvictedPages() float64 {
	if s.WiredTiger == nil {
		return 0
	}
	c := s.WiredTiger.Cache
	return c.PagesEvictedByAppThreads + c.PageEvictAttemptsByAppThreads + c.ModifiedPageEvictAttemptsByAppThreads
}

func (s *serverStatus) appEvictSeconds() float64 {
	if s.WiredTiger == nil {
		return 0
	}
	return s.WiredTiger.Cache.AppThreadTimeEvictingUs / 1e6
}

func (s *serverStatus) checkpointCount() float64 {
	if s.WiredTiger == nil {
		return 0
	}
	if s.WiredTiger.Checkpoint != nil { // 8.0+
		return s.WiredTiger.Checkpoint.Checkpoints
	}
	return s.WiredTiger.Transaction.Checkpoints
}

func (s *serverStatus) checkpointSeconds() float64 {
	if s.WiredTiger == nil {
		return 0
	}
	if s.WiredTiger.Checkpoint != nil { // 8.0+
		return s.WiredTiger.Checkpoint.TotalTimeMs / 1000
	}
	return s.WiredTiger.Transaction.CheckpointTotalTimeMs / 1000
}

func (s *serverStatus) journalBytes() float64 {
	if s.WiredTiger == nil {
		return 0
	}
	return s.WiredTiger.Log.BytesWritten
}

func (s *serverStatus) cacheBytesReadInto() float64 {
	if s.WiredTiger == nil {
		return 0
	}
	return s.WiredTiger.Cache.BytesReadInto
}

func (s *serverStatus) flowControlSeconds() float64 {
	if s.FlowControl == nil {
		return 0
	}
	return float64(s.FlowControl.TimeAcquiringMicros) / 1e6
}

type serverStatusCounters struct {
	opcounters          map[string]float64
	documentsReturned   float64
	opLatencySeconds    map[string]float64
	opLatencyOps        map[string]float64
	scannedKeys         float64
	scannedObjects      float64
	collectionScans     float64
	scanAndOrder        float64
	ttlDeleted          float64
	connectionsCreated  float64
	connectionsRejected float64
	evictedPagesApp     float64
	appEvictSeconds     float64
	flowControlSeconds  float64
	checkpoints         float64
	checkpointSeconds   float64
	journalBytesWritten float64
	writeConflicts      float64
	cacheBytesReadInto  float64
	replApplyOps        float64
	cursorTimedOut      float64
}

func newServerStatusCounters() serverStatusCounters {
	return serverStatusCounters{
		opcounters:       map[string]float64{},
		opLatencySeconds: map[string]float64{},
		opLatencyOps:     map[string]float64{},
	}
}

func (s serverStatusCounters) clone() serverStatusCounters {
	s.opcounters = maps.Clone(s.opcounters)
	s.opLatencySeconds = maps.Clone(s.opLatencySeconds)
	s.opLatencyOps = maps.Clone(s.opLatencyOps)
	return s
}

// collectServerStatus accumulates the counters into c.ssTotals, which is private to the snapshot goroutine.
func (c *Collector) collectServerStatus(ctx context.Context, client *mongo.Client) (*serverStatus, error) {
	res := client.Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}})
	ss := &serverStatus{}
	if err := res.Decode(ss); err != nil {
		return nil, err
	}

	prev := c.ssPrev
	c.ssPrev = ss
	if prev == nil {
		return ss, nil
	}

	cnt := &c.ssTotals

	for _, m := range []struct {
		dst *float64
		get func(*serverStatus) float64
	}{
		{&cnt.documentsReturned, func(s *serverStatus) float64 { return float64(s.Metrics.Document.Returned) }},
		{&cnt.scannedKeys, func(s *serverStatus) float64 { return float64(s.Metrics.QueryExecutor.Scanned) }},
		{&cnt.scannedObjects, func(s *serverStatus) float64 { return float64(s.Metrics.QueryExecutor.ScannedObjects) }},
		{&cnt.collectionScans, func(s *serverStatus) float64 { return float64(s.Metrics.QueryExecutor.CollectionScans.Total) }},
		{&cnt.scanAndOrder, func(s *serverStatus) float64 { return float64(s.Metrics.Operation.ScanAndOrder) }},
		{&cnt.ttlDeleted, func(s *serverStatus) float64 { return float64(s.Metrics.TTL.DeletedDocuments) }},
		{&cnt.connectionsCreated, func(s *serverStatus) float64 { return float64(s.Connections.TotalCreated) }},
		{&cnt.connectionsRejected, func(s *serverStatus) float64 { return float64(s.Connections.Rejected) }},
		{&cnt.evictedPagesApp, (*serverStatus).appEvictedPages},
		{&cnt.appEvictSeconds, (*serverStatus).appEvictSeconds},
		{&cnt.checkpoints, (*serverStatus).checkpointCount},
		{&cnt.checkpointSeconds, (*serverStatus).checkpointSeconds},
		{&cnt.journalBytesWritten, (*serverStatus).journalBytes},
		{&cnt.flowControlSeconds, (*serverStatus).flowControlSeconds},
		{&cnt.cacheBytesReadInto, (*serverStatus).cacheBytesReadInto},
		{&cnt.writeConflicts, func(s *serverStatus) float64 { return float64(s.Metrics.Operation.WriteConflicts) }},
		{&cnt.replApplyOps, func(s *serverStatus) float64 { return float64(s.Metrics.Repl.Apply.Ops) }},
		{&cnt.cursorTimedOut, func(s *serverStatus) float64 { return float64(s.Metrics.Cursor.TimedOut) }},
	} {
		*m.dst += delta(m.get(prev), m.get(ss))
	}

	for op, get := range map[string]func(*serverStatus) int64{
		"insert":  func(s *serverStatus) int64 { return s.Opcounters.Insert },
		"query":   func(s *serverStatus) int64 { return s.Opcounters.Query },
		"update":  func(s *serverStatus) int64 { return s.Opcounters.Update },
		"delete":  func(s *serverStatus) int64 { return s.Opcounters.Delete },
		"getmore": func(s *serverStatus) int64 { return s.Opcounters.Getmore },
		"command": func(s *serverStatus) int64 { return s.Opcounters.Command },
	} {
		cnt.opcounters[op] += delta(get(prev), get(ss))
	}

	if prev.OpLatencies != nil && ss.OpLatencies != nil {
		for typ, l := range map[string][2]opLatencyStats{
			"read":    {prev.OpLatencies.Reads, ss.OpLatencies.Reads},
			"write":   {prev.OpLatencies.Writes, ss.OpLatencies.Writes},
			"command": {prev.OpLatencies.Commands, ss.OpLatencies.Commands},
		} {
			cnt.opLatencySeconds[typ] += delta(l[0].LatencyMicros, l[1].LatencyMicros) / 1e6
			cnt.opLatencyOps[typ] += delta(l[0].Ops, l[1].Ops)
		}
	}

	if c.ckptAt.IsZero() || ss.checkpointCount() > prev.checkpointCount() {
		c.ckptJournalBytes = ss.journalBytes()
		c.ckptAt = time.Now()
	}
	return ss, nil
}

func delta[T int64 | float64](prev, cur T) float64 {
	if cur >= prev {
		return float64(cur - prev)
	}
	return 0
}

func (c *Collector) serverStatusMetrics(ch chan<- prometheus.Metric) {
	ss := c.ss
	if ss == nil {
		return
	}
	cnt := &c.ssCounters

	ch <- common.Gauge(dConnectionsCurrent, float64(ss.Connections.Current))
	ch <- common.Gauge(dConnectionsActive, float64(ss.Connections.Active))
	ch <- common.Gauge(dConnectionsMax, float64(ss.Connections.Current+ss.Connections.Available))
	ch <- common.Counter(dConnectionsCreated, cnt.connectionsCreated)

	for op, v := range cnt.opcounters {
		ch <- common.Counter(dOpcounters, v, op)
	}
	ch <- common.Counter(dDocumentsReturned, cnt.documentsReturned)
	for typ, v := range cnt.opLatencySeconds {
		ch <- common.Counter(dOpLatencyTotal, v, typ)
	}
	for typ, v := range cnt.opLatencyOps {
		ch <- common.Counter(dOpLatencyOps, v, typ)
	}

	qr, qw := float64(ss.GlobalLock.CurrentQueue.Readers), float64(ss.GlobalLock.CurrentQueue.Writers)
	if t := ss.tickets(); t != nil {
		ch <- common.Gauge(dTicketsAvailable, float64(t.Read.Available), "read")
		ch <- common.Gauge(dTicketsAvailable, float64(t.Write.Available), "write")
		qr = max(qr, float64(t.Read.QueueLength))
		qw = max(qw, float64(t.Write.QueueLength))
	}
	ch <- common.Gauge(dQueuedOps, qr, "read")
	ch <- common.Gauge(dQueuedOps, qw, "write")

	if wt := ss.WiredTiger; wt != nil {
		ch <- common.Gauge(dWtCacheUsed, wt.Cache.BytesInCache)
		ch <- common.Gauge(dWtCacheDirty, wt.Cache.DirtyBytes)
		ch <- common.Gauge(dWtCacheMaxBytes, wt.Cache.MaxBytes)
		ch <- common.Counter(dWtEvictedApp, cnt.evictedPagesApp)
		ch <- common.Counter(dWtAppEvictTime, cnt.appEvictSeconds)
		ch <- common.Counter(dWtCheckpoints, cnt.checkpoints)
		ch <- common.Counter(dWtCheckpointTime, cnt.checkpointSeconds)
		ch <- common.Counter(dWtJournalBytes, cnt.journalBytesWritten)
		ch <- common.Counter(dWtCacheReadInto, cnt.cacheBytesReadInto)
		if !c.lastCheckpointAt.IsZero() {
			ch <- common.Gauge(dWtJournalSinceCkpt, wt.Log.BytesWritten-c.journalBytesAtCheckpoint)
			ch <- common.Gauge(dTimeSinceCkpt, time.Since(c.lastCheckpointAt).Seconds())
		}
	}

	ch <- common.Counter(dScannedKeys, cnt.scannedKeys)
	ch <- common.Counter(dScannedObjects, cnt.scannedObjects)
	ch <- common.Counter(dCollectionScans, cnt.collectionScans)
	ch <- common.Counter(dScanAndOrder, cnt.scanAndOrder)
	ch <- common.Counter(dTtlDeleted, cnt.ttlDeleted)
	ch <- common.Counter(dConnectionsRejected, cnt.connectionsRejected)
	ch <- common.Counter(dWriteConflicts, cnt.writeConflicts)

	if ss.FlowControl != nil {
		ch <- common.Counter(dFlowControlTime, cnt.flowControlSeconds)
	}

	ch <- common.Counter(dReplApplyOps, cnt.replApplyOps)
	ch <- common.Gauge(dReplBufferCount, float64(ss.Metrics.Repl.Buffer.Count))
	ch <- common.Gauge(dReplBufferBytes, float64(ss.Metrics.Repl.Buffer.SizeBytes))

	ch <- common.Gauge(dCursorsOpen, float64(ss.Metrics.Cursor.Open.Total))
	ch <- common.Gauge(dCursorsNoTimeout, float64(ss.Metrics.Cursor.Open.NoTimeout))
	ch <- common.Counter(dCursorsTimedOut, cnt.cursorTimedOut)

	ch <- common.Gauge(dPreparedTransactions, ss.preparedTransactions())
}
