package mongo

import (
	"context"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/obfuscate"
	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	longRunningOpSeconds = 10
	topLongRunningOpsN   = 20
	topClientAppsN       = 20
)

type currentOpDoc struct {
	Type           string   `bson:"type"`
	Active         bool     `bson:"active"`
	SecsRunning    int64    `bson:"secs_running"`
	Op             string   `bson:"op"`
	Ns             string   `bson:"ns"`
	Command        bson.D   `bson:"command"`
	AppName        string   `bson:"appName"`
	WaitingForLock bool     `bson:"waitingForLock"`
	PlanSummary    string   `bson:"planSummary"`
	Transaction    bson.Raw `bson:"transaction"`
	Desc           string   `bson:"desc"`
}

type longOpKey struct {
	DB         string
	Collection string
	Query      string
	Plan       string
}

type CurrentOpStats struct {
	WaitingForLock        map[string]int
	ConnectionsByApp      map[string]int
	LongRunning           map[longOpKey]int
	OpenTransactionsByApp map[string]int
	FsyncLocked           bool
}

func (c *Collector) collectCurrentOp(ctx context.Context, client *mongo.Client) (*CurrentOpStats, error) {
	cursor, err := client.Database("admin").Aggregate(ctx, bson.A{
		bson.D{{Key: "$currentOp", Value: bson.D{
			{Key: "allUsers", Value: true},
			{Key: "idleConnections", Value: true},
			{Key: "idleSessions", Value: true},
		}}},
	})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	stats := &CurrentOpStats{
		WaitingForLock:        map[string]int{},
		ConnectionsByApp:      map[string]int{},
		LongRunning:           map[longOpKey]int{},
		OpenTransactionsByApp: map[string]int{},
	}
	for cursor.Next(ctx) {
		var op currentOpDoc
		if err = cursor.Decode(&op); err != nil {
			return nil, err
		}
		stats.ConnectionsByApp[normalizeAppName(op.AppName)]++
		if len(op.Transaction) > 0 {
			stats.OpenTransactionsByApp[normalizeAppName(op.AppName)]++
		}
		if op.Desc == "fsyncLockWorker" {
			stats.FsyncLocked = true
		}
		if op.Type != "op" || !op.Active || op.Op == "none" {
			continue
		}
		db := opDatabase(op.Ns)
		if op.WaitingForLock {
			stats.WaitingForLock[db]++
		}
		if isSystemOp(&op) {
			continue
		}
		if op.SecsRunning >= longRunningOpSeconds && len(op.Command) > 0 {
			coll := ""
			if i := strings.IndexByte(op.Ns, '.'); i >= 0 {
				coll = op.Ns[i+1:]
			}
			plan := op.PlanSummary
			plan = truncateUTF8(plan, 64) // IXSCAN plans embed the index spec
			stats.LongRunning[longOpKey{
				DB:         db,
				Collection: coll,
				Query:      obfuscate.MongoQueryShape(op.Command),
				Plan:       plan,
			}]++
		}
	}
	if err = cursor.Err(); err != nil {
		return nil, err
	}

	stats.LongRunning = common.TopNMapByValue(stats.LongRunning, topLongRunningOpsN)
	stats.ConnectionsByApp = common.TopNMapByValue(stats.ConnectionsByApp, topClientAppsN)
	stats.OpenTransactionsByApp = common.TopNMapByValue(stats.OpenTransactionsByApp, topClientAppsN)

	return stats, nil
}

var (
	reUUIDish      = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}(?:-[0-9a-fA-F]{1,12})?`)
	reK8sPodSuffix = regexp.MustCompile(`-[bcdfghjklmnpqrstvwxz2456789]{5,10}-[bcdfghjklmnpqrstvwxz2456789]{5}$`)
	reHexBlob      = regexp.MustCompile(`\b[0-9a-fA-F]{16,}\b`)
)

func normalizeAppName(app string) string {
	app = reUUIDish.ReplaceAllString(app, "")
	app = reK8sPodSuffix.ReplaceAllString(app, "")
	app = reHexBlob.ReplaceAllString(app, "")
	app = strings.Trim(app, "-_.")
	if app == "" {
		return "unknown"
	}
	return truncateUTF8(app, 128)
}

func opDatabase(ns string) string {
	if i := strings.IndexByte(ns, '.'); i >= 0 {
		return ns[:i]
	}
	return ns
}

func isSystemOp(op *currentOpDoc) bool {
	switch opDatabase(op.Ns) {
	case "admin", "local", "config", "":
		return true
	}
	return false
}

func (c *Collector) currentOpMetrics(ch chan<- prometheus.Metric) {
	if c.currentOp == nil {
		return
	}
	for db, n := range c.currentOp.WaitingForLock {
		ch <- common.Gauge(dOpsWaitingLock, float64(n), db)
	}
	for app, n := range c.currentOp.ConnectionsByApp {
		ch <- common.Gauge(dConnectionsByApp, float64(n), app)
	}
	for app, n := range c.currentOp.OpenTransactionsByApp {
		ch <- common.Gauge(dOpenTransactionsByApp, float64(n), app)
	}
	for k, n := range c.currentOp.LongRunning {
		ch <- common.Gauge(dLongRunningOps, float64(n), k.DB, k.Collection, k.Query, k.Plan)
	}
	fsync := 0.0
	if c.currentOp.FsyncLocked {
		fsync = 1
	}
	ch <- common.Gauge(dFsyncLocked, fsync)
}

// truncateUTF8 cuts s to at most n bytes without splitting a multi-byte character
// (an invalid UTF-8 label value makes the metric fail to be created).
func truncateUTF8(s string, n int) string {
	s = strings.ToValidUTF8(s, "?")
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
