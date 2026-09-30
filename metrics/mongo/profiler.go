package mongo

import (
	"context"
	"errors"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/obfuscate"
	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type profileDoc struct {
	Ts                 time.Time `bson:"ts"`
	Op                 string    `bson:"op"`
	Ns                 string    `bson:"ns"`
	Command            bson.D    `bson:"command"`
	OriginatingCommand bson.D    `bson:"originatingCommand"`
	Millis             int64     `bson:"millis"`
	DocsExamined       int64     `bson:"docsExamined"`
	KeysExamined       int64     `bson:"keysExamined"`
	Nreturned          int64     `bson:"nreturned"`
}

const topQueriesN = 20

type TopQuery struct {
	DB         string
	Collection string
	Query      string

	CallsPerSecond        float64
	TimePerSecond         float64
	DocsReturnedPerSecond float64
	DocsExaminedPerSecond float64
	KeysExaminedPerSecond float64
}

type profilerShapeKey struct {
	db, collection, query string
}

type profilerAgg struct {
	calls, timeSeconds, docsReturned, docsExamined, keysExamined float64
}

func (c *Collector) collectProfiler(ctx context.Context, client *mongo.Client) (map[string]int64, []TopQuery, error) {
	var listResult struct {
		Databases []struct {
			Name string `bson:"name"`
		} `bson:"databases"`
	}
	res := client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "listDatabases", Value: 1},
		{Key: "nameOnly", Value: true},
	})
	if err := res.Decode(&listResult); err != nil {
		return nil, nil, err
	}

	now := time.Now()
	elapsed := now.Sub(c.profilerPrevAt).Seconds()
	firstRun := c.profilerPrevAt.IsZero()
	c.profilerPrevAt = now

	shapes := map[profilerShapeKey]*profilerAgg{}
	levels := map[string]int64{}
	for _, db := range listResult.Databases {
		if mongoSystemDBs[db.Name] {
			continue
		}
		var status struct {
			Was int64 `bson:"was"`
		}
		if err := client.Database(db.Name).RunCommand(ctx, bson.D{{Key: "profile", Value: -1}}).Decode(&status); err != nil {
			c.logger.Warning("profile status for", db.Name+":", err)
		} else {
			levels[db.Name] = status.Was
		}

		coll := client.Database(db.Name).Collection("system.profile")

		lastTs, seen := c.profilerLastTs[db.Name]
		if !seen || firstRun {
			var newest struct {
				Ts time.Time `bson:"ts"`
			}
			err := coll.FindOne(ctx, bson.D{},
				options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}}).SetProjection(bson.D{{Key: "ts", Value: 1}}),
			).Decode(&newest)
			switch {
			case err == nil:
				c.profilerLastTs[db.Name] = newest.Ts
			case errors.Is(err, mongo.ErrNoDocuments):
				c.profilerLastTs[db.Name] = time.Time{} // empty profile: read everything next cycle
			case isNamespaceNotFound(err):
				// profiling not enabled for this database
			default:
				c.logger.Warning("profile watermark for", db.Name+":", err)
			}
			continue
		}
		cursor, err := coll.Find(ctx,
			bson.D{{Key: "ts", Value: bson.D{{Key: "$gt", Value: primitive.NewDateTimeFromTime(lastTs)}}}},
			options.Find().SetSort(bson.D{{Key: "$natural", Value: 1}}),
		)
		if err != nil {
			if isNamespaceNotFound(err) {
				continue // profiling is not enabled for this database
			}
			return nil, nil, err
		}
		maxTs := lastTs
		for cursor.Next(ctx) {
			var doc profileDoc
			if err = cursor.Decode(&doc); err != nil {
				cursor.Close(ctx)
				return nil, nil, err
			}
			if doc.Ts.After(maxTs) {
				maxTs = doc.Ts
			}
			cmd := doc.Command
			if doc.Op == "getmore" && len(doc.OriginatingCommand) > 0 {
				cmd = doc.OriginatingCommand
			}
			if len(cmd) == 0 {
				continue
			}
			query := obfuscate.MongoQueryShape(cmd)
			if query == "" {
				continue
			}
			key := profilerShapeKey{db: db.Name, collection: opCollection(doc.Ns), query: query}
			agg := shapes[key]
			if agg == nil {
				agg = &profilerAgg{}
				shapes[key] = agg
			}
			if doc.Op != "getmore" {
				agg.calls++
			}
			agg.timeSeconds += float64(doc.Millis) / 1000
			agg.docsReturned += float64(doc.Nreturned)
			agg.docsExamined += float64(doc.DocsExamined)
			agg.keysExamined += float64(doc.KeysExamined)
		}
		err = cursor.Err()
		cursor.Close(ctx)
		if err != nil {
			return nil, nil, err
		}
		c.profilerLastTs[db.Name] = maxTs
	}

	if elapsed <= 0 || firstRun {
		return levels, nil, nil
	}

	c.profilerWindow = append(c.profilerWindow, profilerInterval{at: now, duration: elapsed, shapes: shapes})
	cutoff := now.Add(-profilerWindowSeconds * time.Second)
	for len(c.profilerWindow) > 1 && c.profilerWindow[0].at.Before(cutoff) {
		c.profilerWindow = c.profilerWindow[1:]
	}

	total := map[profilerShapeKey]*profilerAgg{}
	var windowDuration float64
	for _, interval := range c.profilerWindow {
		windowDuration += interval.duration
		for key, agg := range interval.shapes {
			t := total[key]
			if t == nil {
				t = &profilerAgg{}
				total[key] = t
			}
			t.calls += agg.calls
			t.timeSeconds += agg.timeSeconds
			t.docsReturned += agg.docsReturned
			t.docsExamined += agg.docsExamined
			t.keysExamined += agg.keysExamined
		}
	}

	var top []TopQuery
	for key, agg := range total {
		top = append(top, TopQuery{
			DB:                    key.db,
			Collection:            key.collection,
			Query:                 key.query,
			CallsPerSecond:        agg.calls / windowDuration,
			TimePerSecond:         agg.timeSeconds / windowDuration,
			DocsReturnedPerSecond: agg.docsReturned / windowDuration,
			DocsExaminedPerSecond: agg.docsExamined / windowDuration,
			KeysExaminedPerSecond: agg.keysExamined / windowDuration,
		})
	}
	top = common.TopN(top, topQueriesN, func(a, b TopQuery) bool {
		if a.TimePerSecond == b.TimePerSecond {
			return a.CallsPerSecond > b.CallsPerSecond
		}
		return a.TimePerSecond > b.TimePerSecond
	})
	return levels, top, nil
}

func (c *Collector) topQueriesMetrics(ch chan<- prometheus.Metric) {
	for db, level := range c.profilingLevels {
		ch <- common.Gauge(dProfilingLevel, float64(level), db)
	}
	for _, q := range c.topQueries {
		ch <- common.Gauge(dTopQueryCalls, q.CallsPerSecond, q.DB, q.Collection, q.Query)
		ch <- common.Gauge(dTopQueryTime, q.TimePerSecond, q.DB, q.Collection, q.Query)
		ch <- common.Gauge(dTopQueryDocsReturned, q.DocsReturnedPerSecond, q.DB, q.Collection, q.Query)
		ch <- common.Gauge(dTopQueryDocsExamined, q.DocsExaminedPerSecond, q.DB, q.Collection, q.Query)
		ch <- common.Gauge(dTopQueryKeysExamined, q.KeysExaminedPerSecond, q.DB, q.Collection, q.Query)
	}
}

const profilerWindowSeconds = 60

type profilerInterval struct {
	at       time.Time
	duration float64
	shapes   map[profilerShapeKey]*profilerAgg
}

func opCollection(ns string) string {
	for i := 0; i < len(ns); i++ {
		if ns[i] == '.' {
			return ns[i+1:]
		}
	}
	return ""
}

func isNamespaceNotFound(err error) bool {
	var e mongo.CommandError
	if errors.As(err, &e) {
		return e.Code == 26 // NamespaceNotFound
	}
	return false
}
