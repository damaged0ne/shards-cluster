package mongo

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ReplConfigMember struct {
	Host        string `bson:"host"`
	ArbiterOnly bool   `bson:"arbiterOnly"`
	Votes       int64  `bson:"votes"`
}

type ReplConfig struct {
	Config struct {
		Id                                 string             `bson:"_id"`
		Members                            []ReplConfigMember `bson:"members"`
		WriteConcernMajorityJournalDefault *bool              `bson:"writeConcernMajorityJournalDefault"`
	} `bson:"config"`
}

func (c *Collector) collectReplConfig(ctx context.Context, client *mongo.Client) (*ReplConfig, error) {
	res := client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetConfig", Value: 1}})
	var cfg ReplConfig
	if err := res.Decode(&cfg); err != nil {
		var e mongo.CommandError
		if errors.As(err, &e) {
			switch e.Code {
			case StatusReplicationNotEnabled, StatusReplicationNotYetInitialized:
				return nil, nil
			}
		}
		return nil, err
	}
	return &cfg, nil
}

type OplogStats struct {
	WindowSeconds float64
	MaxSizeBytes  float64
	UsedSizeBytes float64
}

func (c *Collector) collectOplog(ctx context.Context, client *mongo.Client) (*OplogStats, error) {
	coll := client.Database("local").Collection("oplog.rs")

	var first, last struct {
		Ts primitive.Timestamp `bson:"ts"`
	}
	projection := bson.D{{Key: "ts", Value: 1}, {Key: "_id", Value: 0}}
	err := coll.FindOne(ctx, bson.D{},
		options.FindOne().SetSort(bson.D{{Key: "$natural", Value: 1}}).SetProjection(projection),
	).Decode(&first)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("oplog.rs: %w", err)
	}
	err = coll.FindOne(ctx, bson.D{},
		options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}}).SetProjection(projection),
	).Decode(&last)
	if err != nil {
		return nil, fmt.Errorf("oplog.rs: %w", err)
	}

	var stats struct {
		StorageStats struct {
			MaxSize float64 `bson:"maxSize"`
			Size    float64 `bson:"size"`
		} `bson:"storageStats"`
	}
	cur, err := coll.Aggregate(ctx, bson.A{
		bson.D{{Key: "$collStats", Value: bson.D{{Key: "storageStats", Value: bson.D{}}}}},
	})
	if err != nil {
		return nil, fmt.Errorf("$collStats oplog.rs: %w", err)
	}
	defer cur.Close(ctx)
	if !cur.Next(ctx) {
		if err = cur.Err(); err != nil {
			return nil, fmt.Errorf("$collStats oplog.rs: %w", err)
		}
		return nil, fmt.Errorf("$collStats oplog.rs: no result")
	}
	if err = cur.Decode(&stats); err != nil {
		return nil, fmt.Errorf("$collStats oplog.rs: %w", err)
	}

	s := &OplogStats{
		MaxSizeBytes:  stats.StorageStats.MaxSize,
		UsedSizeBytes: stats.StorageStats.Size,
	}
	if last.Ts.T >= first.Ts.T {
		s.WindowSeconds = float64(last.Ts.T - first.Ts.T)
	}
	return s, nil
}

func (c *Collector) replicationMetrics(ch chan<- prometheus.Metric) {
	isPrimary := false
	if c.rsStatus != nil && c.rsStatus.ReplicaSet != "" {
		rs := c.rsStatus.ReplicaSet
		for _, m := range c.rsStatus.Members {
			if m.Self {
				isPrimary = m.State == "PRIMARY"
				ch <- common.Gauge(dRsStatus, 1, rs, m.State)
				if v := float64(m.LastApplied.Time().UnixMilli()); v > 0 {
					ch <- common.Gauge(dRsLastApplied, v)
				}
			}
		}
	}

	if isPrimary && c.rsConfig != nil && c.rsConfig.Config.Id != "" {
		rs := c.rsConfig.Config.Id
		for _, m := range c.rsConfig.Config.Members {
			ch <- common.Gauge(dRsMemberConfig, 1, rs, m.Host,
				strconv.FormatBool(m.ArbiterOnly),
				strconv.FormatInt(m.Votes, 10),
			)
		}
		wcmjd := "true"
		if v := c.rsConfig.Config.WriteConcernMajorityJournalDefault; v != nil && !*v {
			wcmjd = "false"
		}
		ch <- common.Gauge(dRsConfigInfo, 1, rs, wcmjd)
	}

	if c.oplog != nil {
		ch <- common.Gauge(dOplogWindow, c.oplog.WindowSeconds)
		ch <- common.Gauge(dOplogSize, c.oplog.UsedSizeBytes)
		ch <- common.Gauge(dOplogMaxSize, c.oplog.MaxSizeBytes)
	}
}
