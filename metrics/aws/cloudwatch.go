package aws

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultCloudWatchPeriod = time.Minute
	cloudWatchMaxQueries    = 500 // per GetMetricData request
	// the data points of the last cloudWatchLookback periods are requested: CloudWatch publishes them with a delay
	cloudWatchLookback = 5
)

var (
	dRDSAuroraReplicaLag        = common.Desc("aws_rds_aurora_replica_lag_seconds", "Lag of the Aurora replica behind the writer (CloudWatch AuroraReplicaLag)")
	dRDSServerlessCapacity      = common.Desc("aws_rds_serverless_capacity_acu", "Current capacity of the Aurora Serverless v2 instance in ACUs (CloudWatch ServerlessDatabaseCapacity)")
	dRDSServerlessUtilization   = common.Desc("aws_rds_serverless_acu_utilization_percent", "Capacity of the Aurora Serverless v2 instance relative to its maximum capacity (CloudWatch ACUUtilization)")
	dECServerlessECPU           = common.Desc("aws_elasticache_serverless_ecpu_per_second", "ElastiCache Processing Units consumed per second (CloudWatch ElastiCacheProcessingUnits)")
	dECServerlessBytesUsed      = common.Desc("aws_elasticache_serverless_used_bytes", "Data stored in the cache (CloudWatch BytesUsedForCache)")
	dECServerlessCurConnections = common.Desc("aws_elasticache_serverless_connections", "Number of client connections (CloudWatch CurrConnections)")
)

// cloudWatchAPI is the part of the CloudWatch client used by the integration (replaced by a fake in tests).
type cloudWatchAPI interface {
	GetMetricData(context.Context, *cloudwatch.GetMetricDataInput, ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error)
}

// cwQuery is a CloudWatch metric of a discovered resource, exposed by its collector (key) as desc.
type cwQuery struct {
	key       string // the id of the collector serving the value
	namespace string
	metric    string
	stat      string
	dims      []cwtypes.Dimension
	desc      *prometheus.Desc
	scale     float64 // applied to the value if not 0
	perSecond bool    // the value is divided by the period (a Sum over the period)
}

func (q cwQuery) String() string {
	var dims []string
	for _, d := range q.dims {
		dims = append(dims, aws.ToString(d.Name)+"="+aws.ToString(d.Value))
	}
	return q.namespace + "/" + q.metric + "{" + strings.Join(dims, ",") + "}"
}

type cwValue struct {
	desc  *prometheus.Desc
	value float64
}

// cloudWatchCache holds the last values fetched by the CloudWatch loop, served by the collectors' Collect.
type cloudWatchCache struct {
	lock   sync.RWMutex
	values map[string][]cwValue
}

func newCloudWatchCache() *cloudWatchCache {
	return &cloudWatchCache{values: map[string][]cwValue{}}
}

func (c *cloudWatchCache) set(values map[string][]cwValue) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.values = values
}

func (c *cloudWatchCache) collect(key string, ch chan<- prometheus.Metric) {
	if c == nil {
		return
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
	for _, v := range c.values[key] {
		ch <- common.Gauge(v.desc, v.value)
	}
}

func cloudWatchPeriod(cfg *config.AWSConfig) time.Duration {
	if cfg == nil || cfg.CloudWatchPeriodSeconds <= 0 {
		return defaultCloudWatchPeriod
	}
	minutes := (cfg.CloudWatchPeriodSeconds + 59) / 60 // standard-resolution metrics: a multiple of 60 seconds
	return time.Duration(minutes) * time.Minute
}

// setCloudWatchQueries replaces the queries of the CloudWatch loop and wakes it up if the set has grown,
// so that the metrics of the newly discovered resources don't wait for the next period.
func (d *Discoverer) setCloudWatchQueries(queries []cwQuery) {
	d.cwLock.Lock()
	grown := false
	known := map[string]bool{}
	for _, q := range d.cwQueries {
		known[q.key+q.String()] = true
	}
	for _, q := range queries {
		if !known[q.key+q.String()] {
			grown = true
			break
		}
	}
	d.cwQueries = queries
	d.cwLock.Unlock()
	if grown {
		select {
		case d.cwKick <- struct{}{}:
		default:
		}
	}
}

func (d *Discoverer) cloudWatchQueries() []cwQuery {
	d.cwLock.RLock()
	defer d.cwLock.RUnlock()
	return d.cwQueries
}

// cloudWatchLoop fetches the CloudWatch metrics in the background, once per period, never from Collect.
func (d *Discoverer) cloudWatchLoop() {
	defer close(d.cwDone)
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-d.cwKick:
		case <-t.C:
		}
		cfg, _ := d.config()
		period := cloudWatchPeriod(cfg)
		if queries := d.cloudWatchQueries(); len(queries) > 0 {
			d.cloudwatch.set(d.fetchCloudWatch(d.CloudWatchClient(), queries, period, time.Now()))
		} else {
			d.cloudwatch.set(map[string][]cwValue{})
		}
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		t.Reset(period)
	}
}

// fetchCloudWatch requests the latest data point of each query with GetMetricData, in batches of up to 500 queries.
// The values of a failed batch are not returned: stale values are not served.
func (d *Discoverer) fetchCloudWatch(client cloudWatchAPI, queries []cwQuery, period time.Duration, now time.Time) map[string][]cwValue {
	values := map[string][]cwValue{}
	end := now.Truncate(time.Minute)
	start := end.Add(-cloudWatchLookback * period)
	for offset := 0; offset < len(queries); offset += cloudWatchMaxQueries {
		batch := queries[offset:min(offset+cloudWatchMaxQueries, len(queries))]
		input := &cloudwatch.GetMetricDataInput{
			StartTime:         aws.Time(start),
			EndTime:           aws.Time(end),
			ScanBy:            cwtypes.ScanByTimestampDescending,
			MetricDataQueries: metricDataQueries(batch, period),
		}
		var results []cwtypes.MetricDataResult
		failed := false
		for {
			ctx, cancel := d.apiContext()
			out, err := client.GetMetricData(ctx, input)
			cancel()
			if err != nil {
				d.registerOptionalAPIError("cloudwatch:GetMetricData", err)
				failed = true
				break
			}
			results = append(results, out.MetricDataResults...)
			if aws.ToString(out.NextToken) == "" {
				break
			}
			input.NextToken = out.NextToken
		}
		if failed {
			continue
		}
		mapMetricDataResults(batch, results, period, values)
	}
	return values
}

func metricDataQueries(queries []cwQuery, period time.Duration) []cwtypes.MetricDataQuery {
	res := make([]cwtypes.MetricDataQuery, 0, len(queries))
	for i, q := range queries {
		res = append(res, cwtypes.MetricDataQuery{
			Id: aws.String(fmt.Sprintf("q%d", i)),
			MetricStat: &cwtypes.MetricStat{
				Metric: &cwtypes.Metric{
					Namespace:  aws.String(q.namespace),
					MetricName: aws.String(q.metric),
					Dimensions: q.dims,
				},
				Period: aws.Int32(int32(period / time.Second)),
				Stat:   aws.String(q.stat),
			},
			ReturnData: aws.Bool(true),
		})
	}
	return res
}

// mapMetricDataResults maps the results of a batch (ids q<index in the batch>) to the collectors' values.
// The results are ordered newest first (ScanByTimestampDescending), and the results of an id can be split across pages:
// the first value seen is the latest one.
func mapMetricDataResults(queries []cwQuery, results []cwtypes.MetricDataResult, period time.Duration, values map[string][]cwValue) {
	seen := map[int]bool{}
	for _, r := range results {
		var i int
		if _, err := fmt.Sscanf(aws.ToString(r.Id), "q%d", &i); err != nil || i < 0 || i >= len(queries) || seen[i] {
			continue
		}
		if len(r.Values) == 0 {
			continue
		}
		seen[i] = true
		q := queries[i]
		v := r.Values[0]
		if len(r.Timestamps) == len(r.Values) { // pick the newest one regardless of the order
			newest := 0
			for j, ts := range r.Timestamps {
				if ts.After(r.Timestamps[newest]) {
					newest = j
				}
			}
			v = r.Values[newest]
		}
		if q.perSecond {
			v /= period.Seconds()
		}
		if q.scale != 0 {
			v *= q.scale
		}
		values[q.key] = append(values[q.key], cwValue{desc: q.desc, value: v})
	}
}

func dimension(name, value string) cwtypes.Dimension {
	return cwtypes.Dimension{Name: aws.String(name), Value: aws.String(value)}
}

// auroraQueries returns the CloudWatch queries of an Aurora instance: the replica lag of the readers
// and the capacity of the Serverless v2 instances.
func auroraQueries(key, instanceId, instanceClass string, info auroraInstance, known bool) []cwQuery {
	dims := []cwtypes.Dimension{dimension("DBInstanceIdentifier", instanceId)}
	var res []cwQuery
	if !known || !info.writer { // the writer doesn't report AuroraReplicaLag
		res = append(res, cwQuery{key: key, namespace: "AWS/RDS", metric: "AuroraReplicaLag", stat: "Average", dims: dims, desc: dRDSAuroraReplicaLag, scale: 1e-3})
	}
	if instanceClass == "db.serverless" {
		res = append(res,
			cwQuery{key: key, namespace: "AWS/RDS", metric: "ServerlessDatabaseCapacity", stat: "Average", dims: dims, desc: dRDSServerlessCapacity},
			cwQuery{key: key, namespace: "AWS/RDS", metric: "ACUUtilization", stat: "Average", dims: dims, desc: dRDSServerlessUtilization},
		)
	}
	return res
}

// serverlessCacheQueries returns the CloudWatch queries of an ElastiCache Serverless cache (dimension clusterId).
func serverlessCacheQueries(key, name string) []cwQuery {
	dims := []cwtypes.Dimension{dimension("clusterId", name)}
	return []cwQuery{
		{key: key, namespace: "AWS/ElastiCache", metric: "ElastiCacheProcessingUnits", stat: "Sum", dims: dims, desc: dECServerlessECPU, perSecond: true},
		{key: key, namespace: "AWS/ElastiCache", metric: "BytesUsedForCache", stat: "Average", dims: dims, desc: dECServerlessBytesUsed},
		{key: key, namespace: "AWS/ElastiCache", metric: "CurrConnections", stat: "Average", dims: dims, desc: dECServerlessCurConnections},
	}
}
