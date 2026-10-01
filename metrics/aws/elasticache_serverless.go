package aws

import (
	"context"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	ectypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog"
)

var (
	dECServerlessInfo = common.Desc("aws_elasticache_serverless_info", "ElastiCache Serverless cache info",
		"region", "endpoint", "port", "reader_endpoint", "engine", "engine_version", "cache_name",
	)
	dECServerlessStatus       = common.Desc("aws_elasticache_serverless_status", "Status of the ElastiCache Serverless cache", "status")
	dECServerlessStorageLimit = common.Desc("aws_elasticache_serverless_data_storage_limit_bytes", "Maximum data storage of the cache (CacheUsageLimits)")
	dECServerlessECPULimit    = common.Desc("aws_elasticache_serverless_ecpu_limit_per_second", "Maximum ECPUs per second of the cache (CacheUsageLimits)")
)

// serverlessCachesAPI is the part of the ElastiCache client used to discover the Serverless caches (replaced by a fake in tests).
type serverlessCachesAPI interface {
	elasticache.DescribeServerlessCachesAPIClient
	ListTagsForResource(context.Context, *elasticache.ListTagsForResourceInput, ...func(*elasticache.Options)) (*elasticache.ListTagsForResourceOutput, error)
}

type ECServerlessCollector struct {
	discoverer *Discoverer
	key        string       // <region>/<cache name>: the key of its CloudWatch values
	lock       sync.RWMutex // update is called by the discovery goroutine, Collect by the registry
	region     string
	cache      *ectypes.ServerlessCache
}

func (c *ECServerlessCollector) snapshot() (string, *ectypes.ServerlessCache) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.region, c.cache
}

func (c *ECServerlessCollector) update(region string, cache *ectypes.ServerlessCache) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.region = region
	c.cache = cache
}

func (c *ECServerlessCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("aws_elasticache_serverless_collector", "", nil, nil)
}

func (c *ECServerlessCollector) Collect(ch chan<- prometheus.Metric) {
	region, cache := c.snapshot()
	if cache == nil {
		return
	}
	ch <- common.Gauge(dECServerlessStatus, 1, aws.ToString(cache.Status))
	var address, port, reader string
	if cache.Endpoint != nil {
		address = aws.ToString(cache.Endpoint.Address)
		port = strconv.Itoa(int(aws.ToInt32(cache.Endpoint.Port)))
	}
	if cache.ReaderEndpoint != nil {
		reader = aws.ToString(cache.ReaderEndpoint.Address)
	}
	version := aws.ToString(cache.FullEngineVersion)
	if version == "" {
		version = aws.ToString(cache.MajorEngineVersion)
	}
	ch <- common.Gauge(dECServerlessInfo, 1,
		region, address, port, reader, aws.ToString(cache.Engine), version, aws.ToString(cache.ServerlessCacheName),
	)
	if l := cache.CacheUsageLimits; l != nil {
		if l.DataStorage != nil && l.DataStorage.Maximum != nil {
			ch <- common.Gauge(dECServerlessStorageLimit, float64(aws.ToInt32(l.DataStorage.Maximum))*(1<<30)) // the only unit is GB
		}
		if l.ECPUPerSecond != nil && l.ECPUPerSecond.Maximum != nil {
			ch <- common.Gauge(dECServerlessECPULimit, float64(aws.ToInt32(l.ECPUPerSecond.Maximum)))
		}
	}
	if c.discoverer != nil {
		c.discoverer.cloudwatch.collect(c.key, ch)
	}
}

// listServerlessCaches returns the Serverless caches matching the tag filters (ElasticacheTagFilters).
// DescribeServerlessCaches doesn't return the tags: they are fetched per cache and cached, as for the ElastiCache clusters.
func (d *Discoverer) listServerlessCaches(cfg *config.AWSConfig, svc serverlessCachesAPI) ([]ectypes.ServerlessCache, error) {
	var res []ectypes.ServerlessCache
	paginator := elasticache.NewDescribeServerlessCachesPaginator(svc, &elasticache.DescribeServerlessCachesInput{})
	for paginator.HasMorePages() {
		ctx, cancel := d.apiContext()
		out, err := paginator.NextPage(ctx)
		cancel()
		if err != nil {
			return res, err
		}
		for _, cache := range out.ServerlessCaches {
			if filters := cfg.ElasticacheTagFilters; len(filters) > 0 {
				tags, err := d.elastiCacheTags(svc, aws.ToString(cache.ARN))
				if err != nil {
					klog.Error(err)
					d.registerError(err)
					continue
				}
				if !tagsMatched(filters, tags) {
					klog.Infof("ElastiCache Serverless cache %s (tags: %s) was skipped according to the tag-based filters: %s", aws.ToString(cache.ServerlessCacheName), tags, filters)
					continue
				}
			}
			res = append(res, cache)
		}
	}
	return res, nil
}

func (d *Discoverer) discoverECServerless(cfg *config.AWSConfig, region string, svc serverlessCachesAPI) {
	caches, err := d.listServerlessCaches(cfg, svc)
	if err != nil {
		d.registerOptionalAPIError("elasticache:DescribeServerlessCaches", err)
	}
	seen := map[string]bool{}
	for i := range caches {
		cache := &caches[i]
		id := region + "/" + aws.ToString(cache.ServerlessCacheName)
		seen[id] = true
		if d.ecServerlessCollectors[id] == nil {
			klog.Infoln("new ElastiCache Serverless cache found:", id)
			c := &ECServerlessCollector{discoverer: d, key: id}
			c.update(region, cache)
			if err := prometheus.WrapRegistererWith(ecServerlessLabels(id), d.reg).Register(c); err != nil {
				klog.Error(err)
				continue
			}
			d.ecServerlessCollectors[id] = c
		}
		d.ecServerlessCollectors[id].update(region, cache)
	}
	if err != nil || d.ctx.Err() != nil { // a failed listing: keep the collectors rather than dropping and re-adding them
		return
	}
	for id, c := range d.ecServerlessCollectors {
		if !seen[id] {
			prometheus.WrapRegistererWith(ecServerlessLabels(id), d.reg).Unregister(c)
			delete(d.ecServerlessCollectors, id)
		}
	}
}

func ecServerlessLabels(id string) prometheus.Labels {
	return prometheus.Labels{"ec_serverless_id": id}
}
