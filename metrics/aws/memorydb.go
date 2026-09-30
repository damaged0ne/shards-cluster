package aws

import (
	"context"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/memorydb"
	mdbtypes "github.com/aws/aws-sdk-go-v2/service/memorydb/types"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog"
)

var (
	dMemoryDBInfo = common.Desc("aws_memorydb_info", "MemoryDB cluster info",
		"region", "endpoint", "port", "engine", "engine_version", "node_type", "shards", "tls", "cluster_name",
	)
	dMemoryDBStatus   = common.Desc("aws_memorydb_status", "Status of the MemoryDB cluster", "status")
	dMemoryDBNodeInfo = common.Desc("aws_memorydb_node_info", "MemoryDB node info",
		"shard", "node", "availability_zone", "endpoint", "port", "status",
	)
)

// memoryDBAPI is the part of the MemoryDB client used by the discovery (replaced by a fake in tests).
type memoryDBAPI interface {
	memorydb.DescribeClustersAPIClient
	ListTags(context.Context, *memorydb.ListTagsInput, ...func(*memorydb.Options)) (*memorydb.ListTagsOutput, error)
}

type MemoryDBCollector struct {
	lock    sync.RWMutex // update is called by the discovery goroutine, Collect by the registry
	region  string
	cluster *mdbtypes.Cluster
}

func (c *MemoryDBCollector) snapshot() (string, *mdbtypes.Cluster) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.region, c.cluster
}

func (c *MemoryDBCollector) update(region string, cluster *mdbtypes.Cluster) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.region = region
	c.cluster = cluster
}

func (c *MemoryDBCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("aws_memorydb_collector", "", nil, nil)
}

func (c *MemoryDBCollector) Collect(ch chan<- prometheus.Metric) {
	region, cl := c.snapshot()
	if cl == nil {
		return
	}
	ch <- common.Gauge(dMemoryDBStatus, 1, aws.ToString(cl.Status))
	var address, port string
	if cl.ClusterEndpoint != nil {
		address = aws.ToString(cl.ClusterEndpoint.Address)
		port = strconv.Itoa(int(cl.ClusterEndpoint.Port))
	}
	version := aws.ToString(cl.EnginePatchVersion)
	if version == "" {
		version = aws.ToString(cl.EngineVersion)
	}
	ch <- common.Gauge(dMemoryDBInfo, 1,
		region, address, port, aws.ToString(cl.Engine), version, aws.ToString(cl.NodeType),
		strconv.Itoa(int(aws.ToInt32(cl.NumberOfShards))), strconv.FormatBool(aws.ToBool(cl.TLSEnabled)), aws.ToString(cl.Name),
	)
	for _, shard := range cl.Shards {
		for _, node := range shard.Nodes {
			var address, port string
			if node.Endpoint != nil {
				address = aws.ToString(node.Endpoint.Address)
				port = strconv.Itoa(int(node.Endpoint.Port))
			}
			ch <- common.Gauge(dMemoryDBNodeInfo, 1,
				aws.ToString(shard.Name), aws.ToString(node.Name), aws.ToString(node.AvailabilityZone), address, port, aws.ToString(node.Status),
			)
		}
	}
}

// memoryDBEndpoints returns the endpoints of the nodes of the cluster (the exporter is attached to each node,
// as for the ElastiCache clusters), or the cluster endpoint if the shard details are not available.
func memoryDBEndpoints(cl *mdbtypes.Cluster) []common.Endpoint {
	var res []common.Endpoint
	for _, shard := range cl.Shards {
		for _, node := range shard.Nodes {
			if node.Endpoint != nil && aws.ToString(node.Endpoint.Address) != "" {
				res = append(res, common.Endpoint{Host: aws.ToString(node.Endpoint.Address), Port: strconv.Itoa(int(node.Endpoint.Port))})
			}
		}
	}
	if len(res) == 0 && cl.ClusterEndpoint != nil && aws.ToString(cl.ClusterEndpoint.Address) != "" {
		res = append(res, common.Endpoint{Host: aws.ToString(cl.ClusterEndpoint.Address), Port: strconv.Itoa(int(cl.ClusterEndpoint.Port))})
	}
	return res
}

// listMemoryDBClusters returns the clusters matching the tag filters.
// DescribeClusters doesn't return the tags: they are fetched per cluster and cached.
func (d *Discoverer) listMemoryDBClusters(cfg *config.AWSConfig, svc memoryDBAPI) ([]mdbtypes.Cluster, error) {
	var res []mdbtypes.Cluster
	paginator := memorydb.NewDescribeClustersPaginator(svc, &memorydb.DescribeClustersInput{ShowShardDetails: aws.Bool(true)})
	for paginator.HasMorePages() {
		ctx, cancel := d.apiContext()
		out, err := paginator.NextPage(ctx)
		cancel()
		if err != nil {
			return res, err
		}
		for _, cluster := range out.Clusters {
			if filters := cfg.MemoryDBTagFilters; len(filters) > 0 {
				tags, err := d.memoryDBTags(svc, aws.ToString(cluster.ARN))
				if err != nil {
					d.registerOptionalAPIError("memorydb:ListTags", err)
					continue
				}
				if !tagsMatched(filters, tags) {
					klog.Infof("MemoryDB cluster %s (tags: %s) was skipped according to the tag-based filters: %s", aws.ToString(cluster.Name), tags, filters)
					continue
				}
			}
			res = append(res, cluster)
		}
	}
	return res, nil
}

func (d *Discoverer) memoryDBTags(svc memoryDBAPI, arn string) (map[string]string, error) {
	if tags, ok := d.ecTags.get(arn); ok {
		return tags, nil
	}
	ctx, cancel := d.apiContext()
	defer cancel()
	o, err := svc.ListTags(ctx, &memorydb.ListTagsInput{ResourceArn: aws.String(arn)})
	if err != nil {
		return nil, err
	}
	tags := map[string]string{}
	for _, t := range o.TagList {
		tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}
	d.ecTags.set(arn, tags)
	return tags, nil
}

func (d *Discoverer) discoverMemoryDB(cfg *config.AWSConfig, region string, svc memoryDBAPI) {
	clusters, err := d.listMemoryDBClusters(cfg, svc)
	if err != nil {
		d.registerOptionalAPIError("memorydb:DescribeClusters", err)
	}
	seen := map[string]bool{}
	for i := range clusters {
		cluster := &clusters[i]
		id := region + "/" + aws.ToString(cluster.Name)
		seen[id] = true
		if d.memoryDBCollectors[id] == nil {
			klog.Infoln("new MemoryDB cluster found:", id)
			c := &MemoryDBCollector{}
			c.update(region, cluster)
			if err := prometheus.WrapRegistererWith(memoryDBLabels(id), d.reg).Register(c); err != nil {
				klog.Error(err)
				continue
			}
			d.memoryDBCollectors[id] = c
		}
		d.memoryDBCollectors[id].update(region, cluster)
	}
	if err != nil || d.ctx.Err() != nil { // a failed listing: keep the collectors rather than dropping and re-adding them
		return
	}
	for id, c := range d.memoryDBCollectors {
		if !seen[id] {
			prometheus.WrapRegistererWith(memoryDBLabels(id), d.reg).Unregister(c)
			delete(d.memoryDBCollectors, id)
		}
	}
}

func memoryDBLabels(id string) prometheus.Labels {
	return prometheus.Labels{"memorydb_cluster_id": id}
}
