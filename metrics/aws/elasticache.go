package aws

import (
	"context"
	"net"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	ectypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog"
)

var (
	dECInfo = common.Desc("aws_elasticache_info", "Elasticache instance info",
		"region", "availability_zone", "endpoint", "ipv4", "port",
		"engine", "engine_version", "instance_type", "cluster_id",
	)
	dECStatus = common.Desc("aws_elasticache_status", "Status of the Elasticache instance", "status")
)

type ECCollector struct {
	lock    sync.RWMutex // update is called by the discovery goroutine, Collect by the registry
	region  string
	cluster *ectypes.CacheCluster
	node    *ectypes.CacheNode
	ip      *net.IPAddr
}

func NewECCollector(region string, cluster *ectypes.CacheCluster, node *ectypes.CacheNode) *ECCollector {
	return &ECCollector{region: region, cluster: cluster, node: node}
}

func (c *ECCollector) snapshot() (string, *ectypes.CacheCluster, *ectypes.CacheNode, *net.IPAddr) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.region, c.cluster, c.node, c.ip
}

func (c *ECCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("aws_elasticache_collector", "", nil, nil)
}

func (c *ECCollector) Collect(ch chan<- prometheus.Metric) {
	region, cl, node, ipAddr := c.snapshot()
	if cl == nil || node == nil {
		return
	}
	ch <- common.Gauge(dECStatus, 1, aws.ToString(node.CacheNodeStatus))

	cluster := aws.ToString(cl.ReplicationGroupId)
	if cluster == "" {
		cluster = aws.ToString(cl.CacheClusterId)
	}
	var address, port, ip string
	if node.Endpoint != nil {
		address = aws.ToString(node.Endpoint.Address)
		port = strconv.Itoa(int(aws.ToInt32(node.Endpoint.Port)))
	}
	if ipAddr != nil {
		ip = ipAddr.String()
	}
	ch <- common.Gauge(dECInfo, 1,
		region,
		aws.ToString(node.CustomerAvailabilityZone),
		address,
		ip,
		port,
		aws.ToString(cl.Engine),
		aws.ToString(cl.EngineVersion),
		aws.ToString(cl.CacheNodeType),
		cluster,
	)
}

func (c *ECCollector) Stop() {
}

func (c *ECCollector) update(ctx context.Context, region string, cluster *ectypes.CacheCluster, node *ectypes.CacheNode) {
	var ip *net.IPAddr
	if node.Endpoint != nil {
		var err error
		if ip, err = resolveIP(ctx, aws.ToString(node.Endpoint.Address)); err != nil {
			klog.Errorln(err)
		}
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	c.region = region
	c.cluster = cluster
	c.node = node
	if ip != nil {
		c.ip = ip
	}
}
