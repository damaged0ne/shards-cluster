package aws

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/memorydb"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/maps"
	"k8s.io/klog"
)

const (
	discoveryInterval = time.Minute
	apiTimeout        = 30 * time.Second // per AWS API call, including the retries
	resolveTimeout    = 5 * time.Second
	ecTagsTTL         = 10 * time.Minute
)

var (
	dError = common.Desc("aws_discovery_error", "AWS discovery error", "error")
)

type Discoverer struct {
	k8s      *k8s.K8S
	ctx      context.Context // cancelled on Stop
	cancel   context.CancelFunc
	reg      prometheus.Registerer
	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{} // closed when the discovery goroutine exits

	// cfg, region, awsCfg, the clients and identityPending are replaced by Update (the config goroutine)
	// and read by the discovery goroutine and the collectors' goroutines
	lock                 sync.RWMutex
	cfg                  *config.AWSConfig
	region               string
	awsCfg               aws.Config
	rdsClient            *rds.Client
	elasticacheClient    *elasticache.Client
	cloudwatchLogsClient *cloudwatchlogs.Client
	cloudwatchClient     *cloudwatch.Client
	memorydbClient       *memorydb.Client
	identityPending      bool

	errors      map[string]bool
	errorsLock  sync.RWMutex
	lastSummary string
	deniedAPIs  map[string]bool // the optional APIs not allowed by the IAM policy, reported once

	// accessed only by the discovery goroutine (and by Stop after it has exited)
	rdsCollectors map[string]*RDSCollector
	ecCollectors  map[string]*ECCollector
	ecTags        *tagCache // ElastiCache and MemoryDB tags by ARN

	ecServerlessCollectors map[string]*ECServerlessCollector
	memoryDBCollectors     map[string]*MemoryDBCollector

	auroraLock sync.RWMutex
	aurora     map[string]auroraInstance // by RDS instance id, written by the discovery goroutine, read by Collect

	// the CloudWatch metrics are fetched by their own goroutine, once per period, and served from the cache
	cloudwatch *cloudWatchCache
	cwLock     sync.RWMutex
	cwQueries  []cwQuery
	cwKick     chan struct{}
	cwDone     chan struct{} // closed when the CloudWatch goroutine exits (nil if not started)

	endpointsLock     sync.RWMutex
	rdsEndpoints      map[string]common.Endpoint
	rdsReplicas       map[string][]string
	ecEndpoints       map[string][]common.Endpoint
	ecTLS             map[string]bool // the caches requiring TLS (Serverless)
	memoryDBEndpoints map[string][]common.Endpoint
	memoryDBTLS       map[string]bool
}

func (d *Discoverer) RDSEndpoint(id string) (common.Endpoint, bool) {
	d.endpointsLock.RLock()
	defer d.endpointsLock.RUnlock()
	e, ok := d.rdsEndpoints[id]
	return e, ok
}

func (d *Discoverer) RDSReplicas(source string) []string {
	d.endpointsLock.RLock()
	defer d.endpointsLock.RUnlock()
	return d.rdsReplicas[source]
}

func (d *Discoverer) ElastiCacheEndpoints(clusterId string) []common.Endpoint {
	d.endpointsLock.RLock()
	defer d.endpointsLock.RUnlock()
	return d.ecEndpoints[clusterId]
}

// ElastiCacheRequiresTLS reports whether the cache only accepts TLS connections (ElastiCache Serverless).
func (d *Discoverer) ElastiCacheRequiresTLS(id string) bool {
	d.endpointsLock.RLock()
	defer d.endpointsLock.RUnlock()
	return d.ecTLS[id]
}

// MemoryDBEndpoints returns the endpoints of the nodes of the MemoryDB cluster and whether TLS is enabled.
func (d *Discoverer) MemoryDBEndpoints(name string) ([]common.Endpoint, bool) {
	d.endpointsLock.RLock()
	defer d.endpointsLock.RUnlock()
	return d.memoryDBEndpoints[name], d.memoryDBTLS[name]
}

func (d *Discoverer) publishEndpoints() {
	rds := map[string]common.Endpoint{}
	replicas := map[string][]string{}
	for _, c := range d.rdsCollectors {
		_, instance, _, _ := c.snapshot()
		if instance == nil || instance.Endpoint == nil {
			continue
		}
		id := aws.ToString(instance.DBInstanceIdentifier)
		rds[id] = common.Endpoint{
			Host: aws.ToString(instance.Endpoint.Address),
			Port: strconv.Itoa(int(aws.ToInt32(instance.Endpoint.Port))),
		}
		if source := aws.ToString(instance.ReadReplicaSourceDBInstanceIdentifier); source != "" {
			replicas[source] = append(replicas[source], id)
		}
	}
	ec := map[string][]common.Endpoint{}
	for _, c := range d.ecCollectors {
		_, cluster, node, _ := c.snapshot()
		if cluster == nil || node == nil || node.Endpoint == nil {
			continue
		}
		id := aws.ToString(cluster.CacheClusterId)
		ec[id] = append(ec[id], common.Endpoint{
			Host: aws.ToString(node.Endpoint.Address),
			Port: strconv.Itoa(int(aws.ToInt32(node.Endpoint.Port))),
		})
	}
	ecTLS := map[string]bool{}
	for _, c := range d.ecServerlessCollectors {
		_, cache := c.snapshot()
		if cache == nil || cache.Endpoint == nil {
			continue
		}
		name := aws.ToString(cache.ServerlessCacheName) // `elasticache: <name>`, as for the clusters
		ec[name] = append(ec[name], common.Endpoint{
			Host: aws.ToString(cache.Endpoint.Address),
			Port: strconv.Itoa(int(aws.ToInt32(cache.Endpoint.Port))),
		})
		ecTLS[name] = true
	}
	mdb := map[string][]common.Endpoint{}
	mdbTLS := map[string]bool{}
	for _, c := range d.memoryDBCollectors {
		_, cluster := c.snapshot()
		if cluster == nil {
			continue
		}
		name := aws.ToString(cluster.Name)
		mdb[name] = memoryDBEndpoints(cluster)
		mdbTLS[name] = aws.ToBool(cluster.TLSEnabled)
	}
	d.endpointsLock.Lock()
	d.rdsEndpoints = rds
	d.rdsReplicas = replicas
	d.ecEndpoints = ec
	d.ecTLS = ecTLS
	d.memoryDBEndpoints = mdb
	d.memoryDBTLS = mdbTLS
	d.endpointsLock.Unlock()
}

// buildCloudWatchQueries returns the CloudWatch metrics of the discovered resources for the CloudWatch goroutine.
func (d *Discoverer) buildCloudWatchQueries() []cwQuery {
	var res []cwQuery
	for id, c := range d.rdsCollectors {
		_, instance, _, _ := c.snapshot()
		if !isAurora(instance) {
			continue
		}
		info, known := d.auroraInstance(id)
		res = append(res, auroraQueries(id, aws.ToString(instance.DBInstanceIdentifier), aws.ToString(instance.DBInstanceClass), info, known)...)
	}
	for id, c := range d.ecServerlessCollectors {
		if _, cache := c.snapshot(); cache != nil {
			res = append(res, serverlessCacheQueries(id, aws.ToString(cache.ServerlessCacheName))...)
		}
	}
	return res
}

func NewDiscoverer(cfg *config.AWSConfig, k8s *k8s.K8S, reg prometheus.Registerer) (*Discoverer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	d := &Discoverer{
		k8s:    k8s,
		ctx:    ctx,
		cancel: cancel,
		reg:    reg,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),

		errors:     map[string]bool{},
		deniedAPIs: map[string]bool{},

		rdsCollectors: map[string]*RDSCollector{},
		ecCollectors:  map[string]*ECCollector{},
		ecTags:        newTagCache(ecTagsTTL),

		ecServerlessCollectors: map[string]*ECServerlessCollector{},
		memoryDBCollectors:     map[string]*MemoryDBCollector{},
		cloudwatch:             newCloudWatchCache(),
		cwKick:                 make(chan struct{}, 1),
		cwDone:                 make(chan struct{}),
	}
	if err := d.setConfig(cfg); err != nil {
		cancel()
		return nil, err
	}
	if err := reg.Register(d); err != nil {
		cancel()
		return nil, err
	}

	go d.run(d.discover)
	go d.cloudWatchLoop()
	return d, nil
}

func (d *Discoverer) run(discover func()) {
	defer close(d.done)
	discover()
	t := time.NewTicker(discoveryInterval)
	defer t.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-t.C:
			discover()
		}
	}
}

// setConfig builds the AWS config and the clients outside the lock (it may take a while), then swaps them in.
func (d *Discoverer) setConfig(cfg *config.AWSConfig) error {
	ctx, cancel := d.apiContext()
	awsCfg, err := newAWSConfig(ctx, cfg, d.k8s)
	cancel()
	if err != nil {
		return err
	}
	identityPending := !logIdentity(d.ctx, awsCfg)
	rdsClient := rds.NewFromConfig(awsCfg)
	elasticacheClient := elasticache.NewFromConfig(awsCfg)
	cloudwatchLogsClient := cloudwatchlogs.NewFromConfig(awsCfg)
	cloudwatchClient := cloudwatch.NewFromConfig(awsCfg)
	memorydbClient := memorydb.NewFromConfig(awsCfg)

	d.lock.Lock()
	defer d.lock.Unlock()
	d.cfg = cfg
	d.awsCfg = awsCfg
	d.region = awsCfg.Region
	d.identityPending = identityPending
	d.rdsClient = rdsClient
	d.elasticacheClient = elasticacheClient
	d.cloudwatchLogsClient = cloudwatchLogsClient
	d.cloudwatchClient = cloudwatchClient
	d.memorydbClient = memorydbClient
	return nil
}

// apiContext returns a context for a single AWS API call: bounded by apiTimeout and cancelled on Stop.
func (d *Discoverer) apiContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(d.ctx, apiTimeout)
}

func (d *Discoverer) config() (*config.AWSConfig, string) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.cfg, d.region
}

func (d *Discoverer) RDSClient() *rds.Client {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.rdsClient
}

func (d *Discoverer) ElastiCacheClient() *elasticache.Client {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.elasticacheClient
}

func (d *Discoverer) CloudWatchClient() *cloudwatch.Client {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.cloudwatchClient
}

func (d *Discoverer) MemoryDBClient() *memorydb.Client {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.memorydbClient
}

func (d *Discoverer) CloudWatchLogsClient() *cloudwatchlogs.Client {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.cloudwatchLogsClient
}

// Stop is idempotent. It cancels the in-flight API calls, so waiting for the discovery goroutine is short.
func (d *Discoverer) Stop() {
	d.stopOnce.Do(func() {
		d.cancel()
		close(d.stop)
		<-d.done
		if d.cwDone != nil {
			<-d.cwDone
		}
		for id, c := range d.rdsCollectors {
			prometheus.WrapRegistererWith(rdsLabels(id), d.reg).Unregister(c)
			c.Stop()
		}
		for id, c := range d.ecCollectors {
			prometheus.WrapRegistererWith(ecLabels(id), d.reg).Unregister(c)
			c.Stop()
		}
		for id, c := range d.ecServerlessCollectors {
			prometheus.WrapRegistererWith(ecServerlessLabels(id), d.reg).Unregister(c)
		}
		for id, c := range d.memoryDBCollectors {
			prometheus.WrapRegistererWith(memoryDBLabels(id), d.reg).Unregister(c)
		}
		d.reg.Unregister(d)
	})
}

func (d *Discoverer) Update(cfg *config.AWSConfig) error {
	if current, _ := d.config(); current.Equal(cfg) {
		return nil
	}
	return d.setConfig(cfg)
}

func (d *Discoverer) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("aws_discoverer", "", nil, nil)
}

func (d *Discoverer) Collect(ch chan<- prometheus.Metric) {
	d.errorsLock.RLock()
	defer d.errorsLock.RUnlock()
	if len(d.errors) > 0 {
		for e := range d.errors {
			ch <- common.Gauge(dError, 1, e)
		}
	} else {
		ch <- common.Gauge(dError, 0, "")
	}
}

func (d *Discoverer) registerError(err error) {
	msg := err.Error()
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		msg = apiErr.ErrorMessage()
	}
	d.errorsLock.Lock()
	d.errors[msg] = true
	d.errorsLock.Unlock()
}

// registerOptionalAPIError handles an error of an API needed only by the optional integrations (Aurora cluster info,
// ElastiCache Serverless, MemoryDB, CloudWatch metrics): if the IAM policy doesn't allow the call (e.g. it hasn't been
// updated since these integrations were added), it's reported once instead of failing every discovery cycle.
func (d *Discoverer) registerOptionalAPIError(action string, err error) {
	if d.ctx.Err() != nil { // stopped
		return
	}
	if isAccessDenied(err) {
		d.errorsLock.Lock()
		reported := d.deniedAPIs[action]
		if d.deniedAPIs == nil {
			d.deniedAPIs = map[string]bool{}
		}
		d.deniedAPIs[action] = true
		d.errorsLock.Unlock()
		if !reported {
			klog.Warningf("AWS integration: %s is not allowed by the IAM policy, the related resources/metrics are skipped: %s", action, err)
		}
		return
	}
	klog.Error(err)
	d.registerError(err)
}

func isAccessDenied(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	code := apiErr.ErrorCode()
	return strings.Contains(code, "AccessDenied") || strings.Contains(code, "UnauthorizedOperation") || strings.Contains(code, "NotAuthorized")
}

func (d *Discoverer) discover() {
	d.errorsLock.Lock()
	d.errors = map[string]bool{}
	d.errorsLock.Unlock()
	d.lock.RLock()
	identityPending, awsCfg := d.identityPending, d.awsCfg
	d.lock.RUnlock()
	if identityPending && logIdentity(d.ctx, awsCfg) {
		d.lock.Lock()
		if d.awsCfg.Credentials == awsCfg.Credentials { // not replaced by Update in the meantime
			d.identityPending = false
		}
		d.lock.Unlock()
	}
	cfg, region := d.config()
	d.discoverRDS(cfg, region, d.RDSClient())
	d.discoverAurora(region, d.RDSClient())
	d.discoverEC(cfg, region, d.ElastiCacheClient())
	d.discoverECServerless(cfg, region, d.ElastiCacheClient())
	d.discoverMemoryDB(cfg, region, d.MemoryDBClient())
	if d.ctx.Err() != nil { // stopped
		return
	}
	d.publishEndpoints()
	d.setCloudWatchQueries(d.buildCloudWatchQueries())

	d.errorsLock.RLock()
	errs := maps.Keys(d.errors)
	d.errorsLock.RUnlock()
	summary := fmt.Sprintf("AWS discovery (region=%s): %d RDS instances, %d ElastiCache nodes, %d ElastiCache Serverless caches, %d MemoryDB clusters",
		region, len(d.rdsCollectors), len(d.ecCollectors), len(d.ecServerlessCollectors), len(d.memoryDBCollectors))
	switch {
	case len(errs) > 0:
		klog.Errorf("%s, errors: %s", summary, strings.Join(errs, "; "))
	case summary != d.lastSummary:
		klog.Infoln(summary)
	}
	d.lastSummary = summary
}

func (d *Discoverer) discoverRDS(cfg *config.AWSConfig, region string, svc *rds.Client) {
	seen := map[string]bool{}
	paginator := rds.NewDescribeDBInstancesPaginator(svc, &rds.DescribeDBInstancesInput{})
	for paginator.HasMorePages() {
		ctx, cancel := d.apiContext()
		output, err := paginator.NextPage(ctx)
		cancel()
		if err != nil {
			klog.Error(err)
			d.registerError(err)
			break
		}
		for _, instance := range output.DBInstances {
			if filters := cfg.RDSTagFilters; len(filters) > 0 {
				tags := rdsTags(instance.TagList) // DescribeDBInstances returns the tags, no need for ListTagsForResource
				if !tagsMatched(filters, tags) {
					klog.Infof("RDS instance %s (tags: %s) was skipped according to the tag-based filters: %s", aws.ToString(instance.DBInstanceIdentifier), tags, filters)
					continue
				}
			}
			id := region + "/" + aws.ToString(instance.DBInstanceIdentifier)
			seen[id] = true
			if d.rdsCollectors[id] == nil {
				klog.Infoln("new RDS instance found:", id)
				c := NewRDSCollector(d, region, &instance)
				if err = prometheus.WrapRegistererWith(rdsLabels(id), d.reg).Register(c); err != nil {
					klog.Error(err)
					c.Stop()
					continue
				}
				d.rdsCollectors[id] = c
			}
			d.rdsCollectors[id].update(region, &instance)
		}
	}
	if d.ctx.Err() != nil { // stopped: Stop cleans up the collectors
		return
	}

	for id, c := range d.rdsCollectors {
		if !seen[id] {
			prometheus.WrapRegistererWith(rdsLabels(id), d.reg).Unregister(c)
			delete(d.rdsCollectors, id)
			c.Stop()
		}
	}
}

func (d *Discoverer) discoverEC(cfg *config.AWSConfig, region string, svc *elasticache.Client) {
	seen := map[string]bool{}
	d.ecTags.prune()
	for _, v := range []bool{false, true} {
		input := &elasticache.DescribeCacheClustersInput{
			ShowCacheNodeInfo:                       aws.Bool(true),
			ShowCacheClustersNotInReplicationGroups: aws.Bool(v),
		}
		paginator := elasticache.NewDescribeCacheClustersPaginator(svc, input)
		for paginator.HasMorePages() {
			ctx, cancel := d.apiContext()
			output, err := paginator.NextPage(ctx)
			cancel()
			if err != nil {
				klog.Error(err)
				d.registerError(err)
				break
			}
			for _, cluster := range output.CacheClusters {
				if filters := cfg.ElasticacheTagFilters; len(filters) > 0 {
					// DescribeCacheClusters doesn't return the tags: they are fetched per cluster and cached
					tags, err := d.elastiCacheTags(svc, aws.ToString(cluster.ARN))
					if err != nil {
						klog.Error(err)
						d.registerError(err)
						continue
					}
					if !tagsMatched(filters, tags) {
						klog.Infof("EC cluster %s (tags: %s) was skipped according to the tag-based filters: %s", aws.ToString(cluster.CacheClusterId), tags, filters)
						continue
					}
				}
				for _, node := range cluster.CacheNodes {
					id := region + "/" + aws.ToString(cluster.CacheClusterId) + "/" + aws.ToString(node.CacheNodeId)
					seen[id] = true
					if d.ecCollectors[id] == nil {
						klog.Infoln("new EC instance found:", id)
						c := NewECCollector(region, &cluster, &node)
						if err = prometheus.WrapRegistererWith(ecLabels(id), d.reg).Register(c); err != nil {
							klog.Error(err)
							continue
						}
						d.ecCollectors[id] = c
					}
					d.ecCollectors[id].update(d.ctx, region, &cluster, &node)
				}
			}
		}
	}
	if d.ctx.Err() != nil { // stopped: Stop cleans up the collectors
		return
	}

	for id, c := range d.ecCollectors {
		if !seen[id] {
			prometheus.WrapRegistererWith(ecLabels(id), d.reg).Unregister(c)
			c.Stop()
			delete(d.ecCollectors, id)
		}
	}
}

type elastiCacheTagsAPI interface {
	ListTagsForResource(context.Context, *elasticache.ListTagsForResourceInput, ...func(*elasticache.Options)) (*elasticache.ListTagsForResourceOutput, error)
}

func (d *Discoverer) elastiCacheTags(svc elastiCacheTagsAPI, arn string) (map[string]string, error) {
	if tags, ok := d.ecTags.get(arn); ok {
		return tags, nil
	}
	ctx, cancel := d.apiContext()
	defer cancel()
	o, err := svc.ListTagsForResource(ctx, &elasticache.ListTagsForResourceInput{ResourceName: aws.String(arn)})
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

func rdsTags(tagList []rdstypes.Tag) map[string]string {
	tags := make(map[string]string, len(tagList))
	for _, t := range tagList {
		tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}
	return tags
}

// tagCache caches the tags of resources by ARN for ttl.
type tagCache struct {
	lock    sync.Mutex
	ttl     time.Duration
	now     func() time.Time
	entries map[string]tagCacheEntry
}

type tagCacheEntry struct {
	tags    map[string]string
	expires time.Time
}

func newTagCache(ttl time.Duration) *tagCache {
	return &tagCache{ttl: ttl, now: time.Now, entries: map[string]tagCacheEntry{}}
}

func (c *tagCache) get(arn string) (map[string]string, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	e, ok := c.entries[arn]
	if !ok || !c.now().Before(e.expires) {
		return nil, false
	}
	return e.tags, true
}

func (c *tagCache) set(arn string, tags map[string]string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.entries[arn] = tagCacheEntry{tags: tags, expires: c.now().Add(c.ttl)}
}

// prune drops the expired entries, so the tags of the deleted resources don't pile up.
func (c *tagCache) prune() {
	c.lock.Lock()
	defer c.lock.Unlock()
	now := c.now()
	for arn, e := range c.entries {
		if !now.Before(e.expires) {
			delete(c.entries, arn)
		}
	}
}

// resolveIP resolves the host preferring IPv4, as net.ResolveIPAddr does, but honoring ctx.
func resolveIP(ctx context.Context, host string) (*net.IPAddr, error) {
	ctx, cancel := context.WithTimeout(ctx, resolveTimeout)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no addresses found for %s", host)
	}
	for _, a := range addrs {
		if a.IP.To4() != nil {
			return &a, nil
		}
	}
	return &addrs[0], nil
}

func rdsLabels(id string) prometheus.Labels {
	return prometheus.Labels{"rds_instance_id": id}
}

func ecLabels(id string) prometheus.Labels {
	return prometheus.Labels{"ec_instance_id": id}
}

func newAWSConfig(ctx context.Context, cfg *config.AWSConfig, k8s *k8s.K8S) (aws.Config, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = 6
				o.MaxBackoff = 10 * time.Second
				o.Backoff = retry.NewExponentialJitterBackoff(10 * time.Second)
			})
		}),
	}
	if cfg.AccessKeyID != "" || cfg.SecretAccessKey != "" {
		if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
			return aws.Config{}, fmt.Errorf("both access_key_id and secret_access_key must be set, or neither (to use the default AWS credential chain)")
		}
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, err
	}
	if cfg.Region == "" {
		if awsCfg.Region != "" {
			klog.Infoln("AWS region is not configured, using the one from the environment:", awsCfg.Region)
		} else {
			awsCfg.Region, err = discoverRegion(ctx, awsCfg, k8s)
			if err != nil {
				return aws.Config{}, err
			}
			klog.Infoln("AWS region is not configured, using the discovered one:", awsCfg.Region)
		}
	}
	return awsCfg, nil
}

func logIdentity(ctx context.Context, awsCfg aws.Config) bool {
	if awsCfg.Credentials == nil {
		klog.Errorln("AWS integration: no credentials provider configured")
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	creds, err := awsCfg.Credentials.Retrieve(ctx)
	if err != nil {
		klog.Errorln("AWS integration: failed to obtain credentials:", err)
		return false
	}
	out, err := sts.NewFromConfig(awsCfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		klog.Errorf("AWS integration: region=%s, credentials=%s, failed to get the caller identity: %s", awsCfg.Region, creds.Source, err)
		return false
	}
	klog.Infof("AWS integration: region=%s, credentials=%s, identity=%s", awsCfg.Region, creds.Source, aws.ToString(out.Arn))
	return true
}

func discoverRegion(ctx context.Context, awsCfg aws.Config, k8s *k8s.K8S) (string, error) {
	region, err := k8s.GetNodeRegion(ctx)
	if err != nil {
		klog.Warningln("failed to get the region from the kubernetes nodes:", err)
	}
	if region != "" {
		return region, nil
	}
	imdsCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := imds.NewFromConfig(awsCfg).GetRegion(imdsCtx, &imds.GetRegionInput{})
	if err == nil && out.Region != "" {
		return out.Region, nil
	}
	return "", fmt.Errorf("failed to discover the AWS region (set the AWS_REGION env var): %w", err)
}

func idWithRegion(region, id string) string {
	if id == "" {
		return ""
	}
	if arn.IsARN(id) {
		a, _ := arn.Parse(id)
		region = a.Region
		id = a.Resource
		parts := strings.Split(a.Resource, ":")
		if len(parts) > 1 {
			id = parts[1]
		}
	}
	return region + "/" + id
}

func tagsMatched(filters, tags map[string]string) bool {
	for tagName, desiredValue := range filters {
		value := tags[tagName]
		if matched, _ := filepath.Match(desiredValue, value); !matched {
			return false
		}
	}
	return true
}
