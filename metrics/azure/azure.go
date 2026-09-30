package azure

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/maps"
	"k8s.io/klog"
)

const (
	discoveryInterval = time.Minute
	apiTimeout        = 30 * time.Second // per Azure API call (a page of a listing), including the retries
)

var (
	dError = common.Desc("azure_discovery_error", "Azure discovery error", "error")
)

type Discoverer struct {
	cfg           *config.AzureConfig
	subscriptions []string
	api           azureAPI
	ctx           context.Context // cancelled on Stop
	cancel        context.CancelFunc
	reg           prometheus.Registerer
	stop          chan struct{}
	stopOnce      sync.Once
	done          chan struct{} // closed when the discovery goroutine exits
	monitoring    *Monitoring

	errors      map[string]bool
	errorsLock  sync.RWMutex
	lastSummary string

	// accessed only by the discovery goroutine (and by Stop after it has exited)
	dbCollectors    map[string]*DBCollector
	redisCollectors map[string]*RedisCollector

	endpointsLock  sync.RWMutex
	dbEndpoints    map[string]common.Endpoint // by <engine>/<server name>
	dbReplicas     map[string][]string        // by <engine>/<primary server name>
	redisEndpoints map[string]common.Endpoint // by cache name
	redisTLS       map[string]bool
}

// DBEndpoint returns the endpoint of the PostgreSQL or MySQL flexible server (engine: postgres or mysql).
func (d *Discoverer) DBEndpoint(engine, name string) (common.Endpoint, bool) {
	d.endpointsLock.RLock()
	defer d.endpointsLock.RUnlock()
	e, ok := d.dbEndpoints[engine+"/"+name]
	return e, ok
}

// DBReplicas returns the names of the read replicas of the server.
func (d *Discoverer) DBReplicas(engine, primary string) []string {
	d.endpointsLock.RLock()
	defer d.endpointsLock.RUnlock()
	return d.dbReplicas[engine+"/"+primary]
}

// RedisEndpoint returns the endpoint of the cache and whether it requires TLS (the non-TLS port is disabled).
func (d *Discoverer) RedisEndpoint(name string) (common.Endpoint, bool, bool) {
	d.endpointsLock.RLock()
	defer d.endpointsLock.RUnlock()
	e, ok := d.redisEndpoints[name]
	return e, d.redisTLS[name], ok
}

func NewDiscoverer(cfg *config.AzureConfig, reg prometheus.Registerer) (*Discoverer, error) {
	subscriptions := cfg.SubscriptionIDs
	if len(subscriptions) == 0 {
		if s := os.Getenv("AZURE_SUBSCRIPTION_ID"); s != "" {
			subscriptions = []string{s}
		}
	}
	if len(subscriptions) == 0 {
		return nil, fmt.Errorf("Azure integration: no subscriptions configured (set subscriptionIds or AZURE_SUBSCRIPTION_ID)")
	}
	// DefaultAzureCredential: environment service principal (AZURE_CLIENT_ID/AZURE_TENANT_ID/AZURE_CLIENT_SECRET or
	// AZURE_CLIENT_CERTIFICATE_PATH), workload identity (AZURE_FEDERATED_TOKEN_FILE), managed identity, Azure CLI
	cred, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{TenantID: cfg.TenantID})
	if err != nil {
		return nil, fmt.Errorf("Azure integration: failed to obtain credentials: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := newDiscoverer(ctx, cancel, cfg, subscriptions, reg)
	api, err := newSDKAPI(cred, subscriptions, d.apiContext)
	if err != nil {
		cancel()
		return nil, err
	}
	d.api = api
	if err := reg.Register(d); err != nil {
		cancel()
		return nil, err
	}
	klog.Infof("Azure integration: subscriptions=%s, resource groups=%s, credentials=%s",
		strings.Join(subscriptions, ","), scope(cfg.ResourceGroups), credentialSource())
	go d.run(d.discover)
	return d, nil
}

func newDiscoverer(ctx context.Context, cancel context.CancelFunc, cfg *config.AzureConfig, subscriptions []string, reg prometheus.Registerer) *Discoverer {
	d := &Discoverer{
		cfg:             cfg,
		subscriptions:   subscriptions,
		ctx:             ctx,
		cancel:          cancel,
		reg:             reg,
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
		errors:          map[string]bool{},
		dbCollectors:    map[string]*DBCollector{},
		redisCollectors: map[string]*RedisCollector{},
	}
	d.monitoring = NewMonitoring()
	return d
}

// credentialSource describes the credential DefaultAzureCredential is expected to pick, without revealing any secret.
func credentialSource() string {
	switch {
	case os.Getenv("AZURE_CLIENT_SECRET") != "" || os.Getenv("AZURE_CLIENT_CERTIFICATE_PATH") != "":
		return "service principal (environment)"
	case os.Getenv("AZURE_FEDERATED_TOKEN_FILE") != "":
		return "workload identity"
	default:
		return "managed identity (DefaultAzureCredential)"
	}
}

func scope(values []string) string {
	if len(values) == 0 {
		return "all"
	}
	return strings.Join(values, ",")
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

func (d *Discoverer) Config() *config.AzureConfig {
	return d.cfg
}

// apiContext returns a context for a single Azure API call: bounded by apiTimeout and cancelled on Stop.
func (d *Discoverer) apiContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(d.ctx, apiTimeout)
}

// Stop is idempotent. It cancels the in-flight API calls, so waiting for the discovery goroutine is short.
func (d *Discoverer) Stop() {
	d.stopOnce.Do(func() {
		d.cancel()
		close(d.stop)
		<-d.done
		for id, c := range d.dbCollectors {
			prometheus.WrapRegistererWith(dbLabels(id), d.reg).Unregister(c)
		}
		for id, c := range d.redisCollectors {
			prometheus.WrapRegistererWith(redisLabels(id), d.reg).Unregister(c)
		}
		d.reg.Unregister(d)
	})
}

func (d *Discoverer) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("azure_discoverer", "", nil, nil)
}

func (d *Discoverer) Collect(ch chan<- prometheus.Metric) {
	d.errorsLock.RLock()
	defer d.errorsLock.RUnlock()
	if len(d.errors) == 0 {
		ch <- common.Gauge(dError, 0, "")
		return
	}
	for e := range d.errors {
		ch <- common.Gauge(dError, 1, e)
	}
}

func (d *Discoverer) registerError(err error) {
	if d.ctx.Err() != nil { // stopped: the errors are caused by the cancellation
		return
	}
	msg := err.Error()
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		msg = fmt.Sprintf("%s (HTTP %d)", respErr.ErrorCode, respErr.StatusCode)
	}
	d.errorsLock.Lock()
	d.errors[msg] = true
	d.errorsLock.Unlock()
}

func (d *Discoverer) discover() {
	d.discoverDBs()
	d.discoverRedis()
	if d.ctx.Err() != nil { // stopped
		return
	}
	d.publishEndpoints()
	if len(d.dbCollectors) > 0 || len(d.redisCollectors) > 0 {
		d.monitoring.refresh(d, d.monitoringTargets())
	}

	d.errorsLock.Lock()
	errs := maps.Keys(d.errors)
	d.errors = map[string]bool{}
	d.errorsLock.Unlock()
	slices.Sort(errs)
	summary := fmt.Sprintf("Azure discovery (subscriptions=%s, resource groups=%s): %d PostgreSQL/MySQL flexible servers, %d Redis caches",
		strings.Join(d.subscriptions, ","), scope(d.cfg.ResourceGroups), len(d.dbCollectors), len(d.redisCollectors))
	switch {
	case len(errs) > 0:
		klog.Errorf("%s, errors: %s", summary, strings.Join(errs, "; "))
	case summary != d.lastSummary:
		klog.Infoln(summary)
	}
	d.lastSummary = summary
}

func (d *Discoverer) publishEndpoints() {
	dbs := map[string]common.Endpoint{}
	replicas := map[string][]string{}
	for _, c := range d.dbCollectors {
		i := c.getInfo()
		if i.fqdn != "" {
			dbs[i.engine+"/"+i.name] = common.Endpoint{Host: i.fqdn, Port: i.port}
		}
		if i.primary != "" {
			replicas[i.engine+"/"+i.primary] = append(replicas[i.engine+"/"+i.primary], i.name)
		}
	}
	for k := range replicas {
		slices.Sort(replicas[k])
	}
	redis := map[string]common.Endpoint{}
	redisTLS := map[string]bool{}
	for _, c := range d.redisCollectors {
		i := c.getInfo()
		if i.host == "" {
			continue
		}
		port, tls := i.sslPort, true
		if i.nonSSLPortEnabled && i.port != 0 {
			port, tls = i.port, false
		}
		redis[i.name] = common.Endpoint{Host: i.host, Port: fmt.Sprint(port)}
		redisTLS[i.name] = tls
	}
	d.endpointsLock.Lock()
	d.dbEndpoints = dbs
	d.dbReplicas = replicas
	d.redisEndpoints = redis
	d.redisTLS = redisTLS
	d.endpointsLock.Unlock()
}

func (d *Discoverer) monitoringTargets() []monitoringTarget {
	var res []monitoringTarget
	for _, c := range d.dbCollectors {
		i := c.getInfo()
		groups := postgresMetricGroups
		if i.engine == "mysql" {
			groups = mysqlMetricGroups
		}
		for _, g := range groups {
			if g.replicaOnly && i.primary == "" {
				continue
			}
			res = append(res, monitoringTarget{resourceID: i.id, group: g})
		}
	}
	for _, c := range d.redisCollectors {
		for _, g := range redisMetricGroups {
			res = append(res, monitoringTarget{resourceID: c.getInfo().id, group: g})
		}
	}
	slices.SortFunc(res, func(a, b monitoringTarget) int { return strings.Compare(a.resourceID, b.resourceID) })
	return res
}

func dbLabels(id string) prometheus.Labels {
	return prometheus.Labels{"azure_db_id": id}
}

func redisLabels(id string) prometheus.Labels {
	return prometheus.Labels{"azure_redis_id": id}
}
