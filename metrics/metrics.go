package metrics

import (
	"context"
	"errors"
	"net"
	"slices"
	"sort"

	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/coroot/coroot-cluster-agent/metrics/aws"
	"github.com/coroot/coroot-cluster-agent/metrics/azure"
	"github.com/coroot/coroot-cluster-agent/metrics/gcp"
	"github.com/coroot/coroot-cluster-agent/metrics/ksm"
	"github.com/coroot/coroot-cluster-agent/metrics/mysql"
	"github.com/coroot/coroot-cluster-agent/metrics/oci"
	"github.com/coroot/coroot-cluster-agent/schema/emitter"
	"github.com/coroot/logger"
	"github.com/coroot/logparser"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/exp/maps"
	"k8s.io/klog"
)

const (
	ExportersRecheckInterval = 10 * time.Second
)

type Metrics struct {
	endpoint       *url.URL
	apiKey         string
	listenAddr     string
	ksmAddr        string
	scrapeInterval time.Duration
	scrapeTimeout  time.Duration
	walDir         string

	reg *prometheus.Registry

	targets     map[string]*Target
	targetsLock sync.Mutex

	aws          *aws.Discoverer
	gcp          *gcp.Discoverer
	oci          *oci.Discoverer
	azure        *azure.Discoverer
	cloudErrors  map[string]string
	k8s          *k8s.K8S
	static       *config.Static
	k8sPodEvents <-chan k8s.PodEvent
	ksm          *ksm.KSM

	changeEmitter *emitter.ChangeEmitter

	// startTarget starts the exporter of a target; replaced in tests
	startTarget func(t *Target, credentials Credentials, tlsCreds common.TLSCredentials) error

	stopCh   chan struct{}
	stopOnce sync.Once
	scraper  *scraperState
}

func NewMetrics(k8s *k8s.K8S, static *config.Static) (*Metrics, error) {
	if *flags.MetricsScrapeInterval == 0 {
		klog.Infoln("scrape interval is not set, disabling the scraper")
		return nil, nil
	}

	ms := &Metrics{
		endpoint:       (*flags.CorootURL).JoinPath("/v1/metrics"),
		apiKey:         *flags.APIKey,
		listenAddr:     *flags.ListenAddress,
		scrapeInterval: *flags.MetricsScrapeInterval,
		scrapeTimeout:  *flags.MetricsScrapeTimeout,
		walDir:         *flags.MetricsWALDir,
		reg:            prometheus.NewRegistry(),
		targets:        map[string]*Target{},
		k8s:            k8s,
		static:         static,
		stopCh:         make(chan struct{}),
	}
	ms.startTarget = ms.startTargetExporter

	var err error
	ksmAddr := *flags.KubeStateMetricsListenAddress
	if ksmAddr != "" && k8s != nil {
		ms.ksmAddr = ksmAddr
		ms.ksm, err = ksm.NewKSM(ksmAddr, *flags.KubeStateMetricsMinAge)
		if err != nil {
			return nil, err
		}
	}

	if *flags.TrackDatabaseChanges {
		ms.changeEmitter, err = emitter.NewChangeEmitter()
		if err != nil {
			return nil, err
		}
	}

	klog.Infof("endpoint: %s, scrape interval: %s", ms.endpoint, ms.scrapeInterval)

	return ms, nil
}

func (ms *Metrics) Start() error {
	go ms.discoverFromPods()
	go ms.startExporters()
	if ms.ksm != nil {
		go ms.ksm.Start()
	}
	return ms.runScraper()
}

// Stop stops discovery and scraping, closes the exporters of all targets and flushes the remote-write queue.
// It returns early if ctx is done before everything has been stopped.
func (ms *Metrics) Stop(ctx context.Context) {
	ms.stopOnce.Do(func() {
		close(ms.stopCh)
		if ms.ksm != nil {
			ms.ksm.Stop()
		}
		ms.stopScraper(ctx)
		ms.targetsLock.Lock()
		targets := maps.Values(ms.targets)
		ms.targets = map[string]*Target{}
		ms.targetsLock.Unlock()
		common.RunWithContext(ctx, "stopping exporters", func() {
			var wg sync.WaitGroup
			for _, t := range targets {
				wg.Add(1)
				go func() {
					defer wg.Done()
					t.StopExporter(ms.reg)
				}()
			}
			wg.Wait()
		})
		ms.closeStorage(ctx)
	})
}

func (ms *Metrics) ListenConfigUpdates(updates <-chan config.Config) {
	go func() {
		defer func() { // the updater has been stopped
			ms.updateAWS(nil)
			ms.updateGCP(nil)
			ms.updateOCI(nil)
			ms.updateAzure(nil)
		}()
		for cfg := range updates {
			var targets []*Target
			for _, i := range cfg.ApplicationInstrumentation {
				targets = append(targets, TargetFromConfig(i))
			}
			if ms.static != nil && ms.static.AWS != nil {
				cfg.AWSConfig = ms.static.AWS
			}
			ms.updateAWS(cfg.AWSConfig)
			if ms.static != nil {
				ms.updateGCP(ms.static.GCP)
				ms.updateOCI(ms.static.OCI)
				ms.updateAzure(ms.static.Azure)
				targets = append(targets, ms.resolveDatabases(ms.static.Databases)...)
			}
			ms.discoverFromConfig(targets)
		}
	}()
}

func (ms *Metrics) ListenPodEvents(events <-chan k8s.PodEvent) {
	ms.k8sPodEvents = events
}

// HttpHandler serves the metrics of the targets along with the agent's own metrics
// (Go runtime, process, and the scrape manager/remote-write/WAL metrics registered on the default registerer).
func (ms *Metrics) HttpHandler() http.Handler {
	return promhttp.HandlerFor(
		prometheus.Gatherers{ms.reg, prometheus.DefaultGatherer},
		promhttp.HandlerOpts{
			// a single target returning an invalid or inconsistent metric must not fail the whole response
			ErrorHandling: promhttp.ContinueOnError,
			ErrorLog:      promErrorLog{},
		},
	)
}

type promErrorLog struct{}

func (promErrorLog) Println(v ...interface{}) {
	klog.Errorln(v...)
}

func (ms *Metrics) addTarget(target *Target) {
	klog.Infof("new target: %s", target)
	ms.targetsLock.Lock()
	defer ms.targetsLock.Unlock()
	ms.targets[target.Addr] = target
}

// delTarget removes the target (if it's still the one stored for its address) and stops its exporter.
func (ms *Metrics) delTarget(target *Target) {
	klog.Infof("removing target: %s", target)
	ms.targetsLock.Lock()
	if ms.targets[target.Addr] == target {
		delete(ms.targets, target.Addr)
	}
	ms.targetsLock.Unlock()
	target.StopExporter(ms.reg)
}

func (ms *Metrics) startExporters() {
	ticker := time.NewTicker(ExportersRecheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ms.stopCh:
			return
		case <-ticker.C:
			ms.startPendingExporters()
		}
	}
}

// isCurrent reports whether t is still the active target for its address,
// i.e. it hasn't been removed or replaced by discovery.
func (ms *Metrics) isCurrent(t *Target) bool {
	ms.targetsLock.Lock()
	defer ms.targetsLock.Unlock()
	return ms.targets[t.Addr] == t
}

func (ms *Metrics) startTargetExporter(t *Target, credentials Credentials, tlsCreds common.TLSCredentials) error {
	return t.StartExporter(ms.reg, credentials, tlsCreds, ms.scrapeInterval, ms.scrapeTimeout, ms.changeEmitter, *flags.MaxTablesPerDatabase, *flags.TrackDatabaseSizes, *flags.TrackDatabaseBloat, *flags.ExcludeDatabases)
}

// startTarget starts the exporter of t unless t has been removed or replaced in the meantime.
// Starting can take a while (connecting to the database), so the target is re-checked afterward:
// if discovery removed or replaced it during the start, delTarget couldn't stop it (no exporter yet),
// so it's stopped here instead of being left running as an orphan.
func (ms *Metrics) startTargetIfCurrent(t *Target, credentials Credentials, tlsCreds common.TLSCredentials) {
	if !ms.isCurrent(t) {
		return
	}
	if err := ms.startTarget(t, credentials, tlsCreds); err != nil {
		t.logger.Errorf("failed to start exporter: %s", err)
		return
	}
	if !ms.isCurrent(t) {
		t.logger.Infof("the target has been removed or replaced while its exporter was starting, stopping it")
		t.StopExporter(ms.reg)
	}
}

type secretId struct {
	namespace, name string
}

// secretKeys returns the keys to read from each secret referenced by the targets
// (a secret can be shared by several targets and hold both the credentials and the TLS material).
func secretKeys(targets []*Target) map[secretId][]string {
	id2Keys := map[secretId][]string{}
	for _, t := range targets {
		if s := t.CredentialsSecret; s.Name != "" {
			id := secretId{namespace: s.Namespace, name: s.Name}
			for _, key := range []string{s.UsernameKey, s.PasswordKey} {
				if key != "" {
					id2Keys[id] = append(id2Keys[id], key)
				}
			}
		}
		if s := t.TLSSecret; s.Name != "" {
			id := secretId{namespace: s.Namespace, name: s.Name}
			for _, key := range []string{s.CAKey, s.CertKey, s.KeyKey} {
				if key != "" {
					id2Keys[id] = append(id2Keys[id], key)
				}
			}
		}
	}
	for id, keys := range id2Keys {
		slices.Sort(keys)
		id2Keys[id] = slices.Compact(keys)
	}
	return id2Keys
}

func (ms *Metrics) startPendingExporters() {
	ms.targetsLock.Lock()
	var targets []*Target
	for _, t := range ms.targets {
		if !t.IsExporterStarted() {
			targets = append(targets, t)
		}
	}
	ms.targetsLock.Unlock()

	if len(targets) == 0 {
		return
	}

	id2Keys := secretKeys(targets)
	var err error
	secrets := map[secretId]map[string]string{}
	var isSecretsForbidden bool
	for id, keys := range id2Keys {
		secrets[id], err = ms.k8s.GetSecret(id.namespace, id.name, keys...)
		if err != nil {
			if errors.Is(err, k8s.ErrForbidden) {
				isSecretsForbidden = true
				break
			}
			if errors.Is(err, k8s.ErrNotFound) {
				continue
			}
			klog.Errorf("failed to get secret '%s': %s", id.name, err)
			continue
		}
	}

	if isSecretsForbidden {
		klog.Errorln("Cannot retrieve secrets: access forbidden. Update Coroot Operator to proceed.")
	}

	for _, t := range targets {
		credentials := t.Credentials
		if s := t.CredentialsSecret; s.Name != "" {
			kv := secrets[secretId{namespace: s.Namespace, name: s.Name}]
			switch {
			case isSecretsForbidden:
				t.logger.Errorf("failed to start exporter: secret '%s' forbidden", s.Name)
				continue
			case kv == nil:
				t.logger.Errorf("failed to start exporter: secret '%s' not found", s.Name)
				continue
			default:
				if username := kv[s.UsernameKey]; username != "" {
					credentials.Username = username
				}
				if password := kv[s.PasswordKey]; password != "" {
					credentials.Password = password
				}
			}
		}
		var tlsCreds common.TLSCredentials
		if s := t.TLSSecret; s.Name != "" {
			kv := secrets[secretId{namespace: s.Namespace, name: s.Name}]
			switch {
			case isSecretsForbidden:
				t.logger.Errorf("failed to start exporter: secret '%s' forbidden", s.Name)
				continue
			case kv == nil:
				t.logger.Errorf("failed to start exporter: TLS secret '%s' not found", s.Name)
				continue
			default:
				if (s.CertKey != "") != (s.KeyKey != "") {
					t.logger.Errorf("failed to start exporter: TLS secret '%s': the cert and key keys must be set together", s.Name)
					continue
				}
				if s.CAKey == "" && s.CertKey == "" {
					t.logger.Errorf("failed to start exporter: TLS secret '%s': no keys specified (set the ca-key and/or cert-key/key-key annotations)", s.Name)
					continue
				}
				if s.CAKey != "" {
					tlsCreds.CA = kv[s.CAKey]
				}
				if s.CertKey != "" {
					tlsCreds.Cert = kv[s.CertKey]
					tlsCreds.Key = kv[s.KeyKey]
				}
				if tlsCreds.CA == "" && (tlsCreds.Cert == "" || tlsCreds.Key == "") {
					t.logger.Errorf("failed to start exporter: TLS secret '%s' does not contain the specified keys", s.Name)
					continue
				}
			}
		}
		ms.startTargetIfCurrent(t, credentials, tlsCreds)
	}
}

func (ms *Metrics) updateAWS(cfg *config.AWSConfig) {
	switch {
	case cfg == nil && ms.aws == nil:
	case cfg == nil && ms.aws != nil:
		ms.aws.Stop()
		ms.aws = nil
	case cfg != nil && ms.aws == nil:
		d, err := aws.NewDiscoverer(cfg, ms.k8s, ms.reg)
		if ms.logCloudError("aws", err) {
			ms.aws = d
		}
	default:
		err := ms.aws.Update(cfg)
		if err != nil {
			klog.Errorln(err)
			ms.aws.Stop()
			ms.aws = nil
		}
	}
}

func (ms *Metrics) updateGCP(cfg *config.GCPConfig) {
	if ms.gcp != nil && (cfg == nil || !ms.gcp.Config().Equal(cfg)) { // recreated on a change: rare, and simpler than reconfiguring
		ms.gcp.Stop()
		ms.gcp = nil
	}
	if cfg != nil && ms.gcp == nil {
		if d, err := gcp.NewDiscoverer(cfg, ms.k8s, ms.reg); ms.logCloudError("gcp", err) {
			ms.gcp = d
		}
	}
}

func (ms *Metrics) updateOCI(cfg *config.OCIConfig) {
	if ms.oci != nil && (cfg == nil || !ms.oci.Config().Equal(cfg)) {
		ms.oci.Stop()
		ms.oci = nil
	}
	if cfg != nil && ms.oci == nil {
		if d, err := oci.NewDiscoverer(cfg, ms.k8s, ms.reg, ms.ociLogCounters); ms.logCloudError("oci", err) {
			ms.oci = d
		}
	}
}

func (ms *Metrics) updateAzure(cfg *config.AzureConfig) {
	if ms.azure != nil && (cfg == nil || !ms.azure.Config().Equal(cfg)) { // recreated on a change, as GCP and OCI
		ms.azure.Stop()
		ms.azure = nil
	}
	if cfg != nil && ms.azure == nil {
		if d, err := azure.NewDiscoverer(cfg, ms.reg); ms.logCloudError("azure", err) {
			ms.azure = d
		}
	}
}

// withDefaultParam returns params with key set to value unless it's already set (the TLS mode of the managed databases
// that require TLS: their endpoints are resolved to IP addresses, so the certificates can't be verified against the host names).
func withDefaultParam(params map[string]string, key, value string) map[string]string {
	if params[key] != "" {
		return params
	}
	res := make(map[string]string, len(params)+1)
	for k, v := range params {
		res[k] = v
	}
	res[key] = value
	return res
}

func (ms *Metrics) logCloudError(cloud string, err error) bool {
	if ms.cloudErrors == nil {
		ms.cloudErrors = map[string]string{}
	}
	if err == nil {
		delete(ms.cloudErrors, cloud)
		return true
	}
	if ms.cloudErrors[cloud] != err.Error() {
		klog.Errorln(err)
		ms.cloudErrors[cloud] = err.Error()
	}
	return false
}

func (ms *Metrics) discoverFromPods() {
	for e := range ms.k8sPodEvents {
		ms.handlePodEvent(e)
	}
}

func (ms *Metrics) handlePodEvent(e k8s.PodEvent) {
	switch e.Type {
	case k8s.PodEventTypeAdd, k8s.PodEventTypeChange:
		target := TargetFromPod(e.Pod)
		old := TargetFromPod(e.Old)
		if target == nil {
			if old != nil {
				ms.delPodTarget(old)
			}
			return
		}
		if old != nil && old.Addr != target.Addr { // e.g. the pod IP has changed
			ms.delPodTarget(old)
		}
		ms.targetsLock.Lock()
		t := ms.targets[target.Addr]
		ms.targetsLock.Unlock()
		switch {
		case t == nil:
			ms.addTarget(target)
		case t.Equal(target) && t.podKey == target.podKey:
			return
		default: // a changed target, or another pod (or a configured target) that had the same address
			ms.delTarget(t)
			ms.addTarget(target)
		}
	case k8s.PodEventTypeDelete:
		target := TargetFromPod(e.Pod)
		if target == nil {
			return
		}
		ms.delPodTarget(target)
	}
}

// delPodTarget removes the target stored for target.Addr only if it was discovered from the same pod,
// so that the removal of a pod doesn't remove the target of another pod that has reused its IP address.
func (ms *Metrics) delPodTarget(target *Target) {
	ms.targetsLock.Lock()
	t := ms.targets[target.Addr]
	if t == nil || !t.DiscoveredFromPodAnnotations || t.podKey != target.podKey {
		ms.targetsLock.Unlock()
		return
	}
	delete(ms.targets, target.Addr)
	ms.targetsLock.Unlock()
	klog.Infof("removing target: %s", t)
	t.StopExporter(ms.reg)
}

func (ms *Metrics) resolveDatabases(databases []config.Database) []*Target {
	var res []*Target
	for _, d := range databases {
		var endpoints []common.Endpoint
		var description string
		switch {
		case d.RDS != "":
			description = "rds:" + d.RDS
			if ms.aws == nil {
				klog.Warningf("%s: the AWS integration is not configured, skipping", description)
				continue
			}
			e, ok := ms.aws.RDSEndpoint(d.RDS)
			if !ok {
				klog.Warningf("%s: the RDS instance is not discovered (yet), skipping", description)
				continue
			}
			endpoints = []common.Endpoint{e}
			for _, replica := range ms.aws.RDSReplicas(d.RDS) {
				if e, ok := ms.aws.RDSEndpoint(replica); ok {
					res = append(res, ms.databaseTargets(d, "rds:"+replica, []common.Endpoint{e})...)
				}
			}
		case d.Elasticache != "":
			description = "elasticache:" + d.Elasticache
			if ms.aws == nil {
				klog.Warningf("%s: the AWS integration is not configured, skipping", description)
				continue
			}
			endpoints = ms.aws.ElastiCacheEndpoints(d.Elasticache)
			if len(endpoints) == 0 {
				klog.Warningf("%s: the ElastiCache cluster is not discovered (yet), skipping", description)
				continue
			}
			if ms.aws.ElastiCacheRequiresTLS(d.Elasticache) { // ElastiCache Serverless
				d.Params = withDefaultParam(d.Params, "tls", "skip-verify")
			}
		case d.MemoryDB != "":
			description = "memorydb:" + d.MemoryDB
			if ms.aws == nil {
				klog.Warningf("%s: the AWS integration is not configured, skipping", description)
				continue
			}
			var tls bool
			endpoints, tls = ms.aws.MemoryDBEndpoints(d.MemoryDB)
			if len(endpoints) == 0 {
				klog.Warningf("%s: the MemoryDB cluster is not discovered (yet), skipping", description)
				continue
			}
			if tls {
				d.Params = withDefaultParam(d.Params, "tls", "skip-verify")
			}
		case d.CloudSQL != "":
			description = "cloudsql:" + d.CloudSQL
			if ms.gcp == nil {
				klog.Warningf("%s: the GCP integration is not configured, skipping", description)
				continue
			}
			e, ok := ms.gcp.CloudSQLEndpoint(d.CloudSQL)
			if !ok {
				klog.Warningf("%s: the Cloud SQL instance is not discovered (yet), skipping", description)
				continue
			}
			endpoints = []common.Endpoint{e}
			for _, replica := range ms.gcp.CloudSQLReplicas(d.CloudSQL) {
				if e, ok := ms.gcp.CloudSQLEndpoint(replica); ok {
					res = append(res, ms.databaseTargets(d, "cloudsql:"+replica, []common.Endpoint{e})...)
				}
			}
		case d.Memorystore != "":
			description = "memorystore:" + d.Memorystore
			if ms.gcp == nil {
				klog.Warningf("%s: the GCP integration is not configured, skipping", description)
				continue
			}
			endpoints = ms.gcp.MemorystoreEndpoints(d.Memorystore)
			if len(endpoints) == 0 {
				klog.Warningf("%s: the Memorystore instance is not discovered (yet), skipping", description)
				continue
			}
		case d.OCIDB != "":
			description = "ocidb:" + d.OCIDB
			if ms.oci == nil {
				klog.Warningf("%s: the OCI integration is not configured, skipping", description)
				continue
			}
			if _, ok := ms.oci.DBEndpoint(d.OCIDB); !ok {
				klog.Warningf("%s: the DB system is not discovered (yet), skipping", description)
				continue
			}
			for _, name := range append([]string{d.OCIDB}, ms.oci.DBReplicas(d.OCIDB)...) { // read replicas and standby instances share the credentials
				if e, ok := ms.oci.DBEndpoint(name); ok {
					targets := ms.databaseTargets(d, "ocidb:"+name, []common.Endpoint{e})
					for _, t := range targets {
						t.LogService = ms.oci.DBLogService(name)
					}
					res = append(res, targets...)
				}
			}
			continue
		case d.OCICache != "":
			description = "ocicache:" + d.OCICache
			if ms.oci == nil {
				klog.Warningf("%s: the OCI integration is not configured, skipping", description)
				continue
			}
			e, ok := ms.oci.CacheEndpoint(d.OCICache)
			if !ok {
				klog.Warningf("%s: the cache cluster is not discovered (yet), skipping", description)
				continue
			}
			endpoints = []common.Endpoint{e}
		case d.AzureDB != "":
			description = "azuredb:" + d.AzureDB
			if ms.azure == nil {
				klog.Warningf("%s: the Azure integration is not configured, skipping", description)
				continue
			}
			e, ok := ms.azure.DBEndpoint(d.Type, d.AzureDB)
			if !ok {
				klog.Warningf("%s: the %s flexible server is not discovered (yet), skipping", description, d.Type)
				continue
			}
			// the flexible servers require TLS by default (require_secure_transport)
			if d.Type == "postgres" {
				d.Params = withDefaultParam(d.Params, "sslmode", "require")
			} else {
				d.Params = withDefaultParam(d.Params, "tls", "skip-verify")
			}
			endpoints = []common.Endpoint{e}
			for _, replica := range ms.azure.DBReplicas(d.Type, d.AzureDB) {
				if e, ok := ms.azure.DBEndpoint(d.Type, replica); ok {
					res = append(res, ms.databaseTargets(d, "azuredb:"+replica, []common.Endpoint{e})...)
				}
			}
		case d.AzureRedis != "":
			description = "azureredis:" + d.AzureRedis
			if ms.azure == nil {
				klog.Warningf("%s: the Azure integration is not configured, skipping", description)
				continue
			}
			e, tls, ok := ms.azure.RedisEndpoint(d.AzureRedis)
			if !ok {
				klog.Warningf("%s: the cache is not discovered (yet), skipping", description)
				continue
			}
			if tls {
				d.Params = withDefaultParam(d.Params, "tls", "skip-verify")
			}
			endpoints = []common.Endpoint{e}
		default:
			description = d.Host
			endpoints = []common.Endpoint{{Host: d.Host, Port: d.Port}}
		}
		res = append(res, ms.databaseTargets(d, description, endpoints)...)
	}
	return res
}

func (ms *Metrics) ociLogCounters(name string) []logparser.LogCounter {
	ms.targetsLock.Lock()
	defer ms.targetsLock.Unlock()
	for _, t := range ms.targets {
		if t.Description != "ocidb:"+name {
			continue
		}
		if c, ok := t.collector().(*mysql.Collector); ok {
			return c.ErrorLogCounters()
		}
	}
	return nil
}

func (ms *Metrics) databaseTargets(d config.Database, description string, endpoints []common.Endpoint) []*Target {
	var res []*Target
	for _, e := range endpoints {
		port := e.Port
		if d.Port != "" {
			port = d.Port
		}
		for _, ip := range resolveHost(e.Host) {
			res = append(res, TargetFromConfig(config.ApplicationInstrumentation{
				Type:        d.Type,
				Host:        ip,
				Port:        port,
				Credentials: d.Credentials,
				Params:      d.Params,
				Instance:    description,
			}))
		}
	}
	return res
}

func resolveHost(host string) []string {
	if ip := net.ParseIP(host); ip != nil {
		return []string{host}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		klog.Warningf("failed to resolve %s: %s", host, err)
		return nil
	}
	ips := make([]string, 0, len(addrs))
	for _, a := range addrs {
		ips = append(ips, a.IP.String())
	}
	sort.Strings(ips)
	return ips
}

func (ms *Metrics) discoverFromConfig(targets []*Target) {
	actual := map[string]bool{}
	for _, target := range targets {
		actual[target.Addr] = true
		ms.targetsLock.Lock()
		t := ms.targets[target.Addr]
		ms.targetsLock.Unlock()
		switch {
		case t == nil:
			ms.addTarget(target)
		case t.DiscoveredFromPodAnnotations:
			continue
		case t.Equal(target):
			continue
		default:
			ms.delTarget(t)
			ms.addTarget(target)
		}
	}
	ms.targetsLock.Lock()
	existing := maps.Values(ms.targets)
	ms.targetsLock.Unlock()
	for _, t := range existing {
		if !actual[t.Addr] && !t.DiscoveredFromPodAnnotations {
			ms.delTarget(t)
		}
	}
}

type promLogger struct {
	l logger.Logger
}

func (l *promLogger) Log(keyvals ...interface{}) error {
	l.l.Info(keyvals...)
	return nil
}
