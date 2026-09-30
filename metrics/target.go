package metrics

import (
	"cmp"
	"fmt"
	"maps"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/coroot/coroot-cluster-agent/metrics/clickhouse"
	"github.com/coroot/coroot-cluster-agent/metrics/elasticsearch"
	"github.com/coroot/coroot-cluster-agent/metrics/mongo"
	"github.com/coroot/coroot-cluster-agent/metrics/mysql"
	"github.com/coroot/coroot-cluster-agent/metrics/postgres"
	"github.com/coroot/coroot-cluster-agent/schema/emitter"
	"github.com/coroot/logger"
	"github.com/go-kit/log/level"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	redis "github.com/oliver006/redis_exporter/exporter"
	"github.com/prometheus/client_golang/prometheus"
	memcached "github.com/prometheus/memcached_exporter/pkg/exporter"
	"k8s.io/klog"
)

type TargetType string

const (
	TargetTypePostgres  TargetType = "postgres"
	TargetTypeMysql     TargetType = "mysql"
	TargetTypeRedis     TargetType = "redis"
	TargetTypeMongodb   TargetType = "mongodb"
	TargetTypeMemcached TargetType = "memcached"

	TargetTypeClickhouse    TargetType = "clickhouse"
	TargetTypeElasticsearch TargetType = "elasticsearch"
	TargetTypeOpensearch    TargetType = "opensearch" // an alias of elasticsearch: the same collector and metrics
)

type Credentials struct {
	Username string
	Password string
}

type CredentialsSecret struct {
	Namespace   string
	Name        string
	UsernameKey string
	PasswordKey string
}

type TLSSecret struct {
	Namespace string
	Name      string
	CAKey     string
	CertKey   string
	KeyKey    string
}

type Target struct {
	Type              TargetType
	Addr              string
	Credentials       Credentials
	CredentialsSecret CredentialsSecret
	TLSSecret         TLSSecret
	Params            map[string]string

	Description                  string
	LogService                   string // the service the logs read from the database server (e.g. the MySQL error log) are forwarded as, if any
	DiscoveredFromPodAnnotations bool

	// podKey identifies the pod the target was discovered from (UID, or namespace/name as a fallback);
	// empty for targets that don't come from pod annotations.
	podKey string

	coll           prometheus.Collector
	stop           func()
	collectTimeout time.Duration
	collLock       sync.Mutex

	collecting atomic.Bool   // a collection (possibly an abandoned one that outlived its deadline) is in flight
	timeouts   atomic.Uint64 // collections abandoned at the deadline or skipped because the previous one was still running

	logger logger.Logger
}

func (t *Target) collector() prometheus.Collector {
	t.collLock.Lock()
	defer t.collLock.Unlock()
	return t.coll
}

func (t *Target) Equal(other *Target) bool {
	return t.Type == other.Type &&
		t.Addr == other.Addr &&
		t.Credentials == other.Credentials &&
		t.CredentialsSecret == other.CredentialsSecret &&
		t.TLSSecret == other.TLSSecret &&
		t.LogService == other.LogService &&
		maps.Equal(t.Params, other.Params)
}

func (t *Target) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("exporter", "", nil, nil)
}

var (
	targetCollectDurationDesc = prometheus.NewDesc(
		"coroot_cluster_agent_target_collect_duration_seconds",
		"Duration of the last metrics collection from the database target",
		[]string{"target_type"}, nil,
	)
	targetCollectSuccessDesc = prometheus.NewDesc(
		"coroot_cluster_agent_target_collect_success",
		"Whether the last metrics collection from the database target completed within the deadline (1) or not (0)",
		[]string{"target_type"}, nil,
	)
	targetCollectTimeoutsDesc = prometheus.NewDesc(
		"coroot_cluster_agent_target_collect_timeouts_total",
		"Total number of metrics collections from the database target that were abandoned because they exceeded the deadline",
		[]string{"target_type"}, nil,
	)
)

const collectBufferSize = 1024

// Collect runs the collector of the target with a deadline, so that a slow or unresponsive database
// cannot hold up the whole /metrics response (and thus the metrics of all other targets).
// Metrics produced after the deadline are dropped; the abandoned collection keeps running in the background
// until it finishes on its own, and no new collection is started for the target until then.
func (t *Target) Collect(ch chan<- prometheus.Metric) {
	t.collLock.Lock()
	coll, timeout := t.coll, t.collectTimeout
	t.collLock.Unlock()
	if coll == nil {
		return
	}
	start := time.Now()
	success := collectWithDeadline(coll, ch, timeout, &t.collecting)
	duration := time.Since(start)
	if success {
		klog.V(2).Infof("%s: metrics collection completed in %s", t, duration.Truncate(time.Millisecond))
	} else {
		t.timeouts.Add(1)
		t.logger.Warningf("metrics collection did not complete within %s, the metrics of this target are skipped", timeout)
	}
	tt := string(t.Type)
	ch <- prometheus.MustNewConstMetric(targetCollectDurationDesc, prometheus.GaugeValue, duration.Seconds(), tt)
	ch <- prometheus.MustNewConstMetric(targetCollectSuccessDesc, prometheus.GaugeValue, boolToFloat(success), tt)
	ch <- prometheus.MustNewConstMetric(targetCollectTimeoutsDesc, prometheus.CounterValue, float64(t.timeouts.Load()), tt)
}

// collectWithDeadline forwards the metrics of coll to ch until coll.Collect returns or the timeout expires.
// It returns false if the collection was abandoned at the deadline or was not started because a previous
// collection (tracked by inFlight) is still running.
func collectWithDeadline(coll prometheus.Collector, ch chan<- prometheus.Metric, timeout time.Duration, inFlight *atomic.Bool) bool {
	if !inFlight.CompareAndSwap(false, true) {
		return false
	}
	buf := make(chan prometheus.Metric, collectBufferSize)
	go func() {
		defer inFlight.Store(false)
		defer close(buf)
		coll.Collect(buf)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case m, ok := <-buf:
			if !ok {
				return true
			}
			ch <- m
		case <-timer.C:
			go func() { // let the abandoned collection finish, discarding its metrics
				for range buf {
				}
			}()
			return false
		}
	}
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func (t *Target) Labels() prometheus.Labels {
	return prometheus.Labels{"address": t.Addr}
}

func (t *Target) String() string {
	return fmt.Sprintf("%s://%s (%s)", t.Type, t.Addr, t.Description)
}

func (t *Target) IsExporterStarted() bool {
	return t.collector() != nil
}

func (t *Target) StartExporter(reg prometheus.Registerer, credentials Credentials, tlsCreds common.TLSCredentials, scrapeInterval, scrapeTimeout time.Duration, changeEmitter *emitter.ChangeEmitter, maxTablesPerDB int, trackSizes, trackBloat bool, excludeDatabases []string) error {
	collectTimeout := scrapeTimeout - time.Second
	if collectTimeout <= 0 {
		collectTimeout = time.Second
	}
	coll, stop, err := t.newCollector(credentials, tlsCreds, scrapeInterval, collectTimeout, changeEmitter, maxTablesPerDB, trackSizes, trackBloat, excludeDatabases)
	if err != nil {
		return err
	}
	return t.activate(reg, coll, stop, collectTimeout)
}

// activate registers the target and only then makes the collector visible to Collect and StopExporter.
// If the registration fails (e.g. another target with the same address is still registered),
// the collector is stopped so that no connection pools or background goroutines are leaked,
// and the target stays not-started so it is retried later.
func (t *Target) activate(reg prometheus.Registerer, coll prometheus.Collector, stop func(), collectTimeout time.Duration) error {
	if err := prometheus.WrapRegistererWith(t.Labels(), reg).Register(t); err != nil {
		stop()
		return err
	}
	t.collLock.Lock()
	t.coll = coll
	t.stop = stop
	t.collectTimeout = collectTimeout
	t.collLock.Unlock()
	return nil
}

var tlsConfigSeq atomic.Uint64

// tlsConfigName returns a name for a driver-global TLS config that is unique per exporter start,
// so that stopping an old exporter of the same address can't deregister the config of a new one.
func (t *Target) tlsConfigName() string {
	return fmt.Sprintf("coroot-%s-%d", t.Addr, tlsConfigSeq.Add(1))
}

func (t *Target) newCollector(credentials Credentials, tlsCreds common.TLSCredentials, scrapeInterval, collectTimeout time.Duration, changeEmitter *emitter.ChangeEmitter, maxTablesPerDB int, trackSizes, trackBloat bool, excludeDatabases []string) (prometheus.Collector, func(), error) {
	caCert := tlsCreds.CA
	switch t.Type {

	case TargetTypePostgres:
		userPass := url.UserPassword(credentials.Username, credentials.Password)
		query := url.Values{}
		query.Set("connect_timeout", "1")
		query.Set("statement_timeout", strconv.Itoa(int(collectTimeout.Milliseconds())))
		sslmode := t.Params["sslmode"]
		pqTLSName := ""
		if tlsCreds.CA != "" || (tlsCreds.Cert != "" && tlsCreds.Key != "") {
			cfg, err := common.DatabaseTLSConfig(tlsCreds, false)
			if err != nil {
				return nil, nil, err
			}
			pqTLSName = t.tlsConfigName()
			if err = pq.RegisterTLSConfig(pqTLSName, cfg); err != nil {
				return nil, nil, err
			}
			sslmode = "pqgo-" + pqTLSName
		}
		if sslmode == "" {
			sslmode = "disable"
		}
		query.Set("sslmode", sslmode)
		dsn := fmt.Sprintf("postgresql://%s@%s/postgres?%s", userPass, t.Addr, query.Encode())
		collector, err := postgres.New(dsn, scrapeInterval, collectTimeout, t.logger, changeEmitter, t.Addr, maxTablesPerDB, trackSizes, trackBloat, excludeDatabases)
		if err != nil {
			if pqTLSName != "" {
				_ = pq.RegisterTLSConfig(pqTLSName, nil)
			}
			return nil, nil, err
		}
		return collector, func() {
			_ = collector.Close()
			if pqTLSName != "" {
				_ = pq.RegisterTLSConfig(pqTLSName, nil)
			}
		}, nil

	case TargetTypeMysql:
		tlsParam := t.Params["tls"]
		tlsConfigName := ""
		if (caCert != "" || (tlsCreds.Cert != "" && tlsCreds.Key != "")) && tlsParam != "false" {
			cfg, err := common.DatabaseTLSConfig(tlsCreds, tlsParam == "skip-verify")
			if err != nil {
				return nil, nil, err
			}
			tlsConfigName = t.tlsConfigName()
			if err = gomysql.RegisterTLSConfig(tlsConfigName, cfg); err != nil {
				return nil, nil, err
			}
			tlsParam = tlsConfigName
		}
		if tlsParam == "" {
			tlsParam = "false"
		}
		dsn, err := mysqlDSN(credentials, t.Addr, collectTimeout, tlsParam)
		if err != nil {
			if tlsConfigName != "" {
				gomysql.DeregisterTLSConfig(tlsConfigName)
			}
			return nil, nil, err
		}
		collector, err := mysql.New(dsn, t.logger, scrapeInterval, collectTimeout,
			changeEmitter, t.Addr, maxTablesPerDB, trackSizes, excludeDatabases)
		if err != nil {
			if tlsConfigName != "" {
				gomysql.DeregisterTLSConfig(tlsConfigName)
			}
			return nil, nil, err
		}
		if t.LogService != "" {
			collector.StartErrorLog(t.LogService, t.Description)
		}
		return collector, func() {
			_ = collector.Close()
			if tlsConfigName != "" {
				gomysql.DeregisterTLSConfig(tlsConfigName)
			}
		}, nil

	case TargetTypeRedis:
		opts := redis.Options{
			User:                           credentials.Username,
			Password:                       credentials.Password,
			Namespace:                      "redis",
			ConnectionTimeouts:             collectTimeout,
			RedisMetricsOnly:               true,
			ExcludeLatencyHistogramMetrics: true,
		}
		if strings.Contains(t.Description, "elasticache:") || strings.Contains(t.Description, "memorystore:") || strings.Contains(t.Description, "ocicache:") { // managed services don't allow the CONFIG command
			opts.ConfigCommandName = "-"
		}
		tls := t.Params["tls"]
		if tls == "" && strings.Contains(t.Description, "ocicache:") {
			tls = "skip-verify"
		}
		scheme := "redis"
		if tls != "" {
			scheme = "rediss"
			opts.SkipTLSVerification = tls == "skip-verify"
		}
		dsn := fmt.Sprintf("%s://%s", scheme, t.Addr)
		collector, err := redis.NewRedisExporter(dsn, opts)
		if err != nil {
			return nil, nil, err
		}
		return collector, func() {}, nil

	case TargetTypeMongodb:
		collector := mongo.New(
			t.Addr,
			credentials.Username,
			credentials.Password,
			tlsCreds,
			t.Params,
			scrapeInterval,
			collectTimeout,
			t.logger,
			changeEmitter,
			t.Addr,
			maxTablesPerDB,
			trackSizes,
		)
		return collector, func() { _ = collector.Close() }, nil

	case TargetTypeMemcached:
		collector := memcached.New(
			t.Addr,
			collectTimeout,
			level.NewFilter(&promLogger{l: t.logger}, level.AllowInfo()),
			nil,
		)
		return collector, func() {}, nil

	case TargetTypeClickhouse:
		collector, err := clickhouse.New(t.Addr, credentials.Username, credentials.Password, tlsCreds, t.Params,
			scrapeInterval, collectTimeout, t.logger, excludeDatabases)
		if err != nil {
			return nil, nil, err
		}
		return collector, func() { _ = collector.Close() }, nil

	case TargetTypeElasticsearch, TargetTypeOpensearch:
		collector, err := elasticsearch.New(t.Addr, credentials.Username, credentials.Password, tlsCreds, t.Params,
			scrapeInterval, collectTimeout, t.logger)
		if err != nil {
			return nil, nil, err
		}
		return collector, func() { _ = collector.Close() }, nil
	}
	return nil, nil, fmt.Errorf("unsupported target type: %s", t.Type)
}

// mysqlDSN builds the DSN with the driver's own formatter, so that credentials containing
// special characters in the password ('@', ':', '/', ...) and '@' in the username are handled correctly.
// The DSN format can't represent a username containing ':' (the driver splits user:password at the first ':').
func mysqlDSN(credentials Credentials, addr string, timeout time.Duration, tlsParam string) (string, error) {
	if strings.Contains(credentials.Username, ":") {
		return "", fmt.Errorf("mysql usernames containing ':' are not supported")
	}
	cfg := gomysql.NewConfig()
	cfg.User = credentials.Username
	cfg.Passwd = credentials.Password
	cfg.Net = "tcp"
	cfg.Addr = addr
	cfg.Timeout = timeout
	cfg.TLSConfig = tlsParam
	return cfg.FormatDSN(), nil
}

// StopExporter unregisters the target and stops its collector. It is idempotent and safe to call concurrently
// with StartExporter: only a successfully registered exporter is ever visible here.
func (t *Target) StopExporter(reg prometheus.Registerer) {
	t.collLock.Lock()
	coll, stop := t.coll, t.stop
	t.coll, t.stop = nil, nil
	t.collLock.Unlock()
	if coll == nil {
		return
	}
	prometheus.WrapRegistererWith(t.Labels(), reg).Unregister(t)
	if stop != nil {
		stop()
	}
}

func TargetFromConfig(i config.ApplicationInstrumentation) *Target {
	t := &Target{
		Type: TargetType(i.Type),
		Addr: net.JoinHostPort(i.Host, i.Port),
		Credentials: Credentials{
			Username: i.Credentials.Username,
			Password: i.Credentials.Password,
		},
		Params:      i.Params,
		Description: i.Instance,
	}
	t.logger = logger.NewKlog(t.String())
	return t
}

func TargetFromPod(pod *k8s.Pod) *Target {
	if pod == nil || pod.Annotations == nil || pod.IP == "" {
		return nil
	}

	var t *Target

	if pod.Annotations["coroot.com/postgres-scrape"] == "true" {
		t = &Target{
			Type: TargetTypePostgres,
			Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations["coroot.com/postgres-scrape-port"], "5432")),
			Credentials: Credentials{
				Username: pod.Annotations["coroot.com/postgres-scrape-credentials-username"],
				Password: pod.Annotations["coroot.com/postgres-scrape-credentials-password"],
			},
			CredentialsSecret: CredentialsSecret{
				Namespace:   pod.Id.Namespace,
				Name:        pod.Annotations["coroot.com/postgres-scrape-credentials-secret-name"],
				UsernameKey: pod.Annotations["coroot.com/postgres-scrape-credentials-secret-username-key"],
				PasswordKey: pod.Annotations["coroot.com/postgres-scrape-credentials-secret-password-key"],
			},
			TLSSecret: tlsSecretFromPod(pod, "postgres"),
			Params: map[string]string{
				"sslmode": pod.Annotations["coroot.com/postgres-scrape-param-sslmode"],
			},
		}
	}

	if pod.Annotations["coroot.com/mysql-scrape"] == "true" {
		t = &Target{
			Type: TargetTypeMysql,
			Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations["coroot.com/mysql-scrape-port"], "3306")),
			Credentials: Credentials{
				Username: pod.Annotations["coroot.com/mysql-scrape-credentials-username"],
				Password: pod.Annotations["coroot.com/mysql-scrape-credentials-password"],
			},
			CredentialsSecret: CredentialsSecret{
				Namespace:   pod.Id.Namespace,
				Name:        pod.Annotations["coroot.com/mysql-scrape-credentials-secret-name"],
				UsernameKey: pod.Annotations["coroot.com/mysql-scrape-credentials-secret-username-key"],
				PasswordKey: pod.Annotations["coroot.com/mysql-scrape-credentials-secret-password-key"],
			},
			TLSSecret: tlsSecretFromPod(pod, "mysql"),
			Params: map[string]string{
				"tls": pod.Annotations["coroot.com/mysql-scrape-param-tls"],
			},
		}
	}

	if pod.Annotations["coroot.com/redis-scrape"] == "true" {
		t = &Target{
			Type: TargetTypeRedis,
			Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations["coroot.com/redis-scrape-port"], "6379")),
			Credentials: Credentials{
				Username: pod.Annotations["coroot.com/redis-scrape-credentials-username"],
				Password: pod.Annotations["coroot.com/redis-scrape-credentials-password"],
			},
			CredentialsSecret: CredentialsSecret{
				Namespace:   pod.Id.Namespace,
				Name:        pod.Annotations["coroot.com/redis-scrape-credentials-secret-name"],
				UsernameKey: pod.Annotations["coroot.com/redis-scrape-credentials-secret-username-key"],
				PasswordKey: pod.Annotations["coroot.com/redis-scrape-credentials-secret-password-key"],
			},
		}
	}

	if pod.Annotations["coroot.com/mongodb-scrape"] == "true" {
		t = &Target{
			Type: TargetTypeMongodb,
			Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations["coroot.com/mongodb-scrape-port"], "27017")),
			Credentials: Credentials{
				Username: pod.Annotations["coroot.com/mongodb-scrape-credentials-username"],
				Password: pod.Annotations["coroot.com/mongodb-scrape-credentials-password"],
			},
			CredentialsSecret: CredentialsSecret{
				Namespace:   pod.Id.Namespace,
				Name:        pod.Annotations["coroot.com/mongodb-scrape-credentials-secret-name"],
				UsernameKey: pod.Annotations["coroot.com/mongodb-scrape-credentials-secret-username-key"],
				PasswordKey: pod.Annotations["coroot.com/mongodb-scrape-credentials-secret-password-key"],
			},
			TLSSecret: tlsSecretFromPod(pod, "mongodb"),
			Params: map[string]string{
				"tls":        pod.Annotations["coroot.com/mongodb-scrape-param-tls"],
				"authSource": pod.Annotations["coroot.com/mongodb-scrape-param-auth-source"],
			},
		}
	}

	if pod.Annotations["coroot.com/memcached-scrape"] == "true" {
		t = &Target{
			Type: TargetTypeMemcached,
			Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations["coroot.com/memcached-scrape-port"], "11211")),
		}
	}

	if pod.Annotations["coroot.com/clickhouse-scrape"] == "true" {
		t = targetFromPodAnnotations(pod, TargetTypeClickhouse, "9000", "protocol", "tls")
	}

	if pod.Annotations["coroot.com/elasticsearch-scrape"] == "true" {
		t = targetFromPodAnnotations(pod, TargetTypeElasticsearch, "9200", "tls", "nodes")
	}

	if t != nil {
		t.DiscoveredFromPodAnnotations = true
		t.podKey = pod.Key()
		t.Description = fmt.Sprintf("ns=%s, pod=%s, node=%s", pod.Id.Namespace, pod.Id.Name, pod.Id.NodeName)
		t.logger = logger.NewKlog(t.String())
	}

	return t
}

// targetFromPodAnnotations builds a target from the generic coroot.com/<type>-scrape-* annotations:
// port, credentials (plain or from a secret), TLS secret and the given params (coroot.com/<type>-scrape-param-<name>).
func targetFromPodAnnotations(pod *k8s.Pod, targetType TargetType, defaultPort string, params ...string) *Target {
	prefix := "coroot.com/" + string(targetType) + "-scrape-"
	t := &Target{
		Type: targetType,
		Addr: net.JoinHostPort(pod.IP, cmp.Or(pod.Annotations[prefix+"port"], defaultPort)),
		Credentials: Credentials{
			Username: pod.Annotations[prefix+"credentials-username"],
			Password: pod.Annotations[prefix+"credentials-password"],
		},
		CredentialsSecret: CredentialsSecret{
			Namespace:   pod.Id.Namespace,
			Name:        pod.Annotations[prefix+"credentials-secret-name"],
			UsernameKey: pod.Annotations[prefix+"credentials-secret-username-key"],
			PasswordKey: pod.Annotations[prefix+"credentials-secret-password-key"],
		},
		TLSSecret: tlsSecretFromPod(pod, string(targetType)),
		Params:    map[string]string{},
	}
	for _, p := range params {
		if v := pod.Annotations[prefix+"param-"+p]; v != "" {
			t.Params[p] = v
		}
	}
	return t
}

func tlsSecretFromPod(pod *k8s.Pod, targetType string) TLSSecret {
	name := pod.Annotations["coroot.com/"+targetType+"-scrape-tls-secret-name"]
	if name == "" {
		return TLSSecret{}
	}
	return TLSSecret{
		Namespace: pod.Id.Namespace,
		Name:      name,
		CAKey:     pod.Annotations["coroot.com/"+targetType+"-scrape-tls-secret-ca-key"],
		CertKey:   pod.Annotations["coroot.com/"+targetType+"-scrape-tls-secret-cert-key"],
		KeyKey:    pod.Annotations["coroot.com/"+targetType+"-scrape-tls-secret-key-key"],
	}
}
