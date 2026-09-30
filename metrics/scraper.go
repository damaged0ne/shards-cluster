package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	cfgpkg "github.com/coroot/coroot-cluster-agent/config"
	remoteapi "github.com/prometheus/client_golang/exp/api/remote"
	"github.com/prometheus/client_golang/prometheus"
	promCommon "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/kubernetes"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/agent"
	"k8s.io/klog"
)

const (
	RemoteWriteTimeout  = 30 * time.Second
	RemoteFlushDeadline = time.Minute
	jobName             = "coroot-cluster-agent"
)

type scraperState struct {
	cancelDiscovery context.CancelFunc
	scrapeManager   *scrape.Manager
	storage         storage.Storage
	stopping        atomic.Bool
}

// stopScraper stops the service discovery and the scrape manager.
func (ms *Metrics) stopScraper(ctx context.Context) {
	s := ms.scraper
	if s == nil {
		return
	}
	s.stopping.Store(true)
	s.cancelDiscovery()
	if s.scrapeManager != nil {
		common.RunWithContext(ctx, "stopping scrape manager", s.scrapeManager.Stop)
	}
}

// closeStorage closes the WAL and the remote storage, flushing the pending samples (up to RemoteFlushDeadline).
func (ms *Metrics) closeStorage(ctx context.Context) {
	s := ms.scraper
	if s == nil || s.storage == nil {
		return
	}
	common.RunWithContext(ctx, "closing storage", func() {
		if err := s.storage.Close(); err != nil {
			klog.Errorln("failed to close storage:", err)
		}
	})
}

// promConfig builds the Prometheus configuration of the scraper: the remote-write endpoint,
// the agent's own /metrics (along with kube-state-metrics), the pods annotated for custom metrics,
// and the native /metrics endpoints of the statically configured services (RabbitMQ, etcd).
func (ms *Metrics) promConfig(logger *slog.Logger) (*config.Config, error) {
	// The config is built programmatically, but since Prometheus 0.311 GetScrapeConfigs (used by the scrape manager)
	// refuses to work with a config that wasn't created by config.Load. So an empty config is loaded
	// (which only applies the defaults), and the scrape and remote-write configs are then validated here,
	// as Load would do. This avoids a YAML round-trip, which would replace the secrets with "<secret>".
	cfg, err := config.Load("", logger)
	if err != nil {
		return nil, err
	}
	cfg.GlobalConfig.ScrapeInterval = model.Duration(ms.scrapeInterval)
	cfg.GlobalConfig.ScrapeTimeout = model.Duration(ms.scrapeTimeout)
	cfg.RemoteWriteConfigs = append(cfg.RemoteWriteConfigs,
		&config.RemoteWriteConfig{
			URL:             &promCommon.URL{URL: ms.endpoint},
			Headers:         common.AuthHeaders(ms.apiKey),
			RemoteTimeout:   model.Duration(RemoteWriteTimeout),
			ProtobufMessage: remoteapi.WriteV1MessageType,
			QueueConfig:     config.DefaultQueueConfig,
			HTTPClientConfig: promCommon.HTTPClientConfig{
				TLSConfig: promCommon.TLSConfig{
					InsecureSkipVerify: ms.insecureSkipVerify,
					CAFile:             ms.caFile,
				},
			},
		},
	)
	targets := []model.LabelSet{{
		model.AddressLabel:  model.LabelValue(ms.listenAddr),
		model.InstanceLabel: model.LabelValue(ms.listenAddr),
	}}
	if ms.ksmAddr != "" {
		targets = append(targets, model.LabelSet{
			model.AddressLabel:  model.LabelValue(ms.ksmAddr),
			model.InstanceLabel: model.LabelValue(ms.ksmAddr),
		})
	}

	cfg.ScrapeConfigs = append(cfg.ScrapeConfigs, &config.ScrapeConfig{
		JobName:                       jobName,
		HonorLabels:                   true,
		AlwaysScrapeClassicHistograms: ptr(true),
		MetricsPath:                   "/metrics",
		Scheme:                        "http",
		EnableCompression:             false,
		ServiceDiscoveryConfigs: []discovery.Config{
			discovery.StaticConfig{{Targets: targets}},
		},
		MetricRelabelConfigs: []*relabel.Config{
			{
				Regex:       relabel.MustNewRegexp("customresource_(group|kind|version)"),
				Action:      relabel.LabelDrop,
				Separator:   relabel.DefaultRelabelConfig.Separator,
				Replacement: relabel.DefaultRelabelConfig.Replacement,
			},
		},
	})
	inK8s := inK8sCluster()
	if inK8s {
		klog.Infoln("enabling k8s service discovery")
		cfg.ScrapeConfigs = append(cfg.ScrapeConfigs, k8sDiscovery())
	} else {
		klog.Infoln("not in k8s cluster, disabling k8s service discovery")
	}
	var databases []cfgpkg.Database
	if ms.static != nil {
		databases = ms.static.Databases
	}
	native, err := nativeScrapeConfigs(databases, inK8s)
	if err != nil {
		return nil, err
	}
	cfg.ScrapeConfigs = append(cfg.ScrapeConfigs, native...)

	jobNames := map[string]bool{}
	for _, sc := range cfg.ScrapeConfigs {
		if jobNames[sc.JobName] {
			return nil, fmt.Errorf("duplicate scrape job name %q", sc.JobName)
		}
		jobNames[sc.JobName] = true
		if err = sc.Validate(cfg.GlobalConfig); err != nil {
			return nil, err
		}
	}
	for _, rw := range cfg.RemoteWriteConfigs {
		if err = rw.Validate(cfg.GlobalConfig.MetricNameValidationScheme); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

func ptr[T any](v T) *T {
	return &v
}

func (ms *Metrics) runScraper() error {
	logger := NewLogger()
	cfg, err := ms.promConfig(logger)
	if err != nil {
		return err
	}

	discCtx, cancelDiscovery := context.WithCancel(context.Background())
	state := &scraperState{cancelDiscovery: cancelDiscovery}
	ms.scraper = state

	localStorage := &readyStorage{stats: tsdb.NewDBStats()}
	scraper := &readyScrapeManager{}
	remoteStorage := remote.NewStorage(logger, prometheus.DefaultRegisterer, localStorage.StartTime, ms.walDir, RemoteFlushDeadline, scraper, false)
	fanoutStorage := storage.NewFanout(logger, localStorage, remoteStorage)
	state.storage = fanoutStorage

	if err := remoteStorage.ApplyConfig(cfg); err != nil {
		return err
	}
	sdMetrics, err := discovery.CreateAndRegisterSDMetrics(prometheus.DefaultRegisterer)
	if err != nil {
		return err
	}
	discMgr := discovery.NewManager(discCtx, logger, prometheus.DefaultRegisterer, sdMetrics, discovery.Name("scrape"))
	if discMgr == nil {
		return errors.New("could not create discovery manager")
	}
	c := make(map[string]discovery.Configs)
	scfgs, err := cfg.GetScrapeConfigs()
	if err != nil {
		return err
	}
	for _, v := range scfgs {
		c[v.JobName] = v.ServiceDiscoveryConfigs
	}
	if err = discMgr.ApplyConfig(c); err != nil {
		return err
	}

	go func() {
		if err := discMgr.Run(); err != nil && !state.stopping.Load() {
			klog.Exitln("error running discovery manager:", err)
		}
	}()

	scrapeManager, err := scrape.NewManager(nil, logger, nil, nil, fanoutStorage, prometheus.DefaultRegisterer)
	if err != nil {
		return err
	}
	if err = scrapeManager.ApplyConfig(cfg); err != nil {
		return err
	}
	scraper.Set(scrapeManager)
	state.scrapeManager = scrapeManager
	db, err := agent.Open(logger, prometheus.DefaultRegisterer, remoteStorage, ms.walDir, agent.DefaultOptions())
	if err != nil {
		return err
	}
	localStorage.Set(db, 0)
	db.SetWriteNotified(remoteStorage)
	go func() {
		if err := scrapeManager.Run(discMgr.SyncCh()); err != nil && !state.stopping.Load() {
			klog.Exitln("error running scrape manager:", err)
		}
	}()
	return nil
}

func inK8sCluster() bool {
	return os.Getenv("KUBERNETES_SERVICE_HOST") != "" && os.Getenv("KUBERNETES_SERVICE_PORT") != ""
}

func k8sDiscovery() *config.ScrapeConfig {
	return &config.ScrapeConfig{
		JobName:                       "custom-metrics-k8s-pods",
		HonorLabels:                   true,
		AlwaysScrapeClassicHistograms: ptr(true),
		MetricsPath:                   "/metrics",
		Scheme:                        "http",
		EnableCompression:             false,
		RelabelConfigs: []*relabel.Config{
			{
				SourceLabels: model.LabelNames{"__meta_kubernetes_pod_annotation_coroot_com_scrape_metrics"},
				Action:       relabel.Keep,
				Regex:        relabel.MustNewRegexp("true"),
			},
			{
				SourceLabels: model.LabelNames{"__meta_kubernetes_pod_annotation_coroot_com_metrics_path"},
				Action:       relabel.Replace,
				TargetLabel:  "__metrics_path__",
				Regex:        relabel.MustNewRegexp("(.+)"),
				Replacement:  "$1",
			},
			{
				SourceLabels: model.LabelNames{"__meta_kubernetes_pod_name"},
				TargetLabel:  "pod",
				Action:       relabel.Replace,
				Regex:        relabel.MustNewRegexp("(.+)"),
				Replacement:  "$1",
			},
			{
				SourceLabels: model.LabelNames{"__meta_kubernetes_namespace"},
				TargetLabel:  "namespace",
				Action:       relabel.Replace,
				Regex:        relabel.MustNewRegexp("(.+)"),
				Replacement:  "$1",
			},
			{
				SourceLabels: model.LabelNames{"__address__", "__meta_kubernetes_pod_annotation_coroot_com_metrics_port"},
				TargetLabel:  "__address__",
				Separator:    ";",
				Regex:        relabel.MustNewRegexp(`(.+?)(?::\d+)?;(\d+)`),
				Replacement:  "$1:$2",
				Action:       relabel.Replace,
			},
			{
				SourceLabels: model.LabelNames{"__meta_kubernetes_pod_annotation_coroot_com_metrics_scheme"},
				TargetLabel:  "__scheme__",
				Regex:        relabel.MustNewRegexp("(https?)"),
				Replacement:  "$1",
				Action:       relabel.Replace,
			},
			{
				SourceLabels: model.LabelNames{"__meta_kubernetes_pod_phase"},
				Regex:        relabel.MustNewRegexp("Pending|Succeeded|Failed|Completed"),
				Action:       relabel.Drop,
			},
		},
		ServiceDiscoveryConfigs: []discovery.Config{
			k8sPodSDConfig(),
		},
	}
}

func k8sPodSDConfig() *kubernetes.SDConfig {
	sd := kubernetes.DefaultSDConfig
	sd.Role = kubernetes.RolePod
	sd.Selectors = []kubernetes.SelectorConfig{{Role: "pod"}}
	return &sd
}
