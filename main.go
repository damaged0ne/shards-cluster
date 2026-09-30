package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/config"
	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/coroot/coroot-cluster-agent/metrics"
	"github.com/coroot/coroot-cluster-agent/profiles"
	"github.com/gorilla/mux"
	"k8s.io/klog"
)

var (
	version = "unknown"
)

func main() {
	klog.Infoln("version:", version)

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()

	var ready atomic.Bool

	router := mux.NewRouter()
	router.Use(func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t := time.Now()
			handler.ServeHTTP(w, r)
			switch r.URL.Path {
			case "/metrics", "/health", "/healthz", "/readyz": // scraped/probed constantly
				klog.V(3).Infof("%s %s %d %s", r.Method, r.RequestURI, r.ContentLength, time.Since(t).Truncate(time.Millisecond))
			default:
				klog.V(2).Infof("%s %s %d %s", r.Method, r.RequestURI, r.ContentLength, time.Since(t).Truncate(time.Millisecond))
			}
		})
	})
	if *flags.EnablePprof {
		klog.Infoln("pprof endpoints are enabled at /debug/pprof/")
		router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		router.HandleFunc("/debug/pprof/profile", pprof.Profile)
		router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		router.HandleFunc("/debug/pprof/trace", pprof.Trace)
		router.PathPrefix("/debug/pprof/").HandlerFunc(pprof.Index)
	}
	router.HandleFunc("/health", health).Methods(http.MethodGet)
	router.HandleFunc("/healthz", health).Methods(http.MethodGet)

	static, err := config.LoadStatic(*flags.ConfigFile)
	if err != nil {
		klog.Exitln(err)
	}
	config, err := config.NewUpdater()
	if err != nil {
		klog.Exitln(err)
	}

	k8s, err := k8s.NewK8S()
	if err != nil {
		klog.Exitln(err)
	}

	router.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() || !k8s.Synced() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("OK"))
	}).Methods(http.MethodGet)

	ms, err := metrics.NewMetrics(k8s, static)
	if err != nil {
		klog.Exitln(err)
	}
	if ms != nil {
		config.SubscribeForUpdates(ms)
		k8s.SubscribeForPodEvents(ms)
		router.Handle("/metrics", ms.HttpHandler())
		err = ms.Start()
		if err != nil {
			klog.Exitln(err)
		}
	}

	ps := profiles.NewProfiles()
	if ps != nil {
		k8s.SubscribeForPodEvents(ps)
		ps.Start()
	}

	config.Start()
	k8s.Start()

	srv := &http.Server{
		Addr:              *flags.ListenAddress,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		klog.Infoln("listening on", *flags.ListenAddress)
		serveErr <- srv.ListenAndServe()
	}()
	ready.Store(true)

	exitCode := 0
	select {
	case <-ctx.Done():
		klog.Infoln("received a termination signal, shutting down")
	case err = <-serveErr:
		klog.Errorln("http server failed:", err)
		exitCode = 1
	}
	stopSignals() // a second signal terminates the process immediately
	ready.Store(false)

	shutdown(srv, config, k8s, ms, ps)
	klog.Infoln("shutdown complete")
	klog.Flush()
	os.Exit(exitCode)
}

// shutdown stops the components in dependency order within flags.ShutdownTimeout:
// the http server, the sources of targets (config updates and k8s informers), the profiler,
// the metrics pipeline (scrape manager, exporters, WAL and remote-write flush), and finally the OTLP logs.
func shutdown(srv *http.Server, cfg *config.Updater, k *k8s.K8S, ms *metrics.Metrics, ps *profiles.Profiles) {
	ctx, cancel := context.WithTimeout(context.Background(), *flags.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		klog.Warningln("failed to shut down the http server:", err)
	}
	common.RunWithContext(ctx, "stopping config updater", cfg.Stop)
	common.RunWithContext(ctx, "stopping k8s informers", k.Stop)
	if ps != nil {
		ps.Stop(ctx)
	}
	if ms != nil {
		ms.Stop(ctx)
	}
	// the logs are flushed last, but get some time even if the steps above used up the deadline
	logsCtx, logsCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer logsCancel()
	if err := common.ShutdownLogs(logsCtx); err != nil {
		klog.Warningln("failed to flush logs:", err)
	}
}

func health(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("OK"))
}
