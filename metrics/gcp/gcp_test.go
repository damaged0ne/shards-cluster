package gcp

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func newTestDiscoverer(reg prometheus.Registerer) *Discoverer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Discoverer{
		project:         "p",
		ctx:             ctx,
		cancel:          cancel,
		reg:             reg,
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
		errors:          map[string]bool{},
		monitoring:      NewMonitoring(nil, "p"),
		sqlCollectors:   map[string]*CloudSQLCollector{},
		redisCollectors: map[string]*MemorystoreCollector{},
	}
}

func TestDiscovererStop(t *testing.T) {
	reg := prometheus.NewRegistry()
	d := newTestDiscoverer(reg)
	reg.MustRegister(d)
	c := &MemorystoreCollector{discoverer: d, info: memorystoreInfo{id: "p/r/i"}}
	prometheus.WrapRegistererWith(memorystoreLabels("p/r/i"), reg).MustRegister(c)
	d.redisCollectors["p/r/i"] = c

	started := make(chan struct{})
	go d.run(func() {
		close(started)
		<-d.ctx.Done() // an in-flight API call: returns only when the context is cancelled
	})
	<-started
	stopped := make(chan struct{})
	go func() {
		d.Stop()
		d.Stop() // idempotent
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop blocked")
	}
	if mfs, _ := reg.Gather(); len(mfs) != 0 {
		t.Fatalf("collectors left registered: %v", mfs)
	}
}

func TestCollectorsUpdateConcurrentlyWithCollect(t *testing.T) {
	d := newTestDiscoverer(nil)
	sql := &CloudSQLCollector{discoverer: d, instance: &sqladmin.DatabaseInstance{Name: "db"}}
	ms := &MemorystoreCollector{discoverer: d}
	reg := prometheus.NewRegistry()
	reg.MustRegister(sql, ms)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			sql.update(&sqladmin.DatabaseInstance{Name: "db", State: "RUNNABLE"})
			ms.setInfo(memorystoreInfo{id: "p/r/i", state: "READY"})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if _, err := reg.Gather(); err != nil {
				t.Error(err)
				return
			}
			d.publishEndpoints()
		}
	}()
	wg.Wait()
}
