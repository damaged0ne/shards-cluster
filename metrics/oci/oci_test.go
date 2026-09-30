package oci

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/monitoring"
	"github.com/prometheus/client_golang/prometheus"
)

func newTestDiscoverer(reg prometheus.Registerer) *Discoverer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Discoverer{
		ctx:             ctx,
		cancel:          cancel,
		reg:             reg,
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
		errors:          map[string]bool{},
		monitoring:      NewMonitoring(monitoring.MonitoringClient{}, nil),
		dbCollectors:    map[string]*DBCollector{},
		cacheCollectors: map[string]*CacheCollector{},
	}
}

func TestDiscovererStop(t *testing.T) {
	reg := prometheus.NewRegistry()
	d := newTestDiscoverer(reg)
	reg.MustRegister(d)
	c := &DBCollector{discoverer: d, info: dbInfo{id: "db1", name: "db1"}}
	prometheus.WrapRegistererWith(dbLabels("db1"), reg).MustRegister(c)
	d.dbCollectors["db1"] = c

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

func TestDBCollectorUpdateConcurrentlyWithCollect(t *testing.T) {
	d := newTestDiscoverer(nil)
	c := &DBCollector{discoverer: d, info: dbInfo{id: "db1", name: "db1"}}
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			c.lock.Lock()
			c.info = dbInfo{id: "db1", name: "db1", state: "ACTIVE", host: "10.0.0.1"}
			c.lock.Unlock()
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
