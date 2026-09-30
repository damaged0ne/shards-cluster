package profiles

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/google/pprof/profile"
	"golang.org/x/exp/maps"
	"k8s.io/klog"
)

const (
	goCPUProfileSeconds = 10
	uploadTimeout       = 30 * time.Second
	dialTimeout         = 10 * time.Second
)

type Profiles struct {
	endpoint       *url.URL
	apiKey         string
	scrapeInterval time.Duration
	scrapeTimeout  time.Duration

	httpClient *http.Client

	targets     map[string]*Target
	targetsLock sync.Mutex

	prevCache     map[ProfileKey]map[uint64]int64
	prevCacheLock sync.Mutex

	k8sPodEvents <-chan k8s.PodEvent

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func NewProfiles() *Profiles {
	if *flags.ProfilesScrapeInterval == 0 {
		klog.Infoln("scrape interval is not set, disabling the scraper")
		return nil
	}

	ps := &Profiles{
		endpoint:       (*flags.CorootURL).JoinPath("/v1/profiles"),
		apiKey:         *flags.APIKey,
		scrapeInterval: *flags.ProfilesScrapeInterval,
		scrapeTimeout:  *flags.ProfilesScrapeTimeout,
		// every request has its own deadline (see scrape and upload), the transport timeouts bound the connection setup
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig:       common.TlsConfig(),
				DialContext:           (&net.Dialer{Timeout: dialTimeout}).DialContext,
				TLSHandshakeTimeout:   dialTimeout,
				ResponseHeaderTimeout: uploadTimeout,
				IdleConnTimeout:       90 * time.Second,
			},
		},
		prevCache: map[ProfileKey]map[uint64]int64{},
		targets:   map[string]*Target{},
	}

	ps.ctx, ps.cancel = context.WithCancel(context.Background())
	ps.done = make(chan struct{})

	klog.Infof("endpoint: %s, scrape interval: %s", ps.endpoint, ps.scrapeInterval)

	return ps
}

func (ps *Profiles) ListenPodEvents(events <-chan k8s.PodEvent) {
	ps.k8sPodEvents = events
}

func (ps *Profiles) Start() {
	go ps.discoverFromPods()
	go ps.scrapeLoop()
}

// Stop cancels in-flight scrapes and uploads and waits for the scrape loop to exit (or ctx to be done).
func (ps *Profiles) Stop(ctx context.Context) {
	if ps == nil {
		return
	}
	ps.cancel()
	select {
	case <-ps.done:
	case <-ctx.Done():
	}
}

func (ps *Profiles) scrapeLoop() {
	defer close(ps.done)
	for {
		if ps.ctx.Err() != nil {
			return
		}
		start := time.Now()
		ps.targetsLock.Lock()
		targets := maps.Values(ps.targets)
		ps.targetsLock.Unlock()

		var wg sync.WaitGroup
		for _, t := range targets {
			addr, err := url.Parse("http://" + t.Address)
			if err != nil {
				t.logger.Error(err)
				continue
			}
			for _, profileType := range goProfileTypes {
				wg.Add(1)
				go func(sn string, ls Labels, u *url.URL, pt string) {
					defer wg.Done()
					p, err := ps.scrape(pt, u)
					if err != nil {
						t.logger.Errorf("failed to scrape: %s", err)
						return
					}
					if len(p.Sample) == 0 {
						return
					}
					if p.DurationNanos == 0 {
						p.DurationNanos = ps.scrapeInterval.Nanoseconds()
					}

					ps.diff(sn, ls, SourceGo, pt, p)

					err = ps.upload(sn, ls, p)
					if err != nil {
						t.logger.Errorf("failed to upload: %s", err)
						return
					}
				}(t.ServiceName, t.Labels, addr, profileType)
			}
		}
		wg.Wait()

		duration := time.Since(start)
		klog.Infof("scraped %d targets in %s", len(targets), duration.Truncate(time.Millisecond))
		timer := time.NewTimer(max(ps.scrapeInterval-duration, 0))
		select {
		case <-timer.C:
		case <-ps.ctx.Done():
			timer.Stop()
			return
		}
	}
}

func (ps *Profiles) scrape(profileType string, addr *url.URL) (*profile.Profile, error) {
	u := addr.JoinPath("/debug/pprof", profileType)
	timeout := ps.scrapeTimeout
	if profileType == GoProfileProfile {
		timeout += time.Duration(goCPUProfileSeconds) * time.Second
		q := u.Query()
		q.Set("seconds", strconv.Itoa(goCPUProfileSeconds))
		u.RawQuery = q.Encode()
	}
	ctx, cancel := context.WithTimeout(ps.ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := ps.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, resp.Status)
	}
	p, err := profile.Parse(resp.Body)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func sampleHash(s *profile.Sample) uint64 {
	h := fnv.New64a()
	for _, location := range s.Location {
		for _, line := range location.Line {
			if line.Function == nil {
				continue
			}
			_, _ = h.Write([]byte(line.Function.Name))
			_, _ = h.Write([]byte(line.Function.Filename))
			_, _ = h.Write([]byte(strconv.FormatInt(line.Line, 10)))
		}
	}
	return h.Sum64()
}

// dedupSamples merges the samples with the same stack, summing all their values (every sample type) at once.
func dedupSamples(p *profile.Profile) map[uint64]*profile.Sample {
	samples := make(map[uint64]*profile.Sample, len(p.Sample))
	for _, s := range p.Sample {
		hash := sampleHash(s)
		if existing := samples[hash]; existing == nil {
			samples[hash] = s
		} else {
			for i := range existing.Value {
				if i < len(s.Value) {
					existing.Value[i] += s.Value[i]
				}
			}
		}
	}
	if len(samples) < len(p.Sample) {
		p.Sample = p.Sample[:0]
		for _, s := range samples {
			p.Sample = append(p.Sample, s)
		}
		p.Compact()
	}
	return samples
}

func (ps *Profiles) diff(serviceName string, labels Labels, source Source, profileType string, p *profile.Profile) {
	samples := dedupSamples(p)
	for i, st := range p.SampleType {
		cumulative := false
		switch profileType {
		case GoProfileProfile:
			switch st.Type {
			case "samples":
				st.Type = ""
				continue
			}
		case GoProfileHeap:
			switch st.Type {
			case "alloc_objects", "alloc_space":
				cumulative = true
			}
		case GoProfileMutex, GoProfileBlock:
			cumulative = true
		}

		st.Type = fmt.Sprintf("%s:%s_%s:%s", source, profileType, st.Type, st.Unit)

		if !cumulative {
			continue
		}

		key := ProfileKey{
			ServiceName: serviceName,
			LabelsHash:  labels.Hash(),
			ProfileType: st.Type,
		}
		current := make(map[uint64]int64, len(samples))
		ps.prevCacheLock.Lock()
		prev, hasPrev := ps.prevCache[key]
		for hash, s := range samples {
			value := s.Value[i]
			current[hash] = value
			if !hasPrev {
				continue
			}
			if d := value - prev[hash]; d >= 0 {
				s.Value[i] = d
			}
		}
		ps.prevCache[key] = current // only the stacks seen in this scrape are kept, so the cache doesn't grow unbounded
		ps.prevCacheLock.Unlock()
	}
}

func (ps *Profiles) upload(serviceName string, labels Labels, p *profile.Profile) error {
	u := *ps.endpoint
	q := u.Query()
	for k, v := range labels {
		q.Set(k, v)
	}
	q.Set("service.name", serviceName)
	u.RawQuery = q.Encode()

	buf := bytes.NewBuffer(nil)
	err := p.Write(buf)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ps.ctx, uploadTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), buf)
	if err != nil {
		return err
	}

	common.SetAuthHeaders(req, ps.apiKey)

	resp, err := ps.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func (ps *Profiles) discoverFromPods() {
	for e := range ps.k8sPodEvents {
		ps.handlePodEvent(e)
	}
}

func (ps *Profiles) handlePodEvent(e k8s.PodEvent) {
	switch e.Type {
	case k8s.PodEventTypeAdd, k8s.PodEventTypeChange:
		target := TargetFromPod(e.Pod)
		old := TargetFromPod(e.Old)
		if target == nil {
			if old != nil {
				ps.delTarget(old)
			}
			return
		}
		if old != nil && old.Address != target.Address { // e.g. the pod IP has changed
			ps.delTarget(old)
		}
		ps.targetsLock.Lock()
		t := ps.targets[target.Address]
		ps.targetsLock.Unlock()
		switch {
		case t == nil:
			ps.addTarget(target)
		case t.Equal(target) && t.podKey == target.podKey:
			return
		default:
			ps.replaceTarget(t, target)
		}

	case k8s.PodEventTypeDelete:
		target := TargetFromPod(e.Pod)
		if target == nil {
			return
		}
		ps.delTarget(target)
	}
}

func (ps *Profiles) addTarget(target *Target) {
	ps.targetsLock.Lock()
	defer ps.targetsLock.Unlock()
	klog.Infof("new target: %s", target)
	ps.targets[target.Address] = target
}

func (ps *Profiles) replaceTarget(old, target *Target) {
	ps.targetsLock.Lock()
	defer ps.targetsLock.Unlock()
	ps.forgetTarget(old)
	klog.Infof("new target: %s", target)
	ps.targets[target.Address] = target
}

// delTarget removes the target stored for target.Address only if it was discovered from the same pod,
// so that the removal of a pod doesn't remove the target of another pod that has reused its IP address.
func (ps *Profiles) delTarget(target *Target) {
	ps.targetsLock.Lock()
	defer ps.targetsLock.Unlock()
	t := ps.targets[target.Address]
	if t == nil || t.podKey != target.podKey {
		return
	}
	ps.forgetTarget(t)
	delete(ps.targets, target.Address)
}

// forgetTarget drops the cached state of t; must be called with targetsLock held.
func (ps *Profiles) forgetTarget(t *Target) {
	klog.Infof("removing target: %s", t)
	labelsHash := t.Labels.Hash()
	ps.prevCacheLock.Lock()
	defer ps.prevCacheLock.Unlock()
	for key := range ps.prevCache {
		if t.ServiceName == key.ServiceName && labelsHash == key.LabelsHash {
			delete(ps.prevCache, key)
		}
	}
}
