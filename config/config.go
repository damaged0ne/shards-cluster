package config

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/flags"
	"k8s.io/klog"
)

type Listener interface {
	ListenConfigUpdates(updates <-chan Config)
}

type Updater struct {
	endpoint       *url.URL
	apiKey         string
	updateInterval time.Duration
	httpClient     *http.Client
	subscribers    []chan<- Config
	last           *Config

	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
}

func NewUpdater() (*Updater, error) {
	c := &Updater{
		endpoint:       (*flags.CorootURL).JoinPath("/v1/config"),
		apiKey:         *flags.APIKey,
		updateInterval: *flags.ConfigUpdateInterval,
		httpClient: &http.Client{
			Timeout: *flags.ConfigUpdateTimeout,
			Transport: &http.Transport{
				TLSClientConfig: common.TlsConfig(),
			},
		},
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	klog.Infof("endpoint: %s, update interval: %s", c.endpoint, c.updateInterval)

	return c, nil
}

func (u *Updater) SubscribeForUpdates(l Listener) {
	ch := make(chan Config)
	l.ListenConfigUpdates(ch)
	u.subscribers = append(u.subscribers, ch)
}

func (u *Updater) Start() {
	u.done = make(chan struct{})
	go func() {
		defer close(u.done)
		ticker := time.NewTicker(u.updateInterval)
		defer ticker.Stop()
		for {
			cfg, err := u.fetchConfig()
			if err != nil {
				if u.ctx.Err() != nil {
					return
				}
				klog.Error(err)
				cfg = u.last
				if cfg == nil {
					cfg = &Config{}
				}
			} else {
				u.last = cfg
			}
			for _, s := range u.subscribers {
				select {
				case s <- *cfg:
				case <-u.ctx.Done():
					return
				}
			}
			select {
			case <-ticker.C:
			case <-u.ctx.Done():
				return
			}
		}
	}()
}

// Stop stops the update loop and then closes the subscriber channels
// (closing them while the loop may still send would panic).
func (u *Updater) Stop() {
	u.stopOnce.Do(func() {
		u.cancel()
		if u.done != nil {
			<-u.done
		}
		for _, s := range u.subscribers {
			close(s)
		}
	})
}

func (u *Updater) fetchConfig() (*Config, error) {
	req, err := http.NewRequestWithContext(u.ctx, http.MethodGet, u.endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	common.SetAuthHeaders(req, u.apiKey)
	resp, err := u.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, string(body))
	}
	var cfg Config
	err = json.NewDecoder(resp.Body).Decode(&cfg)
	return &cfg, err
}
