package k8s

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/coroot/coroot-cluster-agent/flags"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

var (
	ErrForbidden = errors.New("forbidden")
	ErrNotFound  = errors.New("not found")
)

type PodEventType int

const (
	PodEventTypeAdd PodEventType = iota
	PodEventTypeChange
	PodEventTypeDelete
)

type PodEventsListener interface {
	ListenPodEvents(events <-chan PodEvent)
}

type PodEvent struct {
	Type PodEventType
	Pod  *Pod
	Old  *Pod
}

type K8S struct {
	client *kubernetes.Clientset
	stopCh chan struct{}

	lock     sync.Mutex
	factory  informers.SharedInformerFactory
	stopped  bool
	synced   atomic.Bool
	stopOnce sync.Once

	sendLock    sync.RWMutex // held for reading while sending to the subscribers, for writing while closing them
	subsClosed  bool
	subscribers []chan<- PodEvent
}

func NewK8S() (*K8S, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		if errors.Is(err, rest.ErrNotInCluster) {
			klog.Infoln("not running inside a kubernetes cluster")
			return nil, nil
		}
		return nil, err
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	k8s := &K8S{
		client: client,
		stopCh: make(chan struct{}),
	}

	return k8s, nil
}

func (k8s *K8S) Start() {
	if k8s == nil {
		return
	}
	k8s.lock.Lock()
	defer k8s.lock.Unlock()
	if k8s.stopped {
		return
	}
	k8s.factory = k8s.start()
	go func() {
		for _, ok := range k8s.factory.WaitForCacheSync(k8s.stopCh) {
			if !ok {
				return
			}
		}
		k8s.synced.Store(true)
	}()
}

// Synced reports whether the informer caches have been synced (true if not running in k8s).
func (k8s *K8S) Synced() bool {
	if k8s == nil {
		return true
	}
	return k8s.synced.Load()
}

// Stop stops the informers, waits for their event handlers to return, and only then closes
// the subscriber channels, so no handler can send on a closed channel.
func (k8s *K8S) Stop() {
	if k8s == nil {
		return
	}
	k8s.stopOnce.Do(func() {
		k8s.lock.Lock()
		k8s.stopped = true
		factory := k8s.factory
		k8s.lock.Unlock()
		close(k8s.stopCh) // also unblocks handlers blocked in sendPodEvent
		if factory != nil {
			factory.Shutdown()
		}
		k8s.sendLock.Lock()
		defer k8s.sendLock.Unlock()
		k8s.subsClosed = true
		for _, s := range k8s.subscribers {
			close(s)
		}
	})
}

func (k8s *K8S) start() informers.SharedInformerFactory {
	factory := informers.NewSharedInformerFactory(k8s.client, 0)

	if !*flags.CollectKubernetesEvents {
		klog.Infoln("events collector disabled")
	} else if eventsLogger, err := NewEventsLogger(); err != nil {
		klog.Errorln("failed to create events logger, events collection disabled:", err)
	} else {
		events := factory.Core().V1().Events().Informer()
		events.SetWatchErrorHandler(func(r *cache.Reflector, err error) {
			if apierrors.IsForbidden(err) {
				klog.Errorln("Cannot watch events: access forbidden. Update Coroot Operator to proceed.")
				return
			}
			cache.DefaultWatchErrorHandler(context.TODO(), r, err)
		})
		startTime := metav1.Now()
		events.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				event := obj.(*corev1.Event)
				if !event.LastTimestamp.IsZero() && event.LastTimestamp.Before(&startTime) {
					return
				}
				eventsLogger.EmitEvent(event)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				old := oldObj.(*corev1.Event)
				event := newObj.(*corev1.Event)
				if event.Count > old.Count {
					eventsLogger.EmitEvent(event)
				}
			},
		})
	}

	if len(k8s.subscribers) > 0 {
		pods := factory.Core().V1().Pods().Informer()
		pods.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    k8s.onPodAdd,
			UpdateFunc: k8s.onPodUpdate,
			DeleteFunc: k8s.onPodDelete,
		})
	}

	factory.Start(k8s.stopCh)
	return factory
}

func (k8s *K8S) onPodAdd(obj interface{}) {
	pod := podFromObj(obj)
	if pod == nil || !pod.Running() {
		return
	}
	k8s.sendPodEvent(PodEvent{Type: PodEventTypeAdd, Pod: pod})
}

func (k8s *K8S) onPodUpdate(oldObj, newObj interface{}) {
	pod := podFromObj(newObj)
	old := podFromObj(oldObj)
	if pod == nil || old == nil || pod.Equal(old) {
		return
	}
	if !pod.Running() {
		if old.Running() { // e.g. Running -> Failed/Succeeded (evicted, completed): the targets must be removed
			k8s.sendPodEvent(PodEvent{Type: PodEventTypeDelete, Pod: old})
		}
		return
	}
	k8s.sendPodEvent(PodEvent{Type: PodEventTypeChange, Pod: pod, Old: old})
}

func (k8s *K8S) onPodDelete(obj interface{}) {
	pod := podFromObj(obj)
	if pod == nil {
		return
	}
	k8s.sendPodEvent(PodEvent{Type: PodEventTypeDelete, Pod: pod})
}

func (k8s *K8S) SubscribeForPodEvents(l PodEventsListener) {
	if k8s == nil {
		return
	}
	ch := make(chan PodEvent)
	l.ListenPodEvents(ch)
	k8s.subscribers = append(k8s.subscribers, ch)
}

func (k8s *K8S) sendPodEvent(e PodEvent) {
	k8s.sendLock.RLock()
	defer k8s.sendLock.RUnlock()
	if k8s.subsClosed {
		return
	}
	for _, s := range k8s.subscribers {
		select {
		case s <- e:
		case <-k8s.stopCh:
			return
		}
	}
}
