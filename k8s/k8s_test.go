package k8s

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testPod(phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "db-0", Namespace: "ns", UID: "uid-1", Annotations: map[string]string{"coroot.com/postgres-scrape": "true"}},
		Status:     corev1.PodStatus{Phase: phase, PodIP: "10.0.0.1"},
	}
}

func newTestK8S() (*K8S, chan PodEvent) {
	ch := make(chan PodEvent, 10)
	return &K8S{stopCh: make(chan struct{}), subscribers: []chan<- PodEvent{ch}}, ch
}

func TestPodLeavesRunning(t *testing.T) {
	k, ch := newTestK8S()
	running, failed := testPod(corev1.PodRunning), testPod(corev1.PodFailed)

	k.onPodUpdate(running, failed)
	select {
	case e := <-ch:
		if e.Type != PodEventTypeDelete || e.Pod.IP != "10.0.0.1" || e.Pod.UID != "uid-1" {
			t.Fatalf("unexpected event: %+v", e)
		}
	default:
		t.Fatal("a pod leaving the Running phase must produce a delete event")
	}

	k.onPodUpdate(failed, failed.DeepCopy())
	pending := testPod(corev1.PodPending)
	k.onPodUpdate(pending, failed)
	if len(ch) != 0 {
		t.Fatalf("no events expected for pods that aren't running, got %d", len(ch))
	}

	k.onPodUpdate(pending, running)
	if e := <-ch; e.Type != PodEventTypeChange {
		t.Fatalf("unexpected event: %+v", e)
	}
}

func TestSendPodEventUnblocksOnStop(t *testing.T) {
	k := &K8S{stopCh: make(chan struct{}), subscribers: []chan<- PodEvent{make(chan PodEvent)}} // nobody reads
	done := make(chan struct{})
	go func() {
		k.onPodAdd(testPod(corev1.PodRunning))
		close(done)
	}()
	k.Stop()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a blocked handler must return when the informers are stopped")
	}
	k.Stop() // idempotent
}
