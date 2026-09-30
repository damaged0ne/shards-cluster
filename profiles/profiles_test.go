package profiles

import (
	"testing"

	"github.com/coroot/coroot-cluster-agent/k8s"
	"github.com/google/pprof/profile"
)

func heapProfile(values ...[]int64) *profile.Profile {
	fn := &profile.Function{ID: 1, Name: "main.alloc", Filename: "main.go"}
	loc := &profile.Location{ID: 1, Line: []profile.Line{{Function: fn, Line: 10}}}
	fn2 := &profile.Function{ID: 2, Name: "main.other", Filename: "main.go"}
	loc2 := &profile.Location{ID: 2, Line: []profile.Line{{Function: fn2, Line: 20}}}
	p := &profile.Profile{
		SampleType: []*profile.ValueType{
			{Type: "alloc_objects", Unit: "count"},
			{Type: "alloc_space", Unit: "bytes"},
			{Type: "inuse_objects", Unit: "count"},
			{Type: "inuse_space", Unit: "bytes"},
		},
		Function: []*profile.Function{fn, fn2},
		Location: []*profile.Location{loc, loc2},
	}
	for i, v := range values {
		l := loc
		if i == len(values)-1 {
			l = loc2 // the last sample has a distinct stack
		}
		p.Sample = append(p.Sample, &profile.Sample{Location: []*profile.Location{l}, Value: append([]int64(nil), v...)})
	}
	return p
}

func findSample(t *testing.T, p *profile.Profile, fn string) []int64 {
	t.Helper()
	for _, s := range p.Sample {
		if s.Location[0].Line[0].Function.Name == fn {
			return s.Value
		}
	}
	t.Fatalf("no sample for %s", fn)
	return nil
}

func equal(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestDiffDuplicateStacks(t *testing.T) {
	ps := &Profiles{prevCache: map[ProfileKey]map[uint64]int64{}}
	ls := Labels{"pod": "p"}

	// two samples with the same stack (e.g. differing only in labels) and one other
	p := heapProfile([]int64{1, 10, 100, 1000}, []int64{2, 20, 200, 2000}, []int64{5, 50, 500, 5000})
	ps.diff("svc", ls, SourceGo, GoProfileHeap, p)
	if len(p.Sample) != 2 {
		t.Fatalf("expected 2 samples after deduplication, got %d", len(p.Sample))
	}
	// the first scrape has nothing to diff with: all the values are summed as is
	if v := findSample(t, p, "main.alloc"); !equal(v, []int64{3, 30, 300, 3000}) {
		t.Fatalf("all sample types must be merged, got %v", v)
	}
	if p.SampleType[0].Type != "go:heap_alloc_objects:count" || p.SampleType[3].Type != "go:heap_inuse_space:bytes" {
		t.Fatalf("unexpected sample types: %v %v", p.SampleType[0], p.SampleType[3])
	}
	if len(ps.prevCache) != 2 {
		t.Fatalf("only the cumulative types must be cached, got %d", len(ps.prevCache))
	}

	// the second scrape: the cumulative types (alloc_*) are diffed, the others (inuse_*) are not;
	// main.other is gone and a new stack only has main.alloc
	p = heapProfile([]int64{4, 40, 150, 1500}, []int64{3, 30, 150, 1500})
	p.Sample = p.Sample[:1]
	p.Sample = append(p.Sample, &profile.Sample{Location: p.Sample[0].Location, Value: []int64{3, 30, 150, 1500}})
	ps.diff("svc", ls, SourceGo, GoProfileHeap, p)
	if v := findSample(t, p, "main.alloc"); !equal(v, []int64{7 - 3, 70 - 30, 300, 3000}) {
		t.Fatalf("unexpected values: %v", v)
	}
	for key, cache := range ps.prevCache {
		if len(cache) != 1 {
			t.Fatalf("%v: the cache must only contain the stacks of the last scrape, got %d", key, len(cache))
		}
	}
}

func testPod(uid, name, ip string) *k8s.Pod {
	return &k8s.Pod{
		Id:          k8s.PodId{Namespace: "ns", Name: name},
		UID:         uid,
		Phase:       "Running",
		IP:          ip,
		Annotations: map[string]string{"coroot.com/profile-scrape": "true", "coroot.com/profile-port": "6060"},
	}
}

func TestPodIPReuse(t *testing.T) {
	ps := &Profiles{prevCache: map[ProfileKey]map[uint64]int64{}, targets: map[string]*Target{}}
	p1 := testPod("uid-1", "app-1", "10.1.0.1")
	p2 := testPod("uid-2", "app-2", "10.1.0.1")
	ps.handlePodEvent(k8s.PodEvent{Type: k8s.PodEventTypeAdd, Pod: p1})
	ps.handlePodEvent(k8s.PodEvent{Type: k8s.PodEventTypeAdd, Pod: p2})
	ps.handlePodEvent(k8s.PodEvent{Type: k8s.PodEventTypeDelete, Pod: p1})
	if tg := ps.targets["10.1.0.1:6060"]; tg == nil || tg.podKey != "uid-2" {
		t.Fatalf("the deletion of the old pod must not remove the target of the new pod: %v", tg)
	}
	ps.handlePodEvent(k8s.PodEvent{Type: k8s.PodEventTypeDelete, Pod: p2})
	if len(ps.targets) != 0 {
		t.Fatal("the target must be removed with its pod")
	}
}
