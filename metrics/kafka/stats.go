package kafka

import (
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
)

type broker struct {
	id      int32
	address string
	rack    string
}

type partitionStats struct {
	partition       int32
	leader          int32
	replicas        int
	isr             int
	currentOffset   int64 // -1 if unknown
	underReplicated bool
}

type topicStats struct {
	name             string
	partitions       int
	underReplicated  int
	offline          int
	currentOffsetSum int64
	hasOffsets       bool // at least one partition end offset is known
	parts            []partitionStats
}

type partitionLag struct {
	partition     int32
	currentOffset int64
	lag           int64
}

type groupTopicLag struct {
	topic            string
	currentOffsetSum int64
	lagSum           int64
	parts            []partitionLag
}

type groupStats struct {
	name    string
	state   string
	members int
	hasInfo bool // described successfully
	topics  []groupTopicLag
}

// endOffsets holds the high watermarks of the monitored partitions (topic -> partition -> offset).
type endOffsets map[string]map[int32]int64

func endOffsetsFrom(listed kadm.ListedOffsets) endOffsets {
	res := endOffsets{}
	for topic, ps := range listed {
		for p, o := range ps {
			if o.Err != nil || o.Offset < 0 || p < 0 {
				continue
			}
			if res[topic] == nil {
				res[topic] = map[int32]int64{}
			}
			res[topic][p] = o.Offset
		}
	}
	return res
}

// committedOffsets holds the offsets committed by a consumer group (topic -> partition -> offset).
type committedOffsets map[string]map[int32]int64

func committedOffsetsFrom(fetched kadm.OffsetResponses) committedOffsets {
	res := committedOffsets{}
	for topic, ps := range fetched {
		for p, o := range ps {
			if o.Err != nil || o.At < 0 {
				continue
			}
			if res[topic] == nil {
				res[topic] = map[int32]int64{}
			}
			res[topic][p] = o.At
		}
	}
	return res
}

func isInternalTopic(t kadm.TopicDetail) bool {
	return t.IsInternal || strings.HasPrefix(t.Topic, "__")
}

// selectTopics returns the sorted names of the topics to monitor and whether the limit was hit.
func selectTopics(topics kadm.TopicDetails, filter *Filter, includeInternal bool, limit int) ([]string, bool) {
	var res []string
	for name, t := range topics {
		if t.Err != nil {
			continue
		}
		if !includeInternal && isInternalTopic(t) {
			continue
		}
		if !filter.Match(name) {
			continue
		}
		res = append(res, name)
	}
	return limitSorted(res, limit)
}

func selectGroups(groups []string, filter *Filter, limit int) ([]string, bool) {
	var res []string
	for _, g := range groups {
		if filter.Match(g) {
			res = append(res, g)
		}
	}
	return limitSorted(res, limit)
}

func limitSorted(names []string, limit int) ([]string, bool) {
	sort.Strings(names)
	if limit > 0 && len(names) > limit {
		return names[:limit], true
	}
	return names, false
}

func computeTopicStats(t kadm.TopicDetail, ends endOffsets, perPartition bool) topicStats {
	ts := topicStats{name: t.Topic, partitions: len(t.Partitions)}
	for _, p := range t.Partitions.Sorted() {
		under := len(p.ISR) < len(p.Replicas)
		if under {
			ts.underReplicated++
		}
		if p.Leader < 0 {
			ts.offline++
		}
		offset := int64(-1)
		if o, ok := ends[t.Topic][p.Partition]; ok {
			offset = o
			ts.currentOffsetSum += o
			ts.hasOffsets = true
		}
		if perPartition {
			ts.parts = append(ts.parts, partitionStats{
				partition:       p.Partition,
				leader:          p.Leader,
				replicas:        len(p.Replicas),
				isr:             len(p.ISR),
				currentOffset:   offset,
				underReplicated: under,
			})
		}
	}
	return ts
}

// computeGroupLag aggregates the lag of a consumer group per topic. Only the partitions with both
// a committed offset and a known end offset are counted (partitions of topics that aren't monitored
// have no end offset and are skipped). A committed offset ahead of the end offset (e.g. after the
// end offset was read) counts as zero lag.
func computeGroupLag(committed committedOffsets, ends endOffsets, perPartition bool) []groupTopicLag {
	var res []groupTopicLag
	for topic, ps := range committed {
		topicEnds, ok := ends[topic]
		if !ok {
			continue
		}
		gt := groupTopicLag{topic: topic}
		found := false
		for p, offset := range ps {
			end, ok := topicEnds[p]
			if !ok {
				continue
			}
			found = true
			lag := max(end-offset, 0)
			gt.currentOffsetSum += offset
			gt.lagSum += lag
			if perPartition {
				gt.parts = append(gt.parts, partitionLag{partition: p, currentOffset: offset, lag: lag})
			}
		}
		if !found {
			continue
		}
		sort.Slice(gt.parts, func(i, j int) bool { return gt.parts[i].partition < gt.parts[j].partition })
		res = append(res, gt)
	}
	sort.Slice(res, func(i, j int) bool { return res[i].topic < res[j].topic })
	return res
}
