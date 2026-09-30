package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/agent"
	"k8s.io/klog/v2"
)

// copy-pasted from Prometheus' main.go

type readyStorage struct {
	mtx             sync.RWMutex
	db              storage.Storage
	startTimeMargin int64
	stats           *tsdb.DBStats
}

func (s *readyStorage) ApplyConfig(conf *config.Config) error {
	return nil
}

func (s *readyStorage) Set(db storage.Storage, startTimeMargin int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.db = db
	s.startTimeMargin = startTimeMargin
}

func (s *readyStorage) get() storage.Storage {
	s.mtx.RLock()
	x := s.db
	s.mtx.RUnlock()
	return x
}

func (s *readyStorage) getStats() *tsdb.DBStats {
	s.mtx.RLock()
	x := s.stats
	s.mtx.RUnlock()
	return x
}

func (s *readyStorage) StartTime() (int64, error) {
	if x := s.get(); x != nil {
		switch db := x.(type) {
		case *agent.DB:
			return db.StartTime()
		default:
			panic(fmt.Sprintf("unknown storage type %T", db))
		}
	}
	return math.MaxInt64, tsdb.ErrNotReady
}

func (s *readyStorage) Querier(mint, maxt int64) (storage.Querier, error) {
	if x := s.get(); x != nil {
		return x.Querier(mint, maxt)
	}
	return nil, tsdb.ErrNotReady
}

func (s *readyStorage) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	if x := s.get(); x != nil {
		return x.ChunkQuerier(mint, maxt)
	}
	return nil, tsdb.ErrNotReady
}

func (s *readyStorage) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	if x := s.get(); x != nil {
		switch db := x.(type) {
		case *agent.DB:
			return nil, agent.ErrUnsupported
		default:
			panic(fmt.Sprintf("unknown storage type %T", db))
		}
	}
	return nil, tsdb.ErrNotReady
}

func (s *readyStorage) Appender(ctx context.Context) storage.Appender {
	if x := s.get(); x != nil {
		return x.Appender(ctx)
	}
	return notReadyAppender{}
}

func (s *readyStorage) AppenderV2(ctx context.Context) storage.AppenderV2 {
	if x := s.get(); x != nil {
		return x.AppenderV2(ctx)
	}
	return notReadyAppenderV2{}
}

type notReadyAppender struct{}

func (n notReadyAppender) SetOptions(*storage.AppendOptions) {}

func (n notReadyAppender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	return 0, tsdb.ErrNotReady
}

func (n notReadyAppender) AppendExemplar(ref storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, tsdb.ErrNotReady
}

func (n notReadyAppender) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, tsdb.ErrNotReady
}

func (n notReadyAppender) AppendHistogramSTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, tsdb.ErrNotReady
}

func (n notReadyAppender) AppendSTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64) (storage.SeriesRef, error) {
	return 0, tsdb.ErrNotReady
}

func (n notReadyAppender) UpdateMetadata(ref storage.SeriesRef, l labels.Labels, m metadata.Metadata) (storage.SeriesRef, error) {
	return 0, tsdb.ErrNotReady
}

func (n notReadyAppender) Commit() error { return tsdb.ErrNotReady }

func (n notReadyAppender) Rollback() error { return tsdb.ErrNotReady }

type notReadyAppenderV2 struct{}

func (n notReadyAppenderV2) Append(ref storage.SeriesRef, l labels.Labels, st, t int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram, opts storage.AOptions) (storage.SeriesRef, error) {
	return 0, tsdb.ErrNotReady
}

func (n notReadyAppenderV2) Commit() error { return tsdb.ErrNotReady }

func (n notReadyAppenderV2) Rollback() error { return tsdb.ErrNotReady }

func (s *readyStorage) Close() error {
	if x := s.get(); x != nil {
		return x.Close()
	}
	return nil
}

func (s *readyStorage) CleanTombstones() error {
	if x := s.get(); x != nil {
		switch db := x.(type) {
		case *agent.DB:
			return agent.ErrUnsupported
		default:
			panic(fmt.Sprintf("unknown storage type %T", db))
		}
	}
	return tsdb.ErrNotReady
}

func (s *readyStorage) Delete(ctx context.Context, mint, maxt int64, ms ...*labels.Matcher) error {
	if x := s.get(); x != nil {
		switch db := x.(type) {
		case *agent.DB:
			return agent.ErrUnsupported
		default:
			panic(fmt.Sprintf("unknown storage type %T", db))
		}
	}
	return tsdb.ErrNotReady
}

func (s *readyStorage) Snapshot(dir string, withHead bool) error {
	if x := s.get(); x != nil {
		switch db := x.(type) {
		case *agent.DB:
			return agent.ErrUnsupported
		default:
			panic(fmt.Sprintf("unknown storage type %T", db))
		}
	}
	return tsdb.ErrNotReady
}

func (s *readyStorage) Stats(statsByLabelName string, limit int) (*tsdb.Stats, error) {
	if x := s.get(); x != nil {
		switch db := x.(type) {
		case *agent.DB:
			return nil, agent.ErrUnsupported
		default:
			panic(fmt.Sprintf("unknown storage type %T", db))
		}
	}
	return nil, tsdb.ErrNotReady
}

func (s *readyStorage) WALReplayStatus() (tsdb.WALReplayStatus, error) {
	if x := s.getStats(); x != nil {
		return x.Head.WALReplayStatus.GetWALReplayStatus(), nil
	}
	return tsdb.WALReplayStatus{}, tsdb.ErrNotReady
}

var ErrNotReady = errors.New("scrape manager not ready")

type readyScrapeManager struct {
	mtx sync.RWMutex
	m   *scrape.Manager
}

func (rm *readyScrapeManager) Set(m *scrape.Manager) {
	rm.mtx.Lock()
	defer rm.mtx.Unlock()

	rm.m = m
}

func (rm *readyScrapeManager) Get() (*scrape.Manager, error) {
	rm.mtx.RLock()
	defer rm.mtx.RUnlock()

	if rm.m != nil {
		return rm.m, nil
	}

	return nil, ErrNotReady
}

// slogHandler forwards the log records of the Prometheus components (scrape manager, remote write, WAL, ...)
// to klog. Debug records are dropped, as the go-kit level filter did before the switch to slog.
type slogHandler struct {
	attrs []slog.Attr
}

func (slogHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= slog.LevelInfo
}

func (h slogHandler) Handle(_ context.Context, r slog.Record) error {
	args := make([]interface{}, 0, (len(h.attrs)+r.NumAttrs())*2+1)
	args = append(args, r.Message)
	for _, a := range h.attrs {
		args = append(args, a.Key, a.Value.Any())
	}
	r.Attrs(func(a slog.Attr) bool {
		args = append(args, a.Key, a.Value.Any())
		return true
	})
	switch {
	case r.Level >= slog.LevelError:
		klog.Errorln(args...)
	case r.Level >= slog.LevelWarn:
		klog.Warningln(args...)
	default:
		klog.Infoln(args...)
	}
	return nil
}

func (h slogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return slogHandler{attrs: append(slices.Clip(h.attrs), attrs...)}
}

func (h slogHandler) WithGroup(string) slog.Handler { return h }

// NewLogger returns a slog logger writing to klog, for the Prometheus libraries.
func NewLogger() *slog.Logger {
	return slog.New(slogHandler{})
}
