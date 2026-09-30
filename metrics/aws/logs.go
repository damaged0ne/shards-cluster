package aws

import (
	"bufio"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/coroot/logparser"
	"k8s.io/klog"
)

const (
	logsRefreshInterval = 30 * time.Second
)

type LogReader struct {
	discoverer *Discoverer
	ctx        context.Context
	instanceId *string
	logs       map[string]*logFileMeta
	ch         chan<- logparser.LogEntry
	stop       chan struct{}
	stopOnce   sync.Once
}

// NewLogReader starts reading the logs of the instance in the background until Stop is called or ctx is cancelled.
func NewLogReader(ctx context.Context, discoverer *Discoverer, instanceId *string, ch chan<- logparser.LogEntry) *LogReader {
	r := &LogReader{
		discoverer: discoverer,
		ctx:        ctx,
		instanceId: instanceId,
		logs:       map[string]*logFileMeta{},
		ch:         ch,
		stop:       make(chan struct{}),
	}
	go func() {
		initialized := r.refresh(true)
		t := time.NewTicker(logsRefreshInterval)
		defer t.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-t.C:
				if ok := r.refresh(!initialized); ok {
					initialized = true
				}
			}
		}
	}()
	return r
}

// Stop is idempotent and doesn't wait for an in-flight refresh.
func (r *LogReader) Stop() {
	r.stopOnce.Do(func() {
		close(r.stop)
	})
}

func (r *LogReader) stopped() bool {
	select {
	case <-r.stop:
		return true
	default:
		return r.ctx.Err() != nil
	}
}

func (r *LogReader) refresh(init bool) bool {
	if r.stopped() {
		return false
	}
	t := time.Now()
	defer func() {
		klog.V(2).Infoln(aws.ToString(r.instanceId), "logs refreshed in", time.Since(t).Truncate(time.Millisecond))
	}()
	ctx, cancel := context.WithTimeout(r.ctx, apiTimeout)
	res, err := r.discoverer.RDSClient().DescribeDBLogFiles(ctx, &rds.DescribeDBLogFilesInput{DBInstanceIdentifier: r.instanceId})
	cancel()
	if err != nil {
		if r.stopped() {
			return false
		}
		klog.Warning("failed to describe log files:", err)
		r.discoverer.registerError(err)
		return false
	}
	seenLogs := map[string]bool{}
	for _, f := range res.DescribeDBLogFiles {
		if r.stopped() {
			return false
		}
		fileName := aws.ToString(f.LogFileName)
		seenLogs[fileName] = true
		meta := r.logs[fileName]
		if meta == nil {
			klog.Info("new log file detected:", fileName)
			meta = &logFileMeta{}
			r.logs[fileName] = meta
		}

		if init {
			var n int32 = 1 // read last line to obtain the marker
			response, err := r.download(fileName, nil, &n)
			if err != nil {
				klog.Warning(err)
				continue
			}
			meta.lastWritten = aws.ToInt64(f.LastWritten)
			meta.marker = aws.ToString(response.Marker)
			continue
		}

		if meta.lastWritten >= aws.ToInt64(f.LastWritten) {
			continue
		}
		response, err := r.download(fileName, &meta.marker, nil)
		if err != nil {
			klog.Warning(err)
			continue
		}
		meta.lastWritten = aws.ToInt64(f.LastWritten)
		meta.marker = aws.ToString(response.Marker)
		r.write(response.LogFileData)
	}

	for name := range r.logs {
		if !seenLogs[name] {
			delete(r.logs, name)
		}
	}
	return true
}

func (r *LogReader) download(logFileName string, marker *string, numberOfLines *int32) (*rds.DownloadDBLogFilePortionOutput, error) {
	request := rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: r.instanceId,
		LogFileName:          &logFileName,
		Marker:               marker,
		NumberOfLines:        numberOfLines,
	}
	ctx, cancel := context.WithTimeout(r.ctx, apiTimeout)
	defer cancel()
	response, err := r.discoverer.RDSClient().DownloadDBLogFilePortion(ctx, &request)
	if err != nil {
		return nil, fmt.Errorf(`failed to download file %s: %s`, logFileName, err)
	}
	return response, nil
}

func (r *LogReader) write(data *string) {
	reader := bufio.NewReader(strings.NewReader(aws.ToString(data)))
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		select {
		case r.ch <- logparser.LogEntry{Content: strings.TrimSuffix(line, "\n"), Level: logparser.LevelUnknown}:
		case <-r.stop: // the parser may be stopped already
			return
		}
	}
}

type logFileMeta struct {
	lastWritten int64
	marker      string
}
