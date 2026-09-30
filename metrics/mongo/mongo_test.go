package mongo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/auth"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
)

// unreachableAddr refuses connections immediately, so no real MongoDB is needed.
const unreachableAddr = "127.0.0.1:1"

func collectMetrics(c *Collector) map[string][]map[string]string {
	ch := make(chan prometheus.Metric, 1000)
	c.Collect(ch)
	close(ch)
	res := map[string][]map[string]string{}
	for m := range ch {
		var pb dto.Metric
		_ = m.Write(&pb)
		labels := map[string]string{}
		for _, l := range pb.Label {
			labels[l.GetName()] = l.GetValue()
		}
		labels["__value"] = fmt.Sprint(pb.GetGauge().GetValue())
		name := m.Desc().String()
		res[name] = append(res[name], labels)
	}
	return res
}

func scrapeErrorLabels(c *Collector) []map[string]string {
	for name, v := range collectMetrics(c) {
		if strings.Contains(name, `"mongo_scrape_error"`) {
			return v
		}
	}
	return nil
}

func TestErrorReason(t *testing.T) {
	sse := topology.ServerSelectionError{
		Wrapped: errors.New("server selection timeout, current topology: { Type: Single, Servers: [{ Addr: 10.1.2.3:27017, Type: Unknown, Last error: dial tcp 10.1.2.3:27017: i/o timeout }] }"),
	}
	sseAuth := topology.ServerSelectionError{Desc: description.Topology{Servers: []description.Server{{LastError: fmt.Errorf("handshake: %w", &auth.Error{})}}}}
	for _, c := range []struct {
		err  error
		want string
	}{
		{sse, "unreachable"},
		{fmt.Errorf("wrapped: %w", sse), "unreachable"},
		{sseAuth, "auth"},
		{&auth.Error{}, "auth"},
		{mongo.CommandError{Code: 18, Name: "AuthenticationFailed", Message: "user alice@10.0.0.1"}, "auth"},
		{mongo.CommandError{Code: 13, Name: "Unauthorized", Message: "not authorized on admin to execute command { serverStatus: 1 }"}, "unauthorized"},
		{mongo.CommandError{Code: 26, Name: "NamespaceNotFound", Message: "ns db.coll not found"}, "command error: NamespaceNotFound"},
		{mongo.CommandError{Code: 1, Name: "bad name with spaces"}, "unknown"},
		{mongo.CommandError{Labels: []string{"NetworkError"}, Message: "connection reset by 10.0.0.1"}, "unreachable"},
		{context.DeadlineExceeded, "timeout"},
		{context.Canceled, "canceled"},
		{mongo.ErrClientDisconnected, "disconnected"},
		{errCollectorClosed, "closed"},
		{errors.New("some error with a unique id 0x1234"), "unknown"},
	} {
		assert.Equal(t, c.want, errorReason(c.err), c.err.Error())
	}
}

func newTestCollector(t *testing.T) *Collector {
	c := New(unreachableAddr, "", "", common.TLSCredentials{}, nil, 50*time.Millisecond, 200*time.Millisecond,
		logger.NewKlog("test"), nil, "", 0, false)
	return c
}

func TestCollectUnreachable(t *testing.T) {
	c := newTestCollector(t)
	defer c.Close()
	labels := scrapeErrorLabels(c)
	require.Len(t, labels, 1)
	assert.Equal(t, "unreachable", labels[0]["error"])
	assert.Equal(t, "1", labels[0]["__value"])
}

// A client that fails a ping (e.g. in Collect) must stay usable by a concurrent snapshot
// that holds a reference to it; it is disconnected only when the last reference is released.
func TestClientRefCounting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Collector{
		ctx:            ctx,
		cancelFunc:     cancel,
		clientOpts:     newTestCollectorOpts(),
		collectTimeout: 200 * time.Millisecond,
		logger:         logger.NewKlog("test"),
		done:           make(chan struct{}),
	}
	close(c.done) // no snapshot goroutine

	held, err := c.acquireClient(ctx) // e.g. a running snapshot
	require.NoError(t, err)

	_, err = c.connectAndPing(ctx) // e.g. Collect: the ping fails, the client gets retired
	require.Error(t, err)

	c.clientLock.Lock()
	assert.True(t, held.retired)
	assert.Nil(t, c.client)
	assert.Equal(t, 1, held.refs)
	c.clientLock.Unlock()
	require.NotNil(t, held.client)
	// still connected: the error is about the server, not a disconnected client
	assert.NotErrorIs(t, held.client.Ping(ctx, nil), mongo.ErrClientDisconnected)

	c.releaseClient(held)
	assert.ErrorIs(t, held.client.Ping(ctx, nil), mongo.ErrClientDisconnected)

	// a new client is created on the next use
	next, err := c.acquireClient(ctx)
	require.NoError(t, err)
	assert.NotSame(t, held, next)
	c.releaseClient(next)

	require.NoError(t, c.Close())
	_, err = c.acquireClient(ctx)
	assert.ErrorIs(t, err, errCollectorClosed)
}

func newTestCollectorOpts() *options.ClientOptions {
	return options.Client().SetHosts([]string{unreachableAddr}).SetDirect(true).
		SetServerSelectionTimeout(200 * time.Millisecond).SetConnectTimeout(200 * time.Millisecond)
}

func TestCloseConcurrentWithSnapshotAndCollect(t *testing.T) {
	c := newTestCollector(t)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					collectMetrics(c)
				}
			}
		}()
	}
	time.Sleep(700 * time.Millisecond) // let the background snapshot goroutine run a few times
	closed := make(chan error)
	go func() { closed <- c.Close() }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
	close(stop)
	wg.Wait()

	labels := scrapeErrorLabels(c)
	require.Len(t, labels, 1)
	assert.Equal(t, "closed", labels[0]["error"])
}
