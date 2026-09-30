package dbtracker

import (
	"context"
	"testing"
	"time"

	"github.com/coroot/coroot-cluster-agent/schema"
	"github.com/stretchr/testify/assert"
)

func Test_TrimTopTables(t *testing.T) {
	dbSizes := map[string]*DBSizeSnapshot{
		"db1": {Tables: []TableSizeEntry{
			{TableKey: schema.TableKey{DB: "db1", Table: "big"}, Size: 1000},
			{TableKey: schema.TableKey{DB: "db1", Table: "small"}, Size: 10},
		}},
		"db2": {Tables: []TableSizeEntry{
			{TableKey: schema.TableKey{DB: "db2", Table: "medium"}, Size: 500},
		}},
	}

	trimTopTables(dbSizes, 2)

	var allTables []TableSizeEntry
	for _, snap := range dbSizes {
		allTables = append(allTables, snap.Tables...)
	}
	assert.Equal(t, 2, len(allTables))
}

type nopLogger struct{}

func (nopLogger) Info(args ...interface{})                    {}
func (nopLogger) Infof(format string, args ...interface{})    {}
func (nopLogger) Warning(args ...interface{})                 {}
func (nopLogger) Warningf(format string, args ...interface{}) {}
func (nopLogger) Error(args ...interface{})                   {}
func (nopLogger) Errorf(format string, args ...interface{})   {}

// Sizes must be safe to call while Track runs in another goroutine (run with -race).
func Test_TrackerSizesConcurrentWithTrack(t *testing.T) {
	calls := 0
	collect := func(ctx context.Context) (schema.Snapshot, map[string]*DBSizeSnapshot, error) {
		calls++
		return schema.Snapshot{}, map[string]*DBSizeSnapshot{
			"db1": {DatabaseSize: float64(calls), Tables: []TableSizeEntry{{TableKey: schema.TableKey{DB: "db1", Table: "t"}, Size: float64(calls)}}},
		}, nil
	}
	tr := NewTracker("test", false, true, collect, nopLogger{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		tr.Run(ctx, time.Second, nil, "")
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		sizes, _ := tr.Sizes()
		if sizes != nil {
			assert.Equal(t, float64(1), sizes["db1"].DatabaseSize)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("tracker didn't publish sizes")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run didn't stop after cancel")
	}
}
