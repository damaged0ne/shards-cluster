package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/blang/semver"
	"github.com/coroot/coroot-cluster-agent/obfuscate"
)

type Connection struct {
	DB            sql.NullString
	User          sql.NullString
	Query         sql.NullString
	State         sql.NullString
	QueryStart    sql.NullTime
	BackendType   sql.NullString
	WaitEventType sql.NullString
	BlockingPid   sql.NullInt32
	XactSeconds   sql.NullFloat64
}

func (c Connection) IsClientBackend() bool {
	return c.BackendType.String == "" || c.BackendType.String == "client backend"
}

func (c Connection) QueryKey() QueryKey {
	return QueryKey{Query: obfuscate.Sql(c.Query.String), User: c.User.String, DB: c.DB.String}
}

type waitEventKey struct {
	eventType string
	event     string
}

type saSnapshot struct {
	ts                time.Time
	connections       map[int]Connection
	autovacuumWorkers float64
	waitEvents        map[waitEventKey]float64
}

// countWaitEvent accounts a backend in the wait event breakdown if it's not idle and is
// currently waiting. The label set is bounded by the wait events known to the server.
func countWaitEvent(m map[waitEventKey]float64, state, waitEventType, waitEvent sql.NullString) {
	if !waitEventType.Valid || waitEventType.String == "" {
		return
	}
	if state.String == "idle" {
		return
	}
	// Activity: background processes idling in their main loops; not a real wait
	if waitEventType.String == "Activity" {
		return
	}
	m[waitEventKey{eventType: waitEventType.String, event: waitEvent.String}]++
}

func (c *Collector) getPgStatActivity(ctx context.Context, version semver.Version, querySizeLimit int) (*saSnapshot, error) {
	snapshot := &saSnapshot{connections: map[int]Connection{}, waitEvents: map[waitEventKey]float64{}}
	var query string
	switch {
	case semver.MustParseRange(">=9.3.0 <9.6.0")(version):
		query = "SELECT s.pid, s.datname, s.usename, LEFT(s.query, %d), s.state, now(), s.query_start, s.waiting, null, null, null, EXTRACT(EPOCH FROM (now() - s.xact_start)), null"
	case semver.MustParseRange(">=9.6.0 <10.0.0")(version):
		query = "SELECT s.pid, s.datname, s.usename, LEFT(s.query, %d), s.state, now(), s.query_start, null, s.wait_event_type, null, (pg_blocking_pids(s.pid))[1], EXTRACT(EPOCH FROM (now() - s.xact_start)), s.wait_event"
	case semver.MustParseRange(">=10.0.0")(version):
		query = "SELECT s.pid, s.datname, s.usename, LEFT(s.query, %d), s.state, now(), s.query_start, null, s.wait_event_type, s.backend_type, (pg_blocking_pids(s.pid))[1], EXTRACT(EPOCH FROM (now() - s.xact_start)), s.wait_event"
	default:
		return nil, fmt.Errorf("postgres version %s is not supported", version)
	}
	query += " FROM pg_stat_activity s JOIN pg_database d ON s.datid = d.oid AND NOT d.datistemplate"
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf(query, querySizeLimit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			conn            Connection
			pid             int
			oldStyleWaiting sql.NullBool
			waitEvent       sql.NullString
		)
		err := rows.Scan(
			&pid, &conn.DB, &conn.User, &conn.Query, &conn.State, &snapshot.ts, &conn.QueryStart,
			&oldStyleWaiting, &conn.WaitEventType, &conn.BackendType, &conn.BlockingPid, &conn.XactSeconds, &waitEvent,
		)
		if err != nil {
			c.logger.Warning("failed to scan pg_stat_activity row:", err)
			continue
		}
		if !c.excludeDatabases[conn.DB.String] {
			countWaitEvent(snapshot.waitEvents, conn.State, conn.WaitEventType, waitEvent)
		}

		if conn.BackendType.String == "autovacuum worker" {
			snapshot.autovacuumWorkers++
		}
		if conn.DB.String == "" || conn.User.String == "" || conn.State.String == "" || c.excludeDatabases[conn.DB.String] {
			continue
		}
		if oldStyleWaiting.Bool {
			conn.WaitEventType.String = "Lock"
		}
		if conn.State.String != "active" && !strings.HasPrefix(conn.State.String, "idle in transaction") {
			conn.Query.String = ""
		}
		snapshot.connections[pid] = conn
	}
	return snapshot, nil
}
