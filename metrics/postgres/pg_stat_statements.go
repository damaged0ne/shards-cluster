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

// Cumulative per-statement counters collected in addition to calls/total time/IO time.
// Their deltas are reported for the top queries as pg_top_query_*_per_second.
const (
	ssRows = iota
	ssSharedBlksHit
	ssSharedBlksRead
	ssSharedBlksDirtied
	ssSharedBlksWritten
	ssLocalBlksHit
	ssLocalBlksRead
	ssLocalBlksDirtied
	ssLocalBlksWritten
	ssTempBlksRead
	ssTempBlksWritten
	ssPlanTime // ms
	ssWalBytes
	ssCountersN
)

// ssCounterColumns maps each counter to its pg_stat_statements column.
var ssCounterColumns = [ssCountersN]string{
	ssRows:              "rows",
	ssSharedBlksHit:     "shared_blks_hit",
	ssSharedBlksRead:    "shared_blks_read",
	ssSharedBlksDirtied: "shared_blks_dirtied",
	ssSharedBlksWritten: "shared_blks_written",
	ssLocalBlksHit:      "local_blks_hit",
	ssLocalBlksRead:     "local_blks_read",
	ssLocalBlksDirtied:  "local_blks_dirtied",
	ssLocalBlksWritten:  "local_blks_written",
	ssTempBlksRead:      "temp_blks_read",
	ssTempBlksWritten:   "temp_blks_written",
	ssPlanTime:          "total_plan_time",
	ssWalBytes:          "wal_bytes",
}

type ssRow struct {
	obfuscatedQueryText string
	calls               sql.NullInt64
	totalTime           sql.NullFloat64
	ioTime              sql.NullFloat64
	counters            [ssCountersN]sql.NullFloat64
	meanExecTime        sql.NullFloat64 // ms, since the last stats reset
	minExecTime         sql.NullFloat64 // ms, since the last stats reset
	maxExecTime         sql.NullFloat64 // ms, since the last stats reset
}

func (r ssRow) QueryKey(id statementId) QueryKey {
	return QueryKey{Query: r.obfuscatedQueryText, User: id.user.String, DB: id.db.String}
}

type statementId struct {
	id   sql.NullInt64
	user sql.NullString
	db   sql.NullString
}

type ssSnapshot struct {
	ts   time.Time
	rows map[statementId]ssRow
}

// ssColumnsQuery lists the columns of the installed pg_stat_statements view. The set depends
// on the extension version (which may lag behind the server version after pg_upgrade),
// so it's detected rather than derived from the server version.
const ssColumnsQuery = `
SELECT a.attname FROM pg_attribute a
WHERE a.attrelid = to_regclass('pg_stat_statements') AND a.attnum > 0 AND NOT a.attisdropped`

// buildStatStatementsQuery returns the query for the given set of available pg_stat_statements
// columns. Missing columns are selected as NULL, so the scan order is always the same:
// datname, rolname, query, queryid, calls, total time, IO time, ssCountersN counters, mean, min, max.
func buildStatStatementsQuery(columns map[string]bool, querySizeLimit int) (string, error) {
	col := func(name string) string {
		if columns[name] {
			return "s." + name
		}
		return "NULL::float8"
	}
	sum := func(names ...string) string {
		var parts []string
		for _, n := range names {
			if columns[n] {
				parts = append(parts, "s."+n)
			}
		}
		if len(parts) == 0 {
			return "NULL::float8"
		}
		return strings.Join(parts, " + ")
	}
	if !columns["queryid"] || !columns["calls"] {
		return "", fmt.Errorf("pg_stat_statements: unsupported extension version (no queryid/calls columns)")
	}
	var totalTime, mean, minT, maxT string
	switch {
	case columns["total_exec_time"]: // 1.8+ (PG13+)
		totalTime = sum("total_plan_time", "total_exec_time")
		mean, minT, maxT = col("mean_exec_time"), col("min_exec_time"), col("max_exec_time")
	case columns["total_time"]: // < 1.8
		totalTime = "s.total_time"
		mean, minT, maxT = col("mean_time"), col("min_time"), col("max_time") // 1.3+ (PG9.5+)
	default:
		return "", fmt.Errorf("pg_stat_statements: unsupported extension version (no total time column)")
	}
	var ioTime string
	if columns["shared_blk_read_time"] { // 1.11+ (PG17+)
		ioTime = sum("shared_blk_read_time", "shared_blk_write_time", "local_blk_read_time", "local_blk_write_time", "temp_blk_read_time", "temp_blk_write_time")
	} else {
		ioTime = sum("blk_read_time", "blk_write_time")
	}
	exprs := []string{"d.datname", "r.rolname", fmt.Sprintf("LEFT(s.query, %d)", querySizeLimit), "s.queryid", "s.calls", totalTime, ioTime}
	for _, c := range ssCounterColumns {
		exprs = append(exprs, col(c))
	}
	exprs = append(exprs, mean, minT, maxT)
	return "SELECT " + strings.Join(exprs, ", ") +
		" FROM pg_stat_statements s JOIN pg_roles r ON r.oid=s.userid JOIN pg_database d ON d.oid=s.dbid AND NOT d.datistemplate", nil
}

func (c *Collector) getStatStatementsColumns(ctx context.Context) (map[string]bool, error) {
	rows, err := c.db.QueryContext(ctx, ssColumnsQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		res[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, fmt.Errorf("pg_stat_statements: %w (is the extension created in the database the agent connects to?)", errViewNotFound)
	}
	return res, nil
}

func (c *Collector) getStatStatements(ctx context.Context, version semver.Version, querySizeLimit int, prev map[statementId]ssRow) (*ssSnapshot, error) {
	if version.LT(semver.Version{Major: 9, Minor: 4}) {
		return nil, fmt.Errorf("postgres version %s is not supported", version)
	}
	columns, err := c.getStatStatementsColumns(ctx)
	if err != nil {
		return nil, err
	}
	query, err := buildStatStatementsQuery(columns, querySizeLimit)
	if err != nil {
		return nil, err
	}
	snapshot := &ssSnapshot{ts: time.Now(), rows: map[statementId]ssRow{}}
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var queryText sql.NullString
	for rows.Next() {
		var id statementId
		r := ssRow{}
		dest := []any{&id.db, &id.user, &queryText, &id.id, &r.calls, &r.totalTime, &r.ioTime}
		for i := range r.counters {
			dest = append(dest, &r.counters[i])
		}
		dest = append(dest, &r.meanExecTime, &r.minExecTime, &r.maxExecTime)
		if err := rows.Scan(dest...); err != nil {
			c.logger.Warning("failed to scan pg_stat_statements row:", err)
			continue
		}
		if id.user.String == "" || id.db.String == "" || !id.id.Valid || c.excludeDatabases[id.db.String] {
			continue
		}
		if p, ok := prev[id]; ok {
			r.obfuscatedQueryText = p.obfuscatedQueryText
		} else {
			r.obfuscatedQueryText = obfuscate.Sql(queryText.String)
		}
		snapshot.rows[id] = r
	}
	return snapshot, rows.Err()
}
