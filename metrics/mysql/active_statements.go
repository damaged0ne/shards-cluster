package mysql

import (
	"context"

	"github.com/coroot/coroot-cluster-agent/obfuscate"
)

type activeStmtKey struct {
	threadID uint64
	eventID  uint64
}

type activeStmt struct {
	qk         queryKey
	elapsedSec float64
}

type activeStatementsSnapshot struct {
	stmts map[activeStmtKey]activeStmt
}

func (c *Collector) queryActiveStatements(ctx context.Context, prev *activeStatementsSnapshot) (*activeStatementsSnapshot, error) {
	snapshot := &activeStatementsSnapshot{stmts: map[activeStmtKey]activeStmt{}}
	q := `
	SELECT
	    ec.THREAD_ID,
	    ec.EVENT_ID,
	    IFNULL(ec.CURRENT_SCHEMA, ''),
	    ec.DIGEST_TEXT,
	    ec.TIMER_WAIT
	FROM performance_schema.events_statements_current ec
	JOIN performance_schema.threads t ON ec.THREAD_ID = t.THREAD_ID
	WHERE t.PROCESSLIST_ID IS NOT NULL
	    AND t.PROCESSLIST_ID != CONNECTION_ID()
	    AND ec.END_EVENT_ID IS NULL
	    AND ec.DIGEST_TEXT IS NOT NULL
	    AND ec.TIMER_WAIT IS NOT NULL`
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var digestText string
	for rows.Next() {
		var k activeStmtKey
		var schema string
		var timerWait uint64
		if err := rows.Scan(&k.threadID, &k.eventID, &schema, &digestText, &timerWait); err != nil {
			c.logger.Warning(err)
			continue
		}
		var obf string
		if prev != nil {
			if p, ok := prev.stmts[k]; ok {
				obf = p.qk.query
			}
		}
		if obf == "" {
			obf = obfuscate.SqlWithDialect(digestText, obfuscate.DialectMySQL)
		}
		snapshot.stmts[k] = activeStmt{
			qk:         queryKey{schema: schema, query: obf},
			elapsedSec: float64(timerWait) / picoSeconds,
		}
	}
	return snapshot, rows.Err()
}
