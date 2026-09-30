package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/blang/semver"
)

type replicationSlot struct {
	name      string
	active    bool
	walStatus string
	retained  sql.Null[float64]
}

type archiverStats struct {
	archived     sql.Null[int64]
	failed       sql.Null[int64]
	lastArchived sql.Null[time.Time]
	lastFailed   sql.Null[time.Time]
}

func (c *Collector) getWalStats(ctx context.Context, version semver.Version, st *pgState) error {
	if err := c.db.QueryRowContext(ctx, `SELECT COALESCE(sum(size), 0) FROM pg_ls_waldir()`).Scan(&st.walSize); err != nil {
		st.walSize = sql.Null[float64]{}
		c.logger.Warning(err)
	}

	a := &archiverStats{}
	if err := c.db.QueryRowContext(ctx, `SELECT archived_count, last_archived_time, failed_count, last_failed_time FROM pg_stat_archiver`).Scan(&a.archived, &a.lastArchived, &a.failed, &a.lastFailed); err != nil {
		c.logger.Warning(err)
	} else if c.archPrev != nil {
		st.archArchived += delta(c.archPrev.archived, a.archived)
		st.archFailed += delta(c.archPrev.failed, a.failed)
	}
	st.archStats = a
	c.archPrev = a

	slotQuery := `SELECT slot_name, active, '' AS wal_status, pg_wal_lsn_diff(CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END, restart_lsn) FROM pg_replication_slots`
	if semver.MustParseRange(">=13.0.0")(version) {
		slotQuery = `SELECT slot_name, active, COALESCE(wal_status, ''), pg_wal_lsn_diff(CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END, restart_lsn) FROM pg_replication_slots`
	}
	rows, err := c.db.QueryContext(ctx, slotQuery)
	if err != nil {
		st.replicationSlots = nil
		return err
	}
	defer rows.Close()
	var slots []replicationSlot
	for rows.Next() {
		var s replicationSlot
		if err := rows.Scan(&s.name, &s.active, &s.walStatus, &s.retained); err != nil {
			c.logger.Warning(err)
			continue
		}
		slots = append(slots, s)
	}
	st.replicationSlots = slots
	return nil
}
