package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
)

type groupReplication struct {
	localState   string
	hasLocal     bool
	size         float64
	online       float64
	certQueue    float64
	applierQueue float64
	conflicts    float64
}

func (c *Collector) groupReplicationSnapshot(ctx context.Context, st *state) {
	gr, err := c.queryGroupReplication(ctx)
	if err != nil {
		c.addScrapeError(st, err)
		st.groupReplication = nil
		return
	}
	st.groupReplication = gr
}

func (c *Collector) queryGroupReplication(ctx context.Context) (*groupReplication, error) {
	var serverUUID string
	if err := c.db.QueryRowContext(ctx, `SELECT @@server_uuid`).Scan(&serverUUID); err != nil {
		return nil, err
	}

	gr := &groupReplication{}
	rows, err := c.db.QueryContext(ctx, `SELECT MEMBER_ID, MEMBER_STATE FROM performance_schema.replication_group_members`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, state string
		if err := rows.Scan(&id, &state); err != nil {
			c.logger.Warning(err)
			continue
		}
		gr.size++
		if strings.EqualFold(state, "ONLINE") {
			gr.online++
		}
		if id == serverUUID {
			gr.localState, gr.hasLocal = state, true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if gr.size == 0 { // GR is configured but not running (plugin stopped / member left)
		return gr, nil
	}

	err = c.db.QueryRowContext(ctx, `
		SELECT COUNT_TRANSACTIONS_IN_QUEUE, COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE, COUNT_CONFLICTS_DETECTED
		FROM performance_schema.replication_group_member_stats
		WHERE MEMBER_ID = ?`, serverUUID).Scan(&gr.certQueue, &gr.applierQueue, &gr.conflicts)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	return gr, nil
}

func (st *state) groupReplicationMetrics(ch chan<- prometheus.Metric) {
	gr := st.groupReplication
	if gr == nil || gr.size == 0 {
		return
	}
	ch <- common.Gauge(dGrClusterSize, gr.size)
	ch <- common.Gauge(dGrMembersOnline, gr.online)
	if gr.hasLocal && gr.localState != "" {
		ch <- common.Gauge(dGrMemberState, 1, strings.ToLower(gr.localState))
	}
	ch <- common.Gauge(dGrTransactionsInQueue, gr.certQueue)
	ch <- common.Gauge(dGrTransactionsRemoteInApplierQueue, gr.applierQueue)
	ch <- common.Counter(dGrConflictsDetected, gr.conflicts)
}
