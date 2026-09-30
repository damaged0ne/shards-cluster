package mysql

import (
	"strconv"
	"strings"

	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
)

const nanoSeconds = 1e9

func wsrepEnabled(variables map[string]string) bool {
	provider := variables["wsrep_provider"]
	return provider != "" && !strings.EqualFold(provider, "none")
}

func (st *state) galeraMetrics(ch chan<- prometheus.Metric) {
	if !st.isGalera {
		return
	}
	metricFromVariable(ch, dWsrepClusterSize, "wsrep_cluster_size", prometheus.GaugeValue, st.globalStatus)
	if comment := st.globalStatus["wsrep_local_state_comment"]; comment != "" {
		ch <- common.Gauge(dWsrepLocalState, 1, strings.ToLower(comment))
	}
	metricFromVariable(ch, dWsrepLocalRecvQueue, "wsrep_local_recv_queue", prometheus.GaugeValue, st.globalStatus)
	metricFromVariable(ch, dWsrepLocalSendQueue, "wsrep_local_send_queue", prometheus.GaugeValue, st.globalStatus)

	if v := st.globalStatus["wsrep_flow_control_paused_ns"]; v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			ch <- common.Counter(dWsrepFlowControlPaused, f/nanoSeconds)
		}
	}
	metricFromVariable(ch, dWsrepCertFailures, "wsrep_local_cert_failures", prometheus.CounterValue, st.globalStatus)
	metricFromVariable(ch, dWsrepBfAborts, "wsrep_local_bf_aborts", prometheus.CounterValue, st.globalStatus)

	metricFromOnOff(ch, dWsrepReady, "wsrep_ready", st.globalStatus)
	metricFromOnOff(ch, dWsrepConnected, "wsrep_connected", st.globalStatus)

	if status := st.globalStatus["wsrep_cluster_status"]; status != "" {
		ch <- common.Gauge(dWsrepClusterStatus, 1, strings.ToLower(status))
	}
}

func metricFromOnOff(ch chan<- prometheus.Metric, desc *prometheus.Desc, name string, variables map[string]string) {
	v, ok := variables[name]
	if !ok {
		return
	}
	value := 0.0
	if strings.EqualFold(v, "ON") {
		value = 1.0
	}
	ch <- common.Gauge(desc, value)
}
