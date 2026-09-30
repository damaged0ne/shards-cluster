package aws

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/coroot/coroot-cluster-agent/flags"
	"github.com/coroot/logparser"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog"
)

const rdsMetricsLogGroupName = "RDSOSMetrics"

var (
	dRDSInfo = common.Desc("aws_rds_info", "RDS instance info",
		"region", "availability_zone", "endpoint", "ipv4", "port",
		"engine", "engine_version", "instance_type", "storage_type", "multi_az",
		"secondary_availability_zone", "cluster_id", "source_instance_id",
	)
	dRDSStatus                      = common.Desc("aws_rds_status", "Status of the RDS instance", "status")
	dRDSAllocatedStorage            = common.Desc("aws_rds_allocated_storage_gibibytes", "Allocated storage size")
	dRDSStorageAutoscalingThreshold = common.Desc("aws_rds_storage_autoscaling_threshold_gibibytes", "Storage autoscaling threshold")
	dRDSStorageProvisionedIOPs      = common.Desc("aws_rds_storage_provisioned_iops", "Number of provisioned IOPs")
	dRDSReadReplicaInfo             = common.Desc("aws_rds_read_replica_info", "Read replica info", "replica_instance_id")
	dRDSBackupRetentionPeriod       = common.Desc("aws_rds_backup_retention_period_days", "Backup retention period")

	dRDSCPUCores  = common.Desc("aws_rds_cpu_cores", "The number of virtual CPUs")
	dRDSCPUUsage  = common.Desc("aws_rds_cpu_usage_percent", "The percentage of the CPU spent in each mode", "mode")
	dRDSIOOps     = common.Desc("aws_rds_io_ops_per_second", "The number of I/O transactions per second", "device", "operation")
	dRDSIObytes   = common.Desc("aws_rds_io_bytes_per_second", "The number of bytes read or written per second", "device", "operation")
	dRDSIOlatency = common.Desc("aws_rds_io_latency_seconds", "The average elapsed time between the submission of an I/O request and its completion (Amazon Aurora only)", "device", "operation")
	dRDSIOawait   = common.Desc("aws_rds_io_await_seconds", "The number of seconds required to respond to requests, including queue time and service time", "device")
	dRDSIOutil    = common.Desc("aws_rds_io_util_percent", "The percentage of CPU time during which requests were issued.", "device")
	dRDSFSTotal   = common.Desc("aws_rds_fs_total_bytes", "The total number of disk space available for the file system", "mount_point")
	dRDSFSUsed    = common.Desc("aws_rds_fs_used_bytes", "The amount of disk space used by files in the file system", "mount_point")
	dRDSMemTotal  = common.Desc("aws_rds_memory_total_bytes", "The total amount of memory")
	dRDSMemCached = common.Desc("aws_rds_memory_cached_bytes", "The amount of memory used as page cache")
	dRDSMemFree   = common.Desc("aws_rds_memory_free_bytes", "The amount of unassigned memory")
	dRDSNetRx     = common.Desc("aws_rds_net_rx_bytes_per_second", "The number of bytes received per second", "interface")
	dRDSNetTx     = common.Desc("aws_rds_net_tx_bytes_per_second", "The number of bytes transmitted per second", "interface")

	dRDSLogMessages = common.Desc("aws_rds_log_messages_total",
		"Number of messages grouped by the automatically extracted repeated pattern",
		"level", "pattern_hash", "sample")
)

const (
	// Enhanced Monitoring publishes a sample every MonitoringInterval (1-60s): it's fetched in the background
	// at about the same pace (but not too often), and the last sample is served by Collect
	osMetricsMinInterval = 15 * time.Second
	osMetricsMaxInterval = time.Minute
)

type RDSCollector struct {
	discoverer *Discoverer
	ctx        context.Context // cancelled on Stop
	cancel     context.CancelFunc
	stop       chan struct{}
	stopOnce   sync.Once

	// written by the discovery goroutine (update) and the OS metrics goroutine, read by Collect
	lock      sync.RWMutex
	region    string
	instance  *rdstypes.DBInstance
	ip        *net.IPAddr
	osMetrics *RDSOSMetrics

	logReader  *LogReader
	logParser  *logparser.Parser
	logEmitter *common.LogEmitter
}

func NewRDSCollector(discoverer *Discoverer, region string, instance *rdstypes.DBInstance) *RDSCollector {
	ctx, cancel := context.WithCancel(discoverer.ctx)
	c := &RDSCollector{discoverer: discoverer, ctx: ctx, cancel: cancel, stop: make(chan struct{}), region: region, instance: instance}

	switch aws.ToString(instance.Engine) {
	case "postgres", "aurora-postgresql", "mysql", "mariadb", "aurora-mysql":
		var onMsg logparser.OnMsgCallbackF
		if *flags.CollectAWSLogs {
			emitter, err := common.NewLogEmitter("/aws/rds/"+region+"/"+aws.ToString(instance.DBInstanceIdentifier), "rds:"+aws.ToString(instance.DBInstanceIdentifier))
			if err != nil {
				klog.Errorln("failed to create the log emitter, logs won't be forwarded:", err)
			} else {
				c.logEmitter = emitter
				onMsg = emitter.Callback()
			}
		}
		ch := make(chan logparser.LogEntry)
		c.logParser = logparser.NewParser(ch, nil, onMsg, common.MultilineCollectorTimeout, common.LogPatternsPerLevel, false, nil)
		c.logReader = NewLogReader(ctx, discoverer, instance.DBInstanceIdentifier, ch)
	}

	go c.osMetricsLoop()
	return c
}

func (c *RDSCollector) snapshot() (string, *rdstypes.DBInstance, *net.IPAddr, *RDSOSMetrics) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.region, c.instance, c.ip, c.osMetrics
}

func enhancedMonitoringEnabled(instance *rdstypes.DBInstance) bool {
	return instance != nil && aws.ToInt32(instance.MonitoringInterval) > 0 && instance.DbiResourceId != nil
}

func (c *RDSCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("aws_rds_collector", "", nil, nil)
}

func (c *RDSCollector) Collect(ch chan<- prometheus.Metric) {
	region, instance, ipAddr, osMetrics := c.snapshot()
	if instance == nil {
		return
	}
	var address, port, ip string
	if instance.Endpoint != nil {
		address = aws.ToString(instance.Endpoint.Address)
		port = strconv.Itoa(int(aws.ToInt32(instance.Endpoint.Port)))
	}
	if ipAddr != nil {
		ip = ipAddr.String()
	}
	ch <- common.Gauge(dRDSStatus, 1, aws.ToString(instance.DBInstanceStatus))
	ch <- common.Gauge(dRDSInfo, 1,
		region,
		aws.ToString(instance.AvailabilityZone),
		address,
		ip,
		port,
		aws.ToString(instance.Engine),
		aws.ToString(instance.EngineVersion),
		aws.ToString(instance.DBInstanceClass),
		aws.ToString(instance.StorageType),
		strconv.FormatBool(aws.ToBool(instance.MultiAZ)),
		aws.ToString(instance.SecondaryAvailabilityZone),
		idWithRegion(region, aws.ToString(instance.DBClusterIdentifier)),
		idWithRegion(region, aws.ToString(instance.ReadReplicaSourceDBInstanceIdentifier)),
	)
	ch <- common.Gauge(dRDSAllocatedStorage, float64(aws.ToInt32(instance.AllocatedStorage)))
	ch <- common.Gauge(dRDSStorageAutoscalingThreshold, float64(aws.ToInt32(instance.MaxAllocatedStorage)))
	ch <- common.Gauge(dRDSStorageProvisionedIOPs, float64(aws.ToInt32(instance.Iops)))
	ch <- common.Gauge(dRDSBackupRetentionPeriod, float64(aws.ToInt32(instance.BackupRetentionPeriod)))
	for _, r := range instance.ReadReplicaDBInstanceIdentifiers {
		ch <- common.Gauge(dRDSReadReplicaInfo, float64(1), idWithRegion(region, r))
	}

	if osMetrics != nil && enhancedMonitoringEnabled(instance) {
		collectOsMetrics(osMetrics, ch)
	}

	if c.logParser != nil {
		for _, lc := range c.logParser.GetCounters() {
			ch <- common.Counter(dRDSLogMessages, float64(lc.Messages), lc.Level.String(), lc.Hash, lc.Sample)
		}
	}
}

// Stop is idempotent.
func (c *RDSCollector) Stop() {
	c.stopOnce.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		if c.stop != nil {
			close(c.stop)
		}
		if c.logReader != nil {
			c.logReader.Stop()
		}
		if c.logParser != nil {
			c.logParser.Stop()
		}
		if c.logEmitter != nil {
			c.logEmitter.Stop()
		}
	})
}

func (c *RDSCollector) update(region string, instance *rdstypes.DBInstance) {
	var ip *net.IPAddr
	if instance.Endpoint != nil {
		var err error
		if ip, err = resolveIP(c.ctx, aws.ToString(instance.Endpoint.Address)); err != nil {
			klog.Warning(err)
		}
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	c.region = region
	c.instance = instance
	if ip != nil {
		c.ip = ip
	}
}

func osMetricsInterval(instance *rdstypes.DBInstance) time.Duration {
	i := time.Duration(aws.ToInt32(instance.MonitoringInterval)) * time.Second
	switch {
	case i < osMetricsMinInterval:
		return osMetricsMinInterval
	case i > osMetricsMaxInterval:
		return osMetricsMaxInterval
	}
	return i
}

func (c *RDSCollector) osMetricsLoop() {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
		}
		c.refreshOsMetrics()
		_, instance, _, _ := c.snapshot()
		t.Reset(osMetricsInterval(instance))
	}
}

func (c *RDSCollector) refreshOsMetrics() {
	_, instance, _, _ := c.snapshot()
	var m *RDSOSMetrics
	if enhancedMonitoringEnabled(instance) {
		m = c.fetchOsMetrics(aws.ToString(instance.DbiResourceId))
	}
	c.lock.Lock()
	c.osMetrics = m // nil on an error: stale values are not served
	c.lock.Unlock()
}

func (c *RDSCollector) fetchOsMetrics(dbiResourceId string) *RDSOSMetrics {
	input := cloudwatchlogs.GetLogEventsInput{
		Limit:         aws.Int32(1),
		StartFromHead: aws.Bool(false),
		LogGroupName:  aws.String(rdsMetricsLogGroupName),
		LogStreamName: aws.String(dbiResourceId),
	}
	ctx, cancel := context.WithTimeout(c.ctx, apiTimeout)
	defer cancel()
	out, err := c.discoverer.CloudWatchLogsClient().GetLogEvents(ctx, &input)
	if err != nil {
		if c.ctx.Err() == nil { // not stopped
			klog.Warningf("failed to read log stream %s:%s: %s", rdsMetricsLogGroupName, dbiResourceId, err)
			c.discoverer.registerError(err)
		}
		return nil
	}
	if len(out.Events) < 1 {
		return nil
	}
	var m RDSOSMetrics
	if err := json.Unmarshal([]byte(aws.ToString(out.Events[0].Message)), &m); err != nil {
		klog.Warningln("failed to parse enhanced monitoring data:", err)
		return nil
	}
	return &m
}

func collectOsMetrics(m *RDSOSMetrics, ch chan<- prometheus.Metric) {
	ch <- common.Gauge(dRDSCPUCores, float64(m.NumVCPUs))
	ch <- common.Gauge(dRDSCPUUsage, m.Cpu.Guest, "guest")
	ch <- common.Gauge(dRDSCPUUsage, m.Cpu.Irq, "irq")
	ch <- common.Gauge(dRDSCPUUsage, m.Cpu.Nice, "nice")
	ch <- common.Gauge(dRDSCPUUsage, m.Cpu.Steal, "steal")
	ch <- common.Gauge(dRDSCPUUsage, m.Cpu.System, "system")
	ch <- common.Gauge(dRDSCPUUsage, m.Cpu.User, "user")
	ch <- common.Gauge(dRDSCPUUsage, m.Cpu.Wait, "wait")

	ch <- common.Gauge(dRDSMemTotal, float64(m.Memory.Total*1000))
	ch <- common.Gauge(dRDSMemCached, float64(m.Memory.Cached*1000))
	ch <- common.Gauge(dRDSMemFree, float64(m.Memory.Free*1000))

	for _, ioStat := range m.PhysicalDeviceIO {
		ch <- common.Gauge(dRDSIOOps, ioStat.ReadIOsPS, ioStat.Device, "read")
		ch <- common.Gauge(dRDSIOOps, ioStat.WriteIOsPS, ioStat.Device, "write")
		ch <- common.Gauge(dRDSIObytes, ioStat.ReadKbPS*1000, ioStat.Device, "read")
		ch <- common.Gauge(dRDSIObytes, ioStat.WriteKb*1000, ioStat.Device, "write")
		ch <- common.Gauge(dRDSIOawait, ioStat.Await/1000, ioStat.Device)
		ch <- common.Gauge(dRDSIOutil, ioStat.Util, ioStat.Device)
	}
	for _, dIO := range m.DiskIO {
		if dIO.Device == "" { // Aurora network disk
			device := "aurora-data"
			if dIO.ReadIOsPS != nil && dIO.WriteIOsPS != nil {
				ch <- common.Gauge(dRDSIOOps, *dIO.ReadIOsPS, device, "read")
				ch <- common.Gauge(dRDSIOOps, *dIO.WriteIOsPS, device, "write")
			}
			if dIO.ReadLatency != nil && dIO.WriteLatency != nil {
				ch <- common.Gauge(dRDSIOlatency, *dIO.ReadLatency/1000, device, "read")
				ch <- common.Gauge(dRDSIOlatency, *dIO.WriteLatency/1000, device, "write")
			}
		}
	}

	for _, fsStat := range m.FileSys {
		ch <- common.Gauge(dRDSFSTotal, float64(fsStat.Total*1000), fsStat.MountPoint)
		ch <- common.Gauge(dRDSFSUsed, float64(fsStat.Used*1000), fsStat.MountPoint)
	}
	for _, iface := range m.NetworkInterfaces {
		ch <- common.Gauge(dRDSNetRx, iface.Rx, iface.Interface)
		ch <- common.Gauge(dRDSNetTx, iface.Tx, iface.Interface)
	}
}

type RDSOSMetrics struct {
	NumVCPUs          int                   `json:"numVCPUs"`
	Cpu               RDSCPUUtilization     `json:"cpuUtilization"`
	Memory            RDSMemory             `json:"memory"`
	PhysicalDeviceIO  []RDSPhysicalDeviceIO `json:"physicalDeviceIO"`
	DiskIO            []RDSAuroraDiskIO     `json:"diskIO"`
	FileSys           []RDSFileSys          `json:"fileSys"`
	NetworkInterfaces []RDSNetInterface     `json:"network"`
}

type RDSNetInterface struct {
	Interface string  `json:"interface"`
	Rx        float64 `json:"rx"`
	Tx        float64 `json:"tx"`
}

type RDSCPUUtilization struct {
	Guest  float64 `json:"guest"`
	Irq    float64 `json:"irq"`
	System float64 `json:"system"`
	Wait   float64 `json:"wait"`
	Idle   float64 `json:"idle"`
	User   float64 `json:"user"`
	Steal  float64 `json:"steal"`
	Nice   float64 `json:"nice"`
	Total  float64 `json:"total"`
}

type RDSMemory struct {
	Writeback      int64 `json:"writeback"`
	HugePagesFree  int64 `json:"hugePagesFree"`
	HugePagesRsvd  int64 `json:"hugePagesRsvd"`
	HugePagesSurp  int64 `json:"hugePagesSurp"`
	Cached         int64 `json:"cached"`
	HugePagesSize  int64 `json:"hugePagesSize"`
	Free           int64 `json:"free"`
	HugePagesTotal int64 `json:"hugePagesTotal"`
	Inactive       int64 `json:"inactive"`
	PageTables     int64 `json:"pageTables"`
	Dirty          int64 `json:"dirty"`
	Mapped         int64 `json:"mapped"`
	Active         int64 `json:"active"`
	Total          int64 `json:"total"`
	Slab           int64 `json:"slab"`
	Buffers        int64 `json:"buffers"`
}
type RDSPhysicalDeviceIO struct {
	WriteKbPS   float64 `json:"writeKbPS"`
	ReadIOsPS   float64 `json:"readIOsPS"`
	Await       float64 `json:"await"`
	ReadKbPS    float64 `json:"readKbPS"`
	RrqmPS      float64 `json:"rrqmPS"`
	Util        float64 `json:"util"`
	AvgQueueLen float64 `json:"avgQueueLen"`
	Tps         float64 `json:"tps"`
	ReadKb      float64 `json:"readKb"`
	Device      string  `json:"device"`
	WriteKb     float64 `json:"writeKb"`
	AvgReqSz    float64 `json:"avgReqSz"`
	WrqmPS      float64 `json:"wrqmPS"`
	WriteIOsPS  float64 `json:"writeIOsPS"`
}

type RDSAuroraDiskIO struct {
	Device          string   `json:"device"`
	ReadLatency     *float64 `json:"readLatency"`
	WriteLatency    *float64 `json:"writeLatency"`
	WriteThroughput *float64 `json:"writeThroughput"`
	ReadThroughput  *float64 `json:"readThroughput"`
	ReadIOsPS       *float64 `json:"readIOsPS"`
	WriteIOsPS      *float64 `json:"writeIOsPS"`
	DiskQueueDepth  *float64 `json:"diskQueueDepth"`
}

type RDSFileSys struct {
	MaxFiles   int64  `json:"maxFiles"`
	MountPoint string `json:"mountPoint"`
	Name       string `json:"name"`
	Total      int64  `json:"total"`
	Used       int64  `json:"used"`
	UsedFiles  int64  `json:"usedFiles"`
}
