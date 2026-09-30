package aws

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	dRDSClusterRole           = common.Desc("aws_rds_cluster_role", "Role of the Aurora instance in its cluster", "role")
	dRDSServerlessMinCapacity = common.Desc("aws_rds_serverless_min_capacity_acu", "Minimum capacity of the Aurora Serverless v2 cluster in ACUs")
	dRDSServerlessMaxCapacity = common.Desc("aws_rds_serverless_max_capacity_acu", "Maximum capacity of the Aurora Serverless v2 cluster in ACUs")
)

// auroraInstance is the cluster-level information of an Aurora instance (DescribeDBClusters).
type auroraInstance struct {
	writer bool
	minACU float64
	maxACU float64
}

// describeDBClustersAPI is the part of the RDS client used to describe the Aurora clusters (replaced by a fake in tests).
type describeDBClustersAPI interface {
	DescribeDBClusters(context.Context, *rds.DescribeDBClustersInput, ...func(*rds.Options)) (*rds.DescribeDBClustersOutput, error)
}

func isAurora(instance *rdstypes.DBInstance) bool {
	return instance != nil && strings.HasPrefix(aws.ToString(instance.Engine), "aurora")
}

// describeAurora returns the Aurora cluster-level information by the instance id (<region>/<instance>).
func (d *Discoverer) describeAurora(region string, svc describeDBClustersAPI) (map[string]auroraInstance, error) {
	res := map[string]auroraInstance{}
	input := &rds.DescribeDBClustersInput{
		// DocumentDB and Neptune clusters are also returned by DescribeDBClusters
		Filters: []rdstypes.Filter{{Name: aws.String("engine"), Values: []string{"aurora-postgresql", "aurora-mysql", "aurora"}}},
	}
	paginator := rds.NewDescribeDBClustersPaginator(svc, input)
	for paginator.HasMorePages() {
		ctx, cancel := d.apiContext()
		out, err := paginator.NextPage(ctx)
		cancel()
		if err != nil {
			return nil, err
		}
		for id, info := range auroraInstances(region, out.DBClusters) {
			res[id] = info
		}
	}
	return res, nil
}

func auroraInstances(region string, clusters []rdstypes.DBCluster) map[string]auroraInstance {
	res := map[string]auroraInstance{}
	for _, cluster := range clusters {
		var minACU, maxACU float64
		if sc := cluster.ServerlessV2ScalingConfiguration; sc != nil {
			minACU, maxACU = aws.ToFloat64(sc.MinCapacity), aws.ToFloat64(sc.MaxCapacity)
		}
		for _, m := range cluster.DBClusterMembers {
			res[region+"/"+aws.ToString(m.DBInstanceIdentifier)] = auroraInstance{
				writer: aws.ToBool(m.IsClusterWriter),
				minACU: minACU,
				maxACU: maxACU,
			}
		}
	}
	return res
}

func (d *Discoverer) auroraInstance(id string) (auroraInstance, bool) {
	d.auroraLock.RLock()
	defer d.auroraLock.RUnlock()
	i, ok := d.aurora[id]
	return i, ok
}

// collectAurora emits the cluster-level metrics of an Aurora instance and its CloudWatch metrics.
func (d *Discoverer) collectAurora(id string, instance *rdstypes.DBInstance, ch chan<- prometheus.Metric) {
	if !isAurora(instance) {
		return
	}
	if a, ok := d.auroraInstance(id); ok {
		role := "reader"
		if a.writer {
			role = "writer"
		}
		ch <- common.Gauge(dRDSClusterRole, 1, role)
		if aws.ToString(instance.DBInstanceClass) == "db.serverless" && a.maxACU > 0 {
			ch <- common.Gauge(dRDSServerlessMinCapacity, a.minACU)
			ch <- common.Gauge(dRDSServerlessMaxCapacity, a.maxACU)
		}
	}
	d.cloudwatch.collect(id, ch)
}

// discoverAurora refreshes the cluster-level information of the discovered Aurora instances (roles, Serverless v2 capacity range).
func (d *Discoverer) discoverAurora(region string, svc describeDBClustersAPI) {
	found := false
	for _, c := range d.rdsCollectors {
		if _, instance, _, _ := c.snapshot(); isAurora(instance) {
			found = true
			break
		}
	}
	var res map[string]auroraInstance
	if found {
		var err error
		if res, err = d.describeAurora(region, svc); err != nil {
			d.registerOptionalAPIError("rds:DescribeDBClusters", err)
			return // the previous information is kept
		}
	}
	d.auroraLock.Lock()
	d.aurora = res
	d.auroraLock.Unlock()
}
