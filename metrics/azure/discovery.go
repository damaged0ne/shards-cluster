package azure

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/mysql/armmysqlflexibleservers"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/postgresql/armpostgresqlflexibleservers/v4"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/redis/armredis/v3"
	"github.com/coroot/coroot-cluster-agent/common"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog"
)

const gib = 1 << 30

var (
	dDBInfo = common.Desc("azure_db_info", "Azure Database for PostgreSQL / MySQL flexible server info",
		"name", "subscription", "resource_group", "location", "zone", "fqdn", "port", "engine", "engine_version", "sku", "tier",
		"high_availability", "replication_role", "primary",
	)
	dDBStatus       = common.Desc("azure_db_status", "State of the flexible server", "status")
	dDBStorageTotal = common.Desc("azure_db_storage_total_bytes", "Provisioned storage of the flexible server")

	dRedisInfo = common.Desc("azure_redis_info", "Azure Cache for Redis info",
		"name", "subscription", "resource_group", "location", "host", "port", "ssl_port", "non_ssl_port_enabled", "engine_version",
		"sku", "family", "capacity", "shards",
	)
	dRedisStatus = common.Desc("azure_redis_status", "Provisioning state of the cache", "status")
)

type dbInfo struct {
	id               string // the ARM resource id, lower-cased
	key              string // <subscription>/<resource group>/<engine>/<name>
	name             string
	subscription     string
	resourceGroup    string
	location, zone   string
	engine, version  string // postgres or mysql
	fqdn, port       string
	sku, tier        string
	highAvailability string
	role             string
	primary          string // the name of the source server of a read replica
	state            string
	storageBytes     float64
	tags             map[string]string
	primaryKey       string // the key of the primary, to apply its tag filters to the replicas
}

type redisInfo struct {
	id                string
	key               string
	name              string
	subscription      string
	resourceGroup     string
	location          string
	host              string
	port, sslPort     int32
	nonSSLPortEnabled bool
	version           string
	sku, family       string
	capacity, shards  int32
	state             string
	tags              map[string]string
}

type DBCollector struct {
	discoverer *Discoverer
	lock       sync.RWMutex // info is replaced by the discovery goroutine and read by Collect
	info       dbInfo
}

func (c *DBCollector) getInfo() dbInfo {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.info
}

func (c *DBCollector) setInfo(i dbInfo) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.info = i
}

func (c *DBCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("azure_db_collector", "", nil, nil)
}

func (c *DBCollector) Collect(ch chan<- prometheus.Metric) {
	i := c.getInfo()
	ch <- common.Gauge(dDBStatus, 1, i.state)
	ch <- common.Gauge(dDBInfo, 1,
		i.name, i.subscription, i.resourceGroup, i.location, i.zone, i.fqdn, i.port, i.engine, i.version, i.sku, i.tier,
		i.highAvailability, i.role, i.primary,
	)
	if i.storageBytes > 0 {
		ch <- common.Gauge(dDBStorageTotal, i.storageBytes)
	}
	c.discoverer.monitoring.collect(i.id, ch)
}

type RedisCollector struct {
	discoverer *Discoverer
	lock       sync.RWMutex
	info       redisInfo
}

func (c *RedisCollector) getInfo() redisInfo {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.info
}

func (c *RedisCollector) setInfo(i redisInfo) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.info = i
}

func (c *RedisCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("azure_redis_collector", "", nil, nil)
}

func (c *RedisCollector) Collect(ch chan<- prometheus.Metric) {
	i := c.getInfo()
	ch <- common.Gauge(dRedisStatus, 1, i.state)
	ch <- common.Gauge(dRedisInfo, 1,
		i.name, i.subscription, i.resourceGroup, i.location, i.host, strconv.Itoa(int(i.port)), strconv.Itoa(int(i.sslPort)),
		strconv.FormatBool(i.nonSSLPortEnabled), i.version, i.sku, i.family, strconv.Itoa(int(i.capacity)), strconv.Itoa(int(i.shards)),
	)
	c.discoverer.monitoring.collect(i.id, ch)
}

// resourceGroups returns the resource groups to list: "" means the whole subscription.
func (d *Discoverer) resourceGroups() []string {
	if len(d.cfg.ResourceGroups) == 0 {
		return []string{""}
	}
	return d.cfg.ResourceGroups
}

func (d *Discoverer) locationMatched(location string) bool {
	if len(d.cfg.Locations) == 0 {
		return true
	}
	for _, l := range d.cfg.Locations {
		if normalizeLocation(l) == normalizeLocation(location) {
			return true
		}
	}
	return false
}

func normalizeLocation(l string) string {
	return strings.ToLower(strings.ReplaceAll(l, " ", ""))
}

func (d *Discoverer) discoverDBs() {
	var found []dbInfo
	failed := false
	for _, s := range d.subscriptions {
		for _, rg := range d.resourceGroups() {
			pg, err := d.api.listPostgres(s, rg)
			if err != nil {
				d.registerError(fmt.Errorf("listing PostgreSQL flexible servers: %w", err))
				failed = true
			}
			for _, server := range pg {
				if i, ok := postgresInfo(server); ok {
					found = append(found, i)
				}
			}
			my, err := d.api.listMySQL(s, rg)
			if err != nil {
				d.registerError(fmt.Errorf("listing MySQL flexible servers: %w", err))
				failed = true
			}
			for _, server := range my {
				if i, ok := mysqlInfo(server); ok {
					found = append(found, i)
				}
			}
		}
	}
	byKey := map[string]dbInfo{}
	for _, i := range found {
		byKey[i.key] = i
	}
	seen := map[string]bool{}
	for _, i := range found {
		if !d.locationMatched(i.location) {
			continue
		}
		filters := d.cfg.PostgresTagFilters
		if i.engine == "mysql" {
			filters = d.cfg.MySQLTagFilters
		}
		primary, hasPrimary := byKey[i.primaryKey]
		if !common.LabelsMatched(filters, i.tags) && (!hasPrimary || !common.LabelsMatched(filters, primary.tags)) { // replicas follow their primary
			klog.Infof("Azure %s flexible server %s (tags: %s) was skipped according to the tag-based filters: %s", i.engine, i.name, i.tags, filters)
			continue
		}
		seen[i.key] = true
		c := d.dbCollectors[i.key]
		if c == nil {
			klog.Infof("new Azure %s flexible server found: %s", i.engine, i.key)
			c = &DBCollector{discoverer: d, info: i}
			if err := prometheus.WrapRegistererWith(dbLabels(i.key), d.reg).Register(c); err != nil {
				klog.Error(err)
				continue
			}
			d.dbCollectors[i.key] = c
		}
		c.setInfo(i)
	}
	if failed || d.ctx.Err() != nil { // a listing failed: keep the collectors rather than dropping and re-adding them
		return
	}
	for key, c := range d.dbCollectors {
		if !seen[key] {
			prometheus.WrapRegistererWith(dbLabels(key), d.reg).Unregister(c)
			delete(d.dbCollectors, key)
		}
	}
}

func (d *Discoverer) discoverRedis() {
	var found []redisInfo
	failed := false
	for _, s := range d.subscriptions {
		for _, rg := range d.resourceGroups() {
			caches, err := d.api.listRedis(s, rg)
			if err != nil {
				d.registerError(fmt.Errorf("listing Azure Cache for Redis instances: %w", err))
				failed = true
			}
			for _, cache := range caches {
				if i, ok := redisCacheInfo(cache); ok {
					found = append(found, i)
				}
			}
		}
	}
	seen := map[string]bool{}
	for _, i := range found {
		if !d.locationMatched(i.location) {
			continue
		}
		if !common.LabelsMatched(d.cfg.RedisTagFilters, i.tags) {
			klog.Infof("Azure Cache for Redis %s (tags: %s) was skipped according to the tag-based filters: %s", i.name, i.tags, d.cfg.RedisTagFilters)
			continue
		}
		seen[i.key] = true
		c := d.redisCollectors[i.key]
		if c == nil {
			klog.Infoln("new Azure Cache for Redis found:", i.key)
			c = &RedisCollector{discoverer: d, info: i}
			if err := prometheus.WrapRegistererWith(redisLabels(i.key), d.reg).Register(c); err != nil {
				klog.Error(err)
				continue
			}
			d.redisCollectors[i.key] = c
		}
		c.setInfo(i)
	}
	if failed || d.ctx.Err() != nil {
		return
	}
	for key, c := range d.redisCollectors {
		if !seen[key] {
			prometheus.WrapRegistererWith(redisLabels(key), d.reg).Unregister(c)
			delete(d.redisCollectors, key)
		}
	}
}

// resourceKey parses an ARM resource id into <subscription>/<resource group>/<kind>/<name>
// (lower-cased: ARM ids are case-insensitive; a PostgreSQL and a MySQL server can have the same name).
func resourceKey(id, kind string) (subscription, resourceGroup, name, key string, ok bool) {
	rid, err := arm.ParseResourceID(id)
	if err != nil || rid.Name == "" {
		return "", "", "", "", false
	}
	subscription, resourceGroup, name = rid.SubscriptionID, rid.ResourceGroupName, rid.Name
	return subscription, resourceGroup, name, strings.ToLower(subscription + "/" + resourceGroup + "/" + kind + "/" + name), true
}

func tags(t map[string]*string) map[string]string {
	res := make(map[string]string, len(t))
	for k, v := range t {
		if v != nil {
			res[k] = *v
		}
	}
	return res
}

func str[T ~string](s *T) string {
	if s == nil {
		return ""
	}
	return string(*s)
}

func postgresInfo(s *armpostgresqlflexibleservers.Server) (dbInfo, bool) {
	if s == nil || s.ID == nil {
		return dbInfo{}, false
	}
	sub, rg, name, key, ok := resourceKey(*s.ID, "postgres")
	if !ok {
		return dbInfo{}, false
	}
	i := dbInfo{
		id: strings.ToLower(*s.ID), key: key, name: name, subscription: sub, resourceGroup: rg,
		location: str(s.Location), engine: "postgres", port: "5432", tags: tags(s.Tags),
	}
	if s.SKU != nil {
		i.sku, i.tier = str(s.SKU.Name), str(s.SKU.Tier)
	}
	if p := s.Properties; p != nil {
		i.fqdn = str(p.FullyQualifiedDomainName)
		i.zone = str(p.AvailabilityZone)
		i.version = str(p.Version)
		if v := str(p.MinorVersion); v != "" && !strings.Contains(i.version, ".") {
			i.version += "." + v
		}
		i.state = str(p.State)
		i.role = str(p.ReplicationRole)
		if i.role == "" && p.Replica != nil {
			i.role = str(p.Replica.Role)
		}
		if p.HighAvailability != nil {
			i.highAvailability = str(p.HighAvailability.Mode)
		}
		if p.Storage != nil && p.Storage.StorageSizeGB != nil {
			i.storageBytes = float64(*p.Storage.StorageSizeGB) * gib
		}
		switch armpostgresqlflexibleservers.ReplicationRole(i.role) {
		case armpostgresqlflexibleservers.ReplicationRoleAsyncReplica, armpostgresqlflexibleservers.ReplicationRoleGeoAsyncReplica:
			// SourceServerResourceID is also set for the servers restored from a backup: only the replicas have a primary
			if _, _, primary, primaryKey, ok := resourceKey(str(p.SourceServerResourceID), "postgres"); ok {
				i.primary, i.primaryKey = primary, primaryKey
			}
		}
	}
	return i, true
}

func mysqlInfo(s *armmysqlflexibleservers.Server) (dbInfo, bool) {
	if s == nil || s.ID == nil {
		return dbInfo{}, false
	}
	sub, rg, name, key, ok := resourceKey(*s.ID, "mysql")
	if !ok {
		return dbInfo{}, false
	}
	i := dbInfo{
		id: strings.ToLower(*s.ID), key: key, name: name, subscription: sub, resourceGroup: rg,
		location: str(s.Location), engine: "mysql", port: "3306", tags: tags(s.Tags),
	}
	if s.SKU != nil {
		i.sku, i.tier = str(s.SKU.Name), str(s.SKU.Tier)
	}
	if p := s.Properties; p != nil {
		i.fqdn = str(p.FullyQualifiedDomainName)
		i.zone = str(p.AvailabilityZone)
		i.version = str(p.Version)
		i.state = str(p.State)
		i.role = str(p.ReplicationRole)
		if p.HighAvailability != nil {
			i.highAvailability = str(p.HighAvailability.Mode)
		}
		if p.Storage != nil && p.Storage.StorageSizeGB != nil {
			i.storageBytes = float64(*p.Storage.StorageSizeGB) * gib
		}
		if armmysqlflexibleservers.ReplicationRole(i.role) == armmysqlflexibleservers.ReplicationRoleReplica {
			if _, _, primary, primaryKey, ok := resourceKey(str(p.SourceServerResourceID), "mysql"); ok {
				i.primary, i.primaryKey = primary, primaryKey
			}
		}
	}
	return i, true
}

func redisCacheInfo(r *armredis.ResourceInfo) (redisInfo, bool) {
	if r == nil || r.ID == nil {
		return redisInfo{}, false
	}
	sub, rg, name, key, ok := resourceKey(*r.ID, "redis")
	if !ok {
		return redisInfo{}, false
	}
	i := redisInfo{
		id: strings.ToLower(*r.ID), key: key, name: name, subscription: sub, resourceGroup: rg,
		location: str(r.Location), tags: tags(r.Tags),
	}
	if p := r.Properties; p != nil {
		i.host = str(p.HostName)
		if p.Port != nil {
			i.port = *p.Port
		}
		if p.SSLPort != nil {
			i.sslPort = *p.SSLPort
		}
		if p.EnableNonSSLPort != nil {
			i.nonSSLPortEnabled = *p.EnableNonSSLPort
		}
		i.version = str(p.RedisVersion)
		i.state = str(p.ProvisioningState)
		if p.SKU != nil {
			i.sku, i.family = str(p.SKU.Name), str(p.SKU.Family)
			if p.SKU.Capacity != nil {
				i.capacity = *p.SKU.Capacity
			}
		}
		if p.ShardCount != nil {
			i.shards = *p.ShardCount
		}
	}
	return i, true
}
