package config

import (
	"slices"

	"golang.org/x/exp/maps"
)

type Config struct {
	ApplicationInstrumentation []ApplicationInstrumentation `json:"application_instrumentation"`

	AWSConfig *AWSConfig `json:"aws_config"`
}

type ApplicationInstrumentation struct {
	Type        string            `json:"type"`
	Host        string            `json:"host"`
	Port        string            `json:"port"`
	Credentials Credentials       `json:"credentials"`
	Params      map[string]string `json:"params"`
	Instance    string            `json:"instance"`
}

type Credentials struct {
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

type Database struct {
	Type        string            `yaml:"type"`
	Host        string            `yaml:"host"`
	Port        string            `yaml:"port"`
	RDS         string            `yaml:"rds"`
	Elasticache string            `yaml:"elasticache"`
	CloudSQL    string            `yaml:"cloudsql"`
	Memorystore string            `yaml:"memorystore"`
	OCIDB       string            `yaml:"ocidb"`
	OCICache    string            `yaml:"ocicache"`
	MemoryDB    string            `yaml:"memorydb"`
	AzureDB     string            `yaml:"azuredb"`
	AzureRedis  string            `yaml:"azureredis"`
	Credentials Credentials       `yaml:"credentials"`
	Params      map[string]string `yaml:"params"`
}

type AWSConfig struct {
	Region          string `json:"region" yaml:"region"`
	AccessKeyID     string `json:"access_key_id" yaml:"accessKeyId"`
	SecretAccessKey string `json:"secret_access_key" yaml:"secretAccessKey"`

	RDSTagFilters         map[string]string `json:"rds_tag_filters" yaml:"rdsTagFilters"`
	ElasticacheTagFilters map[string]string `json:"elasticache_tag_filters" yaml:"elasticacheTagFilters"` // also applied to ElastiCache Serverless caches
	MemoryDBTagFilters    map[string]string `json:"memorydb_tag_filters" yaml:"memorydbTagFilters"`

	// CloudWatchPeriodSeconds is the period of the CloudWatch metrics (Aurora replica lag and Serverless v2 capacity,
	// ElastiCache Serverless usage) and the interval of their refresh: 60 by default, rounded up to a multiple of 60.
	CloudWatchPeriodSeconds int `json:"cloudwatch_period_seconds" yaml:"cloudwatchPeriodSeconds"`
}

func (c *AWSConfig) Equal(other *AWSConfig) bool {
	return c.Region == other.Region &&
		c.AccessKeyID == other.AccessKeyID &&
		c.SecretAccessKey == other.SecretAccessKey &&
		maps.Equal(c.RDSTagFilters, other.RDSTagFilters) &&
		maps.Equal(c.ElasticacheTagFilters, other.ElasticacheTagFilters) &&
		maps.Equal(c.MemoryDBTagFilters, other.MemoryDBTagFilters) &&
		c.CloudWatchPeriodSeconds == other.CloudWatchPeriodSeconds
}

type GCPConfig struct {
	ProjectID               string            `json:"project_id" yaml:"projectId"`
	Region                  string            `json:"region" yaml:"region"`
	CredentialsJSON         string            `json:"credentials_json" yaml:"credentialsJson"`
	CloudSQLLabelFilters    map[string]string `json:"cloudsql_label_filters" yaml:"cloudsqlLabelFilters"`
	MemorystoreLabelFilters map[string]string `json:"memorystore_label_filters" yaml:"memorystoreLabelFilters"`
}

func (c *GCPConfig) Equal(other *GCPConfig) bool {
	return c.ProjectID == other.ProjectID &&
		c.Region == other.Region &&
		c.CredentialsJSON == other.CredentialsJSON &&
		maps.Equal(c.CloudSQLLabelFilters, other.CloudSQLLabelFilters) &&
		maps.Equal(c.MemorystoreLabelFilters, other.MemorystoreLabelFilters)
}

type OCIConfig struct {
	CompartmentIDs  []string          `json:"compartment_ids" yaml:"compartmentIds"`
	Region          string            `json:"region" yaml:"region"`
	TenancyID       string            `json:"tenancy_id" yaml:"tenancyId"` // API key auth: the four fields below
	UserID          string            `json:"user_id" yaml:"userId"`
	Fingerprint     string            `json:"fingerprint" yaml:"fingerprint"`
	PrivateKey      string            `json:"private_key" yaml:"privateKey"`
	DBTagFilters    map[string]string `json:"db_tag_filters" yaml:"dbTagFilters"`
	CacheTagFilters map[string]string `json:"cache_tag_filters" yaml:"cacheTagFilters"`
}

func (c *OCIConfig) Equal(other *OCIConfig) bool {
	return slices.Equal(c.CompartmentIDs, other.CompartmentIDs) &&
		c.Region == other.Region &&
		c.TenancyID == other.TenancyID &&
		c.UserID == other.UserID &&
		c.Fingerprint == other.Fingerprint &&
		c.PrivateKey == other.PrivateKey &&
		maps.Equal(c.DBTagFilters, other.DBTagFilters) &&
		maps.Equal(c.CacheTagFilters, other.CacheTagFilters)
}

// AzureConfig configures the discovery of Azure Database for PostgreSQL / MySQL flexible servers and Azure Cache for Redis.
// Authentication uses DefaultAzureCredential (environment service principal, workload identity or managed identity):
// no secrets are part of this config.
type AzureConfig struct {
	SubscriptionIDs    []string          `json:"subscription_ids" yaml:"subscriptionIds"` // AZURE_SUBSCRIPTION_ID by default
	TenantID           string            `json:"tenant_id" yaml:"tenantId"`
	ResourceGroups     []string          `json:"resource_groups" yaml:"resourceGroups"` // all the resource groups of the subscriptions if empty
	Locations          []string          `json:"locations" yaml:"locations"`            // all the locations if empty
	PostgresTagFilters map[string]string `json:"postgres_tag_filters" yaml:"postgresTagFilters"`
	MySQLTagFilters    map[string]string `json:"mysql_tag_filters" yaml:"mysqlTagFilters"`
	RedisTagFilters    map[string]string `json:"redis_tag_filters" yaml:"redisTagFilters"`
}

func (c *AzureConfig) Equal(other *AzureConfig) bool {
	return slices.Equal(c.SubscriptionIDs, other.SubscriptionIDs) &&
		c.TenantID == other.TenantID &&
		slices.Equal(c.ResourceGroups, other.ResourceGroups) &&
		slices.Equal(c.Locations, other.Locations) &&
		maps.Equal(c.PostgresTagFilters, other.PostgresTagFilters) &&
		maps.Equal(c.MySQLTagFilters, other.MySQLTagFilters) &&
		maps.Equal(c.RedisTagFilters, other.RedisTagFilters)
}
