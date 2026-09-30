package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Static struct {
	AWS       *AWSConfig   `yaml:"aws"`
	GCP       *GCPConfig   `yaml:"gcp"`
	OCI       *OCIConfig   `yaml:"oci"`
	Azure     *AzureConfig `yaml:"azure"`
	Databases []Database   `yaml:"databases"`
}

// nativeScrapeTypes are the types scraped via their native /metrics endpoints (the port defaults to the standard one).
var nativeScrapeTypes = map[string]bool{"rabbitmq": true, "etcd": true}

func LoadStatic(path string) (*Static, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Static
	if err = yaml.Unmarshal([]byte(os.ExpandEnv(string(data))), &s); err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", path, err)
	}
	if err = s.Validate(); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", path, err)
	}
	return &s, nil
}

func (s *Static) Validate() error {
	if s.AWS != nil && (s.AWS.AccessKeyID == "") != (s.AWS.SecretAccessKey == "") {
		return fmt.Errorf("aws: both accessKeyId and secretAccessKey must be set, or neither")
	}
	for i, d := range s.Databases {
		if d.Type == "" {
			return fmt.Errorf("databases[%d]: type is required", i)
		}
		sources := 0
		for _, v := range []string{d.Host, d.RDS, d.Elasticache, d.CloudSQL, d.Memorystore, d.OCIDB, d.OCICache, d.MemoryDB, d.AzureDB, d.AzureRedis} {
			if v != "" {
				sources++
			}
		}
		if sources != 1 {
			return fmt.Errorf("databases[%d]: exactly one of host, rds, elasticache, memorydb, cloudsql, memorystore, ocidb, ocicache, azuredb or azureredis is required", i)
		}
		if d.AzureDB != "" && d.Type != "postgres" && d.Type != "mysql" {
			return fmt.Errorf("databases[%d]: azuredb requires type postgres or mysql", i)
		}
		if nativeScrapeTypes[d.Type] && d.Host == "" {
			return fmt.Errorf("databases[%d]: host is required for %s", i, d.Type)
		}
		if d.Host != "" && d.Port == "" && !nativeScrapeTypes[d.Type] {
			return fmt.Errorf("databases[%d]: port is required with host", i)
		}
	}
	return nil
}
