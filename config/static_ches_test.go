package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadStaticClickhouseElasticsearch(t *testing.T) {
	t.Setenv("CH_PASSWORD", "from-env")
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
databases:
  - type: clickhouse
    host: clickhouse
    port: "9000"
    credentials:
      username: monitoring
      password: ${CH_PASSWORD}
    params:
      tablesExclude: '^system\.'
      topTables: "50"
  - type: elasticsearch
    host: es
    port: "9200"
    credentials:
      username: monitoring
      password: changeme
    params:
      tls: skip-verify
      nodes: _all
  - type: opensearch
    host: opensearch
    port: "9200"
`), 0o600))
	s, err := LoadStatic(path)
	require.NoError(t, err)
	require.Len(t, s.Databases, 3)
	assert.Equal(t, "clickhouse", s.Databases[0].Type)
	assert.Equal(t, "from-env", s.Databases[0].Credentials.Password)
	assert.Equal(t, `^system\.`, s.Databases[0].Params["tablesExclude"])
	assert.Equal(t, "elasticsearch", s.Databases[1].Type)
	assert.Equal(t, "skip-verify", s.Databases[1].Params["tls"])
	assert.Equal(t, "opensearch", s.Databases[2].Type)
}
