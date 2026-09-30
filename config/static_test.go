package config

import "testing"

func TestValidateNativeScrapeTypes(t *testing.T) {
	for _, tc := range []struct {
		db    Database
		valid bool
	}{
		{Database{Type: "rabbitmq", Host: "mq"}, true}, // the port defaults to the standard one
		{Database{Type: "etcd", Host: "etcd", Port: "2379"}, true},
		{Database{Type: "rabbitmq", RDS: "db"}, false},
		{Database{Type: "pgbouncer", Host: "pgbouncer"}, false}, // the port is required
		{Database{Type: "pgbouncer", Host: "pgbouncer", Port: "6432"}, true},
	} {
		err := (&Static{Databases: []Database{tc.db}}).Validate()
		if (err == nil) != tc.valid {
			t.Errorf("%+v: unexpected error: %v", tc.db, err)
		}
	}
}
