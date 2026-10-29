package main

import (
	"context"
	"strings"
	"testing"
)

func TestMigrateRejectsMissingCredentialsBeforeConnecting(t *testing.T) {
	for _, missing := range []string{"MYSQL_ADDR", "MYSQL_ROOT_PASSWORD", "MYSQL_PASSWORD", "PLATFORM1_DB_PASSWORD", "PLATFORM2_DB_PASSWORD"} {
		t.Run(missing, func(t *testing.T) {
			env := map[string]string{"MYSQL_ADDR": "127.0.0.1:1", "MYSQL_ROOT_PASSWORD": "root-secret", "MYSQL_PASSWORD": "hub-secret", "PLATFORM1_DB_PASSWORD": "a-secret", "PLATFORM2_DB_PASSWORD": "b-secret"}
			delete(env, missing)
			e := run(context.Background(), []string{}, func(k string) string { return env[k] })
			if e == nil || !strings.Contains(e.Error(), missing) || strings.Contains(e.Error(), "secret") {
				t.Fatalf("unsafe missing-credential handling: %v", e)
			}
		})
	}
}

func TestNativeBootstrapRequiresKafkaAddressBeforeDatabaseChanges(t *testing.T) {
	err := run(context.Background(), []string{"-kafka"}, func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "HUB_KAFKA_BROKERS") {
		t.Fatalf("missing Kafka configuration was not diagnosed: %v", err)
	}
}
