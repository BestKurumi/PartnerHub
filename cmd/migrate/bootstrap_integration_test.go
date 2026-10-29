//go:build integration

package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

// This test is restricted to the separately provisioned startup Compose project.
// It exercises the real migration entrypoint, including root identity bootstrap.
func TestStartupIdentityBootstrap(t *testing.T) {
	if os.Getenv("STARTUP_BOOTSTRAP_TEST") != "ph-startup-3456ec71" {
		t.Fatal("explicit isolated startup project required")
	}
	for range 2 {
		if err := run(context.Background(), []string{"-dir", "../../deploy/mysql"}, os.Getenv); err != nil {
			t.Fatalf("bootstrap/replay: %v", err)
		}
	}
	root, err := openDB("root", os.Getenv("MYSQL_ROOT_PASSWORD"), os.Getenv("MYSQL_ADDR"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	// Quotes and backslashes must be represented as password data, never SQL.
	password := "probe-'quote\"-\\slash-?"
	defer root.Exec("DROP USER IF EXISTS 'ph_password_probe'@'%'")
	if _, err = root.Exec("CREATE USER IF NOT EXISTS 'ph_password_probe'@'%' IDENTIFIED BY ?", password); err != nil {
		t.Fatal("password parameter rejected")
	}
	probe, err := openDB("ph_password_probe", password, os.Getenv("MYSQL_ADDR"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	if err = probe.Ping(); err != nil {
		t.Fatal("password round trip failed")
	}
	for _, identity := range []struct {
		user, key, schema, forbidden string
		versions                     int
	}{
		{"hub", "MYSQL_PASSWORD", "partner_hub", "mock_platform1", 7},
		{"mock_p1", "PLATFORM1_DB_PASSWORD", "mock_platform1", "partner_hub", 1},
		{"mock_p2", "PLATFORM2_DB_PASSWORD", "mock_platform2", "partner_hub", 1},
	} {
		db, err := openDB(identity.user, os.Getenv(identity.key), os.Getenv("MYSQL_ADDR"), identity.schema)
		if err != nil {
			t.Fatal(err)
		}
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM hub_schema_migration").Scan(&count)
		if err != nil || count != identity.versions {
			t.Fatalf("migration versions %s: %d", identity.schema, count)
		}
		if _, err = db.Exec("USE " + identity.forbidden); err == nil {
			t.Fatal("cross-schema access permitted")
		}
		db.Close()
	}
	// Existing account credentials must never be silently reset on restart.
	err = run(context.Background(), []string{"-dir", "../../deploy/mysql"}, func(key string) string {
		if key == "MYSQL_PASSWORD" {
			return "incorrect-existing-credential"
		}
		return os.Getenv(key)
	})
	if err == nil || strings.Contains(err.Error(), "incorrect-existing-credential") {
		t.Fatal("credential mismatch accepted or leaked")
	}
}
