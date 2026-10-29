//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	driver "github.com/go-sql-driver/mysql"
	"os"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/model"
	hubruntime "partner-hub/internal/runtime"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func migrationDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("RECOVERY_TEST_ADMIN_DSN")
	if dsn == "" {
		t.Fatal("RECOVERY_TEST_ADMIN_DSN required")
	}
	c, e := driver.ParseDSN(dsn)
	if e != nil || c.Addr != "127.0.0.1:13306" {
		t.Fatal("explicit isolated test instance required")
	}
	admin, e := sql.Open("mysql", dsn)
	if e != nil {
		t.Fatal(e)
	}
	name := "it_migration_" + strings.ReplaceAll(model.NewID(), "-", "")
	if _, e = admin.Exec("CREATE DATABASE " + name); e != nil {
		t.Fatal(e)
	}
	c.DBName = name
	db, e := sql.Open("mysql", c.FormatDSN())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		db.Close()
		if _, e := admin.Exec("DROP DATABASE " + name); e != nil {
			t.Error(e)
		}
		admin.Close()
	})
	return db
}
func migrationFile(t *testing.T, dir, name, body string) {
	t.Helper()
	if e := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); e != nil {
		t.Fatal(e)
	}
}
func TestStartupMigrationPreservesDataAndDetectsDrift(t *testing.T) {
	db := migrationDB(t)
	dir := t.TempDir()
	migrationFile(t, dir, "001_data.sql", `CREATE TABLE IF NOT EXISTS sample (id INT PRIMARY KEY, value VARCHAR(80)); INSERT INTO sample VALUES (1,'quote; inside') ON DUPLICATE KEY UPDATE id=id;`)
	for range 2 {
		if e := hubmysql.ApplyMigrations(context.Background(), db, dir); e != nil {
			t.Fatal(e)
		}
	}
	var value string
	if e := db.QueryRow("SELECT value FROM sample WHERE id=1").Scan(&value); e != nil || value != "quote; inside" {
		t.Fatalf("data lost: %q %v", value, e)
	}
	migrationFile(t, dir, "001_data.sql", `DROP TABLE sample;`)
	if e := hubmysql.ApplyMigrations(context.Background(), db, dir); e == nil {
		t.Fatal("checksum drift accepted")
	}
	if e := db.QueryRow("SELECT value FROM sample WHERE id=1").Scan(&value); e != nil {
		t.Fatal(e)
	}
}
func TestStartupMigrationInterruptedDDLSafelyReplays(t *testing.T) {
	db := migrationDB(t)
	dir := t.TempDir()
	migrationFile(t, dir, "001_partial.sql", `CREATE TABLE IF NOT EXISTS retained (id INT PRIMARY KEY); INSERT INTO absent VALUES (1);`)
	if e := hubmysql.ApplyMigrations(context.Background(), db, dir); e == nil {
		t.Fatal("failed DDL marked successful")
	}
	var n int
	if e := db.QueryRow("SELECT COUNT(*) FROM hub_schema_migration").Scan(&n); e != nil || n != 0 {
		t.Fatalf("failed migration recorded %d %v", n, e)
	}
	if _, e := db.Exec("CREATE TABLE absent (id INT PRIMARY KEY)"); e != nil {
		t.Fatal(e)
	}
	if e := hubmysql.ApplyMigrations(context.Background(), db, dir); e != nil {
		t.Fatal(e)
	}
	if e := db.QueryRow("SELECT COUNT(*) FROM absent WHERE id=1").Scan(&n); e != nil || n != 1 {
		t.Fatalf("replay missing %d %v", n, e)
	}
}
func TestStartupMigrationAllSchemasAndLockCancellation(t *testing.T) {
	for _, dir := range []string{"migrations", "mock-platform1", "mock-platform2"} {
		t.Run(dir, func(t *testing.T) {
			db := migrationDB(t)
			for range 2 {
				if e := hubmysql.ApplyMigrations(context.Background(), db, "../../deploy/mysql/"+dir); e != nil {
					t.Fatal(e)
				}
			}
			want := 1
			if dir == "migrations" {
				want = 8
			}
			var n int
			if e := db.QueryRow("SELECT COUNT(*) FROM hub_schema_migration").Scan(&n); e != nil || n != want {
				t.Fatalf("versions=%d want=%d error=%v", n, want, e)
			}
		})
	}
	db := migrationDB(t)
	conn, e := db.Conn(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	var schema string
	_ = conn.QueryRowContext(context.Background(), "SELECT DATABASE()").Scan(&schema)
	var got int
	if e = conn.QueryRowContext(context.Background(), "SELECT GET_LOCK(CONCAT('ph-migrate:',LEFT(SHA2(DATABASE(),256),48)),0)").Scan(&got); e != nil || got != 1 {
		t.Fatal(e)
	}
	defer conn.ExecContext(context.Background(), "DO RELEASE_LOCK(CONCAT('ph-migrate:',LEFT(SHA2(DATABASE(),256),48)))")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if e = hubmysql.ApplyMigrations(ctx, db, "../../deploy/mysql/migrations"); e == nil || time.Since(start) > time.Second {
		t.Fatalf("migration lock ignored caller budget: %v", e)
	}
}

func TestStartupSchemaReadiness(t *testing.T) {
	db := migrationDB(t)
	if e := hubruntime.CheckSchema(context.Background(), db); e == nil {
		t.Fatal("empty schema ready")
	}
	if e := hubmysql.ApplyMigrations(context.Background(), db, "../../deploy/mysql/migrations"); e != nil {
		t.Fatal(e)
	}
	if e := hubruntime.CheckSchema(context.Background(), db); e != nil {
		t.Fatalf("completed schema not ready: %v", e)
	}
	var indexes int
	if e := db.QueryRow("SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='trade_message' AND index_name='idx_messages_trace_time'").Scan(&indexes); e != nil || indexes != 3 {
		t.Fatalf("trace/time index missing columns: %d %v", indexes, e)
	}
	if _, e := db.Exec("DELETE FROM hub_schema_migration WHERE version='008_console_query_indexes.sql'"); e != nil {
		t.Fatal(e)
	}
	if e := hubruntime.CheckSchema(context.Background(), db); e == nil {
		t.Fatal("legacy seven-version schema must request console migration")
	}
	if e := hubmysql.ApplyMigrations(context.Background(), db, "../../deploy/mysql/migrations"); e != nil {
		t.Fatal(e)
	}
	if _, e := db.Exec("DELETE FROM hub_schema_migration WHERE version='007_manual_operation.sql'"); e != nil {
		t.Fatal(e)
	}
	if e := hubruntime.CheckSchema(context.Background(), db); e == nil {
		t.Fatal("incomplete migrations ready")
	}
}
