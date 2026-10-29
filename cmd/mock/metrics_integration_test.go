//go:build integration

package main

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"partner-hub/internal/mock/platform1"
	"partner-hub/internal/mock/platform2"
	"partner-hub/internal/model"
)

// COUNT is deliberately unfiltered in production. Each assertion therefore
// needs its own schema, not merely a unique trace inside a shared receipt table.
func mockMetricsDB(t *testing.T, platform string) *sql.DB {
	t.Helper()
	dsn := os.Getenv("RECOVERY_TEST_ADMIN_DSN")
	config, err := mysqldriver.ParseDSN(dsn)
	if dsn == "" || err != nil || config.Net != "tcp" || config.Addr != "127.0.0.1:13306" {
		t.Fatal("explicit local isolated MySQL admin DSN required")
	}
	config.DBName = ""
	admin, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Fatal("open isolated MySQL admin failed")
	}
	name := "mock_metrics_" + strings.ReplaceAll(model.NewID(), "-", "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name+" CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin"); err != nil {
		admin.Close()
		t.Fatal("create own Mock metrics schema failed")
	}
	var db *sql.DB
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := admin.ExecContext(cleanup, "DROP DATABASE "+name); err != nil {
			t.Error("cleanup own Mock metrics schema failed")
		}
		admin.Close()
	})
	config.DBName, config.ParseTime = name, true
	db, err = sql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Fatal("open own Mock metrics schema failed")
	}
	db.SetMaxOpenConns(2)
	migration := "../../deploy/mysql/mock-platform1/001_received.sql"
	if platform == "PLATFORM2" {
		migration = "../../deploy/mysql/mock-platform2/001_received.sql"
	}
	ddl, err := os.ReadFile(migration)
	if err != nil {
		t.Fatal("read real Mock receipt migration failed")
	}
	for _, statement := range strings.Split(string(ddl), ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal("apply real Mock receipt migration failed")
		}
	}
	return db
}

// Catches incrementing Save-call counts: B replay persists one row, while A's
// separate notifications persist twice even for identical business identities.
func TestMockMetricsDurableReceiptRows(t *testing.T) {
	for _, platform := range []string{"PLATFORM1", "PLATFORM2"} {
		t.Run(platform, func(t *testing.T) {
			db := mockMetricsDB(t, platform)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var baseline int64
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM received_notification").Scan(&baseline); err != nil {
				t.Fatal("receipt baseline query failed")
			}
			trace := model.NewID()
			if platform == "PLATFORM1" {
				store := platform1.NewMySQLStore(db)
				for i := 0; i < 2; i++ {
					if err := store.Save(ctx, platform1.Receipt{ReceiveID: model.NewID(), UserUID: "metrics-fixture", TraceID: trace, Payload: []byte(`{}`)}); err != nil {
						t.Fatal("A receipt save failed")
					}
				}
			} else {
				store := platform2.NewMySQLStore(db)
				for i := 0; i < 2; i++ {
					if err := store.Save(ctx, platform2.Receipt{ReceiveID: model.NewID(), Topic: "metrics-fixture-" + trace, Partition: 0, Offset: 1, UserUID: "metrics-fixture", TraceID: trace, Payload: []byte(`{}`)}); err != nil {
						t.Fatal("B receipt save failed")
					}
				}
			}
			got, err := receiptSampler(db)(ctx)
			if err != nil {
				t.Fatal("receipt sample failed")
			}
			want := baseline + 1
			if platform == "PLATFORM1" {
				want = baseline + 2
			}
			if got != want {
				t.Fatalf("durable rows=%d want=%d", got, want)
			}
		})
	}
}
