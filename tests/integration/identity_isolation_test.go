//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

// Catches accidental cross-schema grants: connection success alone is not isolation.
func TestBindingsIdentityIsolation(t *testing.T) {
	schemas := []string{"partner_hub", "mock_platform1", "mock_platform2"}
	for i, variable := range []string{"HUB_TEST_DSN", "PLATFORM1_TEST_DSN", "PLATFORM2_TEST_DSN"} {
		t.Run(schemas[i], func(t *testing.T) {
			dsn := os.Getenv(variable)
			if dsn == "" {
				t.Fatalf("%s required", variable)
			}
			db, err := sql.Open("mysql", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			var current string
			if err = db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&current); err != nil || current != schemas[i] {
				t.Fatalf("wrong own schema: %s %v", current, err)
			}
			for _, foreign := range schemas {
				if foreign == current {
					continue
				}
				_, err = db.ExecContext(ctx, "USE "+foreign)
				var denied *driver.MySQLError
				if !errors.As(err, &denied) || denied.Number != 1044 {
					t.Fatalf("expected schema authorization denial for %s -> %s, got %v", current, foreign, err)
				}
			}
		})
	}
}
