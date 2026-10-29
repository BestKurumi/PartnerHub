package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"testing"
)

// A schema-less server accepts connections but rejects table reads. The mock
// must fail before starting its HTTP server even though its DB ping succeeds.
type schemaDriver struct{}
type schemaConn struct{ mode string }
type schemaRows struct{}

func (schemaDriver) Open(name string) (driver.Conn, error) { return schemaConn{name}, nil }
func (schemaConn) Prepare(string) (driver.Stmt, error)     { return nil, errors.New("unsupported") }
func (schemaConn) Close() error                            { return nil }
func (schemaConn) Begin() (driver.Tx, error)               { return nil, errors.New("unsupported") }
func (schemaConn) Ping(context.Context) error              { return nil }
func (c schemaConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if c.mode == "missing" {
		return nil, errors.New("table does not exist")
	}
	return schemaRows{}, nil
}
func (schemaRows) Columns() []string         { return []string{"column"} }
func (schemaRows) Close() error              { return nil }
func (schemaRows) Next([]driver.Value) error { return io.EOF }

var registerSchemaTestDriver sync.Once

func TestMockSchemaReadinessRejectsReachableUnmigratedDatabase(t *testing.T) {
	registerSchemaTestDriver.Do(func() { sql.Register("mock-schema-startup-test", schemaDriver{}) })
	for _, platform := range []string{"PLATFORM_A", "PLATFORM_B"} {
		for _, mode := range []string{"missing", "complete"} {
			db, err := sql.Open("mock-schema-startup-test", mode)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.PingContext(context.Background()); err != nil {
				t.Fatal(err)
			}
			err = checkSchema(context.Background(), db, platform)
			db.Close()
			if (err != nil) != (mode == "missing") {
				t.Fatalf("platform=%s schema=%s err=%v", platform, mode, err)
			}
		}
	}
}
