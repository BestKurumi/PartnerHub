package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
	"time"
)

// Simulates the real SQL boundary: a not-yet-archived input, then two committed
// platform facts. Catches ending observation early after uncertain HTTP results.
func TestSettleObservesUncertainSubmittedInputWithoutResubmitting(t *testing.T) {
	for _, disposition := range []string{"transport_error", "invalid_response", "http_unconfirmed", "accepted", "http_rejected"} {
		t.Run(disposition, func(t *testing.T) {
			rows := makeInputs(1, "client")
			now := time.Now().UTC()
			rows[0].SubmittedAt = &now
			rows[0].Disposition = disposition
			fixture := &observationFixture{id: rows[0].MsgID, received: now}
			db := sql.OpenDB(fixture)
			defer db.Close()
			count, err := settle(context.Background(), db, rows, time.Second)
			if err != nil || count != 0 {
				t.Fatalf("query failed: %d %v", count, err)
			}
			if disposition == "http_rejected" {
				if fixture.queries != 1 {
					t.Fatal("explicit rejection unnecessarily waits")
				}
				return
			}
			if fixture.queries < 2 || len(rows[0].Targets) != 2 || rows[0].ReceivedAt == nil {
				t.Fatalf("uncertain input observation ended before commit: queries=%d row=%+v", fixture.queries, rows[0])
			}
			if rows[0].Disposition != disposition {
				t.Fatal("authoritative observation erased HTTP uncertainty")
			}
		})
	}
}

type observationFixture struct {
	id       string
	received time.Time
	queries  int
}

func (f *observationFixture) Connect(context.Context) (driver.Conn, error) {
	return &observationConn{f}, nil
}
func (f *observationFixture) Driver() driver.Driver { return observationDriver{f} }

type observationDriver struct{ f *observationFixture }

func (d observationDriver) Open(string) (driver.Conn, error) { return &observationConn{d.f}, nil }

type observationConn struct{ f *observationFixture }

func (c *observationConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}
func (c *observationConn) Close() error              { return nil }
func (c *observationConn) Begin() (driver.Tx, error) { return nil, errors.New("not supported") }
func (c *observationConn) QueryContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Rows, error) {
	c.f.queries++
	if len(args) != 1 || args[0].Value != c.f.id {
		return nil, errors.New("query escaped run cohort")
	}
	r := &observationRows{}
	if c.f.queries > 1 {
		for _, platform := range []string{"PLATFORM_A", "PLATFORM_B"} {
			r.rows = append(r.rows, []driver.Value{c.f.id, c.f.received, c.f.received.Add(time.Millisecond), platform, "SUCCESS", "SUCCESS", true, c.f.received.Add(time.Millisecond), "", nil})
		}
	}
	return r, nil
}

type observationRows struct {
	rows  [][]driver.Value
	index int
}

func (r *observationRows) Columns() []string {
	return []string{"msg_id", "received_at", "stored_at", "platform_id", "first_outcome", "outcome", "send_started", "finished_at", "lease_owner", "recovered_at"}
}
func (r *observationRows) Close() error { return nil }
func (r *observationRows) Next(dest []driver.Value) error {
	if r.index == len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}
