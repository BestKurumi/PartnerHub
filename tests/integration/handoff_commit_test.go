//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/model"
)

// Only the commit acknowledgement and subsequent confirmation transport are
// faulted. Every transaction statement and commit/rollback reaches real MySQL.
type handoffCommitFault struct {
	rollback     bool
	confirmation string
	afterCommit  func() error
	committed    atomic.Bool
}

type handoffFaultConnector struct {
	driver.Connector
	fault *handoffCommitFault
}

func (c *handoffFaultConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &handoffFaultConn{Conn: conn, fault: c.fault}, nil
}

type handoffFaultConn struct {
	driver.Conn
	fault *handoffCommitFault
}

func (c *handoffFaultConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &handoffFaultTx{Tx: tx, fault: c.fault}, nil
}

func (c *handoffFaultConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *handoffFaultConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.fault.committed.Load() && strings.Contains(query, "first_snapshot") && strings.Contains(query, "FROM trade_message") {
		switch c.fault.confirmation {
		case "unavailable":
			return nil, errors.New("injected primary confirmation unavailable")
		case "wait-for-context":
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

type handoffFaultTx struct {
	driver.Tx
	fault *handoffCommitFault
}

func (tx *handoffFaultTx) Commit() error {
	var err error
	if tx.fault.rollback {
		err = tx.Tx.Rollback()
	} else {
		err = tx.Tx.Commit()
	}
	if err != nil {
		return err
	}
	if tx.fault.afterCommit != nil {
		if err = tx.fault.afterCommit(); err != nil {
			return err
		}
	}
	tx.fault.committed.Store(true)
	return errors.New("injected commit acknowledgement lost")
}

func handoffFaultDB(t *testing.T, fault *handoffCommitFault) *sql.DB {
	t.Helper()
	config, err := mysqldriver.ParseDSN(os.Getenv("HUB_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	connector, err := mysqldriver.NewConnector(config)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(&handoffFaultConnector{Connector: connector, fault: fault})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func assertCompleteHandoff(t *testing.T, db *sql.DB, snapshot model.FirstRoundSnapshot) {
	t.Helper()
	want, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var body, raw []byte
	var revision, status string
	if err = db.QueryRow("SELECT first_snapshot,raw_payload,archive_revision,first_status FROM trade_message WHERE msg_id=?", snapshot.Trade.MsgID).Scan(&body, &raw, &revision, &status); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, want) || !bytes.Equal(raw, snapshot.Trade.RawPayload) || revision != snapshot.ArchiveRevision || status != "RECOVERY_PENDING" {
		t.Fatalf("incomplete original snapshot: revision=%s status=%s", revision, status)
	}
	var tasks, attempts int
	if err = db.QueryRow("SELECT COUNT(*) FROM delivery_task WHERE msg_id=?", snapshot.Trade.MsgID).Scan(&tasks); err != nil || tasks != 2 {
		t.Fatalf("task count=%d err=%v", tasks, err)
	}
	if err = db.QueryRow("SELECT COUNT(*) FROM delivery_attempt a JOIN delivery_task d ON d.delivery_id=a.delivery_id WHERE d.msg_id=?", snapshot.Trade.MsgID).Scan(&attempts); err != nil || attempts != 2 {
		t.Fatalf("attempt count=%d err=%v", attempts, err)
	}
	for i, target := range snapshot.Route.Targets {
		var uid, current, first, outcome string
		var sendStarted bool
		if err = db.QueryRow("SELECT d.platform_uid,d.status,d.first_outcome,a.outcome,a.send_started FROM delivery_task d JOIN delivery_attempt a ON a.delivery_id=d.delivery_id WHERE d.delivery_id=? AND a.attempt_no=1", target.DeliveryID).Scan(&uid, &current, &first, &outcome, &sendStarted); err != nil {
			t.Fatal(err)
		}
		wantState, wantOutcome := "SUCCEEDED", "SUCCESS"
		if i == 1 {
			wantState, wantOutcome = "RETRY_WAIT", "UNKNOWN"
		}
		if uid != target.PlatformUID || current != wantState || first != wantOutcome || outcome != wantOutcome || !sendStarted {
			t.Fatalf("lost first-round fact: uid=%s current=%s first=%s outcome=%s started=%v", uid, current, first, outcome, sendStarted)
		}
	}
	t.Logf("complete durable handoff msgId=%s", snapshot.Trade.MsgID)
}

// Removing primary confirmation would turn the real successful commit into a
// false failure; blindly treating a commit error as success breaks rollback.
func TestHandoffCommitAcknowledgementLost(t *testing.T) {
	db := bindingDB(t)
	handoffSchema(t, db)
	snapshot := abnormalSnapshot(t)
	faultDB := handoffFaultDB(t, &handoffCommitFault{})
	if err := hubmysql.NewHandoffStore(faultDB).Handoff(context.Background(), snapshot); err != nil {
		t.Fatalf("committed exact snapshot was not confirmed: %v", err)
	}
	assertCompleteHandoff(t, db, snapshot)
}

func TestHandoffCommitRolledBackNeverConfirms(t *testing.T) {
	db := bindingDB(t)
	handoffSchema(t, db)
	snapshot := abnormalSnapshot(t)
	faultDB := handoffFaultDB(t, &handoffCommitFault{rollback: true})
	if err := hubmysql.NewHandoffStore(faultDB).Handoff(context.Background(), snapshot); err == nil {
		t.Fatal("uncommitted transaction falsely confirmed")
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM trade_message WHERE msg_id=?", snapshot.Trade.MsgID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rolled back message count=%d err=%v", count, err)
	}
	for _, target := range snapshot.Route.Targets {
		for _, table := range []string{"delivery_task", "delivery_attempt"} {
			if err := db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE delivery_id=?", target.DeliveryID).Scan(&count); err != nil || count != 0 {
				t.Fatalf("rollback left %s rows=%d err=%v", table, count, err)
			}
		}
	}
}

// Assuming a failed confirmation means rollback would lose already committed
// recovery responsibility. It must remain durable despite an error response.
func TestHandoffCommitConfirmationUnavailableRetainsResponsibility(t *testing.T) {
	db := bindingDB(t)
	handoffSchema(t, db)
	snapshot := abnormalSnapshot(t)
	faultDB := handoffFaultDB(t, &handoffCommitFault{confirmation: "unavailable"})
	if err := hubmysql.NewHandoffStore(faultDB).Handoff(context.Background(), snapshot); err == nil {
		t.Fatal("inaccessible primary confirmation falsely confirmed")
	}
	assertCompleteHandoff(t, db, snapshot)
}

// Replacing the original context with a fresh confirmation budget would exceed
// the caller's processing deadline and fail this bounded-wait assertion.
func TestHandoffCommitConfirmationHonorsOriginalBudget(t *testing.T) {
	db := bindingDB(t)
	handoffSchema(t, db)
	snapshot := abnormalSnapshot(t)
	faultDB := handoffFaultDB(t, &handoffCommitFault{confirmation: "wait-for-context"})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := hubmysql.NewHandoffStore(faultDB).Handoff(ctx, snapshot)
	elapsed := time.Since(start)
	if err == nil || !errors.Is(ctx.Err(), context.DeadlineExceeded) || elapsed > 900*time.Millisecond {
		t.Fatalf("confirmation escaped original budget: elapsed=%s err=%v context=%v", elapsed, err, ctx.Err())
	}
	assertCompleteHandoff(t, db, snapshot)
	t.Logf("confirmation stopped at original deadline after %s", elapsed)
}

// Confirming msgId alone is insufficient: an existing but different immutable
// revision or body must not be mistaken for this handoff's successful commit.
func TestHandoffCommitConfirmationRequiresExactSnapshot(t *testing.T) {
	db := bindingDB(t)
	handoffSchema(t, db)
	for _, field := range []string{"archive_revision", "first_snapshot"} {
		t.Run(field, func(t *testing.T) {
			snapshot := abnormalSnapshot(t)
			faultDB := handoffFaultDB(t, &handoffCommitFault{afterCommit: func() error {
				_, err := db.Exec("UPDATE trade_message SET "+field+"=? WHERE msg_id=?", "different-immutable-value", snapshot.Trade.MsgID)
				return err
			}})
			if err := hubmysql.NewHandoffStore(faultDB).Handoff(context.Background(), snapshot); err == nil {
				t.Fatalf("different %s falsely confirmed as exact snapshot", field)
			}
		})
	}
}
