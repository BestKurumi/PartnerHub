//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/model"
)

// All statements and COMMIT reach a fresh real MySQL schema. Only the
// acknowledgement is replaced by the existing transport fault wrapper.
func archiveReplayFaultDB(t *testing.T, db *sql.DB) *sql.DB {
	t.Helper()
	var name string
	if err := db.QueryRow("SELECT DATABASE()").Scan(&name); err != nil {
		t.Fatal(err)
	}
	cfg, err := mysqldriver.ParseDSN(os.Getenv("RECOVERY_TEST_ADMIN_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName, cfg.ParseTime = name, true
	connector, err := mysqldriver.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	faultDB := sql.OpenDB(&handoffFaultConnector{Connector: connector, fault: &handoffCommitFault{}})
	faultDB.SetMaxOpenConns(1)
	t.Cleanup(func() { faultDB.Close() })
	return faultDB
}

func assertArchiveReplayCounts(t *testing.T, db *sql.DB, snapshot model.FirstRoundSnapshot, want int) {
	t.Helper()
	for _, table := range []string{"trade_message", "delivery_task", "delivery_attempt"} {
		var count int
		query, value := "SELECT COUNT(*) FROM "+table+" WHERE msg_id=?", snapshot.Trade.MsgID
		if table == "delivery_attempt" {
			query, value = "SELECT COUNT(*) FROM delivery_attempt WHERE delivery_id=?", snapshot.Attempts[0].Target.DeliveryID
		}
		if err := db.QueryRow(query, value).Scan(&count); err != nil || count != want {
			t.Fatalf("%s count=%d want=%d err=%v", table, count, want, err)
		}
	}
}

// Treating an uncertain COMMIT as rollback, or replacing INSERT NOOP with an
// unconditional insert, would respectively lose responsibility or duplicate it.
func TestArchiveReplayCommitAcknowledgementLost(t *testing.T) {
	db := recoveryDB(t)
	snapshot := archiveSnapshot(t)
	err := hubmysql.NewArchiveStore(archiveReplayFaultDB(t, db)).Save(context.Background(), []model.FirstRoundSnapshot{snapshot})
	if err == nil {
		t.Fatal("lost commit acknowledgement was falsely confirmed")
	}
	assertArchiveReplayCounts(t, db, snapshot, 1)
	if err = hubmysql.NewArchiveStore(db).Save(context.Background(), []model.FirstRoundSnapshot{snapshot}); err != nil {
		t.Fatalf("same complete snapshot cannot be replayed: %v", err)
	}
	assertArchiveReplayCounts(t, db, snapshot, 1)
	var revision, uid, state, outcome string
	if err = db.QueryRow(`SELECT m.archive_revision,d.platform_uid,d.status,a.outcome FROM trade_message m JOIN delivery_task d ON d.msg_id=m.msg_id JOIN delivery_attempt a ON a.delivery_id=d.delivery_id WHERE m.msg_id=? AND a.attempt_no=1`, snapshot.Trade.MsgID).Scan(&revision, &uid, &state, &outcome); err != nil || revision != snapshot.ArchiveRevision || uid != "fixed-before-unbind" || state != "SUCCEEDED" || outcome != "SUCCESS" {
		t.Fatalf("replay changed first-round facts revision=%s uid=%s state=%s outcome=%s err=%v", revision, uid, state, outcome, err)
	}
}

// An ON DUPLICATE update of mutable task fields or old attempt facts would
// rewind the subsequent owner, retry cycle and recorded history.
func TestArchiveReplayPreservesLaterRecoveryState(t *testing.T) {
	db := recoveryDB(t)
	snapshot := archiveSnapshot(t)
	store := hubmysql.NewArchiveStore(db)
	if err := store.Save(context.Background(), []model.FirstRoundSnapshot{snapshot}); err != nil {
		t.Fatal(err)
	}
	id := snapshot.Attempts[0].Target.DeliveryID
	leaseUntil := time.Date(2030, 1, 2, 3, 4, 5, 6000000, time.UTC)
	nextAt := leaseUntil.Add(time.Minute)
	if _, err := db.Exec(`UPDATE delivery_task SET status='IN_FLIGHT',last_outcome='UNKNOWN',cycle_no=3,attempt_count=6,cycle_attempt_count=2,next_attempt_at=?,lease_owner='new-owner',lease_version=9,lease_until=? WHERE delivery_id=?`, nextAt, leaseUntil, id); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE delivery_attempt SET summary='preserve original persisted history' WHERE delivery_id=? AND attempt_no=1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO delivery_attempt(delivery_id,attempt_no,cycle_no,send_started,protocol_version,prepared_payload,started_at,finished_at,outcome,summary,lease_owner,lease_version) VALUES(?,6,3,true,'recovery.v1',?,UTC_TIMESTAMP(3),NULL,'IN_FLIGHT','new attempt','new-owner',9)`, id, []byte(`{"fixed":"later-payload"}`)); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), []model.FirstRoundSnapshot{snapshot}); err != nil {
		t.Fatal(err)
	}
	var state, outcome, owner, uid, first, protocol string
	var cycle, attempts, cycleAttempts, version int
	var next, until time.Time
	var payload []byte
	err := db.QueryRow(`SELECT status,last_outcome,cycle_no,attempt_count,cycle_attempt_count,next_attempt_at,lease_owner,lease_version,lease_until,platform_uid,first_outcome,protocol_version,prepared_payload FROM delivery_task WHERE delivery_id=?`, id).Scan(&state, &outcome, &cycle, &attempts, &cycleAttempts, &next, &owner, &version, &until, &uid, &first, &protocol, &payload)
	if err != nil || state != "IN_FLIGHT" || outcome != "UNKNOWN" || cycle != 3 || attempts != 6 || cycleAttempts != 2 || !next.Equal(nextAt) || owner != "new-owner" || version != 9 || !until.Equal(leaseUntil) || uid != "fixed-before-unbind" || first != "SUCCESS" || protocol != "platform1.v1" || string(payload) != `{"userUid":"fixed-before-unbind"}` {
		t.Fatalf("old archive rewound task state=%s outcome=%s cycle=%d attempts=%d cycleAttempts=%d owner=%s version=%d next=%s until=%s uid=%s first=%s protocol=%s payload=%s err=%v", state, outcome, cycle, attempts, cycleAttempts, owner, version, next, until, uid, first, protocol, payload, err)
	}
	var count int
	if err = db.QueryRow("SELECT COUNT(*) FROM delivery_attempt WHERE delivery_id=?", id).Scan(&count); err != nil || count != 2 {
		t.Fatalf("history count=%d err=%v", count, err)
	}
	var summary string
	if err = db.QueryRow("SELECT summary FROM delivery_attempt WHERE delivery_id=? AND attempt_no=1", id).Scan(&summary); err != nil || summary != "preserve original persisted history" {
		t.Fatalf("old attempt overwritten summary=%s err=%v", summary, err)
	}
	var finished sql.NullTime
	if err = db.QueryRow("SELECT outcome,summary,lease_owner,lease_version,finished_at,prepared_payload FROM delivery_attempt WHERE delivery_id=? AND attempt_no=6", id).Scan(&outcome, &summary, &owner, &version, &finished, &payload); err != nil || outcome != "IN_FLIGHT" || summary != "new attempt" || owner != "new-owner" || version != 9 || finished.Valid || string(payload) != `{"fixed":"later-payload"}` {
		t.Fatalf("new attempt altered outcome=%s summary=%s owner=%s version=%d finished=%v payload=%s err=%v", outcome, summary, owner, version, finished, payload, err)
	}
}

// Removing the stored immutable revision check would falsely confirm a
// conflicting snapshot, allowing the caller to discard its pending record.
func TestArchiveReplayRejectsConflictingRevision(t *testing.T) {
	db := recoveryDB(t)
	snapshot := archiveSnapshot(t)
	store := hubmysql.NewArchiveStore(db)
	if err := store.Save(context.Background(), []model.FirstRoundSnapshot{snapshot}); err != nil {
		t.Fatal(err)
	}
	changed := snapshot
	changed.ArchiveRevision = model.NewID()
	changed.Trade.RawPayload = []byte(`{"conflicting":"immutable snapshot"}`)
	if err := store.Save(context.Background(), []model.FirstRoundSnapshot{changed}); err == nil {
		t.Fatal("conflicting revision was confirmed")
	}
	var revision string
	var raw []byte
	if err := db.QueryRow("SELECT archive_revision,raw_payload FROM trade_message WHERE msg_id=?", snapshot.Trade.MsgID).Scan(&revision, &raw); err != nil || revision != snapshot.ArchiveRevision || string(raw) != string(snapshot.Trade.RawPayload) {
		t.Fatalf("conflict changed original revision=%s raw=%s err=%v", revision, raw, err)
	}
	assertArchiveReplayCounts(t, db, snapshot, 1)
}

// Committing each message separately, or swallowing a late SQL error, would
// leave the first complete message and the second partial task in the batch.
func TestArchiveReplayBatchFailureRollsBackWholeBatch(t *testing.T) {
	db := recoveryDB(t)
	first, second := archiveSnapshot(t), archiveSnapshot(t)
	// Reject only the second attempt INSERT after the first message and the
	// second message/task statements have really reached MySQL.
	_, err := db.Exec(`CREATE TRIGGER reject_selected_archive_attempt BEFORE INSERT ON delivery_attempt FOR EACH ROW BEGIN IF NEW.delivery_id='` + second.Attempts[0].Target.DeliveryID + `' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected archive attempt failure'; END IF; END`)
	if err != nil {
		t.Fatal(err)
	}
	err = hubmysql.NewArchiveStore(db).Save(context.Background(), []model.FirstRoundSnapshot{first, second})
	if err == nil {
		t.Fatal("batch confirmed despite late SQL failure")
	}
	assertArchiveReplayCounts(t, db, first, 0)
	assertArchiveReplayCounts(t, db, second, 0)
	if _, err = db.Exec("DROP TRIGGER reject_selected_archive_attempt"); err != nil {
		t.Fatal(err)
	}
	if err = hubmysql.NewArchiveStore(db).Save(context.Background(), []model.FirstRoundSnapshot{first, second}); err != nil {
		t.Fatalf("retained batch cannot replay after dependency recovers: %v", err)
	}
	assertArchiveReplayCounts(t, db, first, 1)
	assertArchiveReplayCounts(t, db, second, 1)
}
