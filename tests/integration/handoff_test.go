//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"os"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/model"
	"strings"
	"testing"
)

func abnormalSnapshot(t *testing.T) model.FirstRoundSnapshot {
	t.Helper()
	s := archiveSnapshot(t)
	b := s.Attempts[0]
	b.Target = model.RouteTarget{PlatformID: "PLATFORM_B", PlatformUID: "fixed-failed-uid", DeliveryID: model.NewID()}
	b.Request.PlatformID = "PLATFORM_B"
	b.Request.ProtocolVersion = "platform2.v1"
	b.Result.Outcome = model.Unknown
	b.Result.Summary = "broker confirmation unknown"
	s.Route.Targets = append(s.Route.Targets, b.Target)
	s.Attempts = append(s.Attempts, b)
	return s
}
func handoffSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	ensureDeliverySchema(t, db)
	ddl, err := os.ReadFile("../../deploy/mysql/migrations/004_handoff.sql")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, stmt := range strings.Split(string(ddl), ";") {
		if strings.TrimSpace(stmt) != "" {
			if _, err = conn.ExecContext(context.Background(), stmt); err != nil {
				t.Fatal(err)
			}
		}
	}
}
func TestHandoffCompleteAndReplay(t *testing.T) {
	db := bindingDB(t)
	handoffSchema(t, db)
	s := abnormalSnapshot(t)
	store := hubmysql.NewHandoffStore(db)
	if err := store.Handoff(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	handoffSchema(t, db)
	for _, a := range s.Attempts {
		var uid, current, first string
		var attempts, cycle int
		var due sql.NullTime
		if err := db.QueryRow("SELECT platform_uid,status,first_outcome,attempt_count,cycle_attempt_count,next_attempt_at FROM delivery_task WHERE delivery_id=?", a.Target.DeliveryID).Scan(&uid, &current, &first, &attempts, &cycle, &due); err != nil {
			t.Fatal(err)
		}
		want := "SUCCEEDED"
		if a.Result.Outcome != model.Success {
			want = "RETRY_WAIT"
		}
		if uid != a.Target.PlatformUID || current != want || first != string(a.Result.Outcome) || attempts != 1 || cycle != 1 || due.Valid != (want == "RETRY_WAIT") {
			t.Fatalf("incomplete task uid=%s current=%s first=%s tries=%d cycle=%d due=%v", uid, current, first, attempts, cycle, due)
		}
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM delivery_attempt WHERE delivery_id=? AND attempt_no=1 AND outcome=?", a.Target.DeliveryID, string(a.Result.Outcome)).Scan(&n); err != nil || n != 1 {
			t.Fatalf("missing attempt %d %v", n, err)
		}
	}
	if _, err := db.Exec("UPDATE delivery_task SET status='SUCCEEDED',attempt_count=3,cycle_attempt_count=3 WHERE delivery_id=?", s.Attempts[1].Target.DeliveryID); err != nil {
		t.Fatal(err)
	}
	if err := store.Handoff(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	var status, first string
	var count int
	if err := db.QueryRow("SELECT status,first_outcome,attempt_count FROM delivery_task WHERE delivery_id=?", s.Attempts[1].Target.DeliveryID).Scan(&status, &first, &count); err != nil || status != "SUCCEEDED" || first != "UNKNOWN" || count != 3 {
		t.Fatalf("replay reset task %s %s %d %v", status, first, count, err)
	}
	changed := s
	changed.Attempts = append([]model.FirstAttempt(nil), s.Attempts...)
	changed.Attempts[1].Target.PlatformUID = "changed"
	if err := store.Handoff(context.Background(), changed); err == nil {
		t.Fatal("conflicting internal replay accepted")
	}
}
func TestHandoffPermanentAndRouteFailure(t *testing.T) {
	db := bindingDB(t)
	handoffSchema(t, db)
	s := abnormalSnapshot(t)
	s.Attempts[1].Result.Outcome = model.PermanentFailure
	store := hubmysql.NewHandoffStore(db)
	if err := store.Handoff(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	var status string
	var due sql.NullTime
	if err := db.QueryRow("SELECT status,next_attempt_at FROM delivery_task WHERE delivery_id=?", s.Attempts[1].Target.DeliveryID).Scan(&status, &due); err != nil || status != "NEEDS_ATTENTION" || due.Valid {
		t.Fatalf("permanent %s %v %v", status, due, err)
	}
	s = abnormalSnapshot(t)
	s.Route.Status = "RETRY_WAIT"
	s.Route.Targets = []model.RouteTarget{}
	s.Attempts = []model.FirstAttempt{}
	s.RouteError = "binding query unavailable"
	if err := store.Handoff(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM delivery_task WHERE msg_id=?", s.Trade.MsgID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("unknown targets manufactured %d %v", n, err)
	}
	if err := db.QueryRow("SELECT route_status FROM trade_message WHERE msg_id=?", s.Trade.MsgID).Scan(&status); err != nil || status != "RETRY_WAIT" {
		t.Fatalf("route responsibility %s %v", status, err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM route_attempt WHERE msg_id=? AND attempt_no=1 AND finished_at IS NOT NULL", s.Trade.MsgID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("route first attempt %d %v", n, err)
	}
}
func TestHandoffRollback(t *testing.T) {
	db := bindingDB(t)
	handoffSchema(t, db)
	for _, table := range []string{"trade_message", "delivery_task", "delivery_attempt", "route_attempt"} {
		t.Run(table, func(t *testing.T) {
			s := abnormalSnapshot(t)
			if table == "route_attempt" {
				s.Route.Status = "RETRY_WAIT"
				s.Route.Targets = []model.RouteTarget{}
				s.Attempts = []model.FirstAttempt{}
				s.RouteError = "binding unavailable"
			}
			trigger := "it_handoff_" + strings.ReplaceAll(model.NewID(), "-", "")
			condition := "NEW.msg_id='" + s.Trade.MsgID + "'"
			if table == "delivery_task" || table == "delivery_attempt" {
				condition = "NEW.delivery_id='" + s.Attempts[1].Target.DeliveryID + "'"
			}
			_, err := db.Exec("CREATE TRIGGER " + trigger + " BEFORE INSERT ON " + table + " FOR EACH ROW BEGIN IF " + condition + " THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='handoff injection'; END IF; END")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Exec("DROP TRIGGER IF EXISTS " + trigger)
			if err = hubmysql.NewHandoffStore(db).Handoff(context.Background(), s); err == nil || !strings.Contains(err.Error(), "handoff injection") {
				t.Fatalf("wrong injected failure %v", err)
			}
			var count int
			if err = db.QueryRow("SELECT COUNT(*) FROM trade_message WHERE msg_id=?", s.Trade.MsgID).Scan(&count); err != nil || count != 0 {
				t.Fatalf("partial persisted message %d %v", count, err)
			}
			for _, a := range s.Attempts {
				for _, table := range []string{"delivery_task", "delivery_attempt"} {
					if err = db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE delivery_id=?", a.Target.DeliveryID).Scan(&count); err != nil || count != 0 {
						t.Fatalf("partial %s %d %v", table, count, err)
					}
				}
			}
		})
	}
}

func TestHandoffPreparationFailureCountsUnsent(t *testing.T) {
	db := bindingDB(t)
	handoffSchema(t, db)
	s := abnormalSnapshot(t)
	s.Attempts[1].Request = model.PreparedRequest{}
	s.Attempts[1].Result.SendStarted = false
	s.Attempts[1].Result.Outcome = model.RetryableFailure
	s.Attempts[1].Result.Summary = "request preparation failed"
	if err := hubmysql.NewHandoffStore(db).Handoff(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	var count, payloadLen int
	var sent bool
	if err := db.QueryRow(`SELECT d.attempt_count,a.send_started,OCTET_LENGTH(a.prepared_payload) FROM delivery_task d JOIN delivery_attempt a ON a.delivery_id=d.delivery_id WHERE d.delivery_id=?`, s.Attempts[1].Target.DeliveryID).Scan(&count, &sent, &payloadLen); err != nil || count != 1 || sent || payloadLen != 0 {
		t.Fatalf("unsent attempt: count=%d sent=%v payload=%d err=%v", count, sent, payloadLen, err)
	}
}

func TestHandoffRejectsConflictingDeliveryIdentity(t *testing.T) {
	db := bindingDB(t)
	handoffSchema(t, db)
	first := abnormalSnapshot(t)
	store := hubmysql.NewHandoffStore(db)
	if err := store.Handoff(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := abnormalSnapshot(t)
	second.Route.Targets[1].DeliveryID = first.Route.Targets[1].DeliveryID
	second.Attempts[1].Target = second.Route.Targets[1]
	if err := store.Handoff(context.Background(), second); err == nil {
		t.Fatal("accepted partial handoff with another message's delivery identity")
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM trade_message WHERE msg_id=?", second.Trade.MsgID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("partial conflicting message %d %v", n, err)
	}
}
