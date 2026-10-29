//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	mysqldriver "github.com/go-sql-driver/mysql"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"partner-hub/internal/adapter"
	"partner-hub/internal/delivery"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/model"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A command races with the actual scanner on the same persistent target. At
// most one new cycle may be exposed, and a scanner may only claim that cycle.
func TestManualRacesAutomaticClaim(t *testing.T) {
	for _, kind := range []string{delivery.RetryDelivery, delivery.RetryRouting} {
		t.Run(kind, func(t *testing.T) {
			db := manualDB(t)
			r := manualRequest(kind, manualTarget(t, db, kind))
			start := make(chan struct{})
			errs := make(chan error, 2)
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				_, e := hubmysql.NewManualStore(db).ExecuteRetry(context.Background(), r)
				errs <- e
			}()
			go func() {
				defer wg.Done()
				<-start
				if kind == delivery.RetryDelivery {
					cs, e := hubmysql.NewRecoveryStore(db).ClaimDue(context.Background(), "PLATFORM_B", "auto-race", 1)
					if e == nil && len(cs) > 0 && (cs[0].CycleNo != 2 || cs[0].CycleAttempt != 1) {
						e = errors.New("scanner claimed old cycle")
					}
					errs <- e
				} else {
					cs, e := hubmysql.NewRouteRecoveryStore(db).ClaimRoutes(context.Background(), "auto-race", 1)
					if e == nil && len(cs) > 0 && (cs[0].CycleNo != 2 || cs[0].CycleAttempt != 1) {
						e = errors.New("scanner claimed old route cycle")
					}
					errs <- e
				}
			}()
			close(start)
			wg.Wait()
			close(errs)
			for e := range errs {
				if e != nil {
					t.Fatal(e)
				}
			}
			// Repeat the saved command after scanner transition: its result must remain
			// the committed PENDING command result, with no second reopening.
			got, e := hubmysql.NewManualStore(db).ExecuteRetry(context.Background(), r)
			if e != nil || got.RetryCycle != 2 || got.State != "PENDING" {
				t.Fatalf("saved result %+v %v", got, e)
			}
			var state string
			var cycle, attempts int
			q := "SELECT status,cycle_no,cycle_attempt_count FROM delivery_task WHERE delivery_id=?"
			if kind == delivery.RetryRouting {
				q = "SELECT route_status,route_cycle_no,route_cycle_attempt_count FROM trade_message WHERE msg_id=?"
			}
			if e = db.QueryRow(q, r.TargetID).Scan(&state, &cycle, &attempts); e != nil || cycle != 2 || !(state == "PENDING" && attempts == 0 || state == "IN_FLIGHT" && attempts == 1) {
				t.Fatalf("race state %s/%d/%d %v", state, cycle, attempts, e)
			}
		})
	}
}

func TestManualCommitAcknowledgementLossCanBeConfirmed(t *testing.T) {
	db := manualDB(t)
	r := manualRequest(delivery.RetryDelivery, manualTarget(t, db, delivery.RetryDelivery))
	var schema string
	if e := db.QueryRow("SELECT DATABASE()").Scan(&schema); e != nil {
		t.Fatal(e)
	}
	cfg, e := mysqldriver.ParseDSN(os.Getenv("RECOVERY_TEST_ADMIN_DSN"))
	if e != nil {
		t.Fatal(e)
	}
	cfg.DBName = schema
	connector, e := mysqldriver.NewConnector(cfg)
	if e != nil {
		t.Fatal(e)
	}
	uncertain := sql.OpenDB(&handoffFaultConnector{Connector: connector, fault: &handoffCommitFault{}})
	defer uncertain.Close()
	if _, e = hubmysql.NewManualStore(uncertain).ExecuteRetry(context.Background(), r); e == nil || errors.Is(e, delivery.ErrRetryConflict) {
		t.Fatalf("lost ACK must be unavailable: %v", e)
	}
	manualState(t, db, r, "PENDING", 2, 0)
	got, e := hubmysql.NewManualStore(db).ExecuteRetry(context.Background(), r)
	if e != nil || got.RetryCycle != 2 || got.State != "PENDING" {
		t.Fatalf("could not confirm committed command %+v %v", got, e)
	}
	manualState(t, db, r, "PENDING", 2, 0)
}

func TestManualExhaustionRepairAndOriginalUIDSuccess(t *testing.T) {
	db := manualDB(t)
	var fixed atomic.Bool
	var requests atomic.Int32
	var successUID string
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		body, e := io.ReadAll(r.Body)
		if e != nil {
			w.WriteHeader(400)
			return
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(body, &fields) != nil || len(fields) != 8 {
			w.WriteHeader(400)
			return
		}
		if !fixed.Load() {
			w.WriteHeader(429)
			return
		}
		var uid string
		json.Unmarshal(fields["userUid"], &uid)
		mu.Lock()
		successUID = uid
		mu.Unlock()
		io.WriteString(w, `{"code":0}`)
	}))
	defer server.Close()
	ad, e := adapter.NewPlatform1(server.URL)
	if e != nil {
		t.Fatal(e)
	}
	s := archiveSnapshot(t)
	a := &s.Attempts[0]
	p, e := ad.Prepare(model.TradeNotification{SecCode: s.Trade.SecCode, SecName: s.Trade.SecName, Side: s.Trade.Side, Market: s.Trade.Market, TradeType: s.Trade.TradeType, TraceID: s.Trade.TraceID, ExecTime: s.Trade.ExecTime}, a.Target.PlatformUID)
	if e != nil {
		t.Fatal(e)
	}
	a.Request = p
	a.Result = ad.Send(context.Background(), p)
	if a.Result.Outcome != model.RetryableFailure {
		t.Fatalf("first failure %+v", a.Result)
	}
	if e = hubmysql.NewHandoffStore(db).Handoff(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	id := a.Target.DeliveryID
	worker := delivery.NewWorker(hubmysql.NewRecoveryStore(db), ad, "PLATFORM_A", "repair-worker")
	for n := 2; n <= 5; n++ {
		if _, e = db.Exec("UPDATE delivery_task SET next_attempt_at=UTC_TIMESTAMP(3) WHERE delivery_id=?", id); e != nil {
			t.Fatal(e)
		}
		if got, e := worker.RunOnce(context.Background()); e != nil || got != 1 {
			t.Fatalf("failure attempt %d: %d %v", n, got, e)
		}
	}
	r := manualRequest(delivery.RetryDelivery, id)
	manualState(t, db, r, "NEEDS_ATTENTION", 1, 5)
	if requests.Load() != 5 {
		t.Fatalf("external attempts %d", requests.Load())
	}
	fixed.Store(true)
	if _, e = hubmysql.NewManualStore(db).ExecuteRetry(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	manualState(t, db, r, "PENDING", 2, 0)
	if requests.Load() != 5 {
		t.Fatal("manual command sent directly")
	}
	if n, e := worker.RunOnce(context.Background()); e != nil || n != 1 {
		t.Fatalf("repaired worker %d %v", n, e)
	}
	manualState(t, db, r, "SUCCEEDED", 2, 1)
	mu.Lock()
	uid := successUID
	mu.Unlock()
	if uid != a.Target.PlatformUID || requests.Load() != 6 {
		t.Fatalf("changed original UID %q, requests=%d", uid, requests.Load())
	}
	var total, history int
	var first, last string
	if e = db.QueryRow("SELECT attempt_count,first_outcome,last_outcome,(SELECT COUNT(*) FROM delivery_attempt a WHERE a.delivery_id=d.delivery_id) FROM delivery_task d WHERE delivery_id=?", id).Scan(&total, &first, &last, &history); e != nil || total != 6 || history != 6 || first != "RETRYABLE_FAILURE" || last != "SUCCESS" {
		t.Fatalf("lost history total=%d rows=%d first=%s last=%s %v", total, history, first, last, e)
	}
	var oldCycle, newCycle int
	if e = db.QueryRow("SELECT SUM(cycle_no=1),SUM(cycle_no=2) FROM delivery_attempt WHERE delivery_id=?", id).Scan(&oldCycle, &newCycle); e != nil || oldCycle != 5 || newCycle != 1 {
		t.Fatalf("wrong cycle history %d %d %v", oldCycle, newCycle, e)
	}
}

func TestManualRejectsInvalidInputAndBoundsLockWait(t *testing.T) {
	db := manualDB(t)
	r := manualRequest(delivery.RetryDelivery, manualTarget(t, db, delivery.RetryDelivery))
	bad := r
	bad.Operator = ""
	if _, e := hubmysql.NewManualStore(db).ExecuteRetry(context.Background(), bad); !errors.Is(e, delivery.ErrInvalidRetry) {
		t.Fatalf("invalid input %v", e)
	}
	tx, e := db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	var id string
	if e = tx.QueryRow("SELECT delivery_id FROM delivery_task WHERE delivery_id=? FOR UPDATE", r.TargetID).Scan(&id); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, e = hubmysql.NewManualStore(db).ExecuteRetry(ctx, r); e == nil || time.Since(start) > time.Second {
		t.Fatalf("lock wait unbounded %v", e)
	}
	tx.Rollback()
	manualState(t, db, r, "NEEDS_ATTENTION", 1, 5)
	var n int
	if e = db.QueryRow("SELECT COUNT(*) FROM manual_operation").Scan(&n); e != nil || n != 0 {
		t.Fatalf("cancelled placeholder persisted %d %v", n, e)
	}
}

func manualDB(t *testing.T) *sql.DB {
	t.Helper()
	db := routeDB(t)
	b, e := os.ReadFile("../../deploy/mysql/migrations/007_manual_operation.sql")
	if e != nil {
		t.Fatal(e)
	}
	for range 2 {
		for _, q := range strings.Split(string(b), ";") {
			if strings.TrimSpace(q) != "" {
				if _, e = db.Exec(q); e != nil {
					t.Fatal(e)
				}
			}
		}
	}
	return db
}
func manualRequest(kind, id string) delivery.RetryRequest {
	return delivery.RetryRequest{OperationID: model.NewID(), Kind: kind, TargetID: id, Operator: "本地运维", Reason: "故障已修复"}
}
func manualTarget(t *testing.T, db *sql.DB, kind string) string {
	t.Helper()
	if kind == delivery.RetryRouting {
		s := dueRoute(t, db)
		if _, e := db.Exec("UPDATE trade_message SET route_status='NEEDS_ATTENTION',route_cycle_attempt_count=5 WHERE msg_id=?", s.Trade.MsgID); e != nil {
			t.Fatal(e)
		}
		return s.Trade.MsgID
	}
	s := dueRecovery(t, db)
	id := s.Attempts[1].Target.DeliveryID
	if _, e := db.Exec("UPDATE delivery_task SET status='NEEDS_ATTENTION',cycle_attempt_count=5 WHERE delivery_id=?", id); e != nil {
		t.Fatal(e)
	}
	return id
}
func manualState(t *testing.T, db *sql.DB, r delivery.RetryRequest, want string, cycle, attempts int) {
	t.Helper()
	q := "SELECT status,cycle_no,cycle_attempt_count FROM delivery_task WHERE delivery_id=?"
	if r.Kind == delivery.RetryRouting {
		q = "SELECT route_status,route_cycle_no,route_cycle_attempt_count FROM trade_message WHERE msg_id=?"
	}
	var s string
	var c, a int
	if e := db.QueryRow(q, r.TargetID).Scan(&s, &c, &a); e != nil || s != want || c != cycle || a != attempts {
		t.Fatalf("state=%s cycle=%d attempts=%d err=%v", s, c, a, e)
	}
}
func TestManualReopensOriginalCycleAndReusesSavedResult(t *testing.T) {
	for _, kind := range []string{delivery.RetryDelivery, delivery.RetryRouting} {
		t.Run(kind, func(t *testing.T) {
			db := manualDB(t)
			id := manualTarget(t, db, kind)
			r := manualRequest(kind, id)
			store := hubmysql.NewManualStore(db)
			var before, after []byte
			var beforeTotal, afterTotal, beforeHistory, afterHistory int
			var beforeUID, afterUID string
			q := "SELECT m.first_snapshot,d.attempt_count,d.platform_uid,(SELECT COUNT(*) FROM delivery_attempt a WHERE a.delivery_id=d.delivery_id) FROM delivery_task d JOIN trade_message m ON m.msg_id=d.msg_id WHERE d.delivery_id=?"
			if kind == delivery.RetryRouting {
				q = "SELECT first_snapshot,route_attempt_count,'',(SELECT COUNT(*) FROM route_attempt a WHERE a.msg_id=m.msg_id) FROM trade_message m WHERE msg_id=?"
			}
			if e := db.QueryRow(q, id).Scan(&before, &beforeTotal, &beforeUID, &beforeHistory); e != nil {
				t.Fatal(e)
			}
			got, e := store.ExecuteRetry(context.Background(), r)
			if e != nil || got.OperationID != r.OperationID || got.TargetID != id || got.Kind != kind || got.State != "PENDING" || got.RetryCycle != 2 {
				t.Fatalf("new cycle not saved: %+v %v", got, e)
			}
			manualState(t, db, r, "PENDING", 2, 0)
			if e = db.QueryRow(q, id).Scan(&after, &afterTotal, &afterUID, &afterHistory); e != nil || !bytes.Equal(before, after) || beforeTotal != afterTotal || beforeUID != afterUID || beforeHistory != afterHistory {
				t.Fatalf("original facts/history altered: %v", e)
			}
			again, e := store.ExecuteRetry(context.Background(), r)
			if e != nil || again != got {
				t.Fatalf("saved result not reused: %+v %v", again, e)
			}
			manualState(t, db, r, "PENDING", 2, 0)
			var n int
			if e = db.QueryRow("SELECT COUNT(*) FROM manual_operation WHERE operation_id=? AND retry_cycle=2 AND result_state='PENDING'", r.OperationID).Scan(&n); e != nil || n != 1 {
				t.Fatalf("operation not atomic %d %v", n, e)
			}
		})
	}
}
func TestManualRejectsConflictingOperationParameters(t *testing.T) {
	db := manualDB(t)
	r := manualRequest(delivery.RetryDelivery, manualTarget(t, db, delivery.RetryDelivery))
	store := hubmysql.NewManualStore(db)
	if _, e := store.ExecuteRetry(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	cases := []delivery.RetryRequest{r, r, r, r}
	cases[0].Reason += " "
	cases[1].Operator = "other"
	cases[2].TargetID = model.NewID()
	cases[3].Kind = delivery.RetryRouting
	for _, c := range cases {
		if _, e := store.ExecuteRetry(context.Background(), c); !errors.Is(e, delivery.ErrRetryConflict) {
			t.Fatalf("accepted conflicting operation %+v %v", c, e)
		}
	}
}
func TestManualRejectsAbsentAndIneligibleTargets(t *testing.T) {
	for _, kind := range []string{delivery.RetryDelivery, delivery.RetryRouting} {
		t.Run(kind, func(t *testing.T) {
			db := manualDB(t)
			store := hubmysql.NewManualStore(db)
			r := manualRequest(kind, model.NewID())
			if _, e := store.ExecuteRetry(context.Background(), r); !errors.Is(e, delivery.ErrRetryNotFound) {
				t.Fatalf("absent target: %v", e)
			}
			r.TargetID = manualTarget(t, db, kind)
			states := []string{"SUCCEEDED", "IN_FLIGHT", "PENDING", "RETRY_WAIT"}
			q := "UPDATE delivery_task SET status=? WHERE delivery_id=?"
			if kind == delivery.RetryRouting {
				states = []string{"ROUTED", "NO_ROUTE", "IN_FLIGHT", "PENDING", "RETRY_WAIT"}
				q = "UPDATE trade_message SET route_status=? WHERE msg_id=?"
			}
			for _, state := range states {
				if _, e := db.Exec(q, state, r.TargetID); e != nil {
					t.Fatal(e)
				}
				r.OperationID = model.NewID()
				if _, e := store.ExecuteRetry(context.Background(), r); !errors.Is(e, delivery.ErrRetryConflict) {
					t.Fatalf("reopened %s: %v", state, e)
				}
			}
			var n int
			if e := db.QueryRow("SELECT COUNT(*) FROM manual_operation").Scan(&n); e != nil || n != 0 {
				t.Fatalf("rejected command left audit placeholder %d %v", n, e)
			}
		})
	}
}
func TestManualConcurrentSameAndDifferentOperations(t *testing.T) {
	for _, kind := range []string{delivery.RetryDelivery, delivery.RetryRouting} {
		for _, same := range []bool{true, false} {
			t.Run(kind+map[bool]string{true: "same", false: "different"}[same], func(t *testing.T) {
				db := manualDB(t)
				r := manualRequest(kind, manualTarget(t, db, kind))
				other := r
				if !same {
					other.OperationID = model.NewID()
				}
				start := make(chan struct{})
				errs := make(chan error, 2)
				var wg sync.WaitGroup
				for _, req := range []delivery.RetryRequest{r, other} {
					wg.Add(1)
					go func(req delivery.RetryRequest) {
						defer wg.Done()
						<-start
						result, e := hubmysql.NewManualStore(db).ExecuteRetry(context.Background(), req)
						if e == nil && result.RetryCycle != 2 {
							e = errors.New("wrong cycle")
						}
						errs <- e
					}(req)
				}
				close(start)
				wg.Wait()
				close(errs)
				ok, conflicts := 0, 0
				for e := range errs {
					if e == nil {
						ok++
					} else if errors.Is(e, delivery.ErrRetryConflict) {
						conflicts++
					} else {
						t.Fatal(e)
					}
				}
				if same && ok != 2 || !same && (ok != 1 || conflicts != 1) {
					t.Fatalf("ok=%d conflicts=%d", ok, conflicts)
				}
				manualState(t, db, r, "PENDING", 2, 0)
				var n int
				if e := db.QueryRow("SELECT COUNT(*) FROM manual_operation").Scan(&n); e != nil {
					t.Fatal(e)
				}
				if n != 1 {
					t.Fatalf("operations %d", n)
				}
			})
		}
	}
}
func TestManualRollsBackEveryWriteFailure(t *testing.T) {
	for _, kind := range []string{delivery.RetryDelivery, delivery.RetryRouting} {
		for _, step := range []string{"operation", "target", "result"} {
			t.Run(kind+step, func(t *testing.T) {
				db := manualDB(t)
				r := manualRequest(kind, manualTarget(t, db, kind))
				table, event := "manual_operation", "INSERT"
				if step == "target" {
					table = "delivery_task"
					if kind == delivery.RetryRouting {
						table = "trade_message"
					}
					event = "UPDATE"
				}
				if step == "result" {
					event = "UPDATE"
				}
				if _, e := db.Exec("CREATE TRIGGER manual_fail BEFORE " + event + " ON " + table + " FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='manual injection'"); e != nil {
					t.Fatal(e)
				}
				if _, e := hubmysql.NewManualStore(db).ExecuteRetry(context.Background(), r); e == nil || errors.Is(e, delivery.ErrRetryConflict) || errors.Is(e, delivery.ErrRetryNotFound) {
					t.Fatalf("write failure not unavailable %v", e)
				}
				manualState(t, db, r, "NEEDS_ATTENTION", 1, 5)
				var n int
				if e := db.QueryRow("SELECT COUNT(*) FROM manual_operation").Scan(&n); e != nil {
					t.Fatal(e)
				}
				if n != 0 {
					t.Fatalf("partial audit %d", n)
				}
			})
		}
	}
}
