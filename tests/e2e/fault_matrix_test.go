//go:build e2e

package e2e_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
	"partner-hub/internal/model"
)

// Faults affect only the explicitly authorized deployment. No volume or shared
// table is reset. Changing the Mock command uses its existing fault modes.
func faultAMode(t *testing.T, mode string) {
	t.Helper()
	if mode != "accept" && mode != "reject-busy" && mode != "persist-delay" {
		t.Fatal("unsupported fault fixture mode")
	}
	if mode == "accept" {
		startupCompose(t, "up", "-d", "--no-build", "--no-deps", "--wait", "--wait-timeout", "60", "mock-platform1")
		return
	}
	file := filepath.Join(t.TempDir(), "fault.yaml")
	body := "services:\n  mock-platform1:\n    command: [platform1, -listen, ':18081', -mode, " + mode + ", -delay, 3s]\n"
	if err := os.WriteFile(file, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	startupCompose(t, "-f", "docker-compose.yml", "-f", file, "up", "-d", "--no-build", "--no-deps", "--wait", "--wait-timeout", "60", "mock-platform1")
}

func faultBind(t *testing.T, ctx context.Context, platform, client, uid, kind string) {
	t.Helper()
	base := required(t, "HUB_TEST_URL") + "/mock/" + platform + "/demo/binding-events"
	code, body := demoRequest(t, ctx, "POST", base, map[string]string{"eventType": kind, "clientId": client, "platformUid": uid})
	var event struct{ EventID string }
	if code != 202 || json.Unmarshal(body, &event) != nil || event.EventID == "" {
		t.Fatal("fault fixture binding publication failed")
	}
	eventually(t, ctx, func() bool {
		code, raw := demoRequest(t, ctx, "GET", base+"/"+event.EventID, nil)
		var result struct{ Result string }
		return code == 200 && json.Unmarshal(raw, &result) == nil && result.Result == "APPLIED"
	})
}

type faultTaskState struct {
	ID, UID, Status, First         string
	Attempts, Cycle, CycleAttempts int
}

func faultTask(t *testing.T, ctx context.Context, db *sql.DB, msg, platform string) faultTaskState {
	t.Helper()
	var state faultTaskState
	if err := db.QueryRowContext(ctx, "SELECT delivery_id,platform_uid,status,first_outcome,attempt_count,cycle_no,cycle_attempt_count FROM delivery_task WHERE msg_id=? AND platform_id=?", msg, platform).Scan(&state.ID, &state.UID, &state.Status, &state.First, &state.Attempts, &state.Cycle, &state.CycleAttempts); err != nil {
		t.Fatal("saved platform responsibility unavailable", err)
	}
	return state
}

func faultReceipts(t *testing.T, ctx context.Context, db *sql.DB, msg, uid string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM received_notification WHERE trace_id=? AND user_uid=?", "trace-"+msg, uid).Scan(&n); err != nil {
		t.Fatal("independent receipt query failed")
	}
	return n
}

func faultSubmit(t *testing.T, ctx context.Context, client string) (string, model.TradeResponse) {
	t.Helper()
	id := model.NewID()
	r := isolationPost(ctx, required(t, "HUB_TEST_URL"), client, id)
	if r.err != nil || r.code != 202 || r.response.MsgID != id || r.response.Status != "RECOVERY_PENDING" || r.response.Acceptance != "ACCEPTED" || r.response.RecordStatus != "PERSISTED" {
		t.Fatalf("fault handoff must be confirmed: msgId=%s HTTP%d response=%+v err=%v", id, r.code, r.response, r.err)
	}
	return id, r.response
}

// Catches retrying successful peers, mutable retry UIDs, UNKNOWN reported as
// success, unbounded retry cycles, fake NO_ROUTE, and Broker failure confused
// with consumer pause. All sends use the actual two deployed hub processes.
func TestFaultMatrix(t *testing.T) {
	if required(t, "STARTUP_E2E_AUTHORIZED") != startupProject || required(t, "COMPOSE_PROJECT_NAME") != startupProject {
		t.Fatal("authorized isolated deployment required")
	}
	startupHealth(t)
	hub := startupDB(t, "hub", "MYSQL_PASSWORD", "partner_hub")
	a := startupDB(t, "mock_p1", "PLATFORM1_DB_PASSWORD", "mock_platform1")
	b := startupDB(t, "mock_p2", "PLATFORM2_DB_PASSWORD", "mock_platform2")
	for _, scenario := range []string{"A-reject-fixed-UID", "A-saved-response-timeout", "A-exhaustion-manual-cycle"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			client, uidA, uidB := "fault-"+model.NewID(), "A-"+model.NewID(), "B-"+model.NewID()
			faultBind(t, ctx, "p1", client, uidA, "BIND")
			faultBind(t, ctx, "p2", client, uidB, "BIND")
			t.Cleanup(func() { faultAMode(t, "accept") })
			mode, first := "reject-busy", "RETRYABLE_FAILURE"
			if scenario == "A-saved-response-timeout" {
				mode, first = "persist-delay", "UNKNOWN"
			}
			faultAMode(t, mode)
			id, response := faultSubmit(t, ctx, client)
			if len(response.PlatformResults) != 2 {
				t.Fatal("partial failure lost complete target set")
			}
			stateA, stateB := faultTask(t, ctx, hub, id, "PLATFORM_A"), faultTask(t, ctx, hub, id, "PLATFORM_B")
			if stateA.UID != uidA || stateA.First != first || stateB.UID != uidB || stateB.First != "SUCCESS" || stateB.Status != "SUCCEEDED" || stateB.Attempts != 1 {
				t.Fatalf("incorrect saved first-round facts A=%+v B=%+v", stateA, stateB)
			}
			eventually(t, ctx, func() bool { return faultReceipts(t, ctx, b, id, uidB) == 1 })
			if scenario == "A-saved-response-timeout" && faultReceipts(t, ctx, a, id, uidA) < 1 {
				t.Fatal("UNKNOWN fixture did not actually save before response timeout")
			}
			if scenario == "A-reject-fixed-UID" {
				if faultReceipts(t, ctx, a, id, uidA) != 0 {
					t.Fatal("processing-before-reject fixture saved a receipt")
				}
				faultBind(t, ctx, "p1", client, uidA, "UNBIND")
				newUID := "new-" + model.NewID()
				faultBind(t, ctx, "p1", client, newUID, "BIND")
				defer func() {
					cleanup, done := context.WithTimeout(context.Background(), 3*time.Second)
					defer done()
					if faultReceipts(t, cleanup, a, id, newUID) != 0 {
						t.Error("retry used the newly rebound UID")
					}
				}()
			}
			if scenario == "A-exhaustion-manual-cycle" {
				eventually(t, ctx, func() bool { return faultTask(t, ctx, hub, id, "PLATFORM_A").Status == "NEEDS_ATTENTION" })
				stateA = faultTask(t, ctx, hub, id, "PLATFORM_A")
				if stateA.Attempts != 5 || stateA.Cycle != 1 || stateA.CycleAttempts != 5 || faultReceipts(t, ctx, a, id, uidA) != 0 {
					t.Fatalf("bounded failure history incorrect %+v", stateA)
				}
			}
			faultAMode(t, "accept")
			if scenario == "A-exhaustion-manual-cycle" {
				stateA = faultTask(t, ctx, hub, id, "PLATFORM_A")
				if stateA.Status != "NEEDS_ATTENTION" || stateA.Attempts != 5 {
					t.Fatal("exhausted task restarted without a manual command")
				}
				operation := model.NewID()
				command := map[string]string{"operationId": operation, "operator": "fault-matrix", "reason": "A endpoint restored"}
				path := required(t, "HUB_TEST_URL") + "/hub/demo/deliveries/" + stateA.ID + "/retry"
				for i := 0; i < 2; i++ {
					code, raw := demoRequest(t, ctx, "POST", path, command)
					var result struct {
						RetryCycle int
						State      string
					}
					if code != 200 || json.Unmarshal(raw, &result) != nil || result.RetryCycle != 2 || result.State != "PENDING" {
						t.Fatalf("manual command replay failed HTTP%d", code)
					}
				}
				command["reason"] = "different command parameters"
				if code, _ := demoRequest(t, ctx, "POST", path, command); code != 409 {
					t.Fatal("same operationId with different parameters did not conflict")
				}
				var count int
				if err := hub.QueryRowContext(ctx, "SELECT COUNT(*) FROM manual_operation WHERE operation_id=? AND target_id=?", operation, stateA.ID).Scan(&count); err != nil || count != 1 {
					t.Fatal("manual operation was not durably unique")
				}
				t.Logf("manual operationId=%s deliveryId=%s replays one cycle; changed parameters HTTP409", operation, stateA.ID)
			}
			eventually(t, ctx, func() bool { return faultTask(t, ctx, hub, id, "PLATFORM_A").Status == "SUCCEEDED" })
			finalA, finalB := faultTask(t, ctx, hub, id, "PLATFORM_A"), faultTask(t, ctx, hub, id, "PLATFORM_B")
			if finalA.ID != stateA.ID || finalA.UID != uidA || finalA.First != first || finalB.Attempts != 1 || faultReceipts(t, ctx, b, id, uidB) != 1 {
				t.Fatal("recovery changed original responsibility or repeated successful B")
			}
			n := faultReceipts(t, ctx, a, id, uidA)
			if n < 1 || scenario == "A-saved-response-timeout" && n < 2 || scenario != "A-saved-response-timeout" && n != 1 {
				t.Fatalf("actual A receipts inconsistent with fault window: %d", n)
			}
			var history int
			if hub.QueryRowContext(ctx, "SELECT COUNT(*) FROM delivery_attempt WHERE delivery_id=?", finalA.ID).Scan(&history) != nil || history != finalA.Attempts {
				t.Fatal("attempt history does not match cumulative count")
			}
			if scenario == "A-exhaustion-manual-cycle" && (finalA.Cycle != 2 || finalA.CycleAttempts != 1 || finalA.Attempts != 6) {
				t.Fatalf("manual retry did not preserve cumulative history %+v", finalA)
			}
			t.Logf("msgId=%s HTTP202 firstA=%s deliveryA=%s fixedUID=%s attempts=%d cycle=%d actualA=%d actualB=1; final SUCCEEDED", id, first, finalA.ID, uidA, finalA.Attempts, finalA.Cycle, n)
		})
		if t.Failed() {
			return
		}
	}
	t.Run("B-Broker-channel-outage", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
		defer cancel()
		client, uidA, uidB := "broker-"+model.NewID(), "A-"+model.NewID(), "B-"+model.NewID()
		faultBind(t, ctx, "p1", client, uidA, "BIND")
		faultBind(t, ctx, "p2", client, uidB, "BIND")
		kafkaPaused := false
		t.Cleanup(func() {
			if kafkaPaused {
				startupCompose(t, "unpause", "kafka")
			}
		})
		startupCompose(t, "pause", "kafka")
		kafkaPaused = true
		id, _ := faultSubmit(t, ctx, client)
		stateA, stateB := faultTask(t, ctx, hub, id, "PLATFORM_A"), faultTask(t, ctx, hub, id, "PLATFORM_B")
		if stateA.First != "SUCCESS" || stateB.First == "SUCCESS" || stateB.UID != uidB || faultReceipts(t, ctx, a, id, uidA) != 1 || faultReceipts(t, ctx, b, id, uidB) != 0 {
			t.Fatal("Broker outage did not preserve partial-success facts")
		}
		var started bool
		var outcome string
		if hub.QueryRowContext(ctx, "SELECT send_started,outcome FROM delivery_attempt WHERE delivery_id=? AND attempt_no=1", stateB.ID).Scan(&started, &outcome) != nil || !started || outcome != "UNKNOWN" {
			t.Fatal("Broker outage fixture did not exercise an actual send with unconfirmed acknowledgement")
		}
		startupCompose(t, "unpause", "kafka")
		kafkaPaused = false
		eventually(t, ctx, func() bool { return faultTask(t, ctx, hub, id, "PLATFORM_B").Status == "SUCCEEDED" })
		eventually(t, ctx, func() bool { return faultReceipts(t, ctx, b, id, uidB) >= 1 })
		if faultTask(t, ctx, hub, id, "PLATFORM_A").Attempts != 1 || faultReceipts(t, ctx, a, id, uidA) != 1 {
			t.Fatal("successful A repeated after Broker recovery")
		}
		t.Logf("msgId=%s HTTP202 firstB=%s recovered deliveryB=%s actualA=1 actualB=%d; UNKNOWN allows real duplicates", id, stateB.First, stateB.ID, faultReceipts(t, ctx, b, id, uidB))
	})
	if t.Failed() {
		return
	}
	t.Run("cache-miss-binding-DB-blocked", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
		defer cancel()
		client, uid := "route-"+model.NewID(), "A-"+model.NewID()
		faultBind(t, ctx, "p1", client, uid, "BIND")
		cache := redisclient.NewClient(&redisclient.Options{Addr: "127.0.0.1:" + required(t, "REDIS_HOST_PORT")})
		defer cache.Close()
		if err := cache.Del(ctx, "binding:v1:{"+client+"}").Err(); err != nil {
			t.Fatal(err)
		}
		admin := startupDB(t, "root", "MYSQL_ROOT_PASSWORD", "partner_hub")
		lock, err := admin.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		defer lock.ExecContext(context.Background(), "UNLOCK TABLES")
		if _, err := lock.ExecContext(ctx, "LOCK TABLES binding_current WRITE"); err != nil {
			t.Fatal(err)
		}
		id, response := faultSubmit(t, ctx, client)
		var route string
		var targets int
		if len(response.PlatformResults) != 0 || hub.QueryRowContext(ctx, "SELECT route_status FROM trade_message WHERE msg_id=?", id).Scan(&route) != nil || route == "NO_ROUTE" || hub.QueryRowContext(ctx, "SELECT COUNT(*) FROM delivery_task WHERE msg_id=?", id).Scan(&targets) != nil || targets != 0 || faultReceipts(t, ctx, a, id, uid) != 0 {
			t.Fatal("failed binding lookup fabricated empty or partial route")
		}
		if _, err := lock.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
			t.Fatal(err)
		}
		eventually(t, ctx, func() bool {
			var status string
			return hub.QueryRowContext(ctx, "SELECT status FROM delivery_task WHERE msg_id=? AND platform_id='PLATFORM_A' AND platform_uid=?", id, uid).Scan(&status) == nil && status == "SUCCEEDED"
		})
		if faultReceipts(t, ctx, a, id, uid) != 1 {
			t.Fatal("route recovery did not send once to the actual bound UID")
		}
		t.Logf("msgId=%s HTTP202 route=%s no targets while DB blocked; unlock -> ROUTED and one real A receipt", id, route)
	})
}
