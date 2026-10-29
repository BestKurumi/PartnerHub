//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"partner-hub/internal/adapter"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/model"
)

// Unlike the fixture leases in TestFailover, this exercise interrupts an actual
// hub worker after the independent platform has durably stored its notification.
func TestFailoverInterruptedWorker(t *testing.T) {
	if os.Getenv("STARTUP_E2E_AUTHORIZED") != startupProject || os.Getenv("COMPOSE_PROJECT_NAME") != startupProject {
		t.Fatal("isolated failover authorization required")
	}
	startupHealth(t)
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	// An isolated temporary override changes only the test project's platform A.
	override := filepath.Join(t.TempDir(), "delay.yaml")
	if err = os.WriteFile(override, []byte("services:\n  mock-platform1:\n    command: [platform1, -listen, ':18081', -mode, persist-delay, -delay, 30s]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		argv := []string{"compose", "--env-file", required(t, "STARTUP_ENV_FILE"), "-p", startupProject, "-f", filepath.Join(root, "docker-compose.yml"), "-f", override}
		cmd := exec.CommandContext(ctx, "docker", append(argv, args...)...)
		cmd.Dir = root
		if err := cmd.Run(); err != nil {
			t.Fatalf("isolated delayed platform operation failed: %v", err)
		}
	}
	t.Cleanup(func() {
		for _, service := range []string{"hub-1", "hub-2"} {
			cmd := exec.Command("docker", "compose", "--env-file", os.Getenv("STARTUP_ENV_FILE"), "-p", startupProject, "unpause", service)
			cmd.Dir = root
			_ = cmd.Run()
		}
		// Base Compose removes the temporary delayed-response command.
		startupCompose(t, "up", "-d", "--no-build", "--no-deps", "--wait", "--wait-timeout", "90", "mock-platform1", "hub-1", "hub-2")
	})
	run("up", "-d", "--no-build", "--no-deps", "--wait", "--wait-timeout", "60", "mock-platform1")
	startupCompose(t, "pause", "hub-2")
	db := startupDB(t, "hub", "MYSQL_PASSWORD", "partner_hub")
	p1 := startupDB(t, "mock_p1", "PLATFORM1_DB_PASSWORD", "mock_platform1")
	snapshot := failoverSnapshot(t)
	target := model.RouteTarget{PlatformID: "PLATFORM_A", PlatformUID: "interrupted-" + model.NewID(), DeliveryID: model.NewID()}
	snapshot.Route.Status = "ROUTED"
	snapshot.Route.Targets = []model.RouteTarget{target}
	ad, err := adapter.NewPlatform1("http://127.0.0.1:" + required(t, "PLATFORM1_HTTP_PORT") + "/notifications/trades")
	if err != nil {
		t.Fatal(err)
	}
	trade := snapshot.Trade
	prepared, err := ad.Prepare(model.TradeNotification{SecCode: trade.SecCode, SecName: trade.SecName, Side: trade.Side, Market: trade.Market, TradeType: trade.TradeType, TraceID: trade.TraceID, ExecTime: trade.ExecTime}, target.PlatformUID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Attempts = []model.FirstAttempt{{Target: target, Request: prepared, Result: model.SendResult{Outcome: model.RetryableFailure, StartedAt: snapshot.ReceivedAt, FinishedAt: snapshot.ReceivedAt, Summary: "test first round explicitly not sent"}}}
	if err = hubmysql.NewHandoffStore(db).Handoff(context.Background(), snapshot); err != nil {
		t.Fatal("handoff fixture failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var departedOwner string
	var attempt int
	eventually(t, ctx, func() bool {
		var status string
		var receipts int
		if db.QueryRowContext(ctx, "SELECT lease_owner,attempt_count,status FROM delivery_task WHERE delivery_id=?", target.DeliveryID).Scan(&departedOwner, &attempt, &status) != nil {
			return false
		}
		if p1.QueryRowContext(ctx, "SELECT COUNT(*) FROM received_notification WHERE trace_id=? AND user_uid=?", trade.TraceID, target.PlatformUID).Scan(&receipts) != nil {
			return false
		}
		return strings.HasPrefix(departedOwner, "hub-1-") && attempt == 2 && status == "IN_FLIGHT" && receipts == 1
	})
	id := failoverContainer(t, "hub-1")
	startupCompose(t, "stop", "--timeout", "0", "hub-1")
	exit, err := exec.Command("docker", "inspect", "--format", "{{.State.ExitCode}}", id).Output()
	if err != nil || strings.TrimSpace(string(exit)) != "137" {
		t.Fatal("actual sending worker was not forcibly killed")
	}
	var outcome string
	if db.QueryRow("SELECT outcome FROM delivery_attempt WHERE delivery_id=? AND attempt_no=2", target.DeliveryID).Scan(&outcome) != nil || outcome != "IN_FLIGHT" {
		t.Fatal("interrupted attempt was already finalized; test missed crash window")
	}
	// Restore A before the successor can send. No HTTP trade is resubmitted.
	startupCompose(t, "up", "-d", "--no-build", "--no-deps", "--wait", "--wait-timeout", "60", "mock-platform1")
	startupCompose(t, "unpause", "hub-2")
	failoverHub2Health(t)
	eventually(t, ctx, func() bool {
		var status string
		return db.QueryRowContext(ctx, "SELECT status FROM delivery_task WHERE delivery_id=?", target.DeliveryID).Scan(&status) == nil && status == "SUCCEEDED"
	})
	var unknown, receipts int
	if db.QueryRow("SELECT COUNT(*) FROM delivery_attempt WHERE delivery_id=? AND attempt_no=2 AND lease_owner=? AND outcome='UNKNOWN' AND summary='LEASE_EXPIRED'", target.DeliveryID, departedOwner).Scan(&unknown) != nil || unknown != 1 {
		t.Fatal("actual interrupted owner missing immutable UNKNOWN")
	}
	var survivor, uid string
	if db.QueryRow("SELECT lease_owner,outcome FROM delivery_attempt WHERE delivery_id=? AND attempt_no=3", target.DeliveryID).Scan(&survivor, &outcome) != nil || !strings.HasPrefix(survivor, "hub-2-") || outcome != "SUCCESS" {
		t.Fatal("actual successor did not complete retry")
	}
	if db.QueryRow("SELECT platform_uid,attempt_count FROM delivery_task WHERE delivery_id=?", target.DeliveryID).Scan(&uid, &attempt) != nil || uid != target.PlatformUID || attempt != 3 {
		t.Fatal("retry changed UID or exceeded expected attempt history")
	}
	if p1.QueryRow("SELECT COUNT(*) FROM received_notification WHERE trace_id=? AND user_uid=?", trade.TraceID, target.PlatformUID).Scan(&receipts) != nil || receipts != 2 {
		t.Fatalf("expected two actual receipts after confirmation-loss retry; count=%d", receipts)
	}
	// Query is the observable database fact; no runtime cache is substituted.
	code, body := demoRequest(t, ctx, http.MethodGet, required(t, "HUB_TEST_URL")+"/api/v1/messages/"+trade.MsgID, nil)
	var message struct{ MsgID string }
	if code != 200 || json.Unmarshal(body, &message) != nil || message.MsgID != trade.MsgID {
		t.Fatal("survivor database query unavailable")
	}
	t.Logf("actual owner=%s killed exit137 after durable platform receipt; successor=%s UNKNOWN attempt2 → SUCCESS attempt3; receipts=2 same trace/UID; msgId=%s deliveryId=%s", departedOwner, survivor, trade.MsgID, target.DeliveryID)
}
