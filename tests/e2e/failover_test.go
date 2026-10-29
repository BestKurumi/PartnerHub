//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
	"partner-hub/internal/adapter"
	hubmysql "partner-hub/internal/infra/mysql"
	hubredis "partner-hub/internal/infra/redis"
	"partner-hub/internal/ingress"
	"partner-hub/internal/model"
)

func failoverSnapshot(t *testing.T) model.FirstRoundSnapshot {
	t.Helper()
	raw, err := os.ReadFile("../fixtures/trades/normal.json")
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		t.Fatal("fixture invalid")
	}
	id := model.NewID()
	for key, value := range map[string]string{"msgId": id, "clientId": "failover-" + id, "traceId": "failover-" + id} {
		fields[key], _ = json.Marshal(value)
	}
	raw, _ = json.Marshal(fields)
	trade, err := ingress.Decode(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	return model.FirstRoundSnapshot{Trade: trade, Route: model.RouteSnapshot{MsgID: trade.MsgID, Status: "NO_ROUTE", ResolvedAt: now, Targets: []model.RouteTarget{}}, Attempts: []model.FirstAttempt{}, ReceivedAt: now, ArchiveRevision: model.NewID()}
}

func failoverContainer(t *testing.T, service string) string {
	t.Helper()
	cmd := exec.Command("docker", "compose", "--env-file", required(t, "STARTUP_ENV_FILE"), "-p", startupProject, "ps", "-a", "-q", service)
	cmd.Dir = "../.."
	raw, err := cmd.Output()
	if err != nil || strings.TrimSpace(string(raw)) == "" {
		t.Fatal("isolated container unavailable")
	}
	return strings.TrimSpace(string(raw))
}

func failoverHub2Health(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	eventually(t, ctx, func() bool {
		req, _ := http.NewRequestWithContext(ctx, "GET", required(t, "HUB_TEST_URL")+"/readyz", nil)
		resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var body struct {
			InstanceID, Status string
			Checks             map[string]string
		}
		if json.NewDecoder(resp.Body).Decode(&body) != nil || resp.StatusCode != 200 || body.InstanceID != "hub-2" || body.Status != "READY" || len(body.Checks) != 8 {
			return false
		}
		for _, state := range body.Checks {
			if state != "READY" {
				return false
			}
		}
		return true
	})
}

func TestFailover(t *testing.T) {
	if os.Getenv("STARTUP_E2E_AUTHORIZED") != startupProject || os.Getenv("COMPOSE_PROJECT_NAME") != startupProject {
		t.Fatal("isolated failover runner authorization required")
	}
	startupHealth(t)
	db := startupDB(t, "hub", "MYSQL_PASSWORD", "partner_hub")
	p1 := startupDB(t, "mock_p1", "PLATFORM1_DB_PASSWORD", "mock_platform1")
	options := &redisclient.Options{Addr: "127.0.0.1:" + required(t, "REDIS_HOST_PORT"), DB: 0}
	cache := redisclient.NewClient(options)
	defer cache.Close()
	archive := hubredis.NewArchive(options, hubmysql.NewArchiveStore(db))
	defer archive.Close()
	t.Cleanup(func() {
		// Only restore this test's two application containers. No volume deletion.
		for _, service := range []string{"hub-1", "hub-2"} {
			cmd := exec.Command("docker", "compose", "--env-file", os.Getenv("STARTUP_ENV_FILE"), "-p", startupProject, "unpause", service)
			cmd.Dir = "../.."
			_ = cmd.Run()
		}
		startupCompose(t, "up", "-d", "--no-build", "--no-deps", "--wait", "--wait-timeout", "60", "hub-1", "hub-2")
	})
	startupCompose(t, "pause", "hub-1", "hub-2")
	normal := failoverSnapshot(t)
	if err := archive.Stage(context.Background(), normal); err != nil {
		t.Fatal("stage saved normal responsibility failed")
	}
	// A labelled fixture lease prevents hub-1's final flush from archiving this
	// snapshot: the survivor must wait for Redis TIME expiry and claim it itself.
	if err := cache.Eval(context.Background(), `local t=redis.call('TIME');local now=t[1]*1000+math.floor(t[2]/1000);redis.call('HSET',KEYS[1],'owner','fixture-departed-archive','token',ARGV[2],'revision',ARGV[3],'version',1,'expires',now+15000);redis.call('ZADD',KEYS[2],now+15000,ARGV[1]);return 1`, []string{"archive:lease:{" + normal.Trade.MsgID + "}", "archive:pending"}, normal.Trade.MsgID, model.NewID(), normal.ArchiveRevision).Err(); err != nil {
		t.Fatal("prepare labelled archive lease fixture failed")
	}

	abnormal := failoverSnapshot(t)
	target := model.RouteTarget{PlatformID: "PLATFORM_A", PlatformUID: "fixed-" + model.NewID(), DeliveryID: model.NewID()}
	httpAdapter, err := adapter.NewPlatform1("http://127.0.0.1:" + required(t, "PLATFORM1_HTTP_PORT") + "/notifications/trades")
	if err != nil {
		t.Fatal(err)
	}
	trade := abnormal.Trade
	prepared, err := httpAdapter.Prepare(model.TradeNotification{SecCode: trade.SecCode, SecName: trade.SecName, Side: trade.Side, Market: trade.Market, TradeType: trade.TradeType, TraceID: trade.TraceID, ExecTime: trade.ExecTime}, target.PlatformUID)
	if err != nil {
		t.Fatal(err)
	}
	abnormal.Route.Status = "ROUTED"
	abnormal.Route.Targets = []model.RouteTarget{target}
	abnormal.Attempts = []model.FirstAttempt{{Target: target, Request: prepared, Result: model.SendResult{Outcome: model.RetryableFailure, SendStarted: false, StartedAt: abnormal.ReceivedAt, FinishedAt: abnormal.ReceivedAt, Summary: "test fixture explicitly not sent"}}}
	if err = hubmysql.NewHandoffStore(db).Handoff(context.Background(), abnormal); err != nil {
		t.Fatal("save abnormal responsibility failed")
	}
	// Make only this test's task due; ClaimDue is the real database protocol, but
	// the owner is deliberately labelled fixture, never reported as a hub claim.
	if _, err = db.Exec("UPDATE delivery_task SET next_attempt_at=UTC_TIMESTAMP(3) WHERE delivery_id=?", target.DeliveryID); err != nil {
		t.Fatal(err)
	}
	recovery := hubmysql.NewRecoveryStore(db)
	fixtureOwner := "fixture-departed-" + model.NewID()
	claims, err := recovery.ClaimDue(context.Background(), "PLATFORM_A", fixtureOwner, 1)
	if err != nil || len(claims) != 1 || claims[0].Target.DeliveryID != target.DeliveryID {
		t.Fatal("isolated fixture claim failed")
	}
	if err = recovery.SavePrepared(context.Background(), claims[0], prepared); err != nil {
		t.Fatal("fixture prepared metadata failed")
	}
	if cache.Exists(context.Background(), "archive:message:{"+normal.Trade.MsgID+"}").Val() != 1 {
		t.Fatal("normal snapshot was not durably staged before process exit")
	}
	id := failoverContainer(t, "hub-1")
	startupCompose(t, "unpause", "hub-1")
	started := time.Now()
	startupCompose(t, "stop", "--timeout", "30", "hub-1")
	elapsed := time.Since(started)
	raw, err := exec.Command("docker", "inspect", "--format", "{{.State.ExitCode}}", id).Output()
	if err != nil || strings.TrimSpace(string(raw)) != "0" {
		t.Fatal("SIGTERM hub-1 did not exit cleanly with code0")
	}
	if elapsed > 20*time.Second {
		t.Fatalf("SIGTERM exceeded20s: %v", elapsed)
	}
	var stagedCount int
	if db.QueryRow("SELECT COUNT(*) FROM trade_message WHERE msg_id=?", normal.Trade.MsgID).Scan(&stagedCount) != nil || stagedCount != 0 {
		t.Fatal("normal snapshot was archived before survivor resumed; cannot attribute takeover")
	}
	var stagedSnapshot model.FirstRoundSnapshot
	body, readErr := cache.Get(context.Background(), "archive:message:{"+normal.Trade.MsgID+"}").Bytes()
	if readErr != nil || json.Unmarshal(body, &stagedSnapshot) != nil || stagedSnapshot.ArchiveRevision != normal.ArchiveRevision || stagedSnapshot.Trade.MsgID != normal.Trade.MsgID || stagedSnapshot.Route.Status != "NO_ROUTE" {
		t.Fatal("saved full Redis snapshot not intact after hub-1 exit")
	}
	startupCompose(t, "unpause", "hub-2")
	failoverHub2Health(t)
	// Only hub-2 is runnable now. It must archive the saved NO_ROUTE result and
	// wait for the fixture lease before expiry accounting and a new real send.
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	eventually(t, ctx, func() bool {
		var status string
		return db.QueryRowContext(ctx, "SELECT first_status FROM trade_message WHERE msg_id=?", normal.Trade.MsgID).Scan(&status) == nil && status == "NO_ROUTE" && cache.Exists(ctx, "archive:message:{"+normal.Trade.MsgID+"}").Val() == 0
	})
	var earliestOwner string
	if time.Now().Before(claims[0].LeaseDeadline.Add(-time.Second)) {
		if db.QueryRow("SELECT lease_owner FROM delivery_task WHERE delivery_id=?", target.DeliveryID).Scan(&earliestOwner) != nil || earliestOwner != fixtureOwner {
			t.Fatal("survivor reclaimed a still-valid fixture lease")
		}
	}
	eventually(t, ctx, func() bool {
		var status string
		return db.QueryRowContext(ctx, "SELECT status FROM delivery_task WHERE delivery_id=?", target.DeliveryID).Scan(&status) == nil && status == "SUCCEEDED"
	})
	var owner, outcome, uid string
	var count, unknown int
	if db.QueryRow("SELECT lease_owner,outcome FROM delivery_attempt WHERE delivery_id=? AND attempt_no=3", target.DeliveryID).Scan(&owner, &outcome) != nil || !strings.HasPrefix(owner, "hub-2-") || outcome != "SUCCESS" {
		t.Fatal("recovery success was not written by actual surviving hub-2")
	}
	if db.QueryRow("SELECT COUNT(*) FROM delivery_attempt WHERE delivery_id=? AND attempt_no=2 AND lease_owner=? AND outcome='UNKNOWN' AND summary='LEASE_EXPIRED'", target.DeliveryID, fixtureOwner).Scan(&unknown) != nil || unknown != 1 {
		t.Fatal("expired fixture attempt missing immutable UNKNOWN record")
	}
	if db.QueryRow("SELECT platform_uid,attempt_count FROM delivery_task WHERE delivery_id=?", target.DeliveryID).Scan(&uid, &count) != nil || uid != target.PlatformUID || count != 3 {
		t.Fatal("recovery changed original UID or lost history")
	}
	if p1.QueryRow("SELECT COUNT(*) FROM received_notification WHERE trace_id=? AND user_uid=?", abnormal.Trade.TraceID, target.PlatformUID).Scan(&count) != nil || count != 1 {
		t.Fatal("actual platform A receipt does not match the one real recovery send")
	}
	t.Run("survivor-new-input-and-binding-rebalance", TestDemoNormalFlow)
	if t.Failed() {
		return
	}
	t.Logf("SIGTERM hub-1 elapsed=%s exit0; saved normal_msgId=%s archived; recovery_msgId=%s deliveryId=%s owner=%s; fixture lease expires UNKNOWN, one actual external receipt; hub-2 handles fresh inputs and new Kafka bindings", elapsed, normal.Trade.MsgID, abnormal.Trade.MsgID, target.DeliveryID, owner)
	t.Log("fixture owner was not a real stopped-process claim; no worker send was interrupted and no duplicate external receipt was expected; unsaved first-round loss window is not covered or promised")
}
