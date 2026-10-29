//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/ingress"
	"partner-hub/internal/model"
)

func archiveSnapshot(t *testing.T) model.FirstRoundSnapshot {
	t.Helper()
	body, err := os.ReadFile("../../tests/fixtures/trades/normal.json")
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(body, &fields); err != nil {
		t.Fatal(err)
	}
	fields["msgId"], _ = json.Marshal(model.NewID())
	body, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	trade, err := ingress.Decode(strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	target := model.RouteTarget{PlatformID: "PLATFORM_A", PlatformUID: "fixed-before-unbind", DeliveryID: model.NewID()}
	now := time.Now().UTC().Truncate(time.Millisecond)
	return model.FirstRoundSnapshot{Trade: trade, Route: model.RouteSnapshot{MsgID: trade.MsgID, Status: "ROUTED", ResolvedAt: now, Targets: []model.RouteTarget{target}}, Attempts: []model.FirstAttempt{{Target: target, Request: model.PreparedRequest{PlatformID: "PLATFORM_A", ProtocolVersion: "platform1.v1", Payload: []byte(`{"userUid":"fixed-before-unbind"}`)}, Result: model.SendResult{Outcome: model.Success, SendStarted: true, StartedAt: now, FinishedAt: now, Summary: "HTTP200 code0"}}}, ReceivedAt: now, ArchiveRevision: model.NewID()}
}

type failingArchiveStore struct{}

func (failingArchiveStore) Save(context.Context, []model.FirstRoundSnapshot) error {
	return errors.New("DB unavailable")
}

// Stage must save the whole snapshot and pending index atomically, without TTL;
// failed Flush keeps responsibility and successful replay never duplicates rows.
func TestFirstArchiveDurability(t *testing.T) {
	db := recoveryDB(t)
	var err error
	client := bindingRedis(t)
	ctx := context.Background()
	store := hubmysql.NewArchiveStore(db)
	namespace := leaseNamespace(t, client)
	archive := leaseArchive(t, client, namespace, store)
	t.Cleanup(func() { archive.Close() })
	if err = archive.Preflight(ctx); err != nil {
		t.Fatalf("capacity preflight: %v", err)
	}
	snapshot := archiveSnapshot(t)
	id := snapshot.Trade.MsgID
	key := namespace + ":archive:message:{" + id + "}"
	if err = archive.Stage(ctx, snapshot); err != nil {
		t.Fatalf("Stage: %v", err)
	}
	scoreErr := client.ZScore(ctx, namespace+":archive:pending", id).Err()
	ttl, ttlErr := client.PTTL(ctx, key).Result()
	// go-redis preserves Redis -1/-2 sentinels as nanoseconds, not milliseconds.
	if scoreErr != nil || ttlErr != nil || ttl != -time.Nanosecond {
		t.Fatalf("snapshot/index missing or TTL present: score=%v ttl=%v err=%v", scoreErr, ttl, ttlErr)
	}
	var recovered model.FirstRoundSnapshot
	if err = json.Unmarshal([]byte(client.Get(ctx, key).Val()), &recovered); err != nil || recovered.ArchiveRevision != snapshot.ArchiveRevision || len(recovered.Attempts) != 1 || recovered.Attempts[0].Target.PlatformUID != "fixed-before-unbind" {
		t.Fatalf("incomplete Stage: %+v %v", recovered, err)
	}
	if err = archive.Stage(ctx, snapshot); err != nil {
		t.Fatalf("same Stage confirmation: %v", err)
	}
	changed := snapshot
	changed.ArchiveRevision = model.NewID()
	if err = archive.Stage(ctx, changed); err == nil {
		t.Fatal("different snapshot overwrote pending responsibility")
	}
	failing := leaseArchive(t, client, namespace, failingArchiveStore{})
	t.Cleanup(func() { failing.Close() })
	if err = failing.Flush(ctx); err == nil || client.Exists(ctx, key).Val() != 1 {
		t.Fatal("DB failure lost pending snapshot")
	}
	if err = archive.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if client.Exists(ctx, key).Val() != 0 || client.ZScore(ctx, namespace+":archive:pending", id).Err() == nil {
		t.Fatal("confirmed archive not acknowledged")
	}
	for _, table := range []string{"trade_message", "delivery_task", "delivery_attempt"} {
		var n int
		column := "msg_id"
		value := id
		if table == "delivery_attempt" {
			column = "delivery_id"
			value = snapshot.Route.Targets[0].DeliveryID
		}
		if err = db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE "+column+"=?", value).Scan(&n); err != nil || n != 1 {
			t.Fatalf("%s incomplete: n=%d %v", table, n, err)
		}
	}
	if err = archive.Stage(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if err = archive.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	var uid, state, outcome, version string
	if err = db.QueryRow("SELECT platform_uid,status,first_outcome,protocol_version FROM delivery_task WHERE delivery_id=?", snapshot.Route.Targets[0].DeliveryID).Scan(&uid, &state, &outcome, &version); err != nil || uid != "fixed-before-unbind" || state != "SUCCEEDED" || outcome != "SUCCESS" || version != "platform1.v1" {
		t.Fatalf("stored fixed route/result: %s %s %s %s %v", uid, state, outcome, version, err)
	}
}
