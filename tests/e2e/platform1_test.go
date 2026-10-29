//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kgo"
	hubkafka "partner-hub/internal/infra/kafka"
	"partner-hub/internal/model"
)

func required(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Fatalf("%s required; use make demo-p1", key)
	}
	return v
}
func openDB(t *testing.T, key string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", required(t, key))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
func eventually(t *testing.T, ctx context.Context, check func() bool) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if check() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("expected durable state not observed within budget")
		case <-ticker.C:
		}
	}
}

// Uses the running hub process and independent Mock process, never the hub's
// in-memory response as evidence of an external receipt or durable archive.
func TestPlatform1Flow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	db := openDB(t, "HUB_TEST_DSN")
	receiver := openDB(t, "PLATFORM1_TEST_DSN")
	id := model.NewID()
	clientID := "e2e-" + id
	uid := "uid-" + id
	trace := "trace-" + id
	producer, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(required(t, "KAFKA_TEST_BROKERS"), ",")...))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	event := model.BindingEvent{SchemaVersion: 1, EventID: model.NewID(), EventType: "BIND", PlatformID: "PLATFORM_A", ClientID: clientID, PlatformUID: uid, OccurredAt: time.Now().UnixMilli()}
	// Position is internal transport metadata and is absent from the event JSON.
	body, err := json.Marshal(map[string]any{"schemaVersion": event.SchemaVersion, "eventId": event.EventID, "eventType": event.EventType, "platformId": event.PlatformID, "clientId": event.ClientID, "platformUid": event.PlatformUID, "occurredAt": event.OccurredAt})
	if err != nil {
		t.Fatal(err)
	}
	if err = producer.ProduceSync(ctx, &kgo.Record{Topic: "customer.binding.events", Key: hubkafka.BindingKey(clientID, "PLATFORM_A"), Value: body}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	eventually(t, ctx, func() bool {
		var got string
		return db.QueryRowContext(ctx, "SELECT platform_uid FROM binding_current WHERE client_id=? AND platform_id='PLATFORM_A' AND status='BOUND'", clientID).Scan(&got) == nil && got == uid
	})
	fixture, err := os.ReadFile("../fixtures/trades/normal.json")
	if err != nil {
		t.Fatal(err)
	}
	var trade map[string]json.RawMessage
	if err = json.Unmarshal(fixture, &trade); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{"msgId": id, "clientId": clientID, "traceId": trace} {
		trade[key], _ = json.Marshal(value)
	}
	body, err = json.Marshal(trade)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, "POST", required(t, "HUB_TEST_URL")+"/api/v1/trades", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 8193))
	if err != nil {
		t.Fatal(err)
	}
	var result model.TradeResponse
	if json.Unmarshal(data, &result) != nil || response.StatusCode != 200 || result.MsgID != id || result.Status != "SUCCEEDED" || result.Acceptance != "COMPLETED" || result.RecordStatus != "PENDING" || len(result.PlatformResults) != 1 || result.PlatformResults[0].CurrentState != "SUCCEEDED" {
		t.Fatalf("false/missing completion: HTTP %d %s", response.StatusCode, data)
	}
	var receiptID, gotUID string
	var payload []byte
	var count int
	eventually(t, ctx, func() bool {
		return receiver.QueryRowContext(ctx, "SELECT receive_id,user_uid,payload FROM received_notification WHERE trace_id=?", trace).Scan(&receiptID, &gotUID, &payload) == nil
	})
	var fields map[string]json.RawMessage
	if gotUID != uid || json.Unmarshal(payload, &fields) != nil || len(fields) != 8 {
		t.Fatalf("incorrect receipt: uid=%s fields=%s", gotUID, payload)
	}
	for _, field := range []string{"userUid", "securityCode", "securityName", "direction", "execTime", "market", "tradeType", "traceId"} {
		if _, ok := fields[field]; !ok {
			t.Fatalf("missing external field %s", field)
		}
	}
	var status, storedUID, outcome string
	var raw []byte
	eventually(t, ctx, func() bool {
		return db.QueryRowContext(ctx, "SELECT m.first_status,m.raw_payload,d.platform_uid,d.first_outcome FROM trade_message m JOIN delivery_task d ON d.msg_id=m.msg_id WHERE m.msg_id=? AND d.delivery_id=?", id, result.PlatformResults[0].DeliveryID).Scan(&status, &raw, &storedUID, &outcome) == nil
	})
	if status != "SUCCEEDED" || storedUID != uid || outcome != "SUCCESS" || !bytes.Equal(raw, body) {
		t.Fatalf("incomplete archive: status=%s uid=%s outcome=%s", status, storedUID, outcome)
	}
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM delivery_attempt WHERE delivery_id=? AND attempt_no=1 AND outcome='SUCCESS' AND send_started=1", result.PlatformResults[0].DeliveryID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("attempt count=%d err=%v", count, err)
	}
	if err = receiver.QueryRowContext(ctx, "SELECT COUNT(*) FROM received_notification WHERE trace_id=?", trace).Scan(&count); err != nil || count != 1 {
		t.Fatalf("receipt count=%d err=%v", count, err)
	}
	fmt.Printf("msgId=%s receiveId=%s deliveryId=%s archive=%s firstOutcome=%s\n", id, receiptID, result.PlatformResults[0].DeliveryID, status, outcome)
}
