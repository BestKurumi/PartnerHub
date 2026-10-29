//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/twmb/franz-go/pkg/kgo"
	"io"
	"net/http"
	"os"
	hubkafka "partner-hub/internal/infra/kafka"
	"partner-hub/internal/model"
	"strings"
	"testing"
	"time"
)

func setPlatform2Paused(t *testing.T, paused bool) {
	t.Helper()
	body, _ := json.Marshal(map[string]bool{"paused": paused})
	request, err := http.NewRequest("POST", required(t, "PLATFORM2_TEST_URL")+"/mock/p2/demo/consumer", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Demo-Token", required(t, "DEMO_TOKEN"))
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("consumer control: HTTP%d %s", response.StatusCode, data)
	}
}

func TestDualPlatform(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	db := openDB(t, "HUB_TEST_DSN")
	p1 := openDB(t, "PLATFORM1_TEST_DSN")
	p2 := openDB(t, "PLATFORM2_TEST_DSN")
	id := model.NewID()
	clientID := "dual-" + id
	uidA := "A-" + id
	uidB := "B-" + id
	trace := "trace-" + id
	producer, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(required(t, "KAFKA_TEST_BROKERS"), ",")...))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	for platform, uid := range map[string]string{"PLATFORM_A": uidA, "PLATFORM_B": uidB} {
		event := model.BindingEvent{SchemaVersion: 1, EventID: model.NewID(), EventType: "BIND", PlatformID: platform, ClientID: clientID, PlatformUID: uid, OccurredAt: time.Now().UnixMilli()}
		body, _ := json.Marshal(event)
		if err = producer.ProduceSync(ctx, &kgo.Record{Topic: "customer.binding.events", Key: hubkafka.BindingKey(clientID, platform), Value: body}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, ctx, func() bool {
		var n int
		return db.QueryRowContext(ctx, "SELECT COUNT(*) FROM binding_current WHERE client_id=? AND status='BOUND'", clientID).Scan(&n) == nil && n == 2
	})
	setPlatform2Paused(t, true)
	defer setPlatform2Paused(t, false)
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
	body, _ := json.Marshal(trade)
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
	data, _ := io.ReadAll(io.LimitReader(response.Body, 8193))
	var result model.TradeResponse
	if json.Unmarshal(data, &result) != nil || response.StatusCode != 200 || result.Status != "SUCCEEDED" || result.Acceptance != "COMPLETED" || len(result.PlatformResults) != 2 {
		t.Fatalf("dual completion: HTTP%d %s", response.StatusCode, data)
	}
	var n int
	var receiptA, receiptB string
	if err = p1.QueryRowContext(ctx, "SELECT receive_id FROM received_notification WHERE trace_id=? AND user_uid=?", trace, uidA).Scan(&receiptA); err != nil {
		t.Fatal(err)
	}
	if err = p2.QueryRowContext(ctx, "SELECT COUNT(*) FROM received_notification WHERE trace_id=?", trace).Scan(&n); err != nil || n != 0 {
		t.Fatalf("paused B fabricated receipt count=%d %v", n, err)
	}
	setPlatform2Paused(t, false)
	var payloadA, payloadB []byte
	eventually(t, ctx, func() bool {
		return p2.QueryRowContext(ctx, "SELECT receive_id,payload FROM received_notification WHERE trace_id=? AND user_uid=?", trace, uidB).Scan(&receiptB, &payloadB) == nil
	})
	if err = p1.QueryRowContext(ctx, "SELECT payload FROM received_notification WHERE receive_id=?", receiptA).Scan(&payloadA); err != nil {
		t.Fatal(err)
	}
	var fieldsA, fieldsB map[string]json.RawMessage
	if json.Unmarshal(payloadA, &fieldsA) != nil || json.Unmarshal(payloadB, &fieldsB) != nil || len(fieldsA) != 8 || len(fieldsB) != 8 {
		t.Fatalf("external fields A=%s B=%s", payloadA, payloadB)
	}
	var timeA, timeB string
	_ = json.Unmarshal(fieldsA["execTime"], &timeA)
	_ = json.Unmarshal(fieldsB["tradeDateTime"], &timeB)
	if len(timeA) != 19 || len(timeB) != 23 || !strings.HasPrefix(timeB, timeA) {
		t.Fatalf("time mapping A=%s B=%s", timeA, timeB)
	}
	eventually(t, ctx, func() bool {
		return db.QueryRowContext(ctx, "SELECT COUNT(*) FROM delivery_task WHERE msg_id=? AND status='SUCCEEDED'", id).Scan(&n) == nil && n == 2
	})
	for _, platform := range result.PlatformResults {
		if platform.FirstOutcome != model.Success || platform.CurrentState != "SUCCEEDED" {
			t.Fatalf("platform result=%+v", platform)
		}
	}
	t.Logf("msgId=%s A_receiveId=%s A_uid=%s B_receiveId=%s B_uid=%s broker success observed before B consumption", id, receiptA, uidA, receiptB, uidB)
}
