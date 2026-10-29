//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"partner-hub/internal/mock/platform1"
)

// Catches acknowledging a non-durable write, unintended business dedup and schema leakage.
func TestPlatform1Store(t *testing.T) {
	dsn := os.Getenv("PLATFORM1_TEST_DSN")
	if dsn == "" {
		t.Fatal("PLATFORM1_TEST_DSN is required; start isolated test MySQL")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	ddl, err := os.ReadFile("../../deploy/mysql/mock-platform1/001_received.sql")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err = db.ExecContext(ctx, string(ddl)); err != nil {
			t.Fatal(err)
		}
	}
	store := platform1.NewMySQLStore(db)
	r1 := platform1.Receipt{ReceiveID: "d34e78a0-d5ad-4a10-b183-6698632b0071", UserUID: "00009", TraceID: "trace-same", Payload: []byte(`{"userUid":"00009","securityCode":"000001","securityName":"平安银行","direction":"BUY","execTime":"2026-10-22 09:30:00","market":"SZ","tradeType":"TRADE_NORMAL","traceId":"trace-same"}`)}
	r2 := r1
	r2.ReceiveID = "d34e78a0-d5ad-4a10-b183-6698632b0072"
	// Clean only this test's fixed identities; preserve all other receipts and volumes.
	_, err = db.ExecContext(ctx, "DELETE FROM received_notification WHERE receive_id IN (?,?)", r1.ReceiveID, r2.ReceiveID)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []platform1.Receipt{r1, r2} {
		if err = store.Save(ctx, r); err != nil {
			t.Fatalf("save: %v", err)
		}
	}
	// A fresh independent connection must see committed exact UTF-8 bytes.
	reader, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, r := range []platform1.Receipt{r1, r2} {
		var body []byte
		var uid, trace string
		if err = reader.QueryRowContext(ctx, "SELECT payload,user_uid,trace_id FROM received_notification WHERE receive_id=?", r.ReceiveID).Scan(&body, &uid, &trace); err != nil || string(body) != string(r.Payload) || uid != "00009" || trace != "trace-same" {
			t.Fatalf("read committed: body=%s uid=%s trace=%s err=%v", body, uid, trace, err)
		}
	}
	if err = store.Save(ctx, r1); err == nil {
		t.Fatal("primary key storage failure hidden")
	}
	if _, err = reader.ExecContext(ctx, "SELECT * FROM mysql.user"); err == nil {
		t.Fatal("mock account can read server identities")
	}
	if _, err = reader.ExecContext(ctx, "CREATE DATABASE mock_p1_must_not_create"); err == nil {
		t.Fatal("mock account can create unrelated schema")
	}
	closed, _ := sql.Open("mysql", dsn)
	closed.Close()
	if err = platform1.NewMySQLStore(closed).Save(ctx, r1); err == nil {
		t.Fatal("closed DB accepted receipt")
	}
	if t.Failed() {
		return
	}
	t.Logf("two independently committed receipts for same trace/UID: %s, %s; isolated account verified", r1.ReceiveID, r2.ReceiveID)
}

// Catches HTTP ACK preceding actual durable storage and timeout being mistaken
// for nonreceipt. This uses the independent MySQL account, never a memory store.
func TestPlatform1StoredHTTPResponse(t *testing.T) {
	dsn := os.Getenv("PLATFORM1_TEST_DSN")
	if dsn == "" {
		t.Fatal("PLATFORM1_TEST_DSN required")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ddl, err := os.ReadFile("../../deploy/mysql/mock-platform1/001_received.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, string(ddl)); err != nil {
		t.Fatal(err)
	}
	const body = `{"userUid":"00009","securityCode":"000001","securityName":"平安银行","direction":"BUY","execTime":"2026-10-22 09:30:00","market":"SZ","tradeType":"TRADE_NORMAL","traceId":"trace-integration-http"}`
	for _, mode := range []platform1.Mode{platform1.Accept, platform1.RejectBusy, platform1.PersistDelay, platform1.PersistDisconnect} {
		t.Run(string(mode), func(t *testing.T) {
			trace := "trace-integration-http-" + string(mode)
			payload := strings.Replace(body, "trace-integration-http", trace, 1)
			if _, err = db.ExecContext(ctx, "DELETE FROM received_notification WHERE trace_id=?", trace); err != nil {
				t.Fatal(err)
			}
			h, err := platform1.NewHandler(platform1.NewMySQLStore(db), mode, 3*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(h)
			defer server.Close()
			request, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/notifications/trades", strings.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			client := &http.Client{Timeout: time.Second}
			response, sendErr := client.Do(request)
			var receiveID string
			if mode == platform1.Accept || mode == platform1.RejectBusy {
				if sendErr != nil {
					t.Fatal(sendErr)
				}
				defer response.Body.Close()
				responseBody, _ := io.ReadAll(response.Body)
				if mode == platform1.Accept {
					var ack struct {
						Code      int
						ReceiveID string
					}
					if json.Unmarshal(responseBody, &ack) != nil || response.StatusCode != 200 || ack.Code != 0 || ack.ReceiveID == "" {
						t.Fatalf("invalid ack: %d %s", response.StatusCode, responseBody)
					}
					receiveID = ack.ReceiveID
				} else if response.StatusCode != 429 {
					t.Fatalf("busy response: %d", response.StatusCode)
				}
			} else if sendErr == nil {
				response.Body.Close()
				t.Fatalf("expected post-save transport fault, got HTTP %d", response.StatusCode)
			}
			var count int
			if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM received_notification WHERE trace_id=?", trace).Scan(&count); err != nil {
				t.Fatal(err)
			}
			want := 1
			if mode == platform1.RejectBusy {
				want = 0
			}
			if count != want {
				t.Fatalf("durable receipts=%d, want%d", count, want)
			}
			if count == 1 {
				var stored []byte
				var id string
				if err = db.QueryRowContext(ctx, "SELECT receive_id,payload FROM received_notification WHERE trace_id=?", trace).Scan(&id, &stored); err != nil || string(stored) != payload || (receiveID != "" && id != receiveID) {
					t.Fatalf("durable wire receipt mismatch id=%s err=%v", id, err)
				}
				t.Logf("mode=%s receiveId=%s durable=true confirmed=%t", mode, id, receiveID != "")
			}
		})
	}
}
