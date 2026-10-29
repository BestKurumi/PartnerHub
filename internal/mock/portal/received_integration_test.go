//go:build integration

package portal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kgo"
	"partner-hub/internal/model"
)

// Catches using Hub/a different DB, incorrect UID/trace filters, and timestamp-only cursors.
func TestPortalReceivedIsolationAndCursor(t *testing.T) {
	for i, variable := range []string{"PLATFORM1_TEST_DSN", "PLATFORM2_TEST_DSN"} {
		t.Run(variable, func(t *testing.T) {
			dsn := os.Getenv(variable)
			if dsn == "" {
				t.Fatal(variable + " required")
			}
			db, err := sql.Open("mysql", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			uid := "portal-" + model.NewID()
			trace := "trace-" + model.NewID()
			now := "2026-10-22 12:00:00.123"
			for n := 1; n <= 4; n++ {
				id := fmt.Sprintf("00000000-0000-4000-8000-%012d", n)
				user := uid
				if n == 4 {
					user += "-other"
				}
				_, err = db.Exec("INSERT INTO received_notification (receive_id,user_uid,trace_id,payload,received_at"+map[bool]string{true: ",source_topic,source_partition,source_offset", false: ""}[i == 1]+") VALUES (?,?,?,?,?"+map[bool]string{true: ",?,0,?", false: ""}[i == 1]+")", func() []any {
					a := []any{id, user, trace, []byte(`{"userUid":"<script>alert(1)</script>","traceId":"original"}`), now}
					if i == 1 {
						a = append(a, uid, int64(n))
					}
					return a
				}()...)
				if err != nil {
					t.Fatal(err)
				}
			}
			defer db.Exec("DELETE FROM received_notification WHERE trace_id=?", trace)
			cl, _ := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
			defer cl.Close()
			p := []string{"PLATFORM_A", "PLATFORM_B"}[i]
			h, err := NewHandler(Config{p, "local-token", "http://127.0.0.1:1"}, db, cl)
			if err != nil {
				t.Fatal(err)
			}
			path := "/demo/received?uid=" + uid + "&traceId=" + trace + "&limit=2"
			w := request(h, "GET", path, "")
			if w.Code != 200 {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			var page struct {
				Items []struct {
					ReceiveID string          `json:"receiveId"`
					Payload   json.RawMessage `json:"payload"`
					UserUID   string          `json:"userUid"`
				}
				NextCursor string `json:"nextCursor"`
			}
			if err = json.Unmarshal(w.Body.Bytes(), &page); err != nil {
				t.Fatal(err)
			}
			if len(page.Items) != 2 || page.Items[0].ReceiveID != "00000000-0000-4000-8000-000000000003" || page.Items[1].ReceiveID != "00000000-0000-4000-8000-000000000002" || page.NextCursor == "" {
				t.Fatalf("wrong page %+v", page)
			}
			if !strings.Contains(string(page.Items[0].Payload), "script") || page.Items[0].UserUID != uid {
				t.Fatalf("lost actual payload: %+v", page.Items[0])
			}
			w = request(h, "GET", path+"&cursor="+page.NextCursor, "")
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if err = json.Unmarshal(w.Body.Bytes(), &page); err != nil {
				t.Fatal(err)
			}
			if len(page.Items) != 1 || page.Items[0].ReceiveID != "00000000-0000-4000-8000-000000000001" || page.NextCursor != "" {
				t.Fatalf("wrong second page %+v", page)
			}
			w = request(h, "GET", "/demo/received?uid=missing-"+uid, "")
			if w.Code != 200 || !strings.Contains(w.Body.String(), `"items":[]`) {
				t.Fatalf("empty=%d %s", w.Code, w.Body.String())
			}
		})
	}
}

// Catches accepting a publish as effective binding, spoofed platform/key, missing fields or lost UUID.
func TestPortalBindingBrokerAcknowledgement(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKERS")
	if broker == "" {
		t.Fatal("KAFKA_TEST_BROKERS required")
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	consumer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumeTopics("customer.binding.events"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	h, err := NewHandler(Config{"PLATFORM_B", "local-token", "http://127.0.0.1:1"}, &sql.DB{}, cl)
	if err != nil {
		t.Fatal(err)
	}
	uid := "portal-" + model.NewID()
	w := request(h, "POST", "/demo/binding-events", `{"eventType":"BIND","clientId":"`+uid+`","platformUid":"00009"}`)
	if w.Code != 202 {
		t.Fatalf("publish status=%d body=%s", w.Code, w.Body.String())
	}
	var result struct{ EventID, Status string }
	if err = json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.EventID == "" || result.Status != "PUBLISHED" {
		t.Fatalf("not broker-only acknowledgment %+v", result)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		fetches := consumer.PollRecords(ctx, 100)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatal(errs)
		}
		for _, r := range fetches.Records() {
			var fields map[string]json.RawMessage
			json.Unmarshal(r.Value, &fields)
			var e model.BindingEvent
			json.Unmarshal(r.Value, &e)
			if e.EventID != result.EventID {
				continue
			}
			if len(fields) != 7 || e.SchemaVersion != 1 || e.PlatformID != "PLATFORM_B" || e.ClientID != uid || e.PlatformUID != "00009" || e.EventType != "BIND" || e.OccurredAt <= 0 || string(r.Key) != `["`+uid+`","PLATFORM_B"]` {
				t.Fatalf("bad event/key: %s %s", r.Key, r.Value)
			}
			return
		}
	}
	t.Fatal("published event not found")
}
