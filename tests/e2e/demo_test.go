//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"partner-hub/internal/model"
	"testing"
	"time"
)

func demoRequest(t *testing.T, ctx context.Context, method, path string, body any) (int, []byte) {
	t.Helper()
	var data []byte
	if body != nil {
		data, _ = json.Marshal(body)
	}
	r, err := http.NewRequestWithContext(ctx, method, path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Demo-Token", required(t, "DEMO_TOKEN"))
	r.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 35 * time.Second}).Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, data
}
func TestDemoNormalFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	hub := required(t, "HUB_TEST_URL")
	p1 := required(t, "PLATFORM1_TEST_URL") + "/mock/p1/"
	p2 := required(t, "PLATFORM2_TEST_URL") + "/mock/p2/"
	up := required(t, "UPSTREAM_TEST_URL") + "/mock/upstream/"
	for _, page := range []string{hub + "/hub/", p1, p2, up} {
		code, body := demoRequest(t, ctx, "GET", page, nil)
		if code != 200 || !bytes.Contains(bytes.ToLower(body), []byte("<!doctype html>")) {
			t.Fatalf("page %s HTTP%d %s", page, code, body)
		}
	}
	client := "demo-" + model.NewID()
	uidA, uidB := "A-"+model.NewID(), "B-"+model.NewID()
	bind := func(base, kind, uid string) {
		t.Helper()
		code, body := demoRequest(t, ctx, "POST", base+"demo/binding-events", map[string]any{"eventType": kind, "clientId": client, "platformUid": uid})
		var published struct{ EventID, Status string }
		if code != 202 || json.Unmarshal(body, &published) != nil || published.Status != "PUBLISHED" {
			t.Fatalf("publish HTTP%d %s", code, body)
		}
		eventually(t, ctx, func() bool {
			code, body := demoRequest(t, ctx, "GET", base+"demo/binding-events/"+published.EventID, nil)
			var event struct{ Result string }
			return code == 200 && json.Unmarshal(body, &event) == nil && event.Result == "APPLIED"
		})
	}
	bind(p1, "BIND", uidA)
	bind(p2, "BIND", uidB)
	type fact struct {
		MsgID, Status, RecordStatus, TraceID string
		PlatformResults                      []struct{ PlatformID, PlatformUID, FirstOutcome, CurrentState, DeliveryID string }
	}
	send := func() fact {
		t.Helper()
		code, body := demoRequest(t, ctx, "POST", up+"demo/trades", map[string]any{"clientId": client, "tradeType": "ETF", "count": 1, "qps": 1})
		var result struct {
			Items []struct {
				MsgID, Acceptance string
				HTTPStatus        int
			}
		}
		if code != 200 || json.Unmarshal(body, &result) != nil || len(result.Items) != 1 || result.Items[0].Acceptance != "COMPLETED" || result.Items[0].HTTPStatus != 200 {
			t.Fatalf("upstream HTTP%d %s", code, body)
		}
		var message fact
		eventually(t, ctx, func() bool {
			code, body := demoRequest(t, ctx, "GET", up+"demo/messages/"+result.Items[0].MsgID, nil)
			return code == 200 && json.Unmarshal(body, &message) == nil && message.RecordStatus == "PERSISTED"
		})
		return message
	}
	received := func(base, uid, trace string) []map[string]any {
		t.Helper()
		code, body := demoRequest(t, ctx, "GET", base+"demo/received?uid="+url.QueryEscape(uid)+"&traceId="+url.QueryEscape(trace), nil)
		var page struct{ Items []map[string]any }
		if code != 200 || json.Unmarshal(body, &page) != nil {
			t.Fatalf("receipts HTTP%d %s", code, body)
		}
		return page.Items
	}
	first := send()
	if first.Status != "SUCCEEDED" || len(first.PlatformResults) != 2 {
		t.Fatalf("dual persisted facts: %+v", first)
	}
	for _, p := range []struct{ base, uid string }{{p1, uidA}, {p2, uidB}} {
		eventually(t, ctx, func() bool { return len(received(p.base, p.uid, first.TraceID)) == 1 })
		items := received(p.base, p.uid, first.TraceID)
		payload, ok := items[0]["payload"].(map[string]any)
		if !ok || len(payload) != 8 {
			t.Fatalf("receipt whitelist %v", items)
		}
		if len(received(p.base, "another-uid", first.TraceID)) != 0 {
			t.Fatal("UID filter leaked receipt")
		}
	}
	code, body := demoRequest(t, ctx, "GET", hub+"/hub/demo/messages/"+first.MsgID, nil)
	if code != 200 || !bytes.Contains(body, []byte(uidA)) || !bytes.Contains(body, []byte(uidB)) {
		t.Fatalf("hub page query HTTP%d %s", code, body)
	}
	bind(p1, "UNBIND", uidA)
	second := send()
	if second.Status != "SUCCEEDED" || len(second.PlatformResults) != 1 || second.PlatformResults[0].PlatformID != "PLATFORM_B" {
		t.Fatalf("unbound A got new target %+v", second)
	}
	eventually(t, ctx, func() bool { return len(received(p2, uidB, second.TraceID)) == 1 })
	if len(received(p1, uidA, second.TraceID)) != 0 {
		t.Fatal("unbound A received new trade")
	}
	bind(p2, "UNBIND", uidB)
	empty := send()
	if empty.Status != "NO_ROUTE" || len(empty.PlatformResults) != 0 {
		t.Fatalf("empty route %+v", empty)
	}
	bind(p1, "BIND", uidA)
	code, body = demoRequest(t, ctx, "GET", hub+"/api/v1/messages/"+empty.MsgID, nil)
	var unchanged fact
	if code != 200 || json.Unmarshal(body, &unchanged) != nil || unchanged.Status != "NO_ROUTE" || len(unchanged.PlatformResults) != 0 || len(received(p1, uidA, empty.TraceID)) != 0 {
		t.Fatalf("NO_ROUTE changed after bind HTTP%d %s", code, body)
	}
	t.Logf("dual_msgId=%s traceId=%s A_uid=%s B_uid=%s after_unbind_msgId=%s no_route_msgId=%s", first.MsgID, first.TraceID, uidA, uidB, second.MsgID, empty.MsgID)
}
