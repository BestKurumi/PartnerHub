//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"net/url"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/ingress"
	"partner-hub/internal/model"
	"strings"
	"testing"
	"time"
)

func TestConsoleQueryFiltersBeforeLimitAndCursorScope(t *testing.T) {
	db := bindingDB(t)
	ensureDeliverySchema(t, db)
	client, trace := "console-"+model.NewID(), "trace-'literal-"+model.NewID()
	var wanted []string
	snapshots := map[string]model.FirstRoundSnapshot{}
	// Newer nonmatching records must not consume a filtered page.
	for i, state := range []string{"SUCCEEDED", "SUCCEEDED", "RECOVERY_PENDING", "NEEDS_ATTENTION", "NO_ROUTE", "RECOVERY_PENDING"} {
		s := archiveSnapshot(t)
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(s.Trade.RawPayload, &fields); err != nil {
			t.Fatal(err)
		}
		fields["clientId"], _ = json.Marshal(client)
		fields["traceId"], _ = json.Marshal(trace)
		raw, _ := json.Marshal(fields)
		var err error
		s.Trade, err = ingress.Decode(strings.NewReader(string(raw)))
		if err != nil {
			t.Fatal(err)
		}
		s.ReceivedAt = time.Now().UTC().Truncate(time.Millisecond).Add(time.Duration(i) * time.Second)
		if state == "NO_ROUTE" {
			s.Route.Status = "NO_ROUTE"
			s.Route.Targets = nil
			s.Attempts = nil
		}
		if i == 5 {
			b := s.Attempts[0]
			b.Target = model.RouteTarget{PlatformID: "PLATFORM_B", PlatformUID: "fixed-console-b", DeliveryID: model.NewID()}
			b.Request.PlatformID = "PLATFORM_B"
			s.Route.Targets = append(s.Route.Targets, b.Target)
			s.Attempts = append(s.Attempts, b)
		}
		if err = hubmysql.NewArchiveStore(db).Save(context.Background(), []model.FirstRoundSnapshot{s}); err != nil {
			t.Fatal(err)
		}
		if state == "RECOVERY_PENDING" || state == "NEEDS_ATTENTION" {
			taskState := "NEEDS_ATTENTION"
			if state == "RECOVERY_PENDING" {
				taskState = "RETRY_WAIT"
			}
			if _, err = db.Exec("UPDATE delivery_task SET status=? WHERE delivery_id=?", taskState, s.Attempts[0].Target.DeliveryID); err != nil {
				t.Fatal(err)
			}
		}
		if i == 5 {
			if _, err = db.Exec("UPDATE delivery_task SET status='NEEDS_ATTENTION' WHERE delivery_id=?", s.Attempts[1].Target.DeliveryID); err != nil {
				t.Fatal(err)
			}
		}
		if state == "SUCCEEDED" {
			wanted = append(wanted, s.Trade.MsgID)
		}
		snapshots[state] = s
	}
	type page struct {
		Items      []map[string]any
		NextCursor string
	}
	read := func(path string) page {
		t.Helper()
		w := queryGet(db, path)
		var p page
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &p) != nil {
			t.Fatalf("query %s HTTP%d %s", path, w.Code, w.Body.String())
		}
		return p
	}
	base := "/api/v1/messages?clientId=" + url.QueryEscape(client) + "&traceId=" + url.QueryEscape(trace) + "&status=SUCCEEDED&limit=1"
	first := read(base)
	if len(first.Items) != 1 || first.Items[0]["msgId"] != wanted[1] || first.NextCursor == "" {
		t.Fatalf("first filtered page %+v", first)
	}
	second := read(base + "&cursor=" + url.QueryEscape(first.NextCursor))
	if len(second.Items) != 1 || second.Items[0]["msgId"] != wanted[0] || second.NextCursor != "" {
		t.Fatalf("second filtered page %+v", second)
	}
	for _, changed := range []string{strings.Replace(base, "status=SUCCEEDED", "status=NO_ROUTE", 1), strings.Replace(base, url.QueryEscape(trace), "other", 1)} {
		if w := queryGet(db, changed+"&cursor="+url.QueryEscape(first.NextCursor)); w.Code != 400 {
			t.Fatalf("cross-filter cursor HTTP%d %s", w.Code, w.Body.String())
		}
	}
	for _, state := range []string{"RECOVERY_PENDING", "NEEDS_ATTENTION", "NO_ROUTE"} {
		p := read("/api/v1/messages?clientId=" + client + "&status=" + state + "&limit=100")
		expected := 1
		if state == "RECOVERY_PENDING" {
			expected = 2
		}
		if len(p.Items) != expected {
			t.Fatalf("state=%s items=%+v", state, p.Items)
		}
		for _, m := range p.Items {
			if m["status"] != state || m["firstStatus"] != "SUCCEEDED" && state != "NO_ROUTE" {
				t.Fatalf("current/first facts %+v", m)
			}
		}
	}
	mixed := snapshots["RECOVERY_PENDING"]
	p := read("/api/v1/deliveries?clientId=" + client + "&currentState=NEEDS_ATTENTION&limit=1")
	if len(p.Items) != 1 || p.Items[0]["msgId"] != mixed.Trade.MsgID || p.Items[0]["platformId"] != "PLATFORM_B" || p.NextCursor == "" {
		t.Fatalf("mixed task omitted %+v", p)
	}
	changed := "/api/v1/deliveries?clientId=" + client + "&currentState=RETRY_WAIT&limit=1&cursor=" + url.QueryEscape(p.NextCursor)
	if w := queryGet(db, changed); w.Code != 400 {
		t.Fatalf("task cursor crossed status: %d", w.Code)
	}
	// A route requiring attention must remain discoverable even if its message
	// aggregate is RECOVERY_PENDING because a delivery still has pending work.
	attention := snapshots["NEEDS_ATTENTION"]
	if _, err := db.Exec("UPDATE trade_message SET route_status='NEEDS_ATTENTION' WHERE msg_id IN (?,?)", mixed.Trade.MsgID, attention.Trade.MsgID); err != nil {
		t.Fatal(err)
	}
	routeBase := "/api/v1/messages?clientId=" + client + "&routeStatus=NEEDS_ATTENTION&limit=1"
	routes := read(routeBase)
	if len(routes.Items) != 1 || routes.Items[0]["msgId"] != mixed.Trade.MsgID || routes.Items[0]["status"] != "RECOVERY_PENDING" || routes.NextCursor == "" {
		t.Fatalf("mixed attention route omitted: %+v", routes)
	}
	routeNext := read(routeBase + "&cursor=" + url.QueryEscape(routes.NextCursor))
	if len(routeNext.Items) != 1 || routeNext.Items[0]["msgId"] != attention.Trade.MsgID || routeNext.NextCursor != "" {
		t.Fatalf("attention route page: %+v", routeNext)
	}
	if w := queryGet(db, strings.Replace(routeBase, "routeStatus=NEEDS_ATTENTION", "routeStatus=ROUTED", 1)+"&cursor="+url.QueryEscape(routes.NextCursor)); w.Code != 400 {
		t.Fatal("cursor crossed route status")
	}
}
