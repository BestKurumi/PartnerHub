//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"os"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/ingress"
	"partner-hub/internal/model"
	"partner-hub/internal/query"
	"strings"
	"testing"
	"time"
)

func TestArchiveDualAndImmutableReplay(t *testing.T) {
	db := bindingDB(t)
	ensureDeliverySchema(t, db)
	ctx := context.Background()
	// 002 was installed by task1.4; requiring the schema catches setup omissions.
	snapshot := archiveSnapshot(t)
	b := snapshot.Attempts[0]
	b.Target = model.RouteTarget{PlatformID: "PLATFORM_B", PlatformUID: "second-fixed-uid", DeliveryID: model.NewID()}
	b.Request.PlatformID = "PLATFORM_B"
	b.Request.ProtocolVersion = "platform2.v1"
	b.Request.Payload = []byte(`{"extUserId":"second-fixed-uid"}`)
	snapshot.Route.Targets = append(snapshot.Route.Targets, b.Target)
	snapshot.Attempts = append(snapshot.Attempts, b)
	archive := hubmysql.NewArchiveStore(db)
	if err := archive.Save(ctx, []model.FirstRoundSnapshot{snapshot}); err != nil {
		t.Fatal(err)
	}
	var tasks, attempts int
	if err := db.QueryRow("SELECT COUNT(*) FROM delivery_task WHERE msg_id=?", snapshot.Trade.MsgID).Scan(&tasks); err != nil || tasks != 2 {
		t.Fatalf("tasks=%d err=%v", tasks, err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM delivery_attempt a JOIN delivery_task d ON d.delivery_id=a.delivery_id WHERE d.msg_id=?", snapshot.Trade.MsgID).Scan(&attempts); err != nil || attempts != 2 {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
	if _, err := db.Exec("UPDATE delivery_task SET status='NEEDS_ATTENTION' WHERE delivery_id=?", b.Target.DeliveryID); err != nil {
		t.Fatal(err)
	}
	if err := archive.Save(ctx, []model.FirstRoundSnapshot{snapshot}); err != nil {
		t.Fatal(err)
	}
	var current, first string
	if err := db.QueryRow("SELECT status,first_outcome FROM delivery_task WHERE delivery_id=?", b.Target.DeliveryID).Scan(&current, &first); err != nil || current != "NEEDS_ATTENTION" || first != "SUCCESS" {
		t.Fatalf("archive rewrote current state=%s first=%s %v", current, first, err)
	}
	empty := archiveSnapshot(t)
	empty.Route.Status = "NO_ROUTE"
	empty.Route.Targets = []model.RouteTarget{}
	empty.Attempts = []model.FirstAttempt{}
	if err := archive.Save(ctx, []model.FirstRoundSnapshot{empty}); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM delivery_task WHERE msg_id=?", empty.Trade.MsgID).Scan(&tasks); err != nil || tasks != 0 {
		t.Fatalf("NO_ROUTE created task=%d %v", tasks, err)
	}
}

func queryGet(db *sql.DB, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	query.NewHTTP(query.NewService(db)).ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	return w
}
func TestQueryNotAvailableVsUnavailable(t *testing.T) {
	db := bindingDB(t)
	ensureDeliverySchema(t, db)
	id := model.NewID()
	w := queryGet(db, "/api/v1/messages/"+id)
	if w.Code != 404 || !strings.Contains(w.Body.String(), "RECORD_NOT_AVAILABLE") || strings.Contains(w.Body.String(), "resubmit") {
		t.Fatalf("missing-record semantics HTTP%d %s", w.Code, w.Body.String())
	}
	closed, err := sql.Open("mysql", "")
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	w = queryGet(closed, "/api/v1/messages/"+id)
	if w.Code != 503 {
		t.Fatalf("DB failure misreported HTTP%d %s", w.Code, w.Body.String())
	}
}
func TestQueryStablePaginationAndFacts(t *testing.T) {
	db := bindingDB(t)
	ensureDeliverySchema(t, db)
	ctx := context.Background()
	archive := hubmysql.NewArchiveStore(db)
	client := "query-" + model.NewID()
	now := time.Now().UTC().Truncate(time.Millisecond)
	var ids []string
	for range 3 {
		s := archiveSnapshot(t)
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(s.Trade.RawPayload, &fields); err != nil {
			t.Fatal(err)
		}
		fields["clientId"], _ = json.Marshal(client)
		body, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		s.Trade, err = ingress.Decode(strings.NewReader(string(body)))
		if err != nil {
			t.Fatal(err)
		}
		s.ReceivedAt = now
		ids = append(ids, s.Trade.MsgID)
		if err := archive.Save(ctx, []model.FirstRoundSnapshot{s}); err != nil {
			t.Fatal(err)
		}
	}
	var seen []string
	cursor := ""
	for range 3 {
		path := "/api/v1/messages?clientId=" + client + "&limit=1"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		w := queryGet(db, path)
		var page struct {
			Items []struct {
				MsgID        string `json:"msgId"`
				RecordStatus string `json:"recordStatus"`
			} `json:"items"`
			NextCursor string `json:"nextCursor"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &page) != nil || len(page.Items) != 1 || page.Items[0].RecordStatus != "PERSISTED" {
			t.Fatalf("page HTTP%d %s", w.Code, w.Body.String())
		}
		seen = append(seen, page.Items[0].MsgID)
		cursor = page.NextCursor
	}
	if seen[0] == seen[1] || seen[1] == seen[2] || seen[0] == seen[2] || cursor != "" {
		t.Fatalf("cursor lost/duplicated same-timestamp messages: %v next=%s", seen, cursor)
	}
	w := queryGet(db, "/api/v1/messages/"+ids[0])
	var message struct {
		Status, RecordStatus string
		PlatformResults      []struct{ DeliveryID, FirstOutcome, CurrentState, PlatformUID string } `json:"platformResults"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &message) != nil || message.Status != "SUCCEEDED" || message.RecordStatus != "PERSISTED" || len(message.PlatformResults) != 1 || message.PlatformResults[0].FirstOutcome != "SUCCESS" {
		t.Fatalf("detail HTTP%d %s", w.Code, w.Body.String())
	}
	deliveryID := message.PlatformResults[0].DeliveryID
	w = queryGet(db, "/api/v1/deliveries/"+deliveryID+"/attempts")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "SUCCESS") || !strings.Contains(w.Body.String(), "fixed-before-unbind") {
		t.Fatalf("attempts HTTP%d %s", w.Code, w.Body.String())
	}
}

// The second platform failure must roll back the entire batch, including a prior message.
func TestArchiveBatchRollback(t *testing.T) {
	db := bindingDB(t)
	ensureDeliverySchema(t, db)
	first, second := archiveSnapshot(t), archiveSnapshot(t)
	b := second.Attempts[0]
	b.Target = model.RouteTarget{PlatformID: "PLATFORM_B", PlatformUID: "rollback-b", DeliveryID: model.NewID()}
	b.Request.PlatformID = "PLATFORM_B"
	b.Request.ProtocolVersion = "platform2.v1"
	second.Route.Targets = append(second.Route.Targets, b.Target)
	second.Attempts = append(second.Attempts, b)
	trigger := "it_archive_" + strings.ReplaceAll(model.NewID(), "-", "")
	_, err := db.Exec("CREATE TRIGGER " + trigger + " BEFORE INSERT ON delivery_task FOR EACH ROW BEGIN IF NEW.delivery_id='" + b.Target.DeliveryID + "' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='query archive rollback injection'; END IF; END")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec("DROP TRIGGER IF EXISTS " + trigger) })
	err = hubmysql.NewArchiveStore(db).Save(context.Background(), []model.FirstRoundSnapshot{first, second})
	if err == nil || !strings.Contains(err.Error(), "query archive rollback injection") {
		t.Fatalf("wrong failure: %v", err)
	}
	for _, s := range []model.FirstRoundSnapshot{first, second} {
		for _, table := range []string{"trade_message", "delivery_task"} {
			var n int
			if err = db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE msg_id=?", s.Trade.MsgID).Scan(&n); err != nil || n != 0 {
				t.Fatalf("partial %s count=%d %v", table, n, err)
			}
		}
	}
}
func ensureDeliverySchema(t *testing.T, db *sql.DB) {
	t.Helper()
	first, err := os.ReadFile("../../deploy/mysql/migrations/002_delivery_records.sql")
	if err != nil {
		t.Fatal(err)
	}
	manual, err := os.ReadFile("../../deploy/mysql/migrations/007_manual_operation.sql")
	if err != nil {
		t.Fatal(err)
	}
	ddl := append(append(first, '\n'), manual...)
	for _, stmt := range strings.Split(string(ddl), ";") {
		if strings.TrimSpace(stmt) != "" {
			if _, err = db.Exec(stmt); err != nil {
				t.Fatal(err)
			}
		}
	}
}
func TestQueryAggregationAndFixedSnapshot(t *testing.T) {
	db := bindingDB(t)
	ensureDeliverySchema(t, db)
	s := archiveSnapshot(t)
	b := s.Attempts[0]
	b.Target = model.RouteTarget{PlatformID: "PLATFORM_B", PlatformUID: "frozen-b", DeliveryID: model.NewID()}
	b.Request.PlatformID = "PLATFORM_B"
	s.Route.Targets = append(s.Route.Targets, b.Target)
	s.Attempts = append(s.Attempts, b)
	if err := hubmysql.NewArchiveStore(db).Save(context.Background(), []model.FirstRoundSnapshot{s}); err != nil {
		t.Fatal(err)
	}
	for _, state := range []struct{ task, want string }{{"NEEDS_ATTENTION", "NEEDS_ATTENTION"}, {"RETRY_WAIT", "RECOVERY_PENDING"}, {"SUCCEEDED", "SUCCEEDED"}} {
		if _, err := db.Exec("UPDATE delivery_task SET status=? WHERE delivery_id=?", state.task, b.Target.DeliveryID); err != nil {
			t.Fatal(err)
		}
		w := queryGet(db, "/api/v1/messages/"+s.Trade.MsgID)
		var body map[string]json.RawMessage
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &body) != nil {
			t.Fatalf("detail %d %s", w.Code, w.Body.String())
		}
		var status string
		json.Unmarshal(body["status"], &status)
		if status != state.want || !strings.Contains(string(body["routeSnapshot"]), "frozen-b") || !strings.Contains(string(body["platformResults"]), "SUCCESS") {
			t.Fatalf("lost immutable/current facts: %s", w.Body.String())
		}
	}
	if _, err := db.Exec("UPDATE trade_message SET route_status='NEEDS_ATTENTION' WHERE msg_id=?", s.Trade.MsgID); err != nil {
		t.Fatal(err)
	}
	for _, state := range []struct{ task, want string }{{"SUCCEEDED", "NEEDS_ATTENTION"}, {"RETRY_WAIT", "RECOVERY_PENDING"}} {
		if _, err := db.Exec("UPDATE delivery_task SET status=? WHERE delivery_id=?", state.task, b.Target.DeliveryID); err != nil {
			t.Fatal(err)
		}
		w := queryGet(db, "/api/v1/messages/"+s.Trade.MsgID)
		var body struct{ Status string }
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &body) != nil || body.Status != state.want {
			t.Fatalf("route attention mixed state=%s HTTP%d %s", state.task, w.Code, w.Body.String())
		}
	}
	for _, platform := range []string{"PLATFORM_A", "PLATFORM_B"} {
		w := queryGet(db, "/api/v1/messages?msgId="+s.Trade.MsgID+"&platformId="+platform)
		if w.Code != 200 || !strings.Contains(w.Body.String(), s.Trade.MsgID) {
			t.Fatalf("platform filter %s", w.Body.String())
		}
	}
	w := queryGet(db, "/api/v1/messages?clientId=does-not-exist-%27%20OR%201%3D1&limit=1")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"items":[]`) {
		t.Fatalf("filter escaped SQL: %d %s", w.Code, w.Body.String())
	}
}
func TestQueryBindingEventAndLists(t *testing.T) {
	db := bindingDB(t)
	ensureDeliverySchema(t, db)
	client := "query-bind-" + model.NewID()
	repo := hubmysql.NewBindings(db)
	e := bindingEvent(client, 12345, "BIND", "PLATFORM_A", "frozen-a")
	e.EventID = model.NewID()
	e.Position.Offset = time.Now().UnixNano()
	if _, err := repo.Apply(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	e2 := e
	e2.EventID = model.NewID()
	e2.PlatformID = "PLATFORM_B"
	e2.PlatformUID = "frozen-b"
	e2.Position.Offset++
	if _, err := repo.Apply(context.Background(), e2); err != nil {
		t.Fatal(err)
	}
	_, err := db.Exec("UPDATE binding_current SET updated_at='2026-10-22 03:00:00.123' WHERE client_id=?", client)
	if err != nil {
		t.Fatal(err)
	}
	w := queryGet(db, "/api/v1/binding-events/"+e.EventID)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"result":"APPLIED"`) {
		t.Fatalf("event %d %s", w.Code, w.Body.String())
	}
	cursor := ""
	var platforms []string
	for range 2 {
		path := "/api/v1/bindings?clientId=" + client + "&limit=1"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		w = queryGet(db, path)
		var page struct {
			Items []struct {
				PlatformID string `json:"platformId"`
			}
			NextCursor string
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &page) != nil || len(page.Items) != 1 {
			t.Fatalf("bindings %d %s", w.Code, w.Body.String())
		}
		platforms = append(platforms, page.Items[0].PlatformID)
		cursor = page.NextCursor
	}
	if platforms[0] == platforms[1] || cursor != "" {
		t.Fatalf("binding cursor repeats: %v %s", platforms, cursor)
	}
	if err = repo.SaveAnomaly(context.Background(), model.BindingAnomaly{EventID: e.EventID, Reason: "QUERY_TEST", Payload: []byte("sensitive-do-not-expose"), Position: model.BindingPosition{Topic: "query-tests", Offset: time.Now().UnixNano()}}); err != nil {
		t.Fatal(err)
	}
	w = queryGet(db, "/api/v1/anomalies?platformId=PLATFORM_A&limit=100")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "QUERY_TEST") || strings.Contains(w.Body.String(), "sensitive-do-not-expose") {
		t.Fatalf("anomaly facts %d %s", w.Code, w.Body.String())
	}
}

func TestQueryRouteNeedsAttentionWithoutTasks(t *testing.T) {
	db := bindingDB(t)
	ensureDeliverySchema(t, db)
	s := archiveSnapshot(t)
	s.Route.Status = "NO_ROUTE"
	s.Route.Targets = []model.RouteTarget{}
	s.Attempts = []model.FirstAttempt{}
	if err := hubmysql.NewArchiveStore(db).Save(context.Background(), []model.FirstRoundSnapshot{s}); err != nil {
		t.Fatal(err)
	}
	for _, state := range []struct{ route, want string }{{"NEEDS_ATTENTION", "NEEDS_ATTENTION"}, {"RETRY_WAIT", "RECOVERY_PENDING"}, {"IN_FLIGHT", "RECOVERY_PENDING"}, {"NO_ROUTE", "NO_ROUTE"}} {
		if _, err := db.Exec("UPDATE trade_message SET route_status=? WHERE msg_id=?", state.route, s.Trade.MsgID); err != nil {
			t.Fatal(err)
		}
		w := queryGet(db, "/api/v1/messages/"+s.Trade.MsgID)
		var body struct{ Status string }
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &body) != nil || body.Status != state.want {
			t.Fatalf("route=%s want=%s HTTP%d %s", state.route, state.want, w.Code, w.Body.String())
		}
	}
}
