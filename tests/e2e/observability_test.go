//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
	hubmysql "partner-hub/internal/infra/mysql"
	hubredis "partner-hub/internal/infra/redis"
	"partner-hub/internal/model"
)

func observationQuery(t *testing.T, ctx context.Context, expr string) ([]struct {
	Metric map[string]string
	Value  []json.RawMessage
}, bool) {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://127.0.0.1:"+required(t, "PROMETHEUS_HOST_PORT")+"/api/v1/query?query="+url.QueryEscape(expr), nil)
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	var body struct {
		Status string
		Data   struct {
			Result []struct {
				Metric map[string]string
				Value  []json.RawMessage
			}
		}
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&body) != nil || resp.StatusCode != 200 || body.Status != "success" {
		return nil, false
	}
	return body.Data.Result, true
}
func observationValue(t *testing.T, ctx context.Context, expr string) (float64, bool) {
	t.Helper()
	rows, ok := observationQuery(t, ctx, expr)
	if !ok || len(rows) != 1 || len(rows[0].Value) != 2 {
		return 0, false
	}
	var raw string
	if json.Unmarshal(rows[0].Value[1], &raw) != nil {
		return 0, false
	}
	v, err := strconv.ParseFloat(raw, 64)
	return v, err == nil && !math.IsNaN(v) && !math.IsInf(v, 0)
}
func observationWait(t *testing.T, ctx context.Context, expr string, accept func(float64) bool) float64 {
	t.Helper()
	var value float64
	eventually(t, ctx, func() bool { v, ok := observationValue(t, ctx, expr); value = v; return ok && accept(v) })
	return value
}
func observationPause(t *testing.T, ctx context.Context, paused bool) {
	t.Helper()
	code, _ := demoRequest(t, ctx, "POST", required(t, "HUB_TEST_URL")+"/mock/p2/demo/consumer", map[string]bool{"paused": paused})
	if code != 200 {
		t.Fatal("B consumer control failed")
	}
}
func TestObservability(t *testing.T) {
	if required(t, "STARTUP_E2E_AUTHORIZED") != startupProject {
		t.Fatal("explicit isolated project required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	for expr, n := range map[string]float64{`count(up{job="hub"} == 1)`: 2, `count(up{job=~"mock-.*"} == 1)`: 3} {
		observationWait(t, ctx, expr, func(v float64) bool { return v == n })
	}
	for _, id := range []string{"hub-1", "hub-2"} {
		observationWait(t, ctx, `up{job="hub",instance="`+id+`"}`, func(v float64) bool { return v == 1 })
	}
	t.Log("Prometheus actual targets: hub-1 and hub-2 plus three independent Mock endpoints UP")
	t.Run("dashboard-and-labels", func(t *testing.T) {
		req, _ := http.NewRequestWithContext(ctx, "GET", "http://127.0.0.1:"+required(t, "GRAFANA_HOST_PORT")+"/api/dashboards/uid/hub-overview", nil)
		req.SetBasicAuth(required(t, "GRAFANA_USER"), required(t, "GRAFANA_PASSWORD"))
		resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
		if err != nil {
			t.Fatal("Grafana dashboard request failed")
		}
		defer resp.Body.Close()
		var body struct {
			Dashboard struct {
				UID    string
				Panels []struct {
					Title   string
					Targets []struct{ Expr string }
				}
			}
		}
		if json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&body) != nil || resp.StatusCode != 200 || body.Dashboard.UID != "hub-overview" || len(body.Dashboard.Panels) < 12 {
			t.Fatal("provisioned dashboard missing")
		}
		for _, panel := range body.Dashboard.Panels {
			for _, q := range panel.Targets {
				if _, ok := observationQuery(t, ctx, q.Expr); !ok {
					t.Fatalf("invalid dashboard PromQL panel=%s", panel.Title)
				}
			}
		}
		rows, ok := observationQuery(t, ctx, `{__name__=~"hub_.*|mock_.*"}`)
		if !ok || len(rows) == 0 {
			t.Fatal("custom metric samples missing")
		}
		for _, row := range rows {
			for label := range row.Metric {
				lower := strings.ToLower(label)
				if strings.Contains(lower, "msgid") || strings.Contains(lower, "traceid") || strings.Contains(lower, "deliveryid") || strings.Contains(lower, "uid") {
					t.Fatalf("high-cardinality label %s", label)
				}
			}
		}
		t.Logf("Grafana provisioned panels=%d, all PromQL accepted; live custom series=%d without identity labels", len(body.Dashboard.Panels), len(rows))
	})
	if t.Failed() {
		return
	}
	t.Run("broker-success-separate-from-B-consumption", func(t *testing.T) {
		p2 := startupDB(t, "mock_p2", "PLATFORM2_DB_PASSWORD", "mock_platform2")
		origin := required(t, "HUB_TEST_URL")
		observationPause(t, ctx, true)
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
			defer done()
			observationPause(t, cleanup, false)
		}()
		observationWait(t, ctx, `mock_platform2_consumer_paused`, func(v float64) bool { return v == 1 })
		var countBefore int
		if p2.QueryRowContext(ctx, "SELECT COUNT(*) FROM received_notification").Scan(&countBefore) != nil {
			t.Fatal("B real baseline unavailable")
		}
		observationWait(t, ctx, `mock_received_rows{job="mock-platform2"}`, func(v float64) bool { return v == float64(countBefore) })
		lagBefore := observationWait(t, ctx, `mock_platform2_consumer_lag_records`, func(v float64) bool { return v >= 0 })
		firstBefore := observationWait(t, ctx, `sum(hub_first_delivery_total{platform="PLATFORM_B",outcome="SUCCESS"})`, func(v float64) bool { return v >= 0 })
		client, uid := "metrics-"+model.NewID(), "B-"+model.NewID()
		code, raw := demoRequest(t, ctx, "POST", origin+"/mock/p2/demo/binding-events", map[string]any{"eventType": "BIND", "clientId": client, "platformUid": uid})
		var event struct{ EventID string }
		if code != 202 || json.Unmarshal(raw, &event) != nil || event.EventID == "" {
			t.Fatal("B binding publication failed")
		}
		eventually(t, ctx, func() bool {
			code, raw := demoRequest(t, ctx, "GET", origin+"/mock/p2/demo/binding-events/"+event.EventID, nil)
			var e struct{ Result string }
			return code == 200 && json.Unmarshal(raw, &e) == nil && e.Result == "APPLIED"
		})
		code, raw = demoRequest(t, ctx, "POST", origin+"/mock/upstream/demo/trades", map[string]any{"clientId": client, "tradeType": "ETF", "count": 1, "qps": 1})
		var sent struct {
			Items []struct {
				MsgID, TraceID, Acceptance string
				HTTPStatus                 int
			}
		}
		if code != 200 || json.Unmarshal(raw, &sent) != nil || len(sent.Items) != 1 || sent.Items[0].Acceptance != "COMPLETED" || sent.Items[0].HTTPStatus != 200 {
			t.Fatal("hub did not confirm normal first round")
		}
		firstAfter := observationWait(t, ctx, `sum(hub_first_delivery_total{platform="PLATFORM_B",outcome="SUCCESS"})`, func(v float64) bool { return v >= firstBefore+1 })
		lagAfter := observationWait(t, ctx, `mock_platform2_consumer_lag_records`, func(v float64) bool { return v >= lagBefore+1 })
		var traceCount, allCount int
		if p2.QueryRowContext(ctx, "SELECT COUNT(*) FROM received_notification WHERE trace_id=?", sent.Items[0].TraceID).Scan(&traceCount) != nil || traceCount != 0 || p2.QueryRowContext(ctx, "SELECT COUNT(*) FROM received_notification").Scan(&allCount) != nil || allCount != countBefore {
			t.Fatal("paused B fabricated actual receipt")
		}
		observationWait(t, ctx, `mock_received_rows{job="mock-platform2"}`, func(v float64) bool { return v == float64(countBefore) })
		observationWait(t, ctx, `mock_platform2_lag_sample_success`, func(v float64) bool { return v == 1 })
		t.Logf("B paused: hub Broker SUCCESS %.0f→%.0f, external lag %.0f→%.0f, independent DB receipts %d→%d; msgId=%s", firstBefore, firstAfter, lagBefore, lagAfter, countBefore, allCount, sent.Items[0].MsgID)
		observationPause(t, ctx, false)
		eventually(t, ctx, func() bool {
			return p2.QueryRowContext(ctx, "SELECT COUNT(*) FROM received_notification WHERE trace_id=? AND user_uid=?", sent.Items[0].TraceID, uid).Scan(&traceCount) == nil && traceCount == 1
		})
		rowsAfter := observationWait(t, ctx, `mock_received_rows{job="mock-platform2"}`, func(v float64) bool { return v == float64(countBefore+1) })
		lagDrained := observationWait(t, ctx, `mock_platform2_consumer_lag_records`, func(v float64) bool { return v == 0 })
		t.Logf("B resumed: external actual rows %.0f, group lag %.0f; same trace/UID has exactly one independent receipt", rowsAfter, lagDrained)
	})
	if t.Failed() {
		return
	}
	t.Run("slow-archive-retains-snapshot-and-age", func(t *testing.T) {
		db := startupDB(t, "hub", "MYSQL_PASSWORD", "partner_hub")
		admin := startupDB(t, "root", "MYSQL_ROOT_PASSWORD", "partner_hub")
		redisOptions := &redisclient.Options{Addr: "127.0.0.1:" + required(t, "REDIS_HOST_PORT")}
		archive := hubredis.NewArchive(redisOptions, hubmysql.NewArchiveStore(db))
		defer archive.Close()
		cache := redisclient.NewClient(redisOptions)
		defer cache.Close()
		observationWait(t, ctx, `max(hub_archive_pending_count)`, func(v float64) bool { return v == 0 })
		// Hold a real MySQL table READ lock: both hub archive writers block while
		// read-only queries still work. No paused application or fake lease is used.
		conn, err := admin.Conn(ctx)
		if err != nil {
			t.Fatal("lock connection unavailable")
		}
		defer conn.Close()
		if _, err = conn.ExecContext(ctx, "LOCK TABLES trade_message READ"); err != nil {
			t.Fatal("archive write lock unavailable")
		}
		locked := true
		defer func() {
			if locked {
				release, done := context.WithTimeout(context.Background(), 3*time.Second)
				defer done()
				_, _ = conn.ExecContext(release, "UNLOCK TABLES")
			}
		}()
		snapshot := failoverSnapshot(t)
		snapshot.ReceivedAt = time.Now().UTC().Add(-10 * time.Second)
		if err = archive.Stage(ctx, snapshot); err != nil {
			t.Fatal("saved archive fixture staging failed")
		}
		count := observationWait(t, ctx, `max(hub_archive_pending_count)`, func(v float64) bool { return v >= 1 })
		size := observationWait(t, ctx, `max(hub_archive_pending_bytes)`, func(v float64) bool { return v > 0 })
		age := observationWait(t, ctx, `max(hub_archive_pending_oldest_age_seconds)`, func(v float64) bool { return v >= 10 })
		var n int
		if db.QueryRowContext(ctx, "SELECT COUNT(*) FROM trade_message WHERE msg_id=?", snapshot.Trade.MsgID).Scan(&n) != nil || n != 0 || cache.Exists(ctx, "archive:message:{"+snapshot.Trade.MsgID+"}").Val() != 1 {
			t.Fatal("blocked archive lost responsibility or fabricated persistent record")
		}
		t.Logf("actual MySQL archive writes blocked: pending=%.0f bytes=%.0f oldest=%.3fs; full Redis snapshot retained, MySQL record absent", count, size, age)
		if _, err = conn.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
			t.Fatal("release archive lock failed")
		}
		locked = false
		eventually(t, ctx, func() bool {
			return db.QueryRowContext(ctx, "SELECT COUNT(*) FROM trade_message WHERE msg_id=?", snapshot.Trade.MsgID).Scan(&n) == nil && n == 1 && cache.Exists(ctx, "archive:message:{"+snapshot.Trade.MsgID+"}").Val() == 0
		})
		observationWait(t, ctx, `max(hub_archive_pending_count)`, func(v float64) bool { return v == 0 })
		observationWait(t, ctx, `max(hub_archive_pending_bytes)`, func(v float64) bool { return v == 0 })
		observationWait(t, ctx, `max(hub_archive_pending_oldest_age_seconds)`, func(v float64) bool { return v == 0 })
		t.Log("archive recovered: actual hub archived saved NO_ROUTE snapshot once; shared pending rows/bytes/age returned to zero, no external send")
	})
}
