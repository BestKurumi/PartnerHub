//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Exercise the public proxy and real two-instance observations, not stubs.
func TestConsoleOperations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	base := required(t, "HUB_TEST_URL")
	for _, path := range []string{"/api/v1/ops/config", "/api/v1/ops/metrics", "/api/v1/ops/diagnostics", "/hub/demo/ops/config"} {
		req, _ := http.NewRequestWithContext(ctx, "GET", base+path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 401 {
			t.Fatalf("unauthorized ops read %s: %d", path, resp.StatusCode)
		}
	}
	code, body := demoRequest(t, ctx, "GET", base+"/internal/ops/diagnostics", nil)
	if code != 404 {
		t.Fatalf("internal route exposed by proxy: %d", code)
	}
	code, body = demoRequest(t, ctx, "GET", base+"/hub/demo/ops/config", nil)
	var cfg struct {
		GrafanaURL string `json:"grafanaUrl"`
	}
	if code != 200 || json.Unmarshal(body, &cfg) != nil || !strings.Contains(cfg.GrafanaURL, "/d/hub-overview") {
		t.Fatalf("Grafana link absent HTTP%d", code)
	}
	for _, window := range []string{"15m", "1h", "6h"} {
		eventually(t, ctx, func() bool {
			code, body = demoRequest(t, ctx, "GET", base+"/hub/demo/ops/metrics?window="+window, nil)
			var snapshot struct {
				Window  string
				Metrics []struct {
					Key, Status string
					Samples     []struct{ Value float64 }
				}
			}
			if code != 200 || json.Unmarshal(body, &snapshot) != nil || snapshot.Window != window {
				return false
			}
			seen := map[string]bool{}
			for _, metric := range snapshot.Metrics {
				seen[metric.Key] = metric.Status == "ok" && len(metric.Samples) > 0
			}
			return seen["targets_up"] && seen["rss"] && seen["mysql_connections"] && seen["redis_clients"] && seen["kafka_brokers"]
		})
	}
	// Malformed input has no formal message ID. Server request ID must still
	// correlate it to a retained diagnostic event across either replica.
	req, _ := http.NewRequestWithContext(ctx, "POST", base+"/api/v1/trades", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	requestID := resp.Header.Get("X-Request-ID")
	if resp.StatusCode != 400 || requestID == "" {
		t.Fatalf("invalid ingress must have rejection and server request ID: %d", resp.StatusCode)
	}
	code, body = demoRequest(t, ctx, "GET", base+"/hub/demo/ops/diagnostics?q="+requestID, nil)
	var logs struct {
		Items     []struct{ RequestID, MsgID, Kind, Instance, BootID string }
		Instances []struct {
			Instance, Status, BootID string
			Capacity                 int
		}
		Retention struct {
			CapacityPerInstance int
			Persistent          bool
		}
	}
	if code != 200 || json.Unmarshal(body, &logs) != nil || len(logs.Items) != 1 || logs.Items[0].RequestID != requestID || logs.Items[0].Kind != "ingress_invalid" || logs.Items[0].MsgID != "" {
		t.Fatalf("rejected request diagnostic missing HTTP%d", code)
	}
	if len(logs.Instances) != 2 || logs.Retention.CapacityPerInstance != 1000 || logs.Retention.Persistent {
		t.Fatal("wrong diagnostic retention or aggregation")
	}
	for _, instance := range logs.Instances {
		if instance.Status != "ok" || instance.BootID == "" || instance.Capacity != 1000 {
			t.Fatalf("diagnostics peer unavailable: %s", instance.Instance)
		}
	}
	for _, path := range []string{"/hub/demo/ops/metrics?query=up", "/hub/demo/ops/metrics?window=1m", "/hub/demo/ops/diagnostics?peer=http://example.com"} {
		code, _ = demoRequest(t, ctx, "GET", base+path, nil)
		if code != 400 {
			t.Fatalf("arbitrary collector parameters accepted: %s HTTP%d", path, code)
		}
	}
}
