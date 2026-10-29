package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func decode(t *testing.T, s *Service, query string) Snapshot {
	t.Helper()
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/ops/metrics"+query, nil))
	if w.Code != 200 {
		t.Fatalf("HTTP %d %s", w.Code, w.Body.String())
	}
	var out Snapshot
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func metric(t *testing.T, s Snapshot, key string) Metric {
	t.Helper()
	for _, m := range s.Metrics {
		if m.Key == key {
			return m
		}
	}
	t.Fatalf("missing %s", key)
	return Metric{}
}

func TestOperationsDoesNotProbeExternalMockInternals(t *testing.T) {
	var external atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		if strings.Contains(q, "mock") || strings.Contains(q, "hub|mock") {
			external.Add(1)
		}
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
	}))
	defer server.Close()
	s, _ := New(Config{PrometheusURL: server.URL})
	s.sampleCycle(context.Background())
	if external.Load() != 0 {
		t.Fatalf("queried external platform internals %d times", external.Load())
	}
	for _, m := range decode(t, s, "").Metrics {
		if m.Key == "receipts" || m.Key == "b_lag" || m.Key == "b_paused" {
			t.Errorf("external metric exposed: %s", m.Key)
		}
	}
}

func TestEmptySamplingReasonDistinguishesSuccessfulNaNFromQueryFailure(t *testing.T) {
	var down atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if down.Load() {
			http.Error(w, "down", 503)
			return
		}
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"platform":"PLATFORM_A"},"value":[%d,"NaN"]}]}}`, time.Now().Unix())
	}))
	defer server.Close()
	s, _ := New(Config{PrometheusURL: server.URL})
	reason := func() string {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/ops/metrics", nil))
		var out struct {
			Metrics []struct {
				Key, Reason string
				Samples     []Sample
			}
		}
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		for _, m := range out.Metrics {
			if m.Key == "first_p99" {
				if len(m.Samples) != 0 {
					t.Fatal("invented percentile")
				}
				return m.Reason
			}
		}
		t.Fatal("missing first_p99")
		return ""
	}
	if got := reason(); got != "not_sampled" {
		t.Fatalf("initial reason = %q", got)
	}
	s.sampleCycle(context.Background())
	if got := reason(); got != "no_samples" {
		t.Fatalf("successful NaN reason = %q", got)
	}
	down.Store(true)
	s.sampleCycle(context.Background())
	if got := reason(); got != "query_failed" {
		t.Fatalf("failed query reason = %q", got)
	}
}

// Catch request-triggered probes, failed-query data loss and invented empty zeros.
func TestCacheRetainsFailureAndDoesNotProbeOnHTTP(t *testing.T) {
	var calls atomic.Int64
	var mode atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if mode.Load() == 1 {
			http.Error(w, "down", 503)
			return
		}
		result := fmt.Sprintf(`[{"metric":{"status":"202"},"value":[%d,"7"]}]`, time.Now().Unix())
		if mode.Load() == 2 {
			result = `[]`
		}
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":%s}}`, result)
	}))
	defer server.Close()
	s, err := New(Config{PrometheusURL: server.URL, StaleAfter: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	before := decode(t, s, "")
	if metric(t, before, "ingress_rate").Status != "unavailable" || calls.Load() != 0 {
		t.Fatal("request probed backend or invented initial data")
	}
	s.sampleCycle(context.Background())
	good := metric(t, decode(t, s, "?window=1h"), "ingress_rate")
	if good.Status != "ok" || len(good.Samples) != 1 || good.Samples[0].Value != 7 {
		t.Fatalf("sample = %+v", good)
	}
	count := calls.Load()
	decode(t, s, "?window=6h")
	if calls.Load() != count {
		t.Fatal("HTTP probed backend")
	}
	mode.Store(1)
	s.sampleCycle(context.Background())
	bad := metric(t, decode(t, s, "?window=1h"), "ingress_rate")
	if bad.Status != "stale" || len(bad.Samples) != 1 || bad.Samples[0].Value != 7 {
		t.Fatalf("lost stale value: %+v", bad)
	}
	mode.Store(2)
	s.sampleCycle(context.Background())
	empty := metric(t, decode(t, s, ""), "ingress_rate")
	if empty.Status != "unavailable" || len(empty.Samples) != 0 {
		t.Fatalf("empty vector = %+v", empty)
	}
}

func TestStrictParametersAndSafeURLs(t *testing.T) {
	s, _ := New(Config{GrafanaURL: "https://grafana.example/d/hub-overview"})
	for _, q := range []string{"?window=5m", "?window=1h&window=6h", "?query=up", "?url=http://attacker", "?window=%ZZ"} {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/ops/metrics"+q, nil))
		if w.Code != 400 {
			t.Errorf("%s: %d", q, w.Code)
		}
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/ops/metrics", nil))
	if w.Code != 405 {
		t.Errorf("POST: %d", w.Code)
	}
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/ops/config", nil))
	if !strings.Contains(w.Body.String(), `"grafanaUrl":"https://grafana.example/d/hub-overview"`) {
		t.Fatal(w.Body.String())
	}
	for _, url := range []string{"file:///tmp/prom", "http://user:password@localhost", "http://localhost/?token=secret", "http://localhost/#x"} {
		if _, err := New(Config{PrometheusURL: url}); err == nil {
			t.Errorf("accepted unsafe Prometheus URL %s", url)
		}
		if _, err := New(Config{GrafanaURL: url}); err == nil {
			t.Errorf("accepted unsafe Grafana URL %s", url)
		}
	}
}

func TestSamplerFixedExpressionsLimitsAndCancellation(t *testing.T) {
	var redirected atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer target.Close()
	cases := []struct {
		name, body string
		redirect   bool
	}{
		{"oversize", strings.Repeat(" ", 512), false},
		{"too-many", `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"1"]},{"metric":{},"value":[1,"2"]}]}}`, false},
		{"nan", `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"NaN"]}]}}`, false},
		{"wrong-type", `{"status":"success","data":{"resultType":"matrix","result":[]}}`, false},
		{"redirect", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if (r.URL.Path != "/api/v1/query" && r.URL.Path != "/api/v1/query_range") || r.URL.Query().Get("query") == "" {
					t.Error("wrong fixed query endpoint")
				}
				if tc.redirect {
					http.Redirect(w, r, target.URL, 302)
					return
				}
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			s, _ := New(Config{PrometheusURL: server.URL, MaxBytes: 256, MaxSeries: 1})
			s.sampleCycle(context.Background())
			if m := metric(t, decode(t, s, ""), "archive_pending"); m.Status != "unavailable" || len(m.Samples) != 0 {
				t.Fatalf("unsafe result accepted %+v", m)
			}
		})
	}
	if redirected.Load() != 0 {
		t.Fatal("followed redirect")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	s, _ := New(Config{PrometheusURL: server.URL, Timeout: 30 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	time.AfterFunc(20*time.Millisecond, cancel)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sampler did not stop")
	}
}

func TestWhitelistAggregatesSharedGaugesAndHistogramBuckets(t *testing.T) {
	expressions := make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expressions[r.URL.Query().Get("query")] = true
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
	}))
	defer server.Close()
	// One worker protects this test's expression fixture map.
	s, _ := New(Config{PrometheusURL: server.URL})
	s.workers = 1
	s.sampleCycle(context.Background())
	for _, want := range []string{`max(hub_archive_pending_count)`, `max by (platform,status) (hub_recovery_tasks)`, `sum by (status) (rate(hub_ingress_requests_total[1h]))`, `histogram_quantile(0.99, sum by (platform,outcome,le) (rate(hub_first_delivery_duration_seconds_bucket[15m])))`} {
		if !expressions[want] {
			t.Errorf("missing correct aggregation: %s", want)
		}
	}
}

// A reachable Prometheus can still return old scrape data. A successful HTTP
// query must not relabel that old value as a fresh dependency observation.
func TestOldPrometheusSampleRemainsStale(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"3"]}]}}`)
	}))
	defer server.Close()
	s, _ := New(Config{PrometheusURL: server.URL})
	s.sampleCycle(context.Background())
	m := metric(t, decode(t, s, ""), "archive_pending")
	if m.Status != "stale" || len(m.Samples) != 1 || m.Samples[0].Value != 3 {
		t.Fatalf("old sample looks fresh: %+v", m)
	}
}

func TestDependencyFailureIsNotHiddenByHealthyPrometheus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expr := r.URL.Query().Get("query")
		labels := `{"kind":"archive"}`
		value := "1"
		if expr == "hub_observation_up" {
			value = "0"
		}
		if strings.Contains(expr, "last_success_timestamp") {
			value = "1"
		}
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":%s,"value":[%d,%q]}]}}`, labels, time.Now().Unix(), value)
	}))
	defer server.Close()
	s, _ := New(Config{PrometheusURL: server.URL})
	s.sampleCycle(context.Background())
	if m := metric(t, decode(t, s, ""), "archive_pending"); m.Status != "stale" {
		t.Fatalf("dependency failed but value looks healthy: %+v", m)
	}
}

func TestHistogramEmptyOutcomeDoesNotDiscardObservedOutcome(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"platform":"PLATFORM_A","outcome":"SUCCESS"},"value":[%d,"0.1"]},{"metric":{"platform":"PLATFORM_A","outcome":"UNKNOWN"},"value":[%d,"NaN"]}]}}`, time.Now().Unix(), time.Now().Unix())
	}))
	defer server.Close()
	s, _ := New(Config{PrometheusURL: server.URL})
	s.sampleCycle(context.Background())
	m := metric(t, decode(t, s, ""), "first_p99")
	if m.Status != "ok" || len(m.Samples) != 1 || m.Samples[0].Value != .1 {
		t.Fatalf("empty histogram outcome erased valid samples: %+v", m)
	}
}
