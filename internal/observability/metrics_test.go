package observability

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"partner-hub/internal/model"
	"strings"
	"testing"
	"time"
)

// A freshly started, idle instance must expose a real zero baseline so rate()
// can distinguish no traffic from a missing collector after two scrapes.
func TestIdleIngressExportsZeroBaselineThenCountsHTTP(t *testing.T) {
	m := NewMetrics()
	scrape := func() string {
		r := httptest.NewRecorder()
		m.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
		return r.Body.String()
	}
	if !strings.Contains(scrape(), `hub_ingress_requests_total{status="200"} 0`) {
		t.Fatal("idle ingress has no zero baseline")
	}
	m.HTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/api/v1/trades", nil))
	if !strings.Contains(scrape(), `hub_ingress_requests_total{status="200"} 1`) {
		t.Fatal("HTTP request was not counted from its zero baseline")
	}
}

func TestMetricsFirstIncludesNotSentUnknownAndIngressToCompletion(t *testing.T) {
	m := NewMetrics()
	now := time.Now()
	m.First(model.FirstRoundSnapshot{ReceivedAt: now.Add(-2 * time.Second), Trade: model.TradeEvent{MsgID: "secret-msg", TraceID: "secret-trace"}, Attempts: []model.FirstAttempt{
		{Target: model.RouteTarget{PlatformID: "PLATFORM_A", PlatformUID: "secret-uid", DeliveryID: "secret-delivery"}, Result: model.SendResult{Outcome: model.RetryableFailure, SendStarted: false, FinishedAt: now}},
		{Target: model.RouteTarget{PlatformID: "PLATFORM_B"}, Result: model.SendResult{Outcome: model.Unknown, SendStarted: true, FinishedAt: now}},
	}})
	r := httptest.NewRecorder()
	m.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
	s := r.Body.String()
	for _, want := range []string{`hub_first_delivery_total{outcome="RETRYABLE_FAILURE",platform="PLATFORM_A",send_started="false"} 1`, `hub_first_delivery_total{outcome="UNKNOWN",platform="PLATFORM_B",send_started="true"} 1`, `hub_first_delivery_duration_seconds_sum{outcome="UNKNOWN",platform="PLATFORM_B"} 2`, `go_goroutines`, `process_cpu_seconds_total`} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %s", want)
		}
	}
	for _, bad := range []string{"secret-msg", "secret-trace", "secret-uid", "secret-delivery", "msgId", "traceId", "deliveryId"} {
		if strings.Contains(s, bad) {
			t.Errorf("high cardinality data leaked: %s", bad)
		}
	}
	// A private registry must permit a second instance in the same process.
	NewMetrics()
}

func TestLoggerAllowsCorrelationButDropsSecretsAndNestedPayload(t *testing.T) {
	var b bytes.Buffer
	l := NewLogger(&b)
	l.With("platformUid", "sensitive-uid", "msgId", "m1").Info("first delivery", "traceId", "t1", "deliveryId", "d1", "payload", map[string]string{"amount": "sensitive-amount"}, "reason", "password=secret", "authorization", "secret-token")
	var record map[string]any
	if err := json.Unmarshal(b.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["msgId"] != "m1" || record["traceId"] != "t1" || record["deliveryId"] != "d1" {
		t.Fatal("missing correlation", record)
	}
	for _, bad := range []string{"sensitive-uid", "sensitive-amount", "password=secret", "secret-token"} {
		if strings.Contains(b.String(), bad) {
			t.Errorf("sensitive field leaked: %s", bad)
		}
	}
}

func TestRouteObservationCountsZeroAndTwoTargetsPerMessage(t *testing.T) {
	m := NewMetrics()
	m.First(model.FirstRoundSnapshot{Route: model.RouteSnapshot{Status: "NO_ROUTE", Targets: []model.RouteTarget{}}})
	m.First(model.FirstRoundSnapshot{Route: model.RouteSnapshot{Status: "ROUTED", Targets: []model.RouteTarget{{PlatformID: "PLATFORM_A"}, {PlatformID: "PLATFORM_B"}}}})
	r := httptest.NewRecorder()
	m.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
	s := r.Body.String()
	for _, want := range []string{`hub_route_results_total{status="NO_ROUTE"} 1`, `hub_route_results_total{status="ROUTED"} 1`, `hub_first_targets_sum 2`, `hub_first_targets_count 2`} {
		if !strings.Contains(s, want) {
			t.Errorf("missing per-message routing metric %s", want)
		}
	}
}
