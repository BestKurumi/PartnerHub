package runtime_test

import (
	"net/http"
	"net/http/httptest"
	"partner-hub/internal/delivery"
	hubruntime "partner-hub/internal/runtime"
	"strings"
	"testing"
)

func TestPlatformStatusDistinguishesPauseFromDeliveryFacts(t *testing.T) {
	a, b := delivery.DefaultBudgetConfig(), delivery.DefaultBudgetConfig()
	a.Paused = true
	cfg := map[string]delivery.BudgetConfig{"PLATFORM_A": a, "PLATFORM_B": b}
	h := hubruntime.NewPlatformStatusHandler(cfg, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "query unavailable", 503) }))
	cfg["PLATFORM_A"] = b
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/runtime", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"state":"PAUSED"`) || !strings.Contains(w.Body.String(), `"state":"RUNNING"`) || strings.Contains(w.Body.String(), "SUCCEEDED") || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("pause config missing/misreported: HTTP%d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/runtime", nil))
	if w.Code != 405 {
		t.Fatalf("unexpected write control: %d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/messages", nil))
	if w.Code != 503 {
		t.Fatal("changed MySQL query behavior")
	}
}
