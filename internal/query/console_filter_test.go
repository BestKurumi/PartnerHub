package query_test

import (
	"net/http/httptest"
	"partner-hub/internal/query"
	"strings"
	"testing"
)

func TestConsoleFiltersValidatedBeforeDatabase(t *testing.T) {
	h := query.NewHTTP(query.NewService(nil))
	for _, path := range []string{
		"/api/v1/messages?traceId=shared-trace&status=RECOVERY_PENDING",
		"/api/v1/messages?status=NEEDS_ATTENTION",
		"/api/v1/messages?routeStatus=NEEDS_ATTENTION",
		"/api/v1/deliveries?currentState=NEEDS_ATTENTION",
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 503 {
			t.Fatalf("valid filter rejected: %s -> %d %s", path, w.Code, w.Body.String())
		}
	}
	for _, path := range []string{
		"/api/v1/messages?traceId=x&traceId=y", "/api/v1/messages?traceId=%00", "/api/v1/messages?traceId=" + strings.Repeat("x", 129),
		"/api/v1/messages?status=OTHER", "/api/v1/messages?status=SUCCEEDED&status=NO_ROUTE", "/api/v1/messages?currentState=SUCCEEDED",
		"/api/v1/deliveries?currentState=OTHER", "/api/v1/deliveries?status=NEEDS_ATTENTION", "/api/v1/bindings?traceId=x",
		"/api/v1/messages?routeStatus=OTHER", "/api/v1/messages?routeStatus=ROUTED&routeStatus=NO_ROUTE", "/api/v1/deliveries?routeStatus=ROUTED",
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 400 {
			t.Fatalf("invalid filter accepted: %s -> %d", path, w.Code)
		}
	}
}
