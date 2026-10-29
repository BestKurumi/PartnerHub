package query_test

import (
	"net/http/httptest"
	"partner-hub/internal/query"
	"testing"
)

func TestQueryInvalidParameters(t *testing.T) {
	h := query.NewHTTP(query.NewService(nil))
	for _, path := range []string{"/api/v1/messages?limit=0", "/api/v1/messages?limit=101", "/api/v1/messages?limit=abc", "/api/v1/messages?limit=20&limit=10", "/api/v1/messages?cursor=bad", "/api/v1/messages?platformId=OTHER", "/api/v1/messages?from=invalid", "/api/v1/messages?from=2026-10-22T10:00:00Z&to=2026-10-22T09:00:00Z", "/api/v1/messages/invalid", "/api/v1/deliveries/invalid/attempts", "/api/v1/binding-events/invalid"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 400 {
			t.Errorf("%s -> %d want400", path, w.Code)
		}
	}
}

// Malformed, duplicate and cross-resource parameters must fail before database access.
func TestQueryRejectsAmbiguousRequests(t *testing.T) {
	h := query.NewHTTP(query.NewService(nil))
	for _, path := range []string{"/api/v1/messages?%zz=bad", "/api/v1/messages?unknown=1", "/api/v1/messages?clientId=%00", "/api/v1/messages?msgId=bad", "/api/v1/messages?cursor=", "/api/v1/bindings?platformUid="} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 400 {
			t.Fatalf("%s HTTP%d %s", path, w.Code, w.Body.String())
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/messages", nil))
	if w.Code != 405 {
		t.Fatalf("write route HTTP%d", w.Code)
	}
}
