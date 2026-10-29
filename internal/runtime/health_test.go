package runtime_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	hubruntime "partner-hub/internal/runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func readyChecks() map[string]hubruntime.HealthCheck {
	m := map[string]hubruntime.HealthCheck{}
	for _, n := range hubruntime.HealthCheckNames {
		m[n] = func(context.Context) error { return nil }
	}
	return m
}
func healthHandler(t *testing.T, m map[string]hubruntime.HealthCheck) http.Handler {
	t.Helper()
	h, e := hubruntime.NewHealthHandler("hub-1", m, http.NotFoundHandler())
	if e != nil {
		t.Fatal(e)
	}
	return h
}
func TestHealthLivenessIndependentOfDependencies(t *testing.T) {
	var calls atomic.Int32
	m := readyChecks()
	for k := range m {
		m[k] = func(context.Context) error { calls.Add(1); return errors.New("password=secret") }
	}
	h := healthHandler(t, m)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/livez", nil))
	if w.Code != 200 || calls.Load() != 0 {
		t.Fatalf("liveness coupled to dependencies HTTP%d calls=%d", w.Code, calls.Load())
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != 503 || strings.Contains(w.Body.String(), "secret") || !strings.Contains(w.Body.String(), `"schema":"UNAVAILABLE"`) {
		t.Fatalf("readiness missing or unsafe HTTP%d %s", w.Code, w.Body.String())
	}
}
func TestHealthReadinessReportsAllChecksAndNoWrite(t *testing.T) {
	m := readyChecks()
	h := healthHandler(t, m)
	for _, path := range []string{"/readyz", "/livez"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 || !strings.Contains(w.Body.String(), "hub-1") {
			t.Fatalf("missing health: HTTP%d %s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", path, nil))
		if w.Code != 405 {
			t.Fatal("health accepted write")
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
	for _, k := range hubruntime.HealthCheckNames {
		if !strings.Contains(w.Body.String(), `"`+k+`":"READY"`) {
			t.Fatalf("missing %s", k)
		}
	}
}
func TestHealthReadinessBoundsAndPreservesDeadline(t *testing.T) {
	m := readyChecks()
	m["mysql"] = func(c context.Context) error { <-c.Done(); return c.Err() }
	h := healthHandler(t, m)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil).WithContext(ctx))
	if w.Code != 503 || time.Since(start) > 150*time.Millisecond {
		t.Fatalf("deadline enlarged HTTP%d %s", w.Code, time.Since(start))
	}
}
func TestHealthRejectsMissingChecks(t *testing.T) {
	for _, n := range hubruntime.HealthCheckNames {
		m := readyChecks()
		delete(m, n)
		if _, e := hubruntime.NewHealthHandler("hub-1", m, http.NotFoundHandler()); e == nil {
			t.Fatalf("accepted missing %s", n)
		}
	}
}
