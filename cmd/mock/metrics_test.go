package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func scrapeMock(t *testing.T, m *mockMetrics) string {
	t.Helper()
	h := m.withHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "business route", 403) }))
	r := httptest.NewRequest("GET", "http://mock-platform2:28082/metrics", nil)
	r.RemoteAddr = "172.30.41.20:4567"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("metrics status=%d body=%s", w.Code, w.Body.String())
	}
	return w.Body.String()
}

// Catches per-scrape external queries, false zero on sampling failure, dynamic labels,
// and using a process-local Save-call count in place of durable receipt rows.
func TestMockMetricsDurableSnapshotsAndFailure(t *testing.T) {
	var calls atomic.Int64
	var fail atomic.Bool
	m := newMockMetrics(func(context.Context) (int64, error) {
		calls.Add(1)
		if fail.Load() {
			return 0, errors.New("private payload must not leak")
		}
		return 19, nil
	}, func(context.Context) (int64, error) {
		if fail.Load() {
			return 0, errors.New("private kafka address")
		}
		return 7, nil
	}, func() bool { return true })
	m.sample(context.Background())
	body := scrapeMock(t, m)
	for _, want := range []string{"mock_received_rows 19\n", "mock_platform2_consumer_lag_records 7\n", "mock_platform2_consumer_paused 1\n", "mock_receipt_sample_success 1\n", "mock_platform2_lag_sample_success 1\n", "go_goroutines ", "process_start_time_seconds "} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in %s", want, body)
		}
	}
	before := calls.Load()
	_ = scrapeMock(t, m)
	if calls.Load() != before {
		t.Fatal("scrape queried external dependency")
	}
	fail.Store(true)
	m.sample(context.Background())
	body = scrapeMock(t, m)
	for _, want := range []string{"mock_received_rows 19\n", "mock_platform2_consumer_lag_records 7\n", "mock_receipt_sample_success 0\n", "mock_platform2_lag_sample_success 0\n"} {
		if !strings.Contains(body, want) {
			t.Fatalf("failure erased last good observation or health missing: %q", want)
		}
	}
	for _, bad := range []string{"private", "traceId=", "userUid=", "msgId="} {
		if strings.Contains(body, bad) {
			t.Fatalf("sensitive/high-cardinality data in scrape %q", bad)
		}
	}
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "mock_") && strings.Contains(line, "{") {
			t.Fatalf("unexpected labels: %s", line)
		}
	}
}

// Catches coupling lag collection to consumption (which stops while paused),
// or allowing slow dependencies to outlive their sampling budget.
func TestMockMetricsSamplesWhilePausedAndStops(t *testing.T) {
	var lag atomic.Int64
	lag.Store(4)
	m := newMockMetrics(nil, func(ctx context.Context) (int64, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("lag sample has no deadline")
		}
		return lag.Load(), nil
	}, func() bool { return true })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); m.run(ctx, 10*time.Millisecond) }()
	defer cancel()
	until := time.Now().Add(time.Second)
	for !strings.Contains(scrapeMock(t, m), "mock_platform2_consumer_lag_records 4\n") {
		if time.Now().After(until) {
			t.Fatal("paused consumer was not sampled")
		}
		time.Sleep(time.Millisecond)
	}
	lag.Store(11)
	for !strings.Contains(scrapeMock(t, m), "mock_platform2_consumer_lag_records 11\n") {
		if time.Now().After(until) {
			t.Fatal("paused lag did not update")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("metrics sampler did not stop")
	}
}

func TestMockMetricsDoesNotBypassControl(t *testing.T) {
	m := newMockMetrics(nil, nil, nil)
	h := m.withHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "forbidden", 403) }))
	for _, path := range []string{"/demo/consumer", "/demo/binding-events"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", path, nil))
		if w.Code != 403 {
			t.Fatalf("control bypass: %s status %d", path, w.Code)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/metrics", nil))
	if w.Code != 405 {
		t.Fatalf("metrics must be read-only: %d", w.Code)
	}
	_ = scrapeMock(t, m)
}
