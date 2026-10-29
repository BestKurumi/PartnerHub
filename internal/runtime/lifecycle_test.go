package runtime_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	hubruntime "partner-hub/internal/runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestLifecycleDrainingRejectsNewBusinessAndReadiness(t *testing.T) {
	var admitted atomic.Int32
	checks := map[string]hubruntime.HealthCheck{}
	for _, n := range hubruntime.HealthCheckNames {
		checks[n] = func(context.Context) error { return nil }
	}
	h, err := hubruntime.NewHealthHandler("drain-instance", checks, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { admitted.Add(1); w.WriteHeader(204) }))
	if err != nil {
		t.Fatal(err)
	}
	// A drain capability belongs to the real health/business boundary, so main
	// cannot accidentally gate readiness while continuing business admission.
	drain, ok := any(h).(interface{ BeginDrain() })
	if !ok {
		t.Fatal("health/business handler cannot enter graceful draining")
	}
	call := func(path string) int {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest("GET", path, nil))
		return r.Code
	}
	if call("/api/v1/trades") != 204 || call("/readyz") != 200 {
		t.Fatal("live instance rejects admission")
	}
	drain.BeginDrain()
	if code := call("/readyz"); code != 503 {
		t.Errorf("draining readiness=%d", code)
	}
	if code := call("/api/v1/trades"); code != 503 {
		t.Errorf("new trade entered draining instance: %d", code)
	}
	if code := call("/livez"); code != 200 {
		t.Errorf("draining process liveness=%d", code)
	}
	if admitted.Load() != 1 {
		t.Fatalf("new request admitted after drain: %d", admitted.Load())
	}
}

func TestLifecycleDrainingPreservesAlreadyAdmittedRequest(t *testing.T) {
	admitted, release, finished := make(chan struct{}), make(chan struct{}), make(chan int, 1)
	checks := map[string]hubruntime.HealthCheck{}
	for _, n := range hubruntime.HealthCheckNames {
		checks[n] = func(context.Context) error { return nil }
	}
	h, err := hubruntime.NewHealthHandler("drain-instance", checks, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(admitted); <-release; w.WriteHeader(201) }))
	if err != nil {
		t.Fatal(err)
	}
	drain, ok := any(h).(interface{ BeginDrain() })
	if !ok {
		t.Fatal("health/business handler cannot enter graceful draining")
	}
	go func() {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest("POST", "/api/v1/trades", nil))
		finished <- r.Code
	}()
	<-admitted
	drain.BeginDrain()
	close(release)
	select {
	case code := <-finished:
		if code != 201 {
			t.Fatalf("admitted response lost: %d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("admitted request did not complete")
	}
}

// Shutdown regression tests exercise real HTTP shutdown and the side effects
// visible at the persistence/client boundary; no close-call mock is the result.
func TestLifecycleShutdownSavesBeforeClientsClose(t *testing.T) {
	entered, release, response := make(chan struct{}), make(chan struct{}), make(chan int, 1)
	checks := map[string]hubruntime.HealthCheck{}
	for _, n := range hubruntime.HealthCheckNames {
		checks[n] = func(context.Context) error { return nil }
	}
	var saved, closed atomic.Bool
	h, _ := hubruntime.NewHealthHandler("shutdown", checks, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		saved.Store(true)
		w.WriteHeader(201)
	}))
	server := httptest.NewServer(h)
	defer server.Close()
	go func() {
		res, err := http.Post(server.URL+"/api/v1/trades", "application/json", nil)
		if err != nil {
			response <- 0
			return
		}
		defer res.Body.Close()
		response <- res.StatusCode
	}()
	<-entered
	workersDone := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- h.Shutdown(ctx, server.Config, workersDone, func(context.Context) error {
			if !saved.Load() {
				t.Error("archive flush ran before admitted result was saved")
			}
			return nil
		}, func() error { closed.Store(true); return nil })
	}()
	select {
	case err := <-done:
		t.Fatalf("shutdown returned while admitted request active: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if closed.Load() {
		t.Fatal("client closed before result save")
	}
	close(release)
	if code := <-response; code != 201 {
		t.Fatalf("drain erased admitted response: %d", code)
	}
	select {
	case err := <-done:
		t.Fatalf("shutdown returned while claimed worker active: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(workersDone)
	if err := <-done; err != nil || !closed.Load() {
		t.Fatalf("shutdown not fully closed: %v closed=%v", err, closed.Load())
	}
}

func TestLifecycleShutdownDeadlineBoundsEveryPhase(t *testing.T) {
	for _, phase := range []string{"worker", "flush", "close"} {
		t.Run(phase, func(t *testing.T) {
			checks := map[string]hubruntime.HealthCheck{}
			for _, n := range hubruntime.HealthCheckNames {
				checks[n] = func(context.Context) error { return nil }
			}
			h, _ := hubruntime.NewHealthHandler("shutdown", checks, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
			workersDone := make(chan struct{})
			block := make(chan struct{})
			defer close(block)
			if phase != "worker" {
				close(workersDone)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			defer cancel()
			start := time.Now()
			err := h.Shutdown(ctx, &http.Server{}, workersDone, func(context.Context) error {
				if phase == "flush" {
					<-block
				}
				return nil
			}, func() error {
				if phase == "close" {
					<-block
				}
				return nil
			})
			if err != context.DeadlineExceeded || time.Since(start) > 400*time.Millisecond {
				t.Fatalf("%s ignored shared deadline: err=%v elapsed=%v", phase, err, time.Since(start))
			}
		})
	}
}

func TestLifecycleReadinessCannotReturnReadyAfterDrainBegins(t *testing.T) {
	checking, release := make(chan struct{}), make(chan struct{})
	checks := map[string]hubruntime.HealthCheck{}
	for _, n := range hubruntime.HealthCheckNames {
		checks[n] = func(context.Context) error { return nil }
	}
	checks["mysql"] = func(context.Context) error { close(checking); <-release; return nil }
	h, _ := hubruntime.NewHealthHandler("shutdown", checks, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	done := make(chan int, 1)
	go func() {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest("GET", "/readyz", nil))
		done <- r.Code
	}()
	<-checking
	h.BeginDrain()
	close(release)
	if code := <-done; code != 503 {
		t.Fatalf("in-progress readiness announced draining instance as ready: %d", code)
	}
}
