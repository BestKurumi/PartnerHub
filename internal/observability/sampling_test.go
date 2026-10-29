package observability

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestObservationFailureRetainsValueAndExposesStaleness(t *testing.T) {
	m := NewMetrics()
	m.Backlog(4, 800, time.Second)
	m.Observation("archive", nil)
	m.Observation("archive", errors.New("sensitive connection failure"))
	r := httptest.NewRecorder()
	m.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
	s := r.Body.String()
	for _, want := range []string{`hub_archive_pending_count 4`, `hub_observation_up{kind="archive"} 0`, `hub_observation_last_success_timestamp_seconds{kind="archive"}`} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(s, "sensitive connection") {
		t.Error("backend error text leaked into labels")
	}
}
func TestSamplerStopsBeforeClientsCloseAndScrapeDoesNoIO(t *testing.T) {
	m := NewMetrics()
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.RunObservations(ctx, time.Second, []Observation{{Kind: "archive", Sample: func(ctx context.Context) error { close(entered); <-ctx.Done(); return ctx.Err() }}})
	}()
	<-entered
	r := httptest.NewRecorder()
	m.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
	if r.Code != 200 {
		t.Fatal(r.Code)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("sampler ignored shutdown cancellation")
	}
}
