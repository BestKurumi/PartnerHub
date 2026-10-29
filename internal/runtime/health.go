package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"
)

type HealthCheck func(context.Context) error

// Fixed names keep operational output bounded and free of customer identifiers.
var HealthCheckNames = []string{"config", "adapters", "schema", "mysql", "redis", "kafka", "archiveCapacity", "handoffCapacity"}

func NewHealthHandler(instance string, checks map[string]HealthCheck, next http.Handler) (*Lifecycle, error) {
	if instance == "" || next == nil {
		return nil, errors.New("invalid health configuration")
	}
	copied := map[string]HealthCheck{}
	for _, n := range HealthCheckNames {
		if checks[n] == nil {
			return nil, errors.New("missing health check")
		}
		copied[n] = checks[n]
	}
	lifecycle := &Lifecycle{}
	slots := make(chan struct{}, 4)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/livez" && r.URL.Path != "/readyz" {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != "GET" {
			w.Header().Set("Allow", "GET")
			w.WriteHeader(405)
			return
		}
		result := struct {
			Instance string            `json:"instanceId"`
			Status   string            `json:"status"`
			Checks   map[string]string `json:"checks,omitempty"`
		}{Instance: instance, Status: "LIVE"}
		code := 200
		if r.URL.Path == "/readyz" {
			result.Status = "READY"
			result.Checks = map[string]string{}
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			default:
				result.Status = "BUSY"
				w.WriteHeader(503)
				json.NewEncoder(w).Encode(result)
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), time.Second)
			defer cancel()
			var group sync.WaitGroup
			var mu sync.Mutex
			for n, check := range copied {
				group.Add(1)
				go func(name string, check HealthCheck) {
					defer group.Done()
					state := "READY"
					if check(ctx) != nil || ctx.Err() != nil {
						state = "UNAVAILABLE"
					}
					mu.Lock()
					result.Checks[name] = state
					mu.Unlock()
				}(n, check)
			}
			group.Wait()
			for _, state := range result.Checks {
				if state != "READY" {
					result.Status = "NOT_READY"
					code = 503
				}
			}
		}
		if r.URL.Path == "/readyz" && lifecycle.draining.Load() {
			result.Status, code = "DRAINING", http.StatusServiceUnavailable
		}
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(result)
	})
	lifecycle.next = handler
	return lifecycle, nil
}
