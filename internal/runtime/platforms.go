package runtime

import (
	"encoding/json"
	"net/http"
	"partner-hub/internal/delivery"
)

func NewPlatformStatusHandler(configs map[string]delivery.BudgetConfig, next http.Handler) http.Handler {
	type platformStatus struct {
		PlatformID string  `json:"platformId"`
		State      string  `json:"state"`
		Total      int     `json:"totalConcurrency"`
		First      int     `json:"firstConcurrency"`
		Recovery   int     `json:"recoveryConcurrency"`
		Rate       float64 `json:"ratePerSecond"`
		Burst      int     `json:"burst"`
	}
	platforms := make([]platformStatus, 0, 2)
	for _, p := range []string{"PLATFORM_A", "PLATFORM_B"} {
		b, ok := configs[p]
		if !ok {
			continue
		}
		state := "RUNNING"
		if b.Paused {
			state = "PAUSED"
		}
		platforms = append(platforms, platformStatus{p, state, b.Total, b.First, b.Recovery, b.Rate, b.Burst})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/runtime" {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if r.Method != "GET" {
			w.Header().Set("Allow", "GET")
			w.WriteHeader(405)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": "METHOD_NOT_ALLOWED"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"platforms": platforms, "scope": "INSTANCE_CONFIG"})
	})
}
