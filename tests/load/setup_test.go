package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Catches accepting PUBLISHED as an applied relationship, and ignoring terminal conflicts.
func TestBindingWaitsForAuthorityAndRejectsConflict(t *testing.T) {
	for _, result := range []string{"APPLIED", "CONFLICT"} {
		t.Run(result, func(t *testing.T) {
			polls := 0
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Demo-Token") != "test-token" {
					t.Error("missing demo authentication")
				}
				if r.Method == "POST" {
					w.WriteHeader(202)
					json.NewEncoder(w).Encode(map[string]string{"eventId": "event", "status": "PUBLISHED"})
					return
				}
				polls++
				if polls == 1 {
					w.WriteHeader(404)
					json.NewEncoder(w).Encode(map[string]string{"code": "RECORD_NOT_AVAILABLE"})
					return
				}
				state := "PENDING"
				if polls > 1 {
					state = result
				}
				json.NewEncoder(w).Encode(map[string]string{"result": state})
			}))
			defer s.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			e := bind(ctx, s.Client(), s.URL, "test-token", "p1", "client", "uid")
			if polls < 2 || (result == "APPLIED" && e != nil) || (result != "APPLIED" && e == nil) {
				t.Fatalf("unconfirmed binding accepted: polls=%d err=%v", polls, e)
			}
		})
	}
}
