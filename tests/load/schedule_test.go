package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"partner-hub/internal/ingress"
	"sync/atomic"
	"testing"
	"time"
)

// Catches turning the open-loop offered load into a blocking queue and silently retrying timeouts.
func TestSchedulePreservesRejectedSlotsAndUniqueValidPayloads(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		trade, e := ingress.Decode(r.Body)
		if e != nil {
			t.Error(e)
		}
		time.Sleep(60 * time.Millisecond)
		w.WriteHeader(202)
		json.NewEncoder(w).Encode(map[string]string{"msgId": trade.MsgID, "acceptance": "ACCEPTED", "status": "RECOVERY_PENDING"})
	}))
	defer server.Close()
	rows := makeInputs(10, "test-client")
	runSchedule(context.Background(), server.Client(), server.URL, rows, 5*time.Millisecond, 1)
	rejects := 0
	seen := map[string]bool{}
	for _, r := range rows {
		if seen[r.MsgID] || r.MsgID == "" || r.TraceID == "" {
			t.Fatal("input identity reused")
		}
		seen[r.MsgID] = true
		if r.Disposition == "local_rejected" {
			rejects++
		}
	}
	if rejects < 8 || count.Load() != int32(10-rejects) {
		t.Fatalf("offered slots erased or retried: rejected=%d sent=%d", rejects, count.Load())
	}
}
func TestScheduleReportsHTTPAndTransportFailures(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(429) }))
	rows := makeInputs(1, "client")
	runSchedule(context.Background(), server.Client(), server.URL, rows, time.Millisecond, 1)
	if rows[0].Disposition != "http_rejected" || rows[0].HTTPStatus != 429 {
		t.Fatalf("rejection hidden: %+v", rows[0])
	}
	server.Close()
	rows = makeInputs(1, "client")
	runSchedule(context.Background(), server.Client(), server.URL, rows, time.Millisecond, 1)
	if rows[0].Disposition != "transport_error" {
		t.Fatal("uncertain request hidden")
	}
	var payload map[string]any
	if json.Unmarshal(tradePayload(rows[0]), &payload) != nil || len(payload) != 20 {
		t.Fatal("invalid twenty field fixture")
	}
}

func TestSuccessfulHTTPWithWrongIdentityOrMalformedBodyIsUnconfirmed(t *testing.T) {
	for _, body := range []string{`{}`, `<html>`, `{"msgId":"other","acceptance":"COMPLETED","status":"SUCCEEDED"}`} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(body)) }))
		rows := makeInputs(1, "client")
		runSchedule(context.Background(), s.Client(), s.URL, rows, time.Millisecond, 1)
		s.Close()
		if rows[0].Disposition != "invalid_response" || summarize(rows).InvalidResponses != 1 {
			t.Fatalf("false confirmation: %+v", rows[0])
		}
	}
}

func TestServerErrorIsUnconfirmedAndRetainsFailureStatus(t *testing.T) {
	for _, status := range []int{500, 503} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			w.Write([]byte(`{"acceptance":"UNCONFIRMED"}`))
		}))
		rows := makeInputs(1, "client")
		runSchedule(context.Background(), s.Client(), s.URL, rows, time.Millisecond, 1)
		s.Close()
		if rows[0].Disposition != "http_unconfirmed" || rows[0].HTTPStatus != status || summarize(rows).HTTPUnconfirmed != 1 {
			t.Fatalf("5xx falsely treated as before-processing rejection: %+v", rows[0])
		}
	}
}
