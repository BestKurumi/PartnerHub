package ingress_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"partner-hub/internal/ingress"
	"partner-hub/internal/model"
)

type processorFunc func(context.Context, model.TradeEvent) (model.TradeResponse, error)

func (f processorFunc) Process(ctx context.Context, t model.TradeEvent) (model.TradeResponse, error) {
	return f(ctx, t)
}
func requestTrade(h http.Handler, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/api/v1/trades", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestTradeHTTP(t *testing.T) {
	body, err := os.ReadFile("../../tests/fixtures/trades/normal.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, body string
		err        error
		code       int
	}{
		{"completed", string(body), nil, 200}, {"unconfirmed", string(body), ingress.ErrUnconfirmed, 503},
		{"invalid", "{}", nil, 400}, {"oversize", strings.Repeat(" ", 65537), nil, 413},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls int
			h := ingress.NewHTTP(processorFunc(func(ctx context.Context, trade model.TradeEvent) (model.TradeResponse, error) {
				calls++
				return model.TradeResponse{MsgID: trade.MsgID, Status: "SUCCEEDED", Acceptance: "COMPLETED", RecordStatus: "PENDING", PlatformResults: []model.PlatformResult{}}, tt.err
			}), ingress.GateConfig{})
			w := requestTrade(h, tt.body)
			if w.Code != tt.code {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			expected := 0
			if tt.code == 200 || tt.code == 503 {
				expected = 1
			}
			if calls != expected {
				t.Fatalf("side effects=%d", calls)
			}
			var result map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if tt.code == 400 || tt.code == 413 {
				if result["status"] != "REJECTED" || result["acceptance"] != "REJECTED" {
					t.Fatalf("rejection=%v", result)
				}
			}
		})
	}
}
func TestTradeHTTPInFlight(t *testing.T) {
	body, _ := os.ReadFile("../../tests/fixtures/trades/normal.json")
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	h := ingress.NewHTTP(processorFunc(func(context.Context, model.TradeEvent) (model.TradeResponse, error) {
		calls.Add(1)
		close(entered)
		<-release
		return model.TradeResponse{}, nil
	}), ingress.GateConfig{MaxInFlight: 1, Rate: 200, Burst: 40})
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- requestTrade(h, string(body)) }()
	// A stub handler must fail without waiting for the unimplemented processor.
	select {
	case <-entered:
	case w := <-done:
		t.Fatalf("processor not entered: %d", w.Code)
	}
	w := requestTrade(h, string(body))
	close(release)
	<-done
	if w.Code != 429 || w.Header().Get("Retry-After") == "" || calls.Load() != 1 {
		t.Fatalf("unbounded entry: code=%d calls=%d", w.Code, calls.Load())
	}
}
func TestTradeHTTPRate(t *testing.T) {
	body, _ := os.ReadFile("../../tests/fixtures/trades/normal.json")
	var calls int
	h := ingress.NewHTTP(processorFunc(func(context.Context, model.TradeEvent) (model.TradeResponse, error) {
		calls++
		return model.TradeResponse{}, errors.New("unconfirmed")
	}), ingress.GateConfig{MaxInFlight: 64, Rate: 0.001, Burst: 1})
	if w := requestTrade(h, string(body)); w.Code != 503 {
		t.Fatalf("first=%d", w.Code)
	}
	if w := requestTrade(h, string(body)); w.Code != 429 || w.Header().Get("Retry-After") == "" || calls != 1 {
		t.Fatalf("rate rejection=%d calls=%d", w.Code, calls)
	}
}
