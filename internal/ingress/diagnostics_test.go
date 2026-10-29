package ingress_test

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"partner-hub/internal/diagnostics"
	"partner-hub/internal/ingress"
	"partner-hub/internal/model"
)

// Rejection must be observable without calling the processor, trusting partial
// JSON IDs, re-reading the body, or reflecting the incoming request identifier.
func TestIngressDiagnosticsRejectedAndUnconfirmed(t *testing.T) {
	normal, _ := os.ReadFile("../../tests/fixtures/trades/normal.json")
	for _, tt := range []struct {
		name, body, kind string
		code, calls      int
		processError     error
	}{
		{"invalid", `{"msgId":"secret-id","traceId":"secret-trace"}`, "ingress_invalid", 400, 0, nil},
		{"oversize", strings.Repeat(" ", 65537), "ingress_body_too_large", 413, 0, nil},
		{"unconfirmed", string(normal), "ingress_unconfirmed", 503, 1, errors.New("secret-driver-error")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := diagnostics.NewBuffer("hub-1")
			calls := 0
			h := ingress.NewHTTPWithObserver(processorFunc(func(_ context.Context, tr model.TradeEvent) (model.TradeResponse, error) {
				calls++
				return model.TradeResponse{MsgID: tr.MsgID, Status: "DEPENDENCY_UNAVAILABLE"}, tt.processError
			}), ingress.GateConfig{}, diagnostics.ObserveIngress(b))
			r := httptest.NewRequest("POST", "/api/v1/trades", strings.NewReader(tt.body))
			r.Header.Set("X-Request-ID", "secret-incoming-id")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			s := b.Snapshot(diagnostics.Query{})
			if w.Code != tt.code || calls != tt.calls || len(s.Items) != 1 || s.Items[0].Kind != tt.kind {
				t.Fatalf("code=%d calls=%d events=%+v", w.Code, calls, s.Items)
			}
			if w.Header().Get("X-Request-ID") == "" || w.Header().Get("X-Request-ID") == "secret-incoming-id" || s.Items[0].RequestID != w.Header().Get("X-Request-ID") {
				t.Fatal("server request ID missing")
			}
			if tt.calls == 0 && (s.Items[0].MsgID != "" || s.Items[0].TraceID != "") {
				t.Fatal("untrusted IDs retained")
			}
			if tt.calls == 1 && (s.Items[0].MsgID == "" || s.Items[0].TraceID == "") {
				t.Fatal("trusted IDs missing")
			}
		})
	}
}

type readCounter struct{ reads int }

func (r *readCounter) Read([]byte) (int, error) { r.reads++; return 0, errors.New("secret-read-error") }
func (*readCounter) Close() error               { return nil }

func TestIngressDiagnosticsDoesNotReadAgainAndObserverCannotBreakBusiness(t *testing.T) {
	body := &readCounter{}
	b := diagnostics.NewBuffer("hub-1")
	h := ingress.NewHTTPWithObserver(processorFunc(func(context.Context, model.TradeEvent) (model.TradeResponse, error) {
		t.Fatal("processed invalid body")
		return model.TradeResponse{}, nil
	}), ingress.GateConfig{}, diagnostics.ObserveIngress(b))
	r := httptest.NewRequest("POST", "/api/v1/trades", nil)
	r.Body = body
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 400 || body.reads != 1 {
		t.Fatalf("code=%d reads=%d", w.Code, body.reads)
	}
	h = ingress.NewHTTPWithObserver(nil, ingress.GateConfig{}, func(ingress.HTTPObservation) { panic("observer failure") })
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/trades", io.NopCloser(strings.NewReader("{}"))))
	if w.Code != 400 {
		t.Fatal("optional observer changed rejection")
	}
}

func TestIngressDiagnosticsRateLimitAndAbnormalFirstRound(t *testing.T) {
	normal, _ := os.ReadFile("../../tests/fixtures/trades/normal.json")
	b := diagnostics.NewBuffer("hub-1")
	h := ingress.NewHTTPWithObserver(processorFunc(func(context.Context, model.TradeEvent) (model.TradeResponse, error) {
		return model.TradeResponse{Status: "RECOVERY_PENDING", PlatformResults: []model.PlatformResult{{PlatformID: "PLATFORM_B", DeliveryID: "delivery-1", FirstOutcome: model.Unknown}}}, nil
	}), ingress.GateConfig{Rate: 0.001, Burst: 1}, diagnostics.ObserveIngress(b))
	if w := requestTrade(h, string(normal)); w.Code != 202 {
		t.Fatal(w.Code)
	}
	if w := requestTrade(h, string(normal)); w.Code != 429 {
		t.Fatal(w.Code)
	}
	s := b.Snapshot(diagnostics.Query{})
	if len(s.Items) != 2 || s.Items[0].Kind != "ingress_rate_limited" || s.Items[1].Kind != "ingress_first_abnormal" || s.Items[1].Platform != "PLATFORM_B" || s.Items[1].Outcome != "UNKNOWN" {
		t.Fatalf("events=%+v", s.Items)
	}
}
