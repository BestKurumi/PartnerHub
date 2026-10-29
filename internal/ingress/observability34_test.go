package ingress_test

import (
	"bytes"
	"context"
	"github.com/gin-gonic/gin"
	"net/http/httptest"
	"os"
	"partner-hub/internal/delivery"
	"partner-hub/internal/ingress"
	"partner-hub/internal/model"
	"strings"
	"testing"
	"time"
)

type observeRouter34 struct{}

func (observeRouter34) ResolveTargets(context.Context, model.TradeEvent) (model.RouteSnapshot, error) {
	time.Sleep(20 * time.Millisecond)
	return model.RouteSnapshot{Status: "ROUTED", Targets: []model.RouteTarget{{PlatformID: "PLATFORM_A"}}}, nil
}
func TestFirstObservationIncludesRoutingAndNotSent(t *testing.T) {
	s := ingress.NewService(observeRouter34{}, delivery.NewFirstDelivery(nil), &stageArchive{})
	called := 0
	s.ObserveFirst = func(snapshot model.FirstRoundSnapshot) {
		called++
		if len(snapshot.Attempts) != 1 || snapshot.Attempts[0].Result.SendStarted || snapshot.Attempts[0].Result.FinishedAt.Sub(snapshot.ReceivedAt) < 20*time.Millisecond {
			t.Error("incomplete first observation")
		}
	}
	_, _ = s.Process(context.Background(), tradeFixture(t))
	if called != 1 {
		t.Fatalf("first result observer missing: %d", called)
	}
}

func TestTradePanicDoesNotDumpSensitivePanicOrHeaders(t *testing.T) {
	var stderr bytes.Buffer
	old := gin.DefaultErrorWriter
	gin.DefaultErrorWriter = &stderr
	h := ingress.NewHTTP(processorFunc(func(context.Context, model.TradeEvent) (model.TradeResponse, error) {
		panic("secret-full-trade-amount")
	}), ingress.GateConfig{})
	gin.DefaultErrorWriter = old
	body, err := os.ReadFile("../../tests/fixtures/trades/normal.json")
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/api/v1/trades", strings.NewReader(string(body)))
	r.Header.Set("X-Demo-Token", "secret-demo-token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 500 {
		t.Fatal(w.Code)
	}
	for _, secret := range []string{"secret-full-trade-amount", "secret-demo-token"} {
		if strings.Contains(stderr.String(), secret) {
			t.Errorf("panic recovery bypasses safe logger: %s", secret)
		}
	}
}
