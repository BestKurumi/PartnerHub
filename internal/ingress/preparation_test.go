package ingress_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"partner-hub/internal/adapter"
	"partner-hub/internal/delivery"
	"partner-hub/internal/ingress"
	"partner-hub/internal/model"
	"partner-hub/internal/routing"
)

// A deterministic adapter validation failure must be handed to a human on its
// first occurrence, rather than accepted for automatic delivery retries.
func TestFirstPreparationFailureRequiresAttentionWithoutSending(t *testing.T) {
	var sends atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sends.Add(1)
		_, _ = io.WriteString(w, `{"code":0}`)
	}))
	defer server.Close()
	ad, err := adapter.NewPlatform1(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	trade := tradeFixture(t)
	// Exercise the defensive boundary after ingress validation, as with an
	// incompatible persisted notification. The real adapter rejects the enum.
	trade.TradeType = "UNSUPPORTED"
	bindings := &resolvedBindings{snapshot: model.BindingSnapshot{Version: 1, ObservedAt: time.Now(), Targets: []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: "fixed-uid"}}}}
	archive := &stageArchive{}
	handoff := &handoffRepo{}
	service := ingress.NewService(routing.NewRouter(bindings), delivery.NewFirstDelivery(map[string]delivery.Adapter{"PLATFORM_A": ad}), archive, handoff)
	result, err := service.Process(context.Background(), trade)
	if err != nil || result.Status != "NEEDS_ATTENTION" || result.Acceptance != "ACCEPTED" || result.RecordStatus != "PERSISTED" {
		t.Fatalf("invalid notification entered automatic recovery: %+v %v", result, err)
	}
	if len(result.PlatformResults) != 1 || result.PlatformResults[0].FirstOutcome != model.PermanentFailure || result.PlatformResults[0].CurrentState != "NEEDS_ATTENTION" {
		t.Fatalf("permanent first outcome not exposed: %+v", result.PlatformResults)
	}
	if sends.Load() != 0 || archive.calls != 0 || handoff.calls != 1 || len(handoff.snapshot.Attempts) != 1 {
		t.Fatalf("wrong responsibility path: sends=%d archive=%d handoff=%d", sends.Load(), archive.calls, handoff.calls)
	}
	a := handoff.snapshot.Attempts[0]
	if a.Result.Outcome != model.PermanentFailure || a.Result.SendStarted || a.Target.PlatformUID != "fixed-uid" || a.Result.StartedAt.IsZero() || a.Result.FinishedAt.Before(a.Result.StartedAt) {
		t.Fatalf("incomplete unsent permanent attempt: %+v", a)
	}
}
