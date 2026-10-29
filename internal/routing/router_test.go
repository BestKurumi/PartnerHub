package routing_test

import (
	"context"
	"partner-hub/internal/model"
	"partner-hub/internal/routing"
	"testing"
	"time"
)

type bindings struct{ s model.BindingSnapshot }

func (b *bindings) Resolve(context.Context, string) (model.BindingSnapshot, error) { return b.s, nil }

// Later cache changes must not mutate the already determined UID and task ID.
func TestFixedRoute(t *testing.T) {
	b := &bindings{s: model.BindingSnapshot{Version: 1, ObservedAt: time.Now(), Targets: []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: "00001"}, {PlatformID: "PLATFORM_B", PlatformUID: "00002"}}}}
	r, err := routing.NewRouter(b).ResolveTargets(context.Background(), model.TradeEvent{MsgID: "message", ClientID: "customer"})
	b.s.Targets[0].PlatformUID = "changed"
	if err != nil || r.Status != "ROUTED" || len(r.Targets) != 2 || r.Targets[0].PlatformUID != "00001" || r.Targets[0].DeliveryID == "" || r.Targets[0].DeliveryID == r.Targets[1].DeliveryID {
		t.Fatalf("route not fixed: %+v %v", r, err)
	}
}
