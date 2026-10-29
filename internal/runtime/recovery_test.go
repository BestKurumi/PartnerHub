package runtime_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"partner-hub/internal/delivery"
	"partner-hub/internal/model"
	"partner-hub/internal/routing"
	hubruntime "partner-hub/internal/runtime"
)

type source struct{ a, b, route, deliveryExpiry, routeExpiry atomic.Int32 }

func (*source) HasDue(context.Context, string) (bool, error) { return true, nil }

func (s *source) ClaimDue(_ context.Context, p, owner string, limit int) ([]delivery.RecoveryClaim, error) {
	if p == "PLATFORM_A" {
		s.a.Add(1)
	} else {
		s.b.Add(1)
	}
	return nil, nil
}
func (s *source) SavePrepared(context.Context, delivery.RecoveryClaim, model.PreparedRequest) error {
	return errors.New("unexpected prepare")
}
func (s *source) Complete(context.Context, delivery.RecoveryClaim, model.SendResult) error {
	return errors.New("unexpected complete")
}
func (s *source) ClaimRoutes(context.Context, string, int) ([]routing.RouteRecoveryClaim, error) {
	s.route.Add(1)
	return nil, nil
}
func (s *source) CompleteRoute(context.Context, routing.RouteRecoveryClaim, model.RouteSnapshot, error) error {
	return errors.New("unexpected route completion")
}
func (s *source) RecoverExpired(context.Context, int) (int, error) {
	s.deliveryExpiry.Add(1)
	return 0, nil
}
func (s *source) RecoverExpiredRoutes(context.Context, int) (int, error) {
	s.routeExpiry.Add(1)
	return 0, nil
}
func (s *source) ResolveTargets(context.Context, model.TradeEvent) (model.RouteSnapshot, error) {
	return model.RouteSnapshot{}, errors.New("unexpected lookup")
}

type unusedAdapter struct{}

func (unusedAdapter) Prepare(model.TradeNotification, string) (model.PreparedRequest, error) {
	return model.PreparedRequest{}, errors.New("unexpected prepare")
}
func (unusedAdapter) Send(context.Context, model.PreparedRequest) model.SendResult {
	panic("unexpected send")
}
func managerConfig(t *testing.T, s *source) hubruntime.RecoveryConfig {
	t.Helper()
	cfg := delivery.DefaultBudgetConfig()
	cfg.Recovery = 1
	ps := map[string]delivery.BudgetConfig{"PLATFORM_A": cfg, "PLATFORM_B": cfg}
	b, err := delivery.NewPlatformBudget(ps)
	if err != nil {
		t.Fatal(err)
	}
	return hubruntime.RecoveryConfig{Deliveries: s, Routes: s, DeliveryExpiry: s, RouteExpiry: s, Resolver: s, Adapters: map[string]delivery.Adapter{"PLATFORM_A": unusedAdapter{}, "PLATFORM_B": unusedAdapter{}}, Budget: b, Platforms: ps, InstanceID: "hub-test"}
}
func TestRecoveryManagerRunsAllSourcesAndStops(t *testing.T) {
	s := &source{}
	m, err := hubruntime.NewRecoveryManager(managerConfig(t, s))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	timer := time.NewTimer(1800 * time.Millisecond)
	defer timer.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
loop:
	for {
		select {
		case err := <-done:
			t.Fatalf("recovery did not stay active: %v", err)
		case <-timer.C:
			t.Fatalf("missing sources A=%d B=%d route=%d dExpiry=%d rExpiry=%d", s.a.Load(), s.b.Load(), s.route.Load(), s.deliveryExpiry.Load(), s.routeExpiry.Load())
		case <-tick.C:
			if s.a.Load() > 0 && s.b.Load() > 0 && s.route.Load() > 0 && s.deliveryExpiry.Load() > 0 && s.routeExpiry.Load() > 0 {
				break loop
			}
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("manager did not join its workers")
	}
	counts := [5]int32{s.a.Load(), s.b.Load(), s.route.Load(), s.deliveryExpiry.Load(), s.routeExpiry.Load()}
	time.Sleep(120 * time.Millisecond)
	if counts != [5]int32{s.a.Load(), s.b.Load(), s.route.Load(), s.deliveryExpiry.Load(), s.routeExpiry.Load()} {
		t.Fatal("scanner continued after manager exited")
	}
}
func TestRecoveryManagerRejectsMissingDependencies(t *testing.T) {
	c := managerConfig(t, &source{})
	delete(c.Adapters, "PLATFORM_B")
	if _, err := hubruntime.NewRecoveryManager(c); err == nil {
		t.Fatal("missing registered adapter accepted")
	}
	c = managerConfig(t, &source{})
	c.Platforms["PLATFORM_A"] = delivery.BudgetConfig{Recovery: 1000000}
	if _, err := hubruntime.NewRecoveryManager(c); err == nil {
		t.Fatal("unbounded workers accepted")
	}
}
