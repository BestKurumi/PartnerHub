package runtime

import (
	"context"
	"errors"
	"fmt"
	"partner-hub/internal/delivery"
	"partner-hub/internal/model"
	"partner-hub/internal/routing"
	"sync"
	"time"
)

type DeliveryExpiry interface {
	RecoverExpired(context.Context, int) (int, error)
}
type RouteExpiry interface {
	RecoverExpiredRoutes(context.Context, int) (int, error)
}
type RecoveryConfig struct {
	ObserveAttempt func(delivery.RecoveryClaim, model.SendResult, error)
	ObserveExpiry  func(string, int, error)
	Deliveries     delivery.RecoveryRepository
	Routes         routing.RecoveryRepository
	DeliveryExpiry DeliveryExpiry
	RouteExpiry    RouteExpiry
	Resolver       routing.TargetResolver
	Adapters       map[string]delivery.Adapter
	Budget         *delivery.PlatformBudget
	Platforms      map[string]delivery.BudgetConfig
	InstanceID     string
}
type RecoveryManager struct {
	config RecoveryConfig
	owner  string
}

func NewRecoveryManager(c RecoveryConfig) (*RecoveryManager, error) {
	if c.Deliveries == nil || c.Routes == nil || c.DeliveryExpiry == nil || c.RouteExpiry == nil || c.Resolver == nil || c.Budget == nil || c.InstanceID == "" || len(c.InstanceID) > 64 || len(c.Platforms) != 2 {
		return nil, errors.New("incomplete recovery configuration")
	}
	for _, r := range c.InstanceID {
		if r < 33 || r > 126 {
			return nil, errors.New("invalid recovery instance")
		}
	}
	platforms := make(map[string]delivery.BudgetConfig, 2)
	adapters := make(map[string]delivery.Adapter, 2)
	for _, p := range []string{"PLATFORM_A", "PLATFORM_B"} {
		cfg, ok := c.Platforms[p]
		if !ok || cfg.Recovery < 1 || cfg.Recovery > 64 || c.Adapters[p] == nil {
			return nil, errors.New("missing or unsafe recovery platform")
		}
		platforms[p] = cfg
		adapters[p] = c.Adapters[p]
	}
	c.Platforms, c.Adapters = platforms, adapters
	return &RecoveryManager{config: c, owner: c.InstanceID + "-" + model.NewID()}, nil
}
func (m *RecoveryManager) Run(ctx context.Context) error {
	return m.RunDraining(ctx, context.WithoutCancel(ctx))
}

// RunDraining separates stopping scans from the deadline of claimed work.
func (m *RecoveryManager) RunDraining(ctx, work context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var group sync.WaitGroup
	start := func(run func(context.Context) error) { group.Add(1); go func() { defer group.Done(); _ = run(ctx) }() }
	c := m.config
	for _, p := range []string{"PLATFORM_A", "PLATFORM_B"} {
		for i := 0; i < c.Platforms[p].Recovery; i++ {
			w := delivery.NewWorker(c.Deliveries, c.Adapters[p], p, fmt.Sprintf("%s-%s-%d", m.owner, p, i), c.Budget)
			w.Observe = c.ObserveAttempt
			start(func(scan context.Context) error { return w.RunDraining(scan, work) })
		}
	}
	routeWorker := routing.NewRecoveryWorker(c.Routes, c.Resolver, m.owner+"-route")
	start(func(scan context.Context) error { return routeWorker.RunDraining(scan, work) })
	start(func(ctx context.Context) error {
		return scanExpired(ctx, func(ctx context.Context, n int) (int, error) {
			count, err := c.DeliveryExpiry.RecoverExpired(ctx, n)
			if c.ObserveExpiry != nil {
				c.ObserveExpiry("delivery", count, err)
			}
			return count, err
		})
	})
	start(func(ctx context.Context) error {
		return scanExpired(ctx, func(ctx context.Context, n int) (int, error) {
			count, err := c.RouteExpiry.RecoverExpiredRoutes(ctx, n)
			if c.ObserveExpiry != nil {
				c.ObserveExpiry("route", count, err)
			}
			return count, err
		})
	})
	<-ctx.Done()
	group.Wait()
	return ctx.Err()
}
func scanExpired(ctx context.Context, run func(context.Context, int) (int, error)) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			_, _ = run(ctx, 100)
		}
	}
}
