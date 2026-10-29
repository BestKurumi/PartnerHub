package routing

import (
	"context"
	"partner-hub/internal/binding"
	"partner-hub/internal/model"
	"time"
)

type Resolver interface {
	Resolve(context.Context, string) (model.BindingSnapshot, error)
}
type Router struct{ resolver Resolver }

func NewRouter(r Resolver) *Router { return &Router{resolver: r} }
func (r *Router) ResolveTargets(ctx context.Context, trade model.TradeEvent) (model.RouteSnapshot, error) {
	snapshot, err := r.resolver.Resolve(ctx, trade.ClientID)
	if err != nil || !binding.ValidSnapshot(snapshot, time.Now()) {
		return model.RouteSnapshot{}, binding.ErrUnavailable
	}
	route := model.RouteSnapshot{MsgID: trade.MsgID, Status: "NO_ROUTE", ResolvedAt: time.Now().UTC(), Targets: make([]model.RouteTarget, 0, len(snapshot.Targets))}
	for _, target := range snapshot.Targets {
		route.Targets = append(route.Targets, model.RouteTarget{PlatformID: target.PlatformID, PlatformUID: target.PlatformUID, DeliveryID: model.NewID()})
	}
	if len(route.Targets) != 0 {
		route.Status = "ROUTED"
	}
	return route, nil
}
