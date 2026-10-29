package delivery

import (
	"context"
	"partner-hub/internal/model"
	"sync"
	"time"
)

type FirstDelivery struct {
	adapters map[string]Adapter
	budget   *PlatformBudget
}

func NewFirstDelivery(adapters map[string]Adapter, budgets ...*PlatformBudget) *FirstDelivery {
	copyAdapters := make(map[string]Adapter, len(adapters))
	for platform, adapter := range adapters {
		copyAdapters[platform] = adapter
	}
	f := &FirstDelivery{adapters: copyAdapters}
	if len(budgets) > 0 {
		f.budget = budgets[0]
	}
	return f
}
func (f *FirstDelivery) Execute(ctx context.Context, trade model.TradeEvent, route model.RouteSnapshot) model.FirstRoundSnapshot {
	snapshot := model.FirstRoundSnapshot{Trade: trade, Route: route, Attempts: make([]model.FirstAttempt, len(route.Targets)), ArchiveRevision: model.NewID()}
	notification := model.TradeNotification{SecCode: trade.SecCode, SecName: trade.SecName, Side: trade.Side, Market: trade.Market, TradeType: trade.TradeType, TraceID: trade.TraceID, ExecTime: trade.ExecTime}
	var workers sync.WaitGroup
	for i, target := range route.Targets {
		workers.Add(1)
		go func(i int, target model.RouteTarget) {
			defer workers.Done()
			attempt := model.FirstAttempt{Target: target, Result: model.SendResult{Outcome: model.RetryableFailure, StartedAt: time.Now().UTC(), Summary: "adapter unavailable"}}
			if f.budget != nil {
				permit, ok := f.budget.TryAcquire(ctx, target.PlatformID, FirstClass)
				if !ok {
					attempt.Result.Summary = "platform execution unavailable"
					attempt.Result.FinishedAt = time.Now().UTC()
					snapshot.Attempts[i] = attempt
					return
				}
				defer permit.Release()
			}
			if adapter := f.adapters[target.PlatformID]; adapter != nil {
				prepared, err := adapter.Prepare(notification, target.PlatformUID)
				if err == nil {
					attempt.Request = prepared
					send, cancel := context.WithTimeout(ctx, 2*time.Second)
					attempt.Result = adapter.Send(send, prepared)
					cancel()
				} else {
					attempt.Result.Outcome = model.PermanentFailure
					attempt.Result.Summary = "request preparation failed"
				}
			}
			if attempt.Result.FinishedAt.IsZero() {
				attempt.Result.FinishedAt = time.Now().UTC()
			}
			snapshot.Attempts[i] = attempt
		}(i, target)
	}
	workers.Wait()
	return snapshot
}
