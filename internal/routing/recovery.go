package routing

import (
	"context"
	"fmt"
	"time"

	"partner-hub/internal/delivery"
	"partner-hub/internal/model"
)

// RouteRecoveryClaim contains only the identity needed to resolve bindings.
type RouteRecoveryClaim struct {
	MsgID, ClientID, Owner           string
	LeaseVersion                     uint64
	LeaseUntil, LeaseDeadline        time.Time
	AttemptNo, CycleNo, CycleAttempt int
}

type RecoveryRepository interface {
	ClaimRoutes(context.Context, string, int) ([]RouteRecoveryClaim, error)
	CompleteRoute(context.Context, RouteRecoveryClaim, model.RouteSnapshot, error) error
}

type TargetResolver interface {
	ResolveTargets(context.Context, model.TradeEvent) (model.RouteSnapshot, error)
}

type RecoveryWorker struct {
	repository RecoveryRepository
	resolver   TargetResolver
	owner      string
}

func NewRecoveryWorker(repository RecoveryRepository, resolver TargetResolver, owner string) *RecoveryWorker {
	return &RecoveryWorker{repository: repository, resolver: resolver, owner: owner}
}

// RunOnce resolves only after one persisted lease. Failure to save the result
// leaves the original lease for recovery; it never resolves a second time here.
func (w *RecoveryWorker) RunOnce(ctx context.Context) (int, error) {
	return w.runOnce(ctx, ctx)
}

func (w *RecoveryWorker) runOnce(ctx, work context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	claims, err := w.repository.ClaimRoutes(ctx, w.owner, 1)
	if err != nil {
		return 0, fmt.Errorf("claim route recovery: %w", err)
	}
	if len(claims) == 0 {
		return 0, nil
	}
	c := claims[0]
	if err := work.Err(); err != nil {
		return 1, err
	}
	if !time.Now().Before(c.LeaseDeadline) {
		return 1, delivery.ErrLeaseLost
	}
	deadline := time.Now().Add(8 * time.Second)
	if c.LeaseDeadline.Before(deadline) {
		deadline = c.LeaseDeadline
	}
	ctx, finish := context.WithDeadline(work, deadline)
	defer finish()
	if err := ctx.Err(); err != nil {
		return 1, err
	}
	if !time.Now().Before(c.LeaseDeadline) {
		return 1, delivery.ErrLeaseLost
	}
	deadline = time.Now().Add(time.Second)
	if c.LeaseDeadline.Before(deadline) {
		deadline = c.LeaseDeadline
	}
	query, cancel := context.WithDeadline(ctx, deadline)
	snapshot, lookupErr := w.resolver.ResolveTargets(query, model.TradeEvent{MsgID: c.MsgID, ClientID: c.ClientID})
	cancel()
	if err := ctx.Err(); err != nil {
		return 1, err
	}
	if !time.Now().Before(c.LeaseDeadline) {
		return 1, delivery.ErrLeaseLost
	}
	if lookupErr != nil {
		snapshot = model.RouteSnapshot{}
	}
	if err = w.repository.CompleteRoute(ctx, c, snapshot, lookupErr); err != nil {
		return 1, fmt.Errorf("complete route recovery: %w", err)
	}
	return 1, nil
}

// Run does not keep a local claim queue, and backs off even after errors.
func (w *RecoveryWorker) Run(ctx context.Context) error {
	return w.RunDraining(ctx, context.WithoutCancel(ctx))
}

// RunDraining stops new claims with scan; work bounds already claimed execution.
func (w *RecoveryWorker) RunDraining(ctx, work context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, _ = w.runOnce(ctx, work)
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
