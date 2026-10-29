package delivery

import (
	"context"
	"fmt"
	"time"

	"partner-hub/internal/model"
)

// Worker executes one persistent recovery lease at a time. Platform execution
// capacity is acquired before ClaimDue; no claimed queue is kept.
type Worker struct {
	Observe         func(RecoveryClaim, model.SendResult, error)
	repository      RecoveryRepository
	adapter         Adapter
	platform, owner string
	budget          *PlatformBudget
}

func NewWorker(repository RecoveryRepository, adapter Adapter, platform, owner string, budgets ...*PlatformBudget) *Worker {
	w := &Worker{repository: repository, adapter: adapter, platform: platform, owner: owner}
	if len(budgets) > 0 {
		w.budget = budgets[0]
	}
	return w
}

// RunOnce returns the number of claimed attempts (zero or one), including a
// claimed attempt left for lease recovery after a persistence error.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	return w.runOnce(ctx, ctx)
}

func (w *Worker) runOnce(ctx, work context.Context) (claimed int, returnedErr error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var permit *BudgetPermit
	if w.budget != nil {
		// Idle database scans must not reserve rate tokens needed by first
		// delivery. The hint may race; the budget still precedes the real claim.
		due, err := w.repository.HasDue(ctx, w.platform)
		if err != nil {
			return 0, fmt.Errorf("probe recovery: %w", err)
		}
		if !due {
			return 0, nil
		}
		var ok bool
		permit, ok = w.budget.TryAcquire(ctx, w.platform, RecoveryClass)
		if !ok {
			return 0, nil
		}
		// Until a task is claimed this is only an unused scan reservation.
		defer permit.Cancel()
	}
	claims, err := w.repository.ClaimDue(ctx, w.platform, w.owner, 1)
	if err != nil {
		return 0, fmt.Errorf("claim recovery: %w", err)
	}
	if len(claims) == 0 {
		return 0, nil
	}
	// A claimed attempt counts as a scheduling admission even when preparation,
	// persistence, cancellation or a lease deadline prevents the network effect.
	// This defer runs first, making the earlier Cancel a no-op on claimed exits.
	defer permit.Release()
	claim := claims[0]
	var observed model.SendResult
	defer func() {
		if w.Observe != nil {
			w.Observe(claim, observed, returnedErr)
		}
	}()
	leaseDeadline := claim.LeaseDeadline
	if err := work.Err(); err != nil {
		return 1, err
	}
	if !time.Now().Before(claim.LeaseDeadline) {
		return 1, ErrLeaseLost
	}
	deadline := time.Now().Add(8 * time.Second)
	if claim.LeaseDeadline.Before(deadline) {
		deadline = claim.LeaseDeadline
	}
	ctx, finish := context.WithDeadline(work, deadline)
	defer finish()
	if err := ctx.Err(); err != nil {
		return 1, err
	}
	if !time.Now().Before(leaseDeadline) {
		return 1, ErrLeaseLost
	}
	startedAt := time.Now().UTC()
	prepared, err := w.adapter.Prepare(claim.Notification, claim.Target.PlatformUID)
	if err != nil {
		result := model.SendResult{Outcome: model.PermanentFailure, StartedAt: startedAt, FinishedAt: time.Now().UTC(), Summary: "request preparation failed"}
		observed = result
		if err := w.repository.Complete(ctx, claim, result); err != nil {
			return 1, fmt.Errorf("complete preparation failure: %w", err)
		}
		return 1, nil
	}
	// SavePrepared validates the current owner/version and database-time lease;
	// a failed or uncertain write must never permit a network effect.
	if err := w.repository.SavePrepared(ctx, claim, prepared); err != nil {
		return 1, fmt.Errorf("save recovery request: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return 1, err
	}
	if !time.Now().Before(leaseDeadline) {
		return 1, ErrLeaseLost
	}
	deadline = time.Now().Add(2 * time.Second)
	if leaseDeadline.Before(deadline) {
		deadline = leaseDeadline
	}
	send, cancel := context.WithDeadline(ctx, deadline)
	result := w.adapter.Send(send, prepared)
	cancel()
	if result.StartedAt.IsZero() {
		result.StartedAt = startedAt
	}
	if result.FinishedAt.IsZero() {
		result.FinishedAt = time.Now().UTC()
	}
	observed = result
	if err := w.repository.Complete(ctx, claim, result); err != nil {
		return 1, fmt.Errorf("complete recovery: %w", err)
	}
	return 1, nil
}

// Run scans serially with a bounded delay after every result, including errors.
// Durable state remains authoritative when a database operation fails.
func (w *Worker) Run(ctx context.Context) error {
	return w.RunDraining(ctx, context.WithoutCancel(ctx))
}

// RunDraining stops new claims with scan; work bounds already claimed execution.
func (w *Worker) RunDraining(ctx, work context.Context) error {
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
