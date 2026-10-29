package delivery_test

import (
	"context"
	"testing"
	"time"

	"partner-hub/internal/delivery"
	"partner-hub/internal/model"
)

// Both the old claim scan and the new read-only probe use the same blocked DB
// boundary. Four idle scans must not reserve the entire platform rate burst.
type idleScanRepository struct {
	entered chan struct{}
	resume  <-chan struct{}
}

func (r *idleScanRepository) HasDue(ctx context.Context, _ string) (bool, error) {
	r.entered <- struct{}{}
	select {
	case <-r.resume:
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}
func (r *idleScanRepository) ClaimDue(ctx context.Context, _ string, _ string, _ int) ([]delivery.RecoveryClaim, error) {
	_, err := r.HasDue(ctx, "A")
	return nil, err
}
func (*idleScanRepository) SavePrepared(context.Context, delivery.RecoveryClaim, model.PreparedRequest) error {
	return nil
}
func (*idleScanRepository) Complete(context.Context, delivery.RecoveryClaim, model.SendResult) error {
	return nil
}

func TestIdleRecoveryScansDoNotDenyFirstDelivery(t *testing.T) {
	cfg := delivery.DefaultBudgetConfig()
	// Freeze replenishment so the outcome depends on admission, not scheduling.
	cfg.Rate = 0.001
	budget := newBudget(t, map[string]delivery.BudgetConfig{"A": cfg})
	resume := make(chan struct{})
	repo := &idleScanRepository{entered: make(chan struct{}, 4), resume: resume}
	adapter := &budgetBlockingAdapter{entered: make(chan string, 1)}
	done := make(chan error, 4)
	t.Cleanup(func() {
		close(resume)
		for i := 0; i < 4; i++ {
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(time.Second):
				t.Error("idle scan did not finish")
			}
		}
	})
	for i := 0; i < 4; i++ {
		go func() {
			_, err := delivery.NewWorker(repo, adapter, "A", "idle", budget).RunOnce(context.Background())
			done <- err
		}()
	}
	for i := 0; i < 4; i++ {
		select {
		case <-repo.entered:
		case <-time.After(time.Second):
			t.Fatal("idle scan did not reach database boundary")
		}
	}
	snapshot := delivery.NewFirstDelivery(map[string]delivery.Adapter{"A": adapter}, budget).
		Execute(context.Background(), model.TradeEvent{}, budgetRoute("A"))
	if got := snapshot.Attempts[0].Result; !got.SendStarted || got.Outcome != model.Success {
		t.Fatalf("idle database scans denied first delivery: %+v", got)
	}
}
