package delivery_test

import (
	"context"
	"partner-hub/internal/delivery"
	"partner-hub/internal/model"
	"testing"
)

func TestRecoveryObservationMatchesDurableSuccess(t *testing.T) {
	r, a, _, _ := workerFixture(t)
	w := delivery.NewWorker(r, a, "platform1", "worker-one")
	called := 0
	w.Observe = func(c delivery.RecoveryClaim, s model.SendResult, err error) {
		called++
		if s.Outcome != model.Success || !s.SendStarted || err != nil {
			t.Errorf("incorrect outcome: %+v %v", s, err)
		}
	}
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("claimed execution observation missing: %d", called)
	}
}
