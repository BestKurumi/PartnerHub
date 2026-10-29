package delivery_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"partner-hub/internal/delivery"
	"partner-hub/internal/model"
)

// These boundaries replace external database/network operations, while the real
// Worker decides when a persisted attempt is allowed to cause a network effect.
type workerRepository struct {
	claim    func(context.Context, string, string, int) ([]delivery.RecoveryClaim, error)
	save     func(context.Context, delivery.RecoveryClaim, model.PreparedRequest) error
	complete func(context.Context, delivery.RecoveryClaim, model.SendResult) error
}

func (*workerRepository) HasDue(context.Context, string) (bool, error) { return true, nil }

func (r *workerRepository) ClaimDue(ctx context.Context, p, o string, n int) ([]delivery.RecoveryClaim, error) {
	return r.claim(ctx, p, o, n)
}
func (r *workerRepository) SavePrepared(ctx context.Context, c delivery.RecoveryClaim, p model.PreparedRequest) error {
	return r.save(ctx, c, p)
}
func (r *workerRepository) Complete(ctx context.Context, c delivery.RecoveryClaim, s model.SendResult) error {
	return r.complete(ctx, c, s)
}

type workerAdapter struct {
	prepare func(model.TradeNotification, string) (model.PreparedRequest, error)
	send    func(context.Context, model.PreparedRequest) model.SendResult
}

func (a *workerAdapter) Prepare(n model.TradeNotification, u string) (model.PreparedRequest, error) {
	return a.prepare(n, u)
}
func (a *workerAdapter) Send(c context.Context, p model.PreparedRequest) model.SendResult {
	return a.send(c, p)
}

func workerClaim() delivery.RecoveryClaim {
	return delivery.RecoveryClaim{Target: model.RouteTarget{PlatformID: "platform1", PlatformUID: "fixed-uid", DeliveryID: "delivery-one"}, Notification: model.TradeNotification{SecCode: "600000", TraceID: "trace-one"}, Owner: "worker-one", LeaseVersion: 7, LeaseUntil: time.Now().Add(15 * time.Second), LeaseDeadline: time.Now().Add(15 * time.Second), AttemptNo: 3, CycleNo: 1, CycleAttempt: 3}
}

func workerFixture(t *testing.T) (*workerRepository, *workerAdapter, *[]string, *[]model.SendResult) {
	t.Helper()
	claim := workerClaim()
	steps := []string{}
	completed := []model.SendResult{}
	prepared := model.PreparedRequest{PlatformID: "platform1", ProtocolVersion: "version-2", Payload: []byte(`{"userUid":"fixed-uid"}`)}
	repo := &workerRepository{}
	repo.claim = func(ctx context.Context, p, o string, n int) ([]delivery.RecoveryClaim, error) {
		if p != "platform1" || o != "worker-one" || n != 1 {
			t.Errorf("claim must target one platform and one item: %q %q %d", p, o, n)
		}
		steps = append(steps, "claim")
		return []delivery.RecoveryClaim{claim}, nil
	}
	repo.save = func(ctx context.Context, c delivery.RecoveryClaim, p model.PreparedRequest) error {
		if !reflect.DeepEqual(c, claim) || !reflect.DeepEqual(p, prepared) {
			t.Errorf("persist original lease and exact prepared request: %+v %+v", c, p)
		}
		steps = append(steps, "persist")
		return nil
	}
	repo.complete = func(ctx context.Context, c delivery.RecoveryClaim, s model.SendResult) error {
		if !reflect.DeepEqual(c, claim) {
			t.Errorf("completion changed the execution identity: %+v", c)
		}
		steps = append(steps, "complete")
		completed = append(completed, s)
		return nil
	}
	adapter := &workerAdapter{
		prepare: func(n model.TradeNotification, u string) (model.PreparedRequest, error) {
			if u != "fixed-uid" || n.SecCode != "600000" || n.TraceID != "trace-one" {
				t.Errorf("prepare must use fixed target and notification: %+v %q", n, u)
			}
			steps = append(steps, "prepare")
			return prepared, nil
		},
		send: func(ctx context.Context, p model.PreparedRequest) model.SendResult {
			if !reflect.DeepEqual(p, prepared) {
				t.Errorf("network request differs from persisted request: %+v", p)
			}
			if !reflect.DeepEqual(steps, []string{"claim", "prepare", "persist"}) {
				t.Errorf("send before durable prepared request: %v", steps)
			}
			steps = append(steps, "send")
			return model.SendResult{Outcome: model.Success, SendStarted: true, StartedAt: time.Now(), FinishedAt: time.Now(), Summary: "accepted"}
		},
	}
	return repo, adapter, &steps, &completed
}

func TestRecoveryWorkerPersistsPreparedBeforeNetwork(t *testing.T) {
	r, a, steps, completed := workerFixture(t)
	n, err := delivery.NewWorker(r, a, "platform1", "worker-one").RunOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("claimed attempt: n=%d err=%v", n, err)
	}
	if !reflect.DeepEqual(*steps, []string{"claim", "prepare", "persist", "send", "complete"}) {
		t.Fatalf("execution order: %v", *steps)
	}
	if len(*completed) != 1 || (*completed)[0].Outcome != model.Success || !(*completed)[0].SendStarted {
		t.Fatalf("observable result was not saved: %+v", *completed)
	}
}

func TestRecoveryWorkerPreparationFailureConsumesClaimedAttempt(t *testing.T) {
	r, a, steps, completed := workerFixture(t)
	a.prepare = func(model.TradeNotification, string) (model.PreparedRequest, error) {
		*steps = append(*steps, "prepare")
		return model.PreparedRequest{}, errors.New("bad config")
	}
	n, err := delivery.NewWorker(r, a, "platform1", "worker-one").RunOnce(context.Background())
	if n != 1 || err != nil {
		t.Fatalf("preparation failure must be durably completed: n=%d err=%v", n, err)
	}
	if !reflect.DeepEqual(*steps, []string{"claim", "prepare", "complete"}) {
		t.Fatalf("preparation failure caused network or persistence: %v", *steps)
	}
	if len(*completed) != 1 {
		t.Fatalf("result count: %v", *completed)
	}
	s := (*completed)[0]
	if s.Outcome != model.PermanentFailure || s.SendStarted || s.StartedAt.IsZero() || s.FinishedAt.Before(s.StartedAt) || s.Summary == "" {
		t.Fatalf("unrecorded preparation failure: %+v", s)
	}
}

func TestRecoveryWorkerSaveFailureNeverSendsOrReclaims(t *testing.T) {
	for _, failure := range []error{delivery.ErrLeaseLost, errors.New("database unavailable")} {
		t.Run(failure.Error(), func(t *testing.T) {
			r, a, steps, completed := workerFixture(t)
			r.save = func(context.Context, delivery.RecoveryClaim, model.PreparedRequest) error {
				*steps = append(*steps, "persist")
				return failure
			}
			n, err := delivery.NewWorker(r, a, "platform1", "worker-one").RunOnce(context.Background())
			if n != 1 || !errors.Is(err, failure) {
				t.Fatalf("save failure: n=%d err=%v", n, err)
			}
			if !reflect.DeepEqual(*steps, []string{"claim", "prepare", "persist"}) || len(*completed) != 0 {
				t.Fatalf("lease must remain for recovery, without a network effect: %v %+v", *steps, *completed)
			}
		})
	}
}

func TestRecoveryWorkerCompletionFailureNeverResends(t *testing.T) {
	r, a, steps, _ := workerFixture(t)
	failure := errors.New("commit acknowledgement lost")
	r.complete = func(context.Context, delivery.RecoveryClaim, model.SendResult) error {
		*steps = append(*steps, "complete")
		return failure
	}
	n, err := delivery.NewWorker(r, a, "platform1", "worker-one").RunOnce(context.Background())
	if n != 1 || !errors.Is(err, failure) {
		t.Fatalf("completion failure: n=%d err=%v", n, err)
	}
	if !reflect.DeepEqual(*steps, []string{"claim", "prepare", "persist", "send", "complete"}) {
		t.Fatalf("result write failure repeated a network effect: %v", *steps)
	}
}

func TestRecoveryWorkerSendBudget(t *testing.T) {
	for _, leaseBudget := range []time.Duration{time.Second, 15 * time.Second} {
		t.Run(leaseBudget.String(), func(t *testing.T) {
			r, a, _, _ := workerFixture(t)
			claim := workerClaim()
			claim.LeaseUntil = time.Now().Add(leaseBudget)
			claim.LeaseDeadline = time.Now().Add(leaseBudget)
			r.claim = func(context.Context, string, string, int) ([]delivery.RecoveryClaim, error) {
				return []delivery.RecoveryClaim{claim}, nil
			}
			r.save = func(context.Context, delivery.RecoveryClaim, model.PreparedRequest) error { return nil }
			r.complete = func(context.Context, delivery.RecoveryClaim, model.SendResult) error { return nil }
			sent := false
			a.send = func(ctx context.Context, _ model.PreparedRequest) model.SendResult {
				sent = true
				d, ok := ctx.Deadline()
				if !ok || time.Until(d) > leaseBudget || time.Until(d) > 2*time.Second || time.Until(d) < 500*time.Millisecond {
					t.Errorf("send deadline must be bounded by lease and 2s: %v", d)
				}
				return model.SendResult{Outcome: model.Success, SendStarted: true}
			}
			if _, err := delivery.NewWorker(r, a, "platform1", "worker-one").RunOnce(context.Background()); err != nil || !sent {
				t.Fatalf("send not executed with valid bounded context: %v", err)
			}
		})
	}
}

func TestRecoveryWorkerExpiredOrCancelledBeforeSend(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancelled", true: "expired"}[expired], func(t *testing.T) {
			r, a, steps, completed := workerFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			claim := workerClaim()
			if expired {
				claim.LeaseUntil = time.Now().Add(-time.Second)
				claim.LeaseDeadline = time.Now().Add(-time.Second)
			}
			r.claim = func(context.Context, string, string, int) ([]delivery.RecoveryClaim, error) {
				*steps = append(*steps, "claim")
				return []delivery.RecoveryClaim{claim}, nil
			}
			r.save = func(context.Context, delivery.RecoveryClaim, model.PreparedRequest) error {
				*steps = append(*steps, "persist")
				if !expired {
					cancel()
				}
				return nil
			}
			a.send = func(context.Context, model.PreparedRequest) model.SendResult {
				t.Error("send after cancellation or expiration")
				return model.SendResult{}
			}
			n, err := delivery.NewWorker(r, a, "platform1", "worker-one").RunOnce(ctx)
			if n != 1 || err == nil || len(*completed) != 0 {
				t.Fatalf("must leave interrupted lease for durable recovery: n=%d err=%v results=%v", n, err, *completed)
			}
		})
	}
}

func TestRecoveryWorkerCancellationBeforeClaim(t *testing.T) {
	r, a, steps, _ := workerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n, err := delivery.NewWorker(r, a, "platform1", "worker-one").RunOnce(ctx)
	if n != 0 || !errors.Is(err, context.Canceled) || len(*steps) != 0 {
		t.Fatalf("cancelled worker claimed new work: n=%d err=%v steps=%v", n, err, *steps)
	}
}

func TestRecoveryWorkerNoDueAttemptOrClaimFailureDoesNotPrepare(t *testing.T) {
	for _, failure := range []error{nil, errors.New("claim unavailable")} {
		r, a, steps, _ := workerFixture(t)
		r.claim = func(context.Context, string, string, int) ([]delivery.RecoveryClaim, error) {
			*steps = append(*steps, "claim")
			return nil, failure
		}
		n, err := delivery.NewWorker(r, a, "platform1", "worker-one").RunOnce(context.Background())
		if n != 0 || !errors.Is(err, failure) || !reflect.DeepEqual(*steps, []string{"claim"}) {
			t.Fatalf("worker executed without a claimed attempt: n=%d err=%v steps=%v", n, err, *steps)
		}
	}
}

func TestRecoveryWorkerCancellationInterruptsSendWithoutRepetition(t *testing.T) {
	r, a, steps, _ := workerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.send = func(send context.Context, _ model.PreparedRequest) model.SendResult {
		*steps = append(*steps, "send")
		cancel()
		select {
		case <-send.Done():
		case <-time.After(time.Second):
			t.Fatal("parent cancellation did not reach active network request")
		}
		return model.SendResult{Outcome: model.Unknown, SendStarted: true}
	}
	r.complete = func(write context.Context, _ delivery.RecoveryClaim, result model.SendResult) error {
		*steps = append(*steps, "complete")
		if result.Outcome != model.Unknown || !result.SendStarted || write.Err() == nil {
			t.Errorf("cancelled send must preserve uncertainty and caller budget: %+v %v", result, write.Err())
		}
		return write.Err()
	}
	n, err := delivery.NewWorker(r, a, "platform1", "worker-one").RunOnce(ctx)
	if n != 1 || !errors.Is(err, context.Canceled) || !reflect.DeepEqual(*steps, []string{"claim", "prepare", "persist", "send", "complete"}) {
		t.Fatalf("cancellation must leave durable recovery without retrying: n=%d err=%v steps=%v", n, err, *steps)
	}
}

func TestRecoveryWorkerRunBacksOffErrorsAndStopsOnCancel(t *testing.T) {
	r, a, _, _ := workerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	times := []time.Time{}
	r.claim = func(context.Context, string, string, int) ([]delivery.RecoveryClaim, error) {
		mu.Lock()
		defer mu.Unlock()
		times = append(times, time.Now())
		if len(times) == 3 {
			cancel()
		}
		return nil, errors.New("database unavailable")
	}
	start := time.Now()
	err := delivery.NewWorker(r, a, "platform1", "worker-one").Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run did not stop on cancellation: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(times) != 3 || times[1].Sub(times[0]) < 90*time.Millisecond || times[2].Sub(times[1]) < 90*time.Millisecond || time.Since(start) > time.Second {
		t.Fatalf("error scan busy loop or slow cancellation: %v", times)
	}
}

func TestRecoveryWorkerDatabaseClockOffset(t *testing.T) {
	for _, offset := range []time.Duration{-time.Hour, time.Hour} {
		t.Run(offset.String(), func(t *testing.T) {
			repo, ad, _, _ := workerFixture(t)
			c := workerClaim()
			c.LeaseUntil = time.Now().Add(offset)
			c.LeaseDeadline = time.Now().Add(600 * time.Millisecond)
			repo.claim = func(context.Context, string, string, int) ([]delivery.RecoveryClaim, error) {
				return []delivery.RecoveryClaim{c}, nil
			}
			repo.save = func(context.Context, delivery.RecoveryClaim, model.PreparedRequest) error { return nil }
			repo.complete = func(context.Context, delivery.RecoveryClaim, model.SendResult) error { return nil }
			sent := false
			ad.send = func(ctx context.Context, p model.PreparedRequest) model.SendResult {
				sent = true
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 600*time.Millisecond || time.Until(deadline) < 400*time.Millisecond {
					t.Errorf("send did not use DB remaining duration: %v", deadline)
				}
				return model.SendResult{Outcome: model.Success, SendStarted: true}
			}
			n, err := delivery.NewWorker(repo, ad, "platform1", "worker-one").RunOnce(context.Background())
			if err != nil || n != 1 || !sent {
				t.Fatalf("DB-valid lease skipped because of wall-clock offset: n=%d sent=%v err=%v", n, sent, err)
			}
		})
	}
}

func TestRecoveryWorkerSchedulingDelayDoesNotRestartLease(t *testing.T) {
	repo, ad, _, _ := workerFixture(t)
	c := workerClaim()
	c.LeaseDeadline = time.Now().Add(100 * time.Millisecond)
	repo.claim = func(context.Context, string, string, int) ([]delivery.RecoveryClaim, error) {
		time.Sleep(150 * time.Millisecond)
		return []delivery.RecoveryClaim{c}, nil
	}
	repo.save = func(context.Context, delivery.RecoveryClaim, model.PreparedRequest) error { return nil }
	repo.complete = func(context.Context, delivery.RecoveryClaim, model.SendResult) error { return nil }
	sent := false
	ad.send = func(context.Context, model.PreparedRequest) model.SendResult {
		sent = true
		return model.SendResult{Outcome: model.Success, SendStarted: true}
	}
	n, err := delivery.NewWorker(repo, ad, "platform1", "worker-one").RunOnce(context.Background())
	if n != 1 || !errors.Is(err, delivery.ErrLeaseLost) || sent {
		t.Fatalf("scheduling pause restarted lease: n=%d sent=%v err=%v", n, sent, err)
	}
}

// A graceful scan stop must not erase a response that the external receiver
// already returned; RunOnce keeps its explicit caller-cancellation contract.
func TestRecoveryWorkerDrainSavesKnownResultAndStopsClaims(t *testing.T) {
	r, a, _, _ := workerFixture(t)
	scan, stop := context.WithCancel(context.Background())
	defer stop()
	claims := 0
	originalClaim := r.claim
	r.claim = func(ctx context.Context, p, o string, n int) ([]delivery.RecoveryClaim, error) {
		claims++
		return originalClaim(ctx, p, o, n)
	}
	a.send = func(send context.Context, _ model.PreparedRequest) model.SendResult {
		stop()
		if send.Err() != nil {
			t.Errorf("graceful scan stop cancelled active send: %v", send.Err())
		}
		return model.SendResult{Outcome: model.Success, SendStarted: true}
	}
	saved := false
	r.complete = func(write context.Context, c delivery.RecoveryClaim, result model.SendResult) error {
		if write.Err() != nil {
			return write.Err()
		}
		d, ok := write.Deadline()
		if !ok || time.Until(d) > 8*time.Second || d.After(c.LeaseDeadline) {
			t.Error("claimed completion lacks bounded execution/lease deadline")
		}
		saved = result.Outcome == model.Success && result.SendStarted
		return nil
	}
	if err := delivery.NewWorker(r, a, "platform1", "worker-one").Run(scan); !errors.Is(err, context.Canceled) {
		t.Fatalf("scan did not stop: %v", err)
	}
	if !saved || claims != 1 {
		t.Fatalf("graceful drain lost known result or claimed again: saved=%v claims=%d", saved, claims)
	}
}
