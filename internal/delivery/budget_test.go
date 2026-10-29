package delivery_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"partner-hub/internal/delivery"
	"partner-hub/internal/model"
)

func newBudget(t *testing.T, configs map[string]delivery.BudgetConfig) *delivery.PlatformBudget {
	t.Helper()
	b, err := delivery.NewPlatformBudget(configs)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func permit(t *testing.T, b *delivery.PlatformBudget, platform, class string) *delivery.BudgetPermit {
	t.Helper()
	p, ok := b.TryAcquire(context.Background(), platform, class)
	if !ok || p == nil {
		t.Fatalf("no execution resource for %s/%s", platform, class)
	}
	return p
}

// Catches independently configured first/recovery pools exceeding a platform's total.
func TestPlatformBudgetEnforcesClassAndSharedTotal(t *testing.T) {
	b := newBudget(t, map[string]delivery.BudgetConfig{"A": {Total: 3, First: 2, Recovery: 2, Rate: 0.001, Burst: 12}})
	f1, f2 := permit(t, b, "A", delivery.FirstClass), permit(t, b, "A", delivery.FirstClass)
	defer f1.Release()
	defer f2.Release()
	if p, ok := b.TryAcquire(context.Background(), "A", delivery.FirstClass); ok {
		p.Release()
		t.Fatal("first class exceeded 2")
	}
	r := permit(t, b, "A", delivery.RecoveryClass)
	if p, ok := b.TryAcquire(context.Background(), "A", delivery.RecoveryClass); ok {
		p.Release()
		t.Fatal("shared platform total exceeded 3")
	}
	r.Release()
	r = permit(t, b, "A", delivery.RecoveryClass)
	defer r.Release()
	f1.Release()
	f1.Cancel()
	f1.Release()
	f3 := permit(t, b, "A", delivery.FirstClass)
	defer f3.Release()
	if p, ok := b.TryAcquire(context.Background(), "A", delivery.FirstClass); ok {
		p.Release()
		t.Fatal("duplicate release created extra capacity")
	}
}

// Catches per-class rate buckets, refunded sends, or shared rate state between platforms.
func TestPlatformBudgetRateSharedAndPlatformsIsolated(t *testing.T) {
	cfg := delivery.BudgetConfig{Total: 3, First: 2, Recovery: 2, Rate: 0.001, Burst: 2}
	b := newBudget(t, map[string]delivery.BudgetConfig{"A": cfg, "B": cfg})
	permit(t, b, "A", delivery.FirstClass).Release()
	permit(t, b, "A", delivery.RecoveryClass).Release()
	for _, class := range []string{delivery.FirstClass, delivery.RecoveryClass} {
		if p, ok := b.TryAcquire(context.Background(), "A", class); ok {
			p.Release()
			t.Fatal("rate tokens were not shared/consumed")
		}
	}
	permit(t, b, "B", delivery.FirstClass).Release()
}

func TestPlatformBudgetUnusedReservationsRefundAndRefill(t *testing.T) {
	b := newBudget(t, map[string]delivery.BudgetConfig{"A": {Total: 1, First: 1, Recovery: 1, Rate: 0.001, Burst: 1}})
	for i := 0; i < 20; i++ {
		p := permit(t, b, "A", delivery.RecoveryClass)
		p.Cancel()
		p.Release()
		p.Cancel()
	}
	permit(t, b, "A", delivery.FirstClass).Release()
	if p, ok := b.TryAcquire(context.Background(), "A", delivery.FirstClass); ok {
		p.Release()
		t.Fatal("Cancel/Release duplicated rate refund")
	}
	b = newBudget(t, map[string]delivery.BudgetConfig{"A": {Total: 1, First: 1, Recovery: 1, Rate: 100, Burst: 1}})
	permit(t, b, "A", delivery.FirstClass).Release()
	time.Sleep(30 * time.Millisecond)
	permit(t, b, "A", delivery.RecoveryClass).Release()
}

func TestPlatformBudgetRejectsUnavailableWithoutLeaking(t *testing.T) {
	cfg := delivery.BudgetConfig{Total: 1, First: 1, Recovery: 1, Rate: 0.001, Burst: 1}
	paused := cfg
	paused.Paused = true
	b := newBudget(t, map[string]delivery.BudgetConfig{"A": cfg, "PAUSED": paused})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, c := range []struct {
		ctx             context.Context
		platform, class string
	}{{ctx, "A", delivery.FirstClass}, {context.Background(), "MISSING", delivery.FirstClass}, {context.Background(), "PAUSED", delivery.FirstClass}, {context.Background(), "A", "unknown"}} {
		if p, ok := b.TryAcquire(c.ctx, c.platform, c.class); ok {
			p.Release()
			t.Fatalf("unavailable execution accepted: %s/%s", c.platform, c.class)
		}
	}
	permit(t, b, "A", delivery.FirstClass).Release()
}

func TestPlatformBudgetRejectsInvalidConfiguration(t *testing.T) {
	valid := delivery.DefaultBudgetConfig()
	for _, change := range []func(*delivery.BudgetConfig){func(c *delivery.BudgetConfig) { c.Total = 0 }, func(c *delivery.BudgetConfig) { c.First = 0 }, func(c *delivery.BudgetConfig) { c.Recovery = 0 }, func(c *delivery.BudgetConfig) { c.First = c.Total + 1 }, func(c *delivery.BudgetConfig) { c.Recovery = c.Total + 1 }, func(c *delivery.BudgetConfig) { c.Rate = 0 }, func(c *delivery.BudgetConfig) { c.Rate = math.NaN() }, func(c *delivery.BudgetConfig) { c.Rate = math.Inf(1) }, func(c *delivery.BudgetConfig) { c.Burst = 0 }} {
		c := valid
		change(&c)
		if _, err := delivery.NewPlatformBudget(map[string]delivery.BudgetConfig{"A": c}); err == nil {
			t.Errorf("unsafe execution config accepted: %+v", c)
		}
	}
	if _, err := delivery.NewPlatformBudget(map[string]delivery.BudgetConfig{"": valid}); err == nil {
		t.Fatal("empty platform accepted")
	}
}

type budgetBlockingAdapter struct {
	entered chan string
	resume  <-chan struct{}
}

func (a *budgetBlockingAdapter) Prepare(n model.TradeNotification, uid string) (model.PreparedRequest, error) {
	return model.PreparedRequest{PlatformID: "A", ProtocolVersion: "v2", Payload: []byte(uid)}, nil
}
func (a *budgetBlockingAdapter) Send(ctx context.Context, p model.PreparedRequest) model.SendResult {
	start := time.Now().UTC()
	select {
	case a.entered <- string(p.Payload):
	case <-ctx.Done():
		return model.SendResult{Outcome: model.RetryableFailure, StartedAt: start, FinishedAt: time.Now().UTC()}
	}
	if a.resume != nil {
		select {
		case <-a.resume:
		case <-ctx.Done():
			return model.SendResult{Outcome: model.Unknown, SendStarted: true, StartedAt: start, FinishedAt: time.Now().UTC()}
		}
	}
	return model.SendResult{Outcome: model.Success, SendStarted: true, StartedAt: start, FinishedAt: time.Now().UTC()}
}

func budgetRoute(platforms ...string) model.RouteSnapshot {
	r := model.RouteSnapshot{Status: "ROUTED", ResolvedAt: time.Now().UTC()}
	for _, p := range platforms {
		r.Targets = append(r.Targets, model.RouteTarget{PlatformID: p, PlatformUID: p + "-original-uid", DeliveryID: p + "-delivery"})
	}
	return r
}
func awaitBudgetSend(t *testing.T, c <-chan string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(time.Second):
		t.Fatal("expected external send did not begin")
	}
}

// The real FirstDelivery must return a bounded, auditable unsent result for A
// while B actually reaches its Adapter; A's original UID stays fixed.
func TestFirstDeliveryBudgetBlockedPlatformDoesNotBlockOtherTarget(t *testing.T) {
	cfg := delivery.BudgetConfig{Total: 1, First: 1, Recovery: 1, Rate: 1000, Burst: 20}
	b := newBudget(t, map[string]delivery.BudgetConfig{"A": cfg, "B": cfg})
	resume := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(resume) })
	a := &budgetBlockingAdapter{entered: make(chan string, 4), resume: resume}
	bb := &budgetBlockingAdapter{entered: make(chan string, 4)}
	f := delivery.NewFirstDelivery(map[string]delivery.Adapter{"A": a, "B": bb}, b)
	firstDone := make(chan model.FirstRoundSnapshot, 1)
	go func() { firstDone <- f.Execute(context.Background(), model.TradeEvent{}, budgetRoute("A")) }()
	awaitBudgetSend(t, a.entered)
	done := make(chan model.FirstRoundSnapshot, 1)
	go func() { done <- f.Execute(context.Background(), model.TradeEvent{}, budgetRoute("A", "B")) }()
	select {
	case s := <-done:
		if len(s.Attempts) != 2 {
			t.Fatalf("lost responsibility for target: %+v", s)
		}
		x, y := s.Attempts[0], s.Attempts[1]
		if x.Result.Outcome != model.RetryableFailure || x.Result.SendStarted || x.Result.StartedAt.IsZero() || x.Result.FinishedAt.Before(x.Result.StartedAt) || x.Result.Summary == "" || x.Target.PlatformUID != "A-original-uid" {
			t.Fatalf("quota rejection lost unsent facts: %+v", x)
		}
		if y.Result.Outcome != model.Success || !y.Result.SendStarted {
			t.Fatalf("A prevented B delivery: %+v", y)
		}
	case <-time.After(time.Second):
		t.Fatal("FirstDelivery queued waiting for busy platform")
	}
	awaitBudgetSend(t, bb.entered)
	release.Do(func() { close(resume) })
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first delivery did not finish after send unblocked")
	}
	if s := f.Execute(context.Background(), model.TradeEvent{}, budgetRoute("A")); !s.Attempts[0].Result.SendStarted {
		t.Fatal("finished first delivery leaked its capacity")
	}
}

func TestFirstDeliveryPausedAndCanceledHaveNoExternalEffect(t *testing.T) {
	for _, paused := range []bool{true, false} {
		ctx, cancel := context.WithCancel(context.Background())
		if !paused {
			cancel()
		}
		defer cancel()
		cfg := delivery.DefaultBudgetConfig()
		cfg.Paused = paused
		b := newBudget(t, map[string]delivery.BudgetConfig{"A": cfg})
		a := &budgetBlockingAdapter{entered: make(chan string, 1)}
		s := delivery.NewFirstDelivery(map[string]delivery.Adapter{"A": a}, b).Execute(ctx, model.TradeEvent{}, budgetRoute("A"))
		if s.Attempts[0].Result.SendStarted || s.Attempts[0].Result.Outcome != model.RetryableFailure {
			t.Fatalf("unavailable first delivery changed facts: %+v", s)
		}
		select {
		case <-a.entered:
			t.Fatal("paused/cancelled delivery sent")
		default:
		}
	}
}

func TestFirstDeliveryFailureAndCancellationReleaseCapacity(t *testing.T) {
	for _, stage := range []string{"missing-adapter", "prepare", "cancel-during-send"} {
		t.Run(stage, func(t *testing.T) {
			b := newBudget(t, map[string]delivery.BudgetConfig{"A": {Total: 1, First: 1, Recovery: 1, Rate: 0.001, Burst: 4}})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			adapters := map[string]delivery.Adapter{}
			if stage != "missing-adapter" {
				a := &workerAdapter{prepare: func(model.TradeNotification, string) (model.PreparedRequest, error) {
					return model.PreparedRequest{PlatformID: "A", ProtocolVersion: "v2", Payload: []byte(`{}`)}, nil
				}, send: func(context.Context, model.PreparedRequest) model.SendResult {
					cancel()
					return model.SendResult{Outcome: model.Unknown, SendStarted: true, StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC()}
				}}
				if stage == "prepare" {
					a.prepare = func(model.TradeNotification, string) (model.PreparedRequest, error) {
						return model.PreparedRequest{}, errors.New("bad adapter config")
					}
				}
				adapters["A"] = a
			}
			s := delivery.NewFirstDelivery(adapters, b).Execute(ctx, model.TradeEvent{}, budgetRoute("A"))
			result := s.Attempts[0].Result
			if stage == "cancel-during-send" {
				if result.Outcome != model.Unknown || !result.SendStarted {
					t.Fatalf("cancelled external effect lost uncertainty: %+v", result)
				}
			} else {
				want := model.RetryableFailure
				if stage == "prepare" {
					want = model.PermanentFailure
				}
				if result.Outcome != want || result.SendStarted {
					t.Fatalf("non-send failure lost facts: %+v", result)
				}
			}
			permit(t, b, "A", delivery.RecoveryClass).Cancel()
		})
	}
}

// This stateful database boundary exposes pending rows and completed results;
// execution, resource ownership, and network timing use the real Worker.
type budgetRepository struct {
	mu            sync.Mutex
	pending       []delivery.RecoveryClaim
	claimed       int
	results       []model.SendResult
	completeBlock <-chan struct{}
	completing    chan struct{}
}

func (r *budgetRepository) HasDue(context.Context, string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending) > 0, nil
}

func (r *budgetRepository) ClaimDue(context.Context, string, string, int) ([]delivery.RecoveryClaim, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claimed++
	if len(r.pending) == 0 {
		return nil, nil
	}
	c := r.pending[0]
	r.pending = r.pending[1:]
	return []delivery.RecoveryClaim{c}, nil
}
func (r *budgetRepository) SavePrepared(context.Context, delivery.RecoveryClaim, model.PreparedRequest) error {
	return nil
}
func (r *budgetRepository) Complete(ctx context.Context, _ delivery.RecoveryClaim, s model.SendResult) error {
	if r.completing != nil {
		r.completing <- struct{}{}
	}
	if r.completeBlock != nil {
		select {
		case <-r.completeBlock:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, s)
	return nil
}

func TestWorkerAndFirstShareCapacityBeforeClaimUntilResultSaved(t *testing.T) {
	b := newBudget(t, map[string]delivery.BudgetConfig{"A": {Total: 3, First: 2, Recovery: 2, Rate: 1000, Burst: 30}})
	resume := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(resume) })
	a := &budgetBlockingAdapter{entered: make(chan string, 10), resume: resume}
	f := delivery.NewFirstDelivery(map[string]delivery.Adapter{"A": a}, b)
	firstDone := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		go func() { f.Execute(context.Background(), model.TradeEvent{}, budgetRoute("A")); firstDone <- struct{}{} }()
		awaitBudgetSend(t, a.entered)
	}
	r := &budgetRepository{pending: []delivery.RecoveryClaim{workerClaim(), workerClaim()}}
	done := make(chan error, 1)
	w := delivery.NewWorker(r, a, "A", "worker", b)
	go func() { _, err := w.RunOnce(context.Background()); done <- err }()
	awaitBudgetSend(t, a.entered)
	n, err := w.RunOnce(context.Background())
	if n != 0 || err != nil {
		t.Fatalf("worker waited or claimed without total capacity: %d %v", n, err)
	}
	r.mu.Lock()
	remaining, calls := len(r.pending), r.claimed
	r.mu.Unlock()
	if remaining != 1 || calls != 1 {
		t.Fatalf("quota rejection altered persistent queue: pending=%d scans=%d", remaining, calls)
	}
	s := f.Execute(context.Background(), model.TradeEvent{}, budgetRoute("A"))
	if s.Attempts[0].Result.SendStarted {
		t.Fatal("shared pool permitted excess first delivery")
	}
	release.Do(func() { close(resume) })
	for i := 0; i < 2; i++ {
		select {
		case <-firstDone:
		case <-time.After(time.Second):
			t.Fatal("first delivery leaked its execution")
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not finish")
	}
	permit(t, b, "A", delivery.FirstClass).Cancel()
}

func TestWorkerHoldsCapacityThroughCompletion(t *testing.T) {
	b := newBudget(t, map[string]delivery.BudgetConfig{"A": {Total: 1, First: 1, Recovery: 1, Rate: 1000, Burst: 20}})
	finish := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(finish) })
	r := &budgetRepository{pending: []delivery.RecoveryClaim{workerClaim()}, completeBlock: finish, completing: make(chan struct{}, 1)}
	a := &budgetBlockingAdapter{entered: make(chan string, 1)}
	done := make(chan error, 1)
	go func() {
		_, err := delivery.NewWorker(r, a, "A", "worker", b).RunOnce(context.Background())
		done <- err
	}()
	select {
	case <-r.completing:
	case <-time.After(time.Second):
		t.Fatal("worker did not reach result write")
	}
	if p, ok := b.TryAcquire(context.Background(), "A", delivery.FirstClass); ok {
		p.Release()
		t.Fatal("capacity released before result responsibility saved")
	}
	release.Do(func() { close(finish) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("result write did not finish")
	}
	permit(t, b, "A", delivery.FirstClass).Cancel()
}

func TestWorkerEmptyScansAndClaimErrorsRefundRate(t *testing.T) {
	for _, failure := range []error{nil, errors.New("database unavailable")} {
		b := newBudget(t, map[string]delivery.BudgetConfig{"platform1": {Total: 1, First: 1, Recovery: 1, Rate: 0.001, Burst: 1}})
		r, a, steps, _ := workerFixture(t)
		r.claim = func(context.Context, string, string, int) ([]delivery.RecoveryClaim, error) {
			*steps = append(*steps, "scan")
			return nil, failure
		}
		w := delivery.NewWorker(r, a, "platform1", "worker-one", b)
		for i := 0; i < 20; i++ {
			n, err := w.RunOnce(context.Background())
			if n != 0 || !errors.Is(err, failure) {
				t.Fatalf("empty scan result: %d %v", n, err)
			}
		}
		if len(*steps) != 20 {
			t.Fatalf("empty scans exhausted rate: %d", len(*steps))
		}
		permit(t, b, "platform1", delivery.FirstClass).Release()
	}
}

func TestWorkerUnavailableDoesNotClaimAndFailuresReleaseCapacity(t *testing.T) {
	for _, stage := range []string{"paused", "missing", "prepare", "save", "complete", "cancel-after-claim", "cancel-during-send", "expired"} {
		t.Run(stage, func(t *testing.T) {
			cfg := delivery.BudgetConfig{Total: 1, First: 1, Recovery: 1, Rate: 0.001, Burst: 4}
			cfg.Paused = stage == "paused"
			configs := map[string]delivery.BudgetConfig{"platform1": cfg}
			if stage == "missing" {
				configs = map[string]delivery.BudgetConfig{}
			}
			b := newBudget(t, configs)
			r, a, steps, _ := workerFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch stage {
			case "prepare":
				a.prepare = func(model.TradeNotification, string) (model.PreparedRequest, error) {
					return model.PreparedRequest{}, errors.New("bad config")
				}
			case "save":
				r.save = func(context.Context, delivery.RecoveryClaim, model.PreparedRequest) error {
					return errors.New("save failed")
				}
			case "complete":
				r.complete = func(context.Context, delivery.RecoveryClaim, model.SendResult) error {
					return errors.New("commit lost")
				}
			case "cancel-during-send":
				a.send = func(context.Context, model.PreparedRequest) model.SendResult {
					cancel()
					return model.SendResult{Outcome: model.Unknown, SendStarted: true}
				}
			case "cancel-after-claim", "expired":
				r.claim = func(context.Context, string, string, int) ([]delivery.RecoveryClaim, error) {
					c := workerClaim()
					if stage == "expired" {
						c.LeaseDeadline = time.Now().Add(-time.Second)
					} else {
						cancel()
					}
					return []delivery.RecoveryClaim{c}, nil
				}
			}
			n, _ := delivery.NewWorker(r, a, "platform1", "worker-one", b).RunOnce(ctx)
			if stage == "paused" || stage == "missing" {
				if n != 0 || len(*steps) != 0 {
					t.Fatalf("unavailable platform acquired task: %d %v", n, *steps)
				}
				return
			}
			if n != 1 {
				t.Fatalf("claimed responsibility lost: %d", n)
			}
			permit(t, b, "platform1", delivery.FirstClass).Cancel()
		})
	}
}

// Blocking real send branches let every caller attempt admission before slots
// are released: exactly 12 first + 4 recovery run, the others return immediately.
func TestPlatformBudgetConcurrentDefaultClassLimitsAtSendBoundary(t *testing.T) {
	cfg := delivery.DefaultBudgetConfig()
	cfg.Burst = 100
	b := newBudget(t, map[string]delivery.BudgetConfig{"A": cfg})
	resume := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(resume) })
	adapter := &budgetBlockingAdapter{entered: make(chan string, 60), resume: resume}
	route := budgetRoute("A")
	route.Targets[0].PlatformUID = "FIRST"
	first := delivery.NewFirstDelivery(map[string]delivery.Adapter{"A": adapter}, b)
	repo := &budgetRepository{}
	for i := 0; i < 20; i++ {
		c := workerClaim()
		c.Target.PlatformUID = "RECOVERY"
		repo.pending = append(repo.pending, c)
	}
	worker := delivery.NewWorker(repo, adapter, "A", "worker", b)
	done := make(chan error, 60)
	start := make(chan struct{})
	for i := 0; i < 40; i++ {
		go func() {
			<-start
			first.Execute(context.Background(), model.TradeEvent{}, route)
			done <- nil
		}()
	}
	for i := 0; i < 20; i++ {
		go func() { <-start; _, err := worker.RunOnce(context.Background()); done <- err }()
	}
	close(start)
	// Every one of the 44 excess callers must finish while all 16 sends block.
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for i := 0; i < 44; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline.C:
			t.Fatal("excess callers waited locally or exceeded class/total slots")
		}
	}
	counts := map[string]int{}
	for i := 0; i < 16; i++ {
		select {
		case uid := <-adapter.entered:
			counts[uid]++
		case <-deadline.C:
			t.Fatal("configured execution capacity did not reach the send boundary")
		}
	}
	if counts["FIRST"] != 12 || counts["RECOVERY"] != 4 {
		t.Fatalf("default class/total concurrency violated: %v", counts)
	}
	release.Do(func() { close(resume) })
	for i := 0; i < 16; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline.C:
			t.Fatal("active send did not release resources and finish")
		}
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.results) != 4 || len(repo.pending) != 16 {
		t.Fatalf("excess recovery attempts changed durable responsibility: results=%d pending=%d", len(repo.results), len(repo.pending))
	}
}

func TestWorkerRecoveryClassLimitPreservesFirstCapacity(t *testing.T) {
	b := newBudget(t, map[string]delivery.BudgetConfig{"A": {Total: 3, First: 2, Recovery: 1, Rate: 1000, Burst: 20}})
	resume := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(resume) })
	a := &budgetBlockingAdapter{entered: make(chan string, 3), resume: resume}
	repo := &budgetRepository{pending: []delivery.RecoveryClaim{workerClaim(), workerClaim()}}
	done := make(chan error, 1)
	w := delivery.NewWorker(repo, a, "A", "worker", b)
	go func() { _, err := w.RunOnce(context.Background()); done <- err }()
	awaitBudgetSend(t, a.entered)
	if n, err := w.RunOnce(context.Background()); n != 0 || err != nil {
		t.Fatalf("recovery exceeded its own class while total capacity remained: %d %v", n, err)
	}
	f := delivery.NewFirstDelivery(map[string]delivery.Adapter{"A": a}, b)
	firstDone := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		go func() { f.Execute(context.Background(), model.TradeEvent{}, budgetRoute("A")); firstDone <- struct{}{} }()
		awaitBudgetSend(t, a.entered)
	}
	repo.mu.Lock()
	pending, claimed := len(repo.pending), repo.claimed
	repo.mu.Unlock()
	if pending != 1 || claimed != 1 {
		t.Fatalf("class rejection claimed pending work: pending=%d claimed=%d", pending, claimed)
	}
	release.Do(func() { close(resume) })
	for i := 0; i < 2; i++ {
		select {
		case <-firstDone:
		case <-time.After(time.Second):
			t.Fatal("first delivery did not finish")
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("recovery did not finish")
	}
}

func TestWorkerAndFirstShareRateAtExecutionBoundary(t *testing.T) {
	for _, firstStarts := range []bool{true, false} {
		t.Run(map[bool]string{true: "first-consumes", false: "recovery-consumes"}[firstStarts], func(t *testing.T) {
			cfg := delivery.BudgetConfig{Total: 3, First: 2, Recovery: 2, Rate: 0.001, Burst: 1}
			b := newBudget(t, map[string]delivery.BudgetConfig{"A": cfg, "B": cfg})
			a := &budgetBlockingAdapter{entered: make(chan string, 4)}
			f := delivery.NewFirstDelivery(map[string]delivery.Adapter{"A": a, "B": a}, b)
			repo := &budgetRepository{pending: []delivery.RecoveryClaim{workerClaim(), workerClaim()}}
			w := delivery.NewWorker(repo, a, "A", "worker", b)
			if firstStarts {
				s := f.Execute(context.Background(), model.TradeEvent{}, budgetRoute("A"))
				if !s.Attempts[0].Result.SendStarted {
					t.Fatal("first did not consume execution token")
				}
			} else {
				if n, err := w.RunOnce(context.Background()); n != 1 || err != nil {
					t.Fatalf("recovery did not consume execution token: %d %v", n, err)
				}
			}
			if n, err := w.RunOnce(context.Background()); n != 0 || err != nil {
				t.Fatalf("worker bypassed shared rate: %d %v", n, err)
			}
			s := f.Execute(context.Background(), model.TradeEvent{}, budgetRoute("A", "B"))
			if s.Attempts[0].Result.SendStarted || s.Attempts[0].Result.Outcome != model.RetryableFailure || !s.Attempts[1].Result.SendStarted || s.Attempts[1].Result.Outcome != model.Success {
				t.Fatalf("shared rate or platform isolation failed: %+v", s.Attempts)
			}
			repo.mu.Lock()
			defer repo.mu.Unlock()
			want := 0
			if !firstStarts {
				want = 1
			}
			if repo.claimed != want {
				t.Fatalf("worker claimed without shared rate: scans=%d want=%d", repo.claimed, want)
			}
		})
	}
}
