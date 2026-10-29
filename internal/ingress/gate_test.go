package ingress_test

import (
	"context"
	"errors"
	"partner-hub/internal/infra"
	"partner-hub/internal/ingress"
	"partner-hub/internal/model"
	"sync"
	"testing"
	"time"
)

type capacitySource func(context.Context) (infra.ArchiveCapacity, error)

func (f capacitySource) Capacity(c context.Context) (infra.ArchiveCapacity, error) { return f(c) }
func capacitySample() infra.ArchiveCapacity {
	return infra.ArchiveCapacity{Pending: 10, Bytes: 1024, UsedMemory: 4 << 20, MaxMemory: 256 << 20, SampledAt: time.Now()}
}
func newGate(t *testing.T, source capacitySource, config ingress.ArchiveGateConfig) *ingress.ArchiveGate {
	t.Helper()
	g, e := ingress.NewArchiveGate(&stageArchive{}, source, config)
	if e != nil {
		t.Fatal(e)
	}
	return g
}
func TestArchiveGateHighLowWatermark(t *testing.T) {
	for _, dimension := range []string{"count", "bytes", "memory"} {
		t.Run(dimension, func(t *testing.T) {
			sample := capacitySample()
			g := newGate(t, func(context.Context) (infra.ArchiveCapacity, error) {
				sample.SampledAt = time.Now()
				return sample, nil
			}, ingress.DefaultArchiveGateConfig())
			for _, step := range []struct {
				percent int64
				reject  bool
			}{{10, false}, {80, true}, {70, true}, {60, false}, {70, false}, {90, true}, {61, true}, {59, false}} {
				sample.Pending, sample.Bytes, sample.UsedMemory = 10, 1024, 4<<20
				switch dimension {
				case "count":
					sample.Pending = 10000 * step.percent / 100
				case "bytes":
					sample.Bytes = (128 << 20) * step.percent / 100
				case "memory":
					sample.UsedMemory = (256 << 20) * step.percent / 100
				}
				// Ignore pessimistic in-flight bytes in this hysteresis-specific case.
				custom := ingress.DefaultArchiveGateConfig()
				custom.ReserveRequests = 1
				if dimension == "memory" {
					sample.MaxMemory = 1 << 40
					sample.UsedMemory = sample.MaxMemory * step.percent / 100
				}
				if step.percent == 10 {
					g = newGate(t, func(context.Context) (infra.ArchiveCapacity, error) {
						sample.SampledAt = time.Now()
						return sample, nil
					}, custom)
				}
				err := g.Preflight(context.Background())
				if (err != nil) != step.reject {
					t.Fatalf("%s %d%% reject=%v got %v", dimension, step.percent, step.reject, err)
				}
			}
		})
	}
}
func TestArchiveGateRejectsUnknownStaleAndUnsafeCapacity(t *testing.T) {
	for _, which := range []string{"error", "stale", "future", "zero", "negative-count", "negative-bytes", "no-memory-limit", "over-limit", "reserve-bytes", "reserve-memory"} {
		t.Run(which, func(t *testing.T) {
			sample := capacitySample()
			var sourceErr error
			switch which {
			case "error":
				sourceErr = errors.New("dependency unavailable")
			case "stale":
				sample.SampledAt = time.Now().Add(-time.Second)
			case "future":
				sample.SampledAt = time.Now().Add(time.Second)
			case "zero":
				sample.SampledAt = time.Time{}
			case "negative-count":
				sample.Pending = -1
			case "negative-bytes":
				sample.Bytes = -1
			case "no-memory-limit":
				sample.MaxMemory = 0
			case "over-limit":
				sample.Pending = 10001
			case "reserve-bytes":
				sample.Bytes = 75 << 20
			case "reserve-memory":
				sample.UsedMemory = 150 << 20
			}
			g := newGate(t, func(context.Context) (infra.ArchiveCapacity, error) { return sample, sourceErr }, ingress.DefaultArchiveGateConfig())
			if !errors.Is(g.Preflight(context.Background()), ingress.ErrArchiveCapacity) {
				t.Fatal("unsafe capacity accepted")
			}
		})
	}
}
func TestArchiveGateRejectsOutOfOrderSamples(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	calls := 0
	g := newGate(t, func(context.Context) (infra.ArchiveCapacity, error) {
		s := capacitySample()
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			close(started)
			<-release
		} else {
			s.Pending = 8000
		}
		return s, nil
	}, ingress.DefaultArchiveGateConfig())
	done := make(chan error, 1)
	go func() { done <- g.Preflight(context.Background()) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("capacity source not sampled")
	}
	if g.Preflight(context.Background()) == nil {
		t.Fatal("high sample accepted")
	}
	close(release)
	if <-done == nil {
		t.Fatal("older low sample reopened gate")
	}
}
func TestArchiveGateBoundsDependencyCall(t *testing.T) {
	var deadline time.Duration
	g := newGate(t, func(ctx context.Context) (infra.ArchiveCapacity, error) {
		d, ok := ctx.Deadline()
		if ok {
			deadline = time.Until(d)
		}
		return capacitySample(), nil
	}, ingress.DefaultArchiveGateConfig())
	_ = g.Preflight(context.Background())
	if deadline <= 0 || deadline > 100*time.Millisecond {
		t.Fatalf("unbounded dependency context %v", deadline)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if g.Preflight(ctx) == nil {
		t.Fatal("cancelled request accepted")
	}
}
func TestArchiveGateValidatesConfiguration(t *testing.T) {
	for _, mutate := range []func(*ingress.ArchiveGateConfig){func(c *ingress.ArchiveGateConfig) { c.MaxPending = 0 }, func(c *ingress.ArchiveGateConfig) { c.MaxBytes = 0 }, func(c *ingress.ArchiveGateConfig) { c.ReserveRequests = -1 }, func(c *ingress.ArchiveGateConfig) { c.ReserveRequests = c.MaxPending }, func(c *ingress.ArchiveGateConfig) { c.ReserveRequests = c.MaxBytes / ingress.MaxArchiveSnapshotBytes }, func(c *ingress.ArchiveGateConfig) { c.MaxSampleAge = 0 }, func(c *ingress.ArchiveGateConfig) { c.MaxSampleAge = time.Second }} {
		c := ingress.DefaultArchiveGateConfig()
		mutate(&c)
		if c.Validate() == nil {
			t.Fatalf("unsafe configuration %+v", c)
		}
	}
}

// This gate acts before routing or external side effects, including handoff.
type forbiddenRouter struct{ calls int }

func (r *forbiddenRouter) ResolveTargets(context.Context, model.TradeEvent) (model.RouteSnapshot, error) {
	r.calls++
	return model.RouteSnapshot{}, nil
}

type forbiddenFirst struct{ calls int }

func (f *forbiddenFirst) Execute(context.Context, model.TradeEvent, model.RouteSnapshot) model.FirstRoundSnapshot {
	f.calls++
	return model.FirstRoundSnapshot{}
}
func TestArchiveGateStopsBeforeRoutingAndSending(t *testing.T) {
	a := &stageArchive{}
	sample := capacitySample()
	sample.Pending = 8000
	g, e := ingress.NewArchiveGate(a, capacitySource(func(context.Context) (infra.ArchiveCapacity, error) { return sample, nil }), ingress.DefaultArchiveGateConfig())
	if e != nil {
		t.Fatal(e)
	}
	r, f := &forbiddenRouter{}, &forbiddenFirst{}
	s := ingress.NewService(r, f, g)
	_, e = s.Process(context.Background(), tradeFixture(t))
	if !errors.Is(e, ingress.ErrUnconfirmed) || r.calls != 0 || f.calls != 0 || a.calls != 0 {
		t.Fatalf("capacity refusal had effects r=%d f=%d stage=%d err=%v", r.calls, f.calls, a.calls, e)
	}
}
