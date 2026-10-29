package binding_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"partner-hub/internal/binding"
	"partner-hub/internal/model"
)

// A query boundary double controls only external DB latency/failure.
type lookupRepository struct {
	load func(context.Context, string) (model.BindingSnapshot, error)
}

func (r lookupRepository) Load(c context.Context, id string) (model.BindingSnapshot, error) {
	return r.load(c, id)
}
func (r lookupRepository) Apply(context.Context, model.BindingEvent) (model.BindingResult, error) {
	return model.BindingResult{Outcome: "APPLIED"}, nil
}
func (r lookupRepository) SaveAnomaly(context.Context, model.BindingAnomaly) error { return nil }

type snapshotCache struct {
	mu     sync.Mutex
	values map[string]model.BindingSnapshot
	fail   bool
}

func (c *snapshotCache) Get(ctx context.Context, id string) (model.BindingSnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail {
		return model.BindingSnapshot{}, binding.ErrCacheMiss
	}
	v, ok := c.values[id]
	if !ok {
		return v, binding.ErrCacheMiss
	}
	return v, nil
}
func (c *snapshotCache) Put(ctx context.Context, id string, v model.BindingSnapshot) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail {
		return binding.ErrCacheMiss
	}
	if c.values == nil {
		c.values = map[string]model.BindingSnapshot{}
	}
	c.values[id] = v
	return nil
}
func (c *snapshotCache) Invalidate(ctx context.Context, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail {
		return binding.ErrCacheMiss
	}
	delete(c.values, id)
	return nil
}
func boundSnapshot() model.BindingSnapshot {
	return model.BindingSnapshot{Version: 1, ObservedAt: time.Now().UTC(), Targets: []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: "A1"}, {PlatformID: "PLATFORM_B", PlatformUID: "B1"}}}
}

// Catches turning cache failure/DB error into a false empty/NO_ROUTE result.
func TestBindingResolverFallbackAndErrors(t *testing.T) {
	for _, tt := range []struct {
		name                     string
		cached                   model.BindingSnapshot
		cacheFail, dbFail, empty bool
	}{
		{"cache miss", model.BindingSnapshot{}, false, false, false},
		{"cache unavailable", model.BindingSnapshot{}, true, false, false},
		{"corrupt", model.BindingSnapshot{Version: 99}, false, false, false},
		{"duplicate target", model.BindingSnapshot{Version: 1, ObservedAt: time.Now(), Targets: []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: "a"}, {PlatformID: "PLATFORM_A", PlatformUID: "b"}}}, false, false, false},
		{"expired positive", model.BindingSnapshot{Version: 1, ObservedAt: time.Now().Add(-31 * time.Second), Targets: []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: "stale"}}}, false, false, false},
		{"future sample", model.BindingSnapshot{Version: 1, ObservedAt: time.Now().Add(time.Hour), Targets: []model.BindingTarget{{PlatformID: "PLATFORM_A", PlatformUID: "stale"}}}, false, false, false},
		{"empty db", model.BindingSnapshot{}, false, false, true},
		{"db error", model.BindingSnapshot{}, false, true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cache := &snapshotCache{values: map[string]model.BindingSnapshot{"customer": tt.cached}, fail: tt.cacheFail}
			repo := lookupRepository{load: func(context.Context, string) (model.BindingSnapshot, error) {
				if tt.dbFail {
					return model.BindingSnapshot{}, errors.New("db unavailable")
				}
				if tt.empty {
					return model.BindingSnapshot{Targets: []model.BindingTarget{}}, nil
				}
				return boundSnapshot(), nil
			}}
			r := binding.NewResolver(repo, cache)
			got, err := r.Resolve(context.Background(), "customer")
			if tt.dbFail {
				if !errors.Is(err, binding.ErrUnavailable) || len(got.Targets) != 0 {
					t.Fatalf("DB failure became route: %+v %v", got, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := 2
			if tt.empty {
				want = 0
			}
			if len(got.Targets) != want || got.Targets == nil || got.ObservedAt.IsZero() {
				t.Fatalf("not complete/explicit snapshot: %+v", got)
			}
		})
	}
}

func TestBindingResolverUsesCompleteCache(t *testing.T) {
	for _, targets := range [][]model.BindingTarget{{{PlatformID: "PLATFORM_A", PlatformUID: "A1"}, {PlatformID: "PLATFORM_B", PlatformUID: "B1"}}, {}} {
		cache := &snapshotCache{values: map[string]model.BindingSnapshot{"customer": {Version: 1, ObservedAt: time.Now(), Targets: targets}}}
		repo := lookupRepository{load: func(context.Context, string) (model.BindingSnapshot, error) {
			return model.BindingSnapshot{}, errors.New("must not hit failing DB on valid cache")
		}}
		r := binding.NewResolver(repo, cache)
		got, err := r.Resolve(context.Background(), "customer")
		if err != nil || len(got.Targets) != len(targets) {
			t.Fatalf("cache: %+v %v", got, err)
		}
	}
}

func TestBindingResolverCoalescesColdCustomer(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var queries atomic.Int32
	repo := lookupRepository{load: func(ctx context.Context, id string) (model.BindingSnapshot, error) {
		if queries.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-release:
			return boundSnapshot(), nil
		case <-ctx.Done():
			return model.BindingSnapshot{}, ctx.Err()
		}
	}}
	cache := &snapshotCache{}
	r := binding.NewResolver(repo, cache)
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snapshot, err := r.Resolve(context.Background(), "same")
			if err == nil && len(snapshot.Targets) != 2 {
				err = fmt.Errorf("incomplete: %+v", snapshot)
			}
			errs <- err
		}()
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("DB not reached")
	}
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if queries.Load() != 1 {
		t.Fatalf("cold cache stampede: %d", queries.Load())
	}
}

func TestBindingResolverLimitsConcurrentDB(t *testing.T) {
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	repo := lookupRepository{load: func(ctx context.Context, id string) (model.BindingSnapshot, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return boundSnapshot(), nil
		case <-ctx.Done():
			return model.BindingSnapshot{}, ctx.Err()
		}
	}}
	r := binding.NewResolver(repo, nil)
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func(i int) { defer wg.Done(); r.Resolve(context.Background(), fmt.Sprintf("c%d", i)) }(i)
	}
	for range 4 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("query slots not reached")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	s, err := r.Resolve(ctx, "overflow")
	if !errors.Is(err, binding.ErrUnavailable) || len(s.Targets) != 0 {
		t.Fatalf("budget exceeded became route: %+v %v", s, err)
	}
	close(release)
	wg.Wait()
}

func TestBindingResolverDBTimeout(t *testing.T) {
	repo := lookupRepository{load: func(ctx context.Context, id string) (model.BindingSnapshot, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > time.Second+10*time.Millisecond {
			return boundSnapshot(), nil
		}
		<-ctx.Done()
		return model.BindingSnapshot{}, ctx.Err()
	}}
	r := binding.NewResolver(repo, nil)
	if s, err := r.Resolve(context.Background(), "customer"); !errors.Is(err, binding.ErrUnavailable) || len(s.Targets) != 0 {
		t.Fatalf("unbounded DB/false route: %+v %v", s, err)
	}
}

// Catches unbounded distinct-customer misses even when each DB query is fast.
func TestBindingResolverRateBudget(t *testing.T) {
	var queries int
	repo := lookupRepository{load: func(context.Context, string) (model.BindingSnapshot, error) { queries++; return boundSnapshot(), nil }}
	r := binding.NewResolver(repo, nil)
	start := time.Now()
	accepted := 0
	rejected := 0
	for i := 0; i < 100; i++ {
		snapshot, err := r.Resolve(context.Background(), fmt.Sprintf("burst-%d", i))
		if err == nil {
			accepted++
			if len(snapshot.Targets) != 2 {
				t.Fatal("incomplete result")
			}
		} else {
			rejected++
			if !errors.Is(err, binding.ErrUnavailable) {
				t.Fatal(err)
			}
		}
	}
	if rejected == 0 || accepted > 4+int(time.Since(start).Seconds()*50)+1 || queries != accepted {
		t.Fatalf("no bounded 50/s source budget: queries=%d accepted=%d rejected=%d", queries, accepted, rejected)
	}
}
