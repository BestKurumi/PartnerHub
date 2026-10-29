package binding

import (
	"context"
	"errors"
	"partner-hub/internal/model"
	"time"

	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
)

var ErrUnavailable = errors.New("binding lookup unavailable")
var ErrCacheMiss = errors.New("binding cache unavailable")

type Resolver struct {
	ObserveCache  func(string)
	ObserveSource func(string)
	repository    Repository
	cache         Cache
	group         singleflight.Group
	slots         chan struct{}
	limiter       *rate.Limiter
}

func NewResolver(repository Repository, cache Cache) *Resolver {
	return &Resolver{repository: repository, cache: cache, slots: make(chan struct{}, 4), limiter: rate.NewLimiter(50, 4)}
}
func (r *Resolver) Resolve(ctx context.Context, clientID string) (model.BindingSnapshot, error) {
	if !validBindingText(clientID) || ctx.Err() != nil {
		return model.BindingSnapshot{}, ErrUnavailable
	}
	if snapshot, ok := r.cached(ctx, clientID); ok {
		return cloneSnapshot(snapshot), nil
	}
	result := r.group.DoChan(clientID, func() (any, error) {
		if snapshot, ok := r.cached(ctx, clientID); ok {
			return snapshot, nil
		}
		select {
		case r.slots <- struct{}{}:
			defer func() { <-r.slots }()
		default:
			r.source("rejected")
			return nil, ErrUnavailable
		}
		if !r.limiter.Allow() {
			r.source("rejected")
			return nil, ErrUnavailable
		}
		query, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		observed := time.Now().UTC()
		snapshot, err := r.repository.Load(query, clientID)
		if err != nil {
			r.source("error")
			return nil, ErrUnavailable
		}
		snapshot.Version = 1
		snapshot.ObservedAt = observed
		if snapshot.Targets == nil {
			snapshot.Targets = []model.BindingTarget{}
		}
		if !ValidSnapshot(snapshot, time.Now()) {
			r.source("error")
			return nil, ErrUnavailable
		}
		r.source("success")
		if r.cache != nil {
			write, cancel := context.WithTimeout(query, 100*time.Millisecond)
			_ = r.cache.Put(write, clientID, snapshot)
			cancel()
		}
		return snapshot, nil
	})
	select {
	case <-ctx.Done():
		return model.BindingSnapshot{}, ErrUnavailable
	case value := <-result:
		if value.Err != nil {
			return model.BindingSnapshot{}, ErrUnavailable
		}
		return cloneSnapshot(value.Val.(model.BindingSnapshot)), nil
	}
}

func (r *Resolver) cached(ctx context.Context, id string) (model.BindingSnapshot, bool) {
	if r.cache == nil {
		if r.ObserveCache != nil {
			r.ObserveCache("miss")
		}
		return model.BindingSnapshot{}, false
	}
	read, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	snapshot, err := r.cache.Get(read, id)
	ok := err == nil && ValidSnapshot(snapshot, time.Now())
	if r.ObserveCache != nil {
		result := "miss"
		if ok {
			result = "hit"
		} else if err != nil && !errors.Is(err, ErrCacheMiss) {
			result = "error"
		}
		r.ObserveCache(result)
	}
	return snapshot, ok
}
func SnapshotTTL(s model.BindingSnapshot) time.Duration {
	if len(s.Targets) == 0 {
		return 5 * time.Second
	}
	return 30 * time.Second
}
func ValidSnapshot(s model.BindingSnapshot, now time.Time) bool {
	if s.Version != 1 || s.Targets == nil || s.ObservedAt.IsZero() || s.ObservedAt.After(now) || !s.ObservedAt.Add(SnapshotTTL(s)).After(now) || len(s.Targets) > 2 {
		return false
	}
	seen := map[string]bool{}
	for _, t := range s.Targets {
		if (t.PlatformID != "PLATFORM_A" && t.PlatformID != "PLATFORM_B") || !validBindingText(t.PlatformUID) || seen[t.PlatformID] {
			return false
		}
		seen[t.PlatformID] = true
	}
	return true
}
func cloneSnapshot(s model.BindingSnapshot) model.BindingSnapshot {
	s.Targets = append(make([]model.BindingTarget, 0, len(s.Targets)), s.Targets...)
	return s
}

func (r *Resolver) source(result string) {
	if r.ObserveSource != nil {
		r.ObserveSource(result)
	}
}
