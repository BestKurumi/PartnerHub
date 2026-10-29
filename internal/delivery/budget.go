package delivery

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

const FirstClass = "FIRST"
const RecoveryClass = "RECOVERY"

type BudgetConfig struct {
	Total, First, Recovery int
	Rate                   float64
	Burst                  int
	Paused                 bool
}

func DefaultBudgetConfig() BudgetConfig {
	return BudgetConfig{Total: 16, First: 12, Recovery: 4, Rate: 100, Burst: 4}
}

// PlatformBudget is local to one hub instance. First and recovery entry points
// must share this same object; each platform owns independent resources.
type PlatformBudget struct {
	platforms map[string]*platformBudgetState
}

type platformBudgetState struct {
	mu                      sync.Mutex
	config                  BudgetConfig
	active, first, recovery int
	tokens                  float64
	updated                 time.Time
}

// BudgetPermit reserves both an execution slot and a rate token without a local
// waiting queue. Release consumes the token; Cancel refunds an unused reservation
// (for example an empty recovery scan). Exactly one of them can take effect.
type BudgetPermit struct {
	state *platformBudgetState
	class string
	once  sync.Once
}

func NewPlatformBudget(configs map[string]BudgetConfig) (*PlatformBudget, error) {
	b := &PlatformBudget{platforms: make(map[string]*platformBudgetState, len(configs))}
	for platform, cfg := range configs {
		if platform == "" || cfg.Total <= 0 || cfg.First <= 0 || cfg.Recovery <= 0 || cfg.First > cfg.Total || cfg.Recovery > cfg.Total || cfg.Rate <= 0 || math.IsNaN(cfg.Rate) || math.IsInf(cfg.Rate, 0) || cfg.Burst <= 0 {
			return nil, fmt.Errorf("invalid platform execution budget for %q", platform)
		}
		b.platforms[platform] = &platformBudgetState{config: cfg, tokens: float64(cfg.Burst), updated: time.Now()}
	}
	return b, nil
}

func (b *PlatformBudget) TryAcquire(ctx context.Context, platform, class string) (*BudgetPermit, bool) {
	if b == nil || ctx.Err() != nil || (class != FirstClass && class != RecoveryClass) {
		return nil, false
	}
	s := b.platforms[platform]
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil || s.config.Paused || s.active >= s.config.Total || (class == FirstClass && s.first >= s.config.First) || (class == RecoveryClass && s.recovery >= s.config.Recovery) {
		return nil, false
	}
	s.refill(time.Now())
	if s.tokens < 1 {
		return nil, false
	}
	s.tokens--
	s.active++
	if class == FirstClass {
		s.first++
	} else {
		s.recovery++
	}
	return &BudgetPermit{state: s, class: class}, true
}

// Refill uses elapsed monotonic time. The token bucket permits an initial Burst
// plus Rate*elapsed admissions; Rate is not an arbitrary rolling-second ceiling.
func (s *platformBudgetState) refill(now time.Time) {
	if elapsed := now.Sub(s.updated).Seconds(); elapsed > 0 {
		s.tokens = math.Min(float64(s.config.Burst), s.tokens+elapsed*s.config.Rate)
		s.updated = now
	}
}

func (p *BudgetPermit) Release() { p.finish(false) }
func (p *BudgetPermit) Cancel()  { p.finish(true) }
func (p *BudgetPermit) finish(refund bool) {
	if p == nil {
		return
	}
	p.once.Do(func() {
		s := p.state
		if s == nil {
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.active--
		if p.class == FirstClass {
			s.first--
		} else {
			s.recovery--
		}
		if refund {
			s.refill(time.Now())
			s.tokens = math.Min(float64(s.config.Burst), s.tokens+1)
		}
	})
}
