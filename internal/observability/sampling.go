package observability

import (
	"context"
	"time"
)

// Observation is one fixed backend observation. Scrapes only read local metrics.
// Each cycle is serial, never overlaps, and has a shared two-second deadline.
type Observation struct {
	Kind   string
	Sample func(context.Context) error
}

func (m *Metrics) RunObservations(ctx context.Context, interval time.Duration, samples []Observation) {
	if interval < time.Second {
		interval = time.Second
	}
	for {
		if ctx.Err() != nil {
			return
		}
		cycle, cancel := context.WithTimeout(ctx, 2*time.Second)
		for _, sample := range samples {
			if ctx.Err() != nil {
				break
			}
			one, finish := context.WithTimeout(cycle, 600*time.Millisecond)
			err := sample.Sample(one)
			finish()
			m.Observation(sample.Kind, err)
		}
		cancel()
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
