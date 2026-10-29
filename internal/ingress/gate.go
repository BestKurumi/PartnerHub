package ingress

import (
	"context"
	"errors"
	"partner-hub/internal/infra"
	"sync"
	"time"
)

const MaxArchiveSnapshotBytes int64 = 256 * 1024

var ErrArchiveCapacity = errors.New("archive capacity unavailable")

type ArchiveCapacitySource interface {
	Capacity(context.Context) (infra.ArchiveCapacity, error)
}
type ArchiveGateConfig struct {
	MaxPending, MaxBytes, ReserveRequests int64
	MaxSampleAge                          time.Duration
}

func DefaultArchiveGateConfig() ArchiveGateConfig {
	return ArchiveGateConfig{10000, 128 * 1024 * 1024, 308, 250 * time.Millisecond}
}
func (c ArchiveGateConfig) Validate() error {
	if c.MaxPending < 1 || c.MaxPending > 1000000000 || c.MaxBytes < 1 || c.MaxBytes > 1<<40 || c.ReserveRequests < 1 || c.ReserveRequests >= c.MaxPending*60/100 || c.ReserveRequests >= c.MaxBytes*80/100/MaxArchiveSnapshotBytes || c.MaxSampleAge <= 0 || c.MaxSampleAge > 250*time.Millisecond {
		return ErrArchiveCapacity
	}
	return nil
}

// ArchiveGate uses fresh observations and conservative two-instance headroom.
// It never reserves msgId or grants first-send ownership in Redis.
type ArchiveGate struct {
	infra.ResultArchive
	source ArchiveCapacitySource
	config ArchiveGateConfig
	mu     sync.Mutex
	paused bool
	latest time.Time
}

func NewArchiveGate(archive infra.ResultArchive, source ArchiveCapacitySource, config ArchiveGateConfig) (*ArchiveGate, error) {
	if archive == nil || source == nil {
		return nil, ErrArchiveCapacity
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &ArchiveGate{ResultArchive: archive, source: source, config: config}, nil
}
func (g *ArchiveGate) Preflight(ctx context.Context) error {
	sampled := time.Now()
	check, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	sample, err := g.source.Capacity(check)
	g.mu.Lock()
	defer g.mu.Unlock()
	// An older concurrent observation cannot reopen a gate closed more recently.
	if sampled.Before(g.latest) {
		return ErrArchiveCapacity
	}
	g.latest = sampled
	age := time.Since(sample.SampledAt)
	if err != nil || check.Err() != nil || sample.SampledAt.IsZero() || age < 0 || age > g.config.MaxSampleAge || sample.Pending < 0 || sample.Bytes < 0 || sample.UsedMemory < 0 || sample.MaxMemory <= 0 || sample.MaxMemory > 1<<50 {
		g.paused = true
		return ErrArchiveCapacity
	}
	c := g.config
	reserveBytes := c.ReserveRequests * MaxArchiveSnapshotBytes
	high := sample.Pending >= c.MaxPending*80/100 || sample.Bytes >= c.MaxBytes*80/100 || sample.UsedMemory >= sample.MaxMemory*80/100
	headroom := sample.Pending >= c.MaxPending*80/100-c.ReserveRequests || sample.Bytes >= c.MaxBytes*80/100-reserveBytes || sample.UsedMemory >= sample.MaxMemory*80/100-reserveBytes
	low := sample.Pending <= c.MaxPending*60/100 && sample.Bytes <= c.MaxBytes*60/100 && sample.UsedMemory <= sample.MaxMemory*60/100
	if high || headroom {
		g.paused = true
	}
	if g.paused && low && !headroom {
		g.paused = false
	}
	if g.paused {
		return ErrArchiveCapacity
	}
	return nil
}
