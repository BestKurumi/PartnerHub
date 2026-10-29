package binding

import (
	"context"
	"log/slog"
	"partner-hub/internal/model"
	"time"
)

type Repository interface {
	Apply(context.Context, model.BindingEvent) (model.BindingResult, error)
	Load(context.Context, string) (model.BindingSnapshot, error)
	SaveAnomaly(context.Context, model.BindingAnomaly) error
}
type Cache interface {
	Get(context.Context, string) (model.BindingSnapshot, error)
	Put(context.Context, string, model.BindingSnapshot) error
	Invalidate(context.Context, string) error
}
type Service struct {
	repository Repository
	cache      Cache
}

func NewService(repository Repository, cache Cache) *Service {
	return &Service{repository: repository, cache: cache}
}
func (s *Service) Apply(ctx context.Context, e model.BindingEvent) (model.BindingResult, error) {
	if err := ValidateEvent(e); err != nil {
		return model.BindingResult{}, err
	}
	result, err := s.repository.Apply(ctx, e)
	if err != nil {
		return model.BindingResult{}, err
	}
	if s.cache != nil && result.Outcome != "EVENT_ID_CONFLICT" {
		invalidate, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		if err := s.cache.Invalidate(invalidate, e.ClientID); err != nil {
			slog.Warn("binding cache invalidation failed; TTL convergence required")
		}
	}
	return result, nil
}
