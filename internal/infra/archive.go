package infra

import (
	"context"
	"errors"
	"partner-hub/internal/model"
	"time"
)

// ArchiveCapacity is a bounded observation, never an atomic global reservation.
type ArchiveCapacity struct {
	Durable                               bool
	Pending, Bytes, UsedMemory, MaxMemory int64
	SampledAt                             time.Time
}

type ResultArchive interface {
	Stage(context.Context, model.FirstRoundSnapshot) error
	Flush(context.Context) error
	Preflight(context.Context) error
}

func ValidateNormalSnapshot(s model.FirstRoundSnapshot) error {
	invalid := errors.New("invalid normal archive snapshot")
	if s.Trade.MsgID == "" || s.Route.MsgID != s.Trade.MsgID || s.ArchiveRevision == "" || s.ReceivedAt.IsZero() || s.Route.ResolvedAt.IsZero() || len(s.Trade.RawPayload) == 0 || len(s.Trade.RawPayload) > 65536 || len(s.Route.Targets) > 2 || len(s.Attempts) != len(s.Route.Targets) {
		return invalid
	}
	if len(s.Route.Targets) == 0 {
		if s.Route.Status != "NO_ROUTE" {
			return invalid
		}
		return nil
	}
	if s.Route.Status != "ROUTED" {
		return invalid
	}
	seen := map[string]bool{}
	for i, target := range s.Route.Targets {
		a := s.Attempts[i]
		if (target.PlatformID != "PLATFORM_A" && target.PlatformID != "PLATFORM_B") || target.PlatformUID == "" || target.DeliveryID == "" || seen[target.PlatformID] || a.Target != target || a.Result.Outcome != model.Success || !a.Result.SendStarted || a.Result.StartedAt.IsZero() || a.Result.FinishedAt.Before(a.Result.StartedAt) || a.Request.PlatformID != target.PlatformID || a.Request.ProtocolVersion == "" || len(a.Request.Payload) == 0 || len(a.Request.Payload) > 8192 || len(a.Result.Summary) > 2048 {
			return invalid
		}
		seen[target.PlatformID] = true
	}
	return nil
}
