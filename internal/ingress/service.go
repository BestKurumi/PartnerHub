package ingress

import (
	"context"
	"errors"
	"partner-hub/internal/delivery"
	"partner-hub/internal/infra"
	"partner-hub/internal/model"
	"time"
)

type receivedAtKey struct{}

var ErrUnconfirmed = errors.New("result unconfirmed")

type Router interface {
	ResolveTargets(context.Context, model.TradeEvent) (model.RouteSnapshot, error)
}
type FirstDelivery interface {
	Execute(context.Context, model.TradeEvent, model.RouteSnapshot) model.FirstRoundSnapshot
}
type Service struct {
	ObserveFirst func(model.FirstRoundSnapshot)
	router       Router
	first        FirstDelivery
	archive      infra.ResultArchive
	handoff      delivery.HandoffRepository
}

func NewService(r Router, f FirstDelivery, a infra.ResultArchive, handoff ...delivery.HandoffRepository) *Service {
	var h delivery.HandoffRepository
	if len(handoff) > 0 {
		h = handoff[0]
	}
	return &Service{router: r, first: f, archive: a, handoff: h}
}
func (s *Service) Process(ctx context.Context, trade model.TradeEvent) (model.TradeResponse, error) {
	response := model.TradeResponse{MsgID: trade.MsgID, Status: "DEPENDENCY_UNAVAILABLE", Acceptance: "UNCONFIRMED", RecordStatus: "UNCONFIRMED", StatusURL: "/api/v1/messages/" + trade.MsgID, PlatformResults: []model.PlatformResult{}}
	if ctx.Err() != nil {
		return response, ErrUnconfirmed
	}
	receivedAt := time.Now().UTC()
	if arrival, ok := ctx.Value(receivedAtKey{}).(time.Time); ok {
		receivedAt = arrival
	}
	deadline := receivedAt.Add(8 * time.Second)
	if callerDeadline, ok := ctx.Deadline(); ok && callerDeadline.Before(deadline) {
		deadline = callerDeadline
	}
	// Disconnects do not erase already-started side effects. Preserve only the
	// original deadline, and keep all work inside that service budget.
	process, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer cancel()
	if err := s.archive.Preflight(process); err != nil {
		return response, ErrUnconfirmed
	}
	route, err := s.router.ResolveTargets(process, trade)
	if err != nil {
		snapshot := model.FirstRoundSnapshot{Trade: trade, ReceivedAt: receivedAt, ArchiveRevision: model.NewID(), RouteError: "binding query unavailable", Route: model.RouteSnapshot{MsgID: trade.MsgID, Status: "RETRY_WAIT", ResolvedAt: time.Now().UTC(), Targets: []model.RouteTarget{}}, Attempts: []model.FirstAttempt{}}
		if s.ObserveFirst != nil {
			s.ObserveFirst(snapshot)
		}
		return s.acceptHandoff(process, snapshot, response, "RECOVERY_PENDING")
	}
	snapshot := s.first.Execute(process, trade, route)
	snapshot.ReceivedAt = receivedAt
	if s.ObserveFirst != nil {
		s.ObserveFirst(snapshot)
	}
	allSucceeded := len(snapshot.Attempts) == len(route.Targets)
	for _, attempt := range snapshot.Attempts {
		state := "RETRY_WAIT"
		if attempt.Result.Outcome == model.Success {
			state = "SUCCEEDED"
		} else {
			allSucceeded = false
		}
		if attempt.Result.Outcome == model.PermanentFailure {
			state = "NEEDS_ATTENTION"
		}
		response.PlatformResults = append(response.PlatformResults, model.PlatformResult{PlatformID: attempt.Target.PlatformID, DeliveryID: attempt.Target.DeliveryID, FirstOutcome: attempt.Result.Outcome, CurrentState: state})
	}
	if !allSucceeded {
		status := "NEEDS_ATTENTION"
		for _, attempt := range snapshot.Attempts {
			if attempt.Result.Outcome == model.Unknown || attempt.Result.Outcome == model.RetryableFailure {
				status = "RECOVERY_PENDING"
			}
		}
		return s.acceptHandoff(process, snapshot, response, status)
	}
	if err := s.archive.Stage(process, snapshot); err != nil {
		response.Status = "HANDOFF_UNCONFIRMED"
		return response, ErrUnconfirmed
	}
	response.Status, response.Acceptance, response.RecordStatus = "SUCCEEDED", "COMPLETED", "PENDING"
	if route.Status == "NO_ROUTE" {
		response.Status = "NO_ROUTE"
	}
	return response, nil
}

func (s *Service) acceptHandoff(ctx context.Context, snapshot model.FirstRoundSnapshot, response model.TradeResponse, status string) (model.TradeResponse, error) {
	response.Status = "HANDOFF_UNCONFIRMED"
	if s.handoff == nil || s.handoff.Handoff(ctx, snapshot) != nil {
		return response, ErrUnconfirmed
	}
	response.Status, response.Acceptance, response.RecordStatus = status, "ACCEPTED", "PERSISTED"
	return response, nil
}
