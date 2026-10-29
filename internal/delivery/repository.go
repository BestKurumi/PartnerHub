package delivery

import (
	"context"
	"errors"
	"partner-hub/internal/model"
	"time"
)

// HandoffRepository confirms the complete first-round responsibility in MySQL.
type HandoffRepository interface {
	Handoff(context.Context, model.FirstRoundSnapshot) error
}

// RecoveryClaim is a bounded execution lease for one fixed platform target.
// Notification excludes raw trade data and sensitive amount/quantity/price.
type RecoveryClaim struct {
	ReceivedAt                       time.Time
	Target                           model.RouteTarget
	Notification                     model.TradeNotification
	Owner                            string
	LeaseVersion                     uint64
	LeaseUntil                       time.Time
	LeaseDeadline                    time.Time // Same-process monotonic deadline sampled before DB time, never persisted.
	AttemptNo, CycleNo, CycleAttempt int
}

var ErrLeaseLost = errors.New("recovery lease no longer valid")

type RecoveryRepository interface {
	// HasDue is a non-locking hint, not a lease. ClaimDue must recheck eligibility.
	HasDue(context.Context, string) (bool, error)
	ClaimDue(context.Context, string, string, int) ([]RecoveryClaim, error)
	SavePrepared(context.Context, RecoveryClaim, model.PreparedRequest) error
	Complete(context.Context, RecoveryClaim, model.SendResult) error
}
