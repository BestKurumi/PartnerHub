package model

import "time"

// TradeNotification is the only trade value accepted across the adapter boundary.
type TradeNotification struct {
	SecCode, SecName, Side, Market, TradeType, TraceID string
	ExecTime                                           time.Time
}

type PreparedRequest struct {
	PlatformID, ProtocolVersion string
	Payload                     []byte
}

type SendOutcome string

const (
	Success          SendOutcome = "SUCCESS"
	RetryableFailure SendOutcome = "RETRYABLE_FAILURE"
	PermanentFailure SendOutcome = "PERMANENT_FAILURE"
	Unknown          SendOutcome = "UNKNOWN"
)

type SendResult struct {
	Outcome               SendOutcome
	SendStarted           bool
	StartedAt, FinishedAt time.Time
	Summary               string
}
