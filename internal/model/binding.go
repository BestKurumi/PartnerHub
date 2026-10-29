package model

import "time"

type BindingPosition struct {
	Topic     string
	Partition int32
	Offset    int64
}
type BindingEvent struct {
	SchemaVersion int             `json:"schemaVersion"`
	EventID       string          `json:"eventId"`
	EventType     string          `json:"eventType"`
	PlatformID    string          `json:"platformId"`
	ClientID      string          `json:"clientId"`
	PlatformUID   string          `json:"platformUid"`
	OccurredAt    int64           `json:"occurredAt"`
	Position      BindingPosition `json:"-"`
}
type BindingResult struct {
	EventID, Outcome string
	Revision         uint64
	Replayed         bool
}
type BindingTarget struct {
	PlatformID  string `json:"platformId"`
	PlatformUID string `json:"platformUid"`
}
type BindingSnapshot struct {
	Version    int             `json:"version"`
	ObservedAt time.Time       `json:"observedAt"`
	Targets    []BindingTarget `json:"targets"`
}
type BindingAnomaly struct {
	Position        BindingPosition
	EventID, Reason string
	Payload         []byte
}
