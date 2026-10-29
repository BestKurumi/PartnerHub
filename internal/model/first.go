package model

import "time"

type RouteTarget struct {
	PlatformID  string `json:"platformId"`
	PlatformUID string `json:"platformUid"`
	DeliveryID  string `json:"deliveryId"`
}
type RouteSnapshot struct {
	MsgID      string        `json:"msgId"`
	Status     string        `json:"routeStatus"`
	ResolvedAt time.Time     `json:"resolvedAt"`
	Targets    []RouteTarget `json:"targets"`
}
type FirstAttempt struct {
	Target  RouteTarget     `json:"target"`
	Request PreparedRequest `json:"request"`
	Result  SendResult      `json:"result"`
}
type FirstRoundSnapshot struct {
	RouteError      string         `json:"routeError,omitempty"`
	Trade           TradeEvent     `json:"trade"`
	Route           RouteSnapshot  `json:"route"`
	Attempts        []FirstAttempt `json:"attempts"`
	ReceivedAt      time.Time      `json:"receivedAt"`
	ArchiveRevision string         `json:"archiveRevision"`
}
type PlatformResult struct {
	PlatformID   string      `json:"platformId"`
	DeliveryID   string      `json:"deliveryId"`
	FirstOutcome SendOutcome `json:"firstOutcome"`
	CurrentState string      `json:"currentState"`
}
type TradeResponse struct {
	MsgID           string           `json:"msgId"`
	Status          string           `json:"status"`
	Acceptance      string           `json:"acceptance"`
	RecordStatus    string           `json:"recordStatus"`
	StatusURL       string           `json:"statusUrl"`
	PlatformResults []PlatformResult `json:"platformResults"`
}
