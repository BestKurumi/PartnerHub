package model

import (
	"encoding/json"
	"time"
)

// TradeEvent stays inside the broker boundary; adapters receive a separate,
// explicitly restricted notification rather than this complete event.
type TradeEvent struct {
	MsgID        string
	EventType    string
	ProduceTime  int64
	Source       string
	ClientID     string
	AccountID    string
	OrderID      string
	BusinessID   string
	TraceID      string
	SecCode      string
	SecName      string
	Side         string
	OrderType    string
	ExecQuantity json.Number
	ExecPrice    json.Number
	ExecAmount   json.Number
	ExecTime     time.Time
	OrderStatus  string
	Market       string
	TradeType    string
	RawPayload   []byte
}
