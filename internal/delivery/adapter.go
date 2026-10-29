package delivery

import (
	"context"
	"partner-hub/internal/model"
)

type Adapter interface {
	Prepare(model.TradeNotification, string) (model.PreparedRequest, error)
	Send(context.Context, model.PreparedRequest) model.SendResult
}
