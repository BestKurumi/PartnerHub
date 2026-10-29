package delivery_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"partner-hub/internal/delivery"
)

type retryRepo struct {
	calls int
	got   delivery.RetryRequest
	err   error
}

func (r *retryRepo) ExecuteRetry(ctx context.Context, request delivery.RetryRequest) (delivery.RetryResult, error) {
	r.calls++
	r.got = request
	return delivery.RetryResult{OperationID: request.OperationID, Kind: request.Kind, TargetID: request.TargetID, RetryCycle: 2, State: "PENDING"}, r.err
}
func validRetry() delivery.RetryRequest {
	return delivery.RetryRequest{OperationID: "ab22b1bb-fcc2-41a4-a762-cbcc980ba12a", TargetID: "22342b23-e2ce-412a-a9f5-664a7b975578", Kind: delivery.RetryDelivery, Operator: "demo operator", Reason: "已修复连接配置"}
}
func TestManualCommandSavesIntentOnly(t *testing.T) {
	r := &retryRepo{}
	req := validRetry()
	got, err := delivery.NewRetryCommand(r).Execute(context.Background(), req)
	if err != nil || got.State != "PENDING" || got.RetryCycle != 2 || r.calls != 1 || r.got != req {
		t.Fatalf("manual intent not saved: %+v %v calls=%d", got, err, r.calls)
	}
	for _, want := range []error{delivery.ErrRetryConflict, delivery.ErrRetryNotFound, errors.New("DB unavailable")} {
		r.err = want
		_, err = delivery.NewRetryCommand(r).Execute(context.Background(), req)
		if !errors.Is(err, want) {
			t.Fatalf("lost result: %v", err)
		}
	}
}
func TestManualCommandRejectsInvalidIntent(t *testing.T) {
	for name, edit := range map[string]func(*delivery.RetryRequest){
		"operation":       func(r *delivery.RetryRequest) { r.OperationID = "invalid" },
		"uuid length":     func(r *delivery.RetryRequest) { r.OperationID = "aa--aaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" },
		"target":          func(r *delivery.RetryRequest) { r.TargetID = "" },
		"kind":            func(r *delivery.RetryRequest) { r.Kind = "TRADE" },
		"operator":        func(r *delivery.RetryRequest) { r.Operator = "  " },
		"reason":          func(r *delivery.RetryRequest) { r.Reason = "" },
		"control":         func(r *delivery.RetryRequest) { r.Reason = "repair\nconfig" },
		"operator length": func(r *delivery.RetryRequest) { r.Operator = strings.Repeat("字", 129) },
		"reason length":   func(r *delivery.RetryRequest) { r.Reason = strings.Repeat("字", 513) },
		"utf8":            func(r *delivery.RetryRequest) { r.Reason = string([]byte{255}) },
	} {
		t.Run(name, func(t *testing.T) {
			req := validRetry()
			edit(&req)
			r := &retryRepo{}
			_, err := delivery.NewRetryCommand(r).Execute(context.Background(), req)
			if !errors.Is(err, delivery.ErrInvalidRetry) || r.calls != 0 {
				t.Fatalf("invalid intent reached persistence: %v calls=%d", err, r.calls)
			}
			if delivery.ValidateRetryRequest(req) == nil {
				t.Fatal("repository validator accepted invalid intent")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &retryRepo{}
	_, err := delivery.NewRetryCommand(r).Execute(ctx, validRetry())
	if !errors.Is(err, context.Canceled) || r.calls != 0 {
		t.Fatalf("cancelled intent executed %v", err)
	}
}
