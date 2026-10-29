package main

import (
	"errors"
	"log/slog"
	"testing"

	"partner-hub/internal/delivery"
	"partner-hub/internal/diagnostics"
	"partner-hub/internal/model"
)

func TestRecoveryAnomaliesRetainedWithoutRawError(t *testing.T) {
	buffer := diagnostics.NewBuffer("hub-test")
	previous := slog.Default()
	slog.SetDefault(slog.New(diagnostics.NewLogHandler(nil, buffer)))
	defer slog.SetDefault(previous)
	claim := delivery.RecoveryClaim{}
	claim.Target.DeliveryID = model.NewID()
	claim.Target.PlatformID = "PLATFORM_B"
	claim.Notification.TraceID = "recovery-test"
	logRecoveryResult(claim, model.SendResult{Outcome: model.Success}, nil)
	if len(buffer.Snapshot(diagnostics.Query{Limit: 20}).Items) != 0 {
		t.Fatal("normal recovery entered diagnostic warning log")
	}
	logRecoveryResult(claim, model.SendResult{Outcome: model.Unknown}, errors.New("password=never-retain"))
	s := buffer.Snapshot(diagnostics.Query{Limit: 20})
	if len(s.Items) != 1 || s.Items[0].Kind != "recovery_result" || s.Items[0].DeliveryID != claim.Target.DeliveryID || s.Items[0].Outcome != string(model.Unknown) || s.Items[0].Level != "WARN" {
		t.Fatalf("failed recovery missing safe diagnostic: %+v", s.Items)
	}
}
