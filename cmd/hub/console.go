package main

import (
	"context"
	"log/slog"
	"net/http"
	"partner-hub/internal/delivery"
	"partner-hub/internal/model"
)

// Internal diagnostics are mounted separately, never forwarded through this
// public/query backend. Both public mounts apply demo authorization outside it.
func consoleBackend(query, ops, diagnostics http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/api/v1/ops/diagnostics", diagnostics)
	mux.Handle("/api/v1/ops/", ops)
	mux.Handle("/", query)
	return mux
}

func logRecoveryResult(claim delivery.RecoveryClaim, result model.SendResult, err error) {
	level := slog.LevelInfo
	if err != nil || result.Outcome != model.Success {
		level = slog.LevelWarn
	}
	slog.Log(context.Background(), level, "recovery result", "deliveryId", claim.Target.DeliveryID, "traceId", claim.Notification.TraceID, "platform", claim.Target.PlatformID, "outcome", string(result.Outcome))
}
