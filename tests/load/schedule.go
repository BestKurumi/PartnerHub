package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"partner-hub/internal/model"
	"sync"
	"time"
)

func makeInputs(n int, client string) []Input {
	rows := make([]Input, n)
	for i := range rows {
		id := model.NewID()
		rows[i] = Input{MsgID: id, TraceID: "trace-" + id, ClientID: client, Disposition: "planned", Targets: map[string]Target{}}
	}
	return rows
}

// Explicit legal twenty-field fixture. Never retries an uncertain submission.
func tradePayload(r Input) []byte {
	b, _ := json.Marshal(map[string]any{"msgId": r.MsgID, "eventType": "TRADE_EXECUTION", "produceTime": time.Now().UnixMilli(), "source": "counter_system", "clientId": r.ClientID, "accountId": "000456", "orderId": "000789", "businessId": "000012", "traceId": r.TraceID, "secCode": "000001", "secName": "PingAn", "side": "BUY", "orderType": "LIMIT", "execQuantity": json.Number("100.123456789012"), "execPrice": json.Number("0.123456789012"), "execAmount": json.Number("12.34"), "execTime": "2026-10-23 09:30:00.123", "orderStatus": "FILLED", "market": "SZ", "tradeType": "NORMAL"})
	return b
}

func runSchedule(ctx context.Context, c *http.Client, base string, rows []Input, interval time.Duration, limit int) {
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	start := time.Now()
	for i := range rows {
		r := &rows[i]
		r.PlannedAt = start.Add(time.Duration(i) * interval)
		timer := time.NewTimer(time.Until(r.PlannedAt))
		select {
		case <-ctx.Done():
			timer.Stop()
			wg.Wait()
			return
		case <-timer.C:
		}
		offered := time.Now()
		r.OfferedAt = &offered
		// The schedule never waits for a worker: every offered slot remains visible.
		select {
		case sem <- struct{}{}:
		default:
			r.Disposition = "local_rejected"
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			payload := tradePayload(*r)
			r.Bytes = len(payload)
			now := time.Now()
			r.SubmittedAt = &now
			defer func() { finished := time.Now(); r.HTTPFinishedAt = &finished }()
			req, e := http.NewRequestWithContext(ctx, "POST", base+"/api/v1/trades", bytes.NewReader(payload))
			if e != nil {
				r.Disposition = "transport_error"
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, e := c.Do(req)
			if e != nil {
				r.Disposition = "transport_error"
				return
			}
			defer resp.Body.Close()
			r.HTTPStatus = resp.StatusCode
			if resp.StatusCode != 200 && resp.StatusCode != 202 {
				_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 8193))
				r.Disposition = "http_rejected"
				if resp.StatusCode >= 500 {
					r.Disposition = "http_unconfirmed"
				}
				return
			}
			raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 8193))
			var response model.TradeResponse
			if readErr != nil {
				r.Disposition = "transport_error"
				return
			}
			if len(raw) > 8192 || json.Unmarshal(raw, &response) != nil || response.MsgID != r.MsgID {
				r.Disposition = "invalid_response"
				return
			}
			confirmed := resp.StatusCode == 200 && response.Acceptance == "COMPLETED" && (response.Status == "SUCCEEDED" || response.Status == "NO_ROUTE") || resp.StatusCode == 202 && response.Acceptance == "ACCEPTED" && response.Status == "RECOVERY_PENDING" || resp.StatusCode == 200 && response.Acceptance == "ACCEPTED" && response.Status == "NEEDS_ATTENTION"
			if confirmed {
				r.Disposition = "accepted"
			} else {
				r.Disposition = "invalid_response"
			}
		}()
	}
	wg.Wait()
}
