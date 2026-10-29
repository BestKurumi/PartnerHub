package adapter_test

import (
	"context"
	"encoding/json"
	"partner-hub/internal/adapter"
	"partner-hub/internal/model"
	"testing"
	"time"
)

func TestPlatform2Contract(t *testing.T) {
	a, err := adapter.NewPlatform2([]string{"127.0.0.1:1"}, "platform2.trade.notifications")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	for i, kind := range []string{"NORMAL", "IPO", "ETF", "MARGIN", "BLOCK"} {
		n := notification()
		n.TradeType = kind
		p, err := a.Prepare(n, "00009")
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		var got map[string]any
		if json.Unmarshal(p.Payload, &got) != nil {
			t.Fatalf("invalid payload %s", p.Payload)
		}
		want := map[string]any{"extUserId": "00009", "stockCode": n.SecCode, "stockName": n.SecName, "operateType": "BUY", "tradeDateTime": "2026-10-22 09:30:00.999", "marketNo": "SZ", "bizType": float64(i + 1), "traceId": n.TraceID}
		if len(got) != 8 || p.PlatformID != "PLATFORM_B" || p.ProtocolVersion != "platform2.v1" {
			t.Fatalf("contract: %+v %s", p, got)
		}
		for key, value := range want {
			if got[key] != value {
				t.Fatalf("%s: %v want %v", key, got[key], value)
			}
		}
	}
	n := notification()
	n.TradeType = "unsupported"
	if _, err = a.Prepare(n, "UID"); err == nil {
		t.Fatal("unknown type accepted")
	}
	if _, err = a.Prepare(notification(), ""); err == nil {
		t.Fatal("empty UID accepted")
	}
}

func TestPlatform2BrokerUnconfirmed(t *testing.T) {
	a, err := adapter.NewPlatform2([]string{"127.0.0.1:1"}, "platform2.trade.notifications")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	p, err := a.Prepare(notification(), "uid")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	started := time.Now()
	result := a.Send(ctx, p)
	if result.Outcome != model.Unknown || !result.SendStarted || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("unbounded or false confirmation: %+v", result)
	}
}
func TestPlatform2PreSendAndConfig(t *testing.T) {
	for _, tt := range []struct {
		brokers []string
		topic   string
	}{{nil, "platform2.trade.notifications"}, {[]string{"broker:9092"}, ""}, {[]string{""}, "platform2.trade.notifications"}} {
		a, err := adapter.NewPlatform2(tt.brokers, tt.topic)
		if err == nil {
			a.Close()
			t.Fatal("invalid config accepted")
		}
	}
	a, err := adapter.NewPlatform2([]string{"127.0.0.1:1"}, "platform2.trade.notifications")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if r := a.Send(context.Background(), model.PreparedRequest{}); r.Outcome != model.PermanentFailure || r.SendStarted {
		t.Fatalf("invalid prepared: %+v", r)
	}
	p, err := a.Prepare(notification(), "uid")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r := a.Send(ctx, p); r.Outcome != model.RetryableFailure || r.SendStarted || r.StartedAt.IsZero() || r.FinishedAt.Before(r.StartedAt) {
		t.Fatalf("canceled before send: %+v", r)
	}
}
