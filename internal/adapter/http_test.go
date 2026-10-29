package adapter_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"partner-hub/internal/adapter"
	"partner-hub/internal/delivery"
	"partner-hub/internal/model"
)

var _ delivery.Adapter = (*adapter.HTTP)(nil)

func notification() model.TradeNotification {
	return model.TradeNotification{SecCode: "000001", SecName: "平安银行", Side: "BUY", Market: "SZ", TradeType: "NORMAL", TraceID: "trace-001", ExecTime: time.Date(2026, 10, 22, 1, 30, 0, 999000000, time.UTC)}
}

// Catches missing/extra fields, leaking full trades, enum defaults and UTC formatting.
func TestPlatform1Contract(t *testing.T) {
	for _, tt := range []struct{ in, want string }{{"NORMAL", "TRADE_NORMAL"}, {"IPO", "TRADE_IPO"}, {"ETF", "TRADE_ETF"}, {"MARGIN", "TRADE_MARGIN"}, {"BLOCK", "TRADE_BLOCK"}} {
		t.Run(tt.in, func(t *testing.T) {
			a, err := adapter.NewPlatform1("http://127.0.0.1:9999/notifications/trades")
			if err != nil {
				t.Fatal(err)
			}
			n := notification()
			n.TradeType = tt.in
			p, err := a.Prepare(n, "00009")
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]string
			if err = json.Unmarshal(p.Payload, &got); err != nil {
				t.Fatal(err)
			}
			want := map[string]string{"userUid": "00009", "securityCode": "000001", "securityName": "平安银行", "direction": "BUY", "execTime": "2026-10-22 09:30:00", "market": "SZ", "tradeType": tt.want, "traceId": "trace-001"}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("wire contract: got %v, want %v", got, want)
			}
			if p.PlatformID != "PLATFORM_A" || p.ProtocolVersion == "" {
				t.Fatalf("missing replay metadata: %+v", p)
			}
		})
	}
}

// Catches defaulting invalid enums or silently constructing unusable requests.
func TestPlatform1PrepareRejectsInvalid(t *testing.T) {
	a, _ := adapter.NewPlatform1("http://localhost/notifications/trades")
	for _, tt := range []struct {
		name string
		edit func(*model.TradeNotification)
		uid  string
	}{
		{"type", func(n *model.TradeNotification) { n.TradeType = "UNKNOWN" }, "uid"},
		{"side", func(n *model.TradeNotification) { n.Side = "HOLD" }, "uid"},
		{"market", func(n *model.TradeNotification) { n.Market = "HK" }, "uid"},
		{"time", func(n *model.TradeNotification) { n.ExecTime = time.Time{} }, "uid"},
		{"name", func(n *model.TradeNotification) { n.SecName = "" }, "uid"},
		{"uid", func(n *model.TradeNotification) {}, ""},
		{"uid length", func(n *model.TradeNotification) {}, strings.Repeat("界", 129)},
		{"code length", func(n *model.TradeNotification) { n.SecCode = strings.Repeat("界", 65) }, "uid"},
		{"control", func(n *model.TradeNotification) { n.TraceID = "trace\n" }, "uid"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			n := notification()
			tt.edit(&n)
			if p, err := a.Prepare(n, tt.uid); err == nil || len(p.Payload) != 0 {
				t.Fatalf("invalid prepared: %+v %v", p, err)
			}
		})
	}
}

// Catches treating status alone as success and leaking arbitrary response content.
func TestPlatform1ResponseClassification(t *testing.T) {
	for _, tt := range []struct {
		name   string
		status int
		body   string
		want   model.SendOutcome
	}{
		{"ok", 200, `{"code":0}`, model.Success},
		{"ok message", 200, `{"code":0,"message":"secret-price-123"}`, model.Success},
		{"busy", 200, `{"code":"BUSY"}`, model.RetryableFailure},
		{"escaped busy", 200, `{"code":"B\u0055SY"}`, model.RetryableFailure},
		{"429", 429, `rate limited`, model.RetryableFailure},
		{"bad field", 400, `{"code":"INVALID_FIELD"}`, model.PermanentFailure},
		{"unprocessable", 422, `bad`, model.PermanentFailure},
		{"unauthorized", 401, `bad`, model.PermanentFailure},
		{"forbidden", 403, `bad`, model.PermanentFailure},
		{"server", 500, `{"code":0}`, model.Unknown},
		{"204", 204, ``, model.Unknown},
		{"empty", 200, ``, model.Unknown},
		{"missing", 200, `{}`, model.Unknown},
		{"null", 200, `{"code":null}`, model.Unknown},
		{"string zero", 200, `{"code":"0"}`, model.Unknown},
		{"business failure", 200, `{"code":42}`, model.Unknown},
		{"duplicate", 200, `{"code":9,"code":0}`, model.Unknown},
		{"trailing", 200, `{"code":0}{}`, model.Unknown},
		{"large", 200, `{"code":0,"message":"` + strings.Repeat("s", 8192) + `"}`, model.Unknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				var got map[string]any
				_ = json.Unmarshal(b, &got)
				if r.Method != "POST" || r.URL.Path != "/notifications/trades" || r.Header.Get("Content-Type") != "application/json" || len(got) != 8 {
					t.Error("bad HTTP request")
				}
				for _, h := range []string{"Idempotency-Key", "X-Msg-Id", "Authorization"} {
					if r.Header.Get(h) != "" {
						t.Errorf("unexpected %s", h)
					}
				}
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer s.Close()
			a, _ := adapter.NewPlatform1(s.URL + "/notifications/trades")
			p, err := a.Prepare(notification(), "uid")
			if err != nil {
				t.Fatal(err)
			}
			r := a.Send(context.Background(), p)
			if r.Outcome != tt.want || !r.SendStarted {
				t.Fatalf("result: %+v, want %s started", r, tt.want)
			}
			if r.StartedAt.IsZero() || r.FinishedAt.Before(r.StartedAt) {
				t.Fatalf("invalid attempt times: %+v", r)
			}
			if len(r.Summary) > 2048 || strings.Contains(r.Summary, "secret-price-123") || strings.Contains(r.Summary, strings.Repeat("s", 50)) {
				t.Fatal("response leaked into summary")
			}
		})
	}
}

func TestPlatform1NeverFollowsRedirect(t *testing.T) {
	var received atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received.Add(1); io.WriteString(w, `{"code":0}`) }))
	defer target.Close()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer s.Close()
	a, _ := adapter.NewPlatform1(s.URL)
	p, _ := a.Prepare(notification(), "uid")
	if r := a.Send(context.Background(), p); r.Outcome != model.Unknown {
		t.Fatalf("redirect result: %+v", r)
	}
	if received.Load() != 0 {
		t.Fatal("redirect caused unapproved delivery")
	}
}

func TestPlatform1PreConnectFailure(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	l.Close()
	a, _ := adapter.NewPlatform1("http://" + address)
	p, _ := a.Prepare(notification(), "uid")
	if r := a.Send(context.Background(), p); r.Outcome != model.RetryableFailure || r.SendStarted {
		t.Fatalf("preconnect: %+v", r)
	}
}

func TestPlatform1SentTimeoutIsUnknown(t *testing.T) {
	received := make(chan struct{}, 1)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		received <- struct{}{}
		<-r.Context().Done()
	}))
	defer s.Close()
	a, _ := adapter.NewPlatform1(s.URL)
	p, _ := a.Prepare(notification(), "uid")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	r := a.Send(ctx, p)
	if r.Outcome != model.Unknown || !r.SendStarted {
		t.Fatalf("sent timeout: %+v", r)
	}
	select {
	case <-received:
	default:
		t.Fatal("server never received")
	}
}

func TestPlatform1InvalidEndpoint(t *testing.T) {
	for _, endpoint := range []string{"", "file:///tmp/notify", "http://u:p@localhost/notify", "http://localhost/#x"} {
		if _, err := adapter.NewPlatform1(endpoint); err == nil {
			t.Errorf("accepted endpoint %q", endpoint)
		}
	}
}
