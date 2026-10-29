package platform1_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"partner-hub/internal/adapter"
	"partner-hub/internal/mock/platform1"
	"partner-hub/internal/model"
)

const legal = `{"userUid":"00009","securityCode":"000001","securityName":"平安银行","direction":"BUY","execTime":"2026-10-22 09:30:00","market":"SZ","tradeType":"TRADE_NORMAL","traceId":"trace-001"}`

// Only the slow external commit boundary is controlled; HTTP parsing stays real.
type controlledStore struct {
	mu               sync.Mutex
	rows             []platform1.Receipt
	err              error
	entered, release chan struct{}
}

func (s *controlledStore) Save(ctx context.Context, r platform1.Receipt) error {
	if s.entered != nil {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.rows = append(s.rows, r)
	return nil
}
func (s *controlledStore) snapshot() []platform1.Receipt {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]platform1.Receipt(nil), s.rows...)
}
func handler(t *testing.T, s platform1.Store, m platform1.Mode, d time.Duration) http.Handler {
	t.Helper()
	h, e := platform1.NewHandler(s, m, d)
	if e != nil {
		t.Fatal(e)
	}
	return h
}
func post(h http.Handler, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/notifications/trades", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// Catches acknowledging before commit and accidental traceId business deduplication.
func TestPlatform1MockSavesBeforeSuccess(t *testing.T) {
	s := &controlledStore{entered: make(chan struct{}), release: make(chan struct{})}
	h := handler(t, s, platform1.Accept, 0)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- post(h, legal) }()
	select {
	case <-s.entered:
	case <-done:
		t.Fatal("responded before saving")
	case <-time.After(time.Second):
		t.Fatal("save not reached")
	}
	select {
	case <-done:
		t.Fatal("acknowledged uncommitted receipt")
	default:
	}
	close(s.release)
	w := <-done
	var response struct {
		Code      int
		ReceiveID string
	}
	if json.Unmarshal(w.Body.Bytes(), &response) != nil || w.Code != 200 || response.Code != 0 || response.ReceiveID == "" {
		t.Fatalf("ack: %d %s", w.Code, w.Body.String())
	}
	rows := s.snapshot()
	if len(rows) != 1 || string(rows[0].Payload) != legal || rows[0].UserUID != "00009" || rows[0].TraceID != "trace-001" || rows[0].ReceiveID != response.ReceiveID {
		t.Fatalf("receipt: %+v", rows)
	}
	s.entered = nil
	post(h, legal)
	rows = s.snapshot()
	if len(rows) != 2 || rows[0].ReceiveID == rows[1].ReceiveID {
		t.Fatal("repeat delivery was deduplicated")
	}
}

// Catches permissive field/encoding decoding or accepting sensitive/extra values.
func TestPlatform1MockStrictContract(t *testing.T) {
	for name, body := range map[string]string{
		"extra":        strings.TrimSuffix(legal, "}") + `,"execAmount":123}`,
		"missing":      strings.Replace(legal, `"traceId":"trace-001"`, `"other":"trace-001"`, 1),
		"null":         strings.Replace(legal, `"00009"`, `null`, 1),
		"number":       strings.Replace(legal, `"00009"`, `9`, 1),
		"empty":        strings.Replace(legal, `"00009"`, `""`, 1),
		"duplicate":    strings.TrimSuffix(legal, "}") + `,"user\u0055id":"x"}`,
		"extra value":  legal + `{}`,
		"type":         strings.Replace(legal, "TRADE_NORMAL", "NORMAL", 1),
		"side":         strings.Replace(legal, "BUY", "HOLD", 1),
		"market":       strings.Replace(legal, `"SZ"`, `"HK"`, 1),
		"millis":       strings.Replace(legal, "09:30:00", "09:30:00.123", 1),
		"invalid date": strings.Replace(legal, "2026-10-22", "2026-02-30", 1),
		"too long":     strings.Replace(legal, "00009", strings.Repeat("界", 129), 1),
		"control":      strings.Replace(legal, "00009", `uid\n`, 1),
		"invalid utf8": strings.Replace(legal, "平安银行", string([]byte{0xff}), 1),
		"body limit":   legal + strings.Repeat(" ", 65536),
	} {
		t.Run(name, func(t *testing.T) {
			s := &controlledStore{}
			w := post(handler(t, s, platform1.Accept, 0), body)
			if w.Code != 400 && w.Code != 413 {
				t.Fatalf("accepted: %d %s", w.Code, w.Body.String())
			}
			if len(s.snapshot()) != 0 {
				t.Fatal("invalid payload persisted")
			}
		})
	}
	for _, trade := range []string{"TRADE_NORMAL", "TRADE_IPO", "TRADE_ETF", "TRADE_MARGIN", "TRADE_BLOCK"} {
		s := &controlledStore{}
		w := post(handler(t, s, platform1.Accept, 0), strings.Replace(legal, "TRADE_NORMAL", trade, 1))
		if w.Code != 200 {
			t.Errorf("valid type %s: %d", trade, w.Code)
		}
	}
}

func TestPlatform1MockSaveFailureNeverSuccess(t *testing.T) {
	s := &controlledStore{err: errors.New("db secret-password")}
	w := post(handler(t, s, platform1.Accept, 0), legal)
	if w.Code != 503 || strings.Contains(w.Body.String(), "secret-password") || len(s.snapshot()) != 0 {
		t.Fatalf("save failure: %d %s", w.Code, w.Body.String())
	}
}

func TestPlatform1MockRejectsBeforeProcessing(t *testing.T) {
	s := &controlledStore{}
	w := post(handler(t, s, platform1.RejectBusy, 0), legal)
	if w.Code != 429 || len(s.snapshot()) != 0 {
		t.Fatalf("busy: %d, rows %d", w.Code, len(s.snapshot()))
	}
}

func TestPlatform1MockPersistedButUnconfirmed(t *testing.T) {
	for _, mode := range []platform1.Mode{platform1.PersistDelay, platform1.PersistDisconnect} {
		t.Run(string(mode), func(t *testing.T) {
			s := &controlledStore{}
			server := httptest.NewServer(handler(t, s, mode, 200*time.Millisecond))
			defer server.Close()
			a, _ := adapter.NewPlatform1(server.URL + "/notifications/trades")
			p, e := a.Prepare(model.TradeNotification{SecCode: "000001", SecName: "平安银行", Side: "BUY", Market: "SZ", TradeType: "NORMAL", TraceID: "trace-001", ExecTime: time.Date(2026, 10, 22, 1, 30, 0, 0, time.UTC)}, "00009")
			if e != nil {
				t.Fatal(e)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			result := a.Send(ctx, p)
			if result.Outcome != model.Unknown || !result.SendStarted || len(s.snapshot()) != 1 {
				t.Fatalf("must be saved and unknown: %+v rows=%d", result, len(s.snapshot()))
			}
		})
	}
}

func TestPlatform1MockRouteAndContentType(t *testing.T) {
	s := &controlledStore{}
	h := handler(t, s, platform1.Accept, 0)
	for _, tt := range []struct {
		method, path, content string
		want                  int
	}{{"GET", "/notifications/trades", "application/json", 405}, {"POST", "/other", "application/json", 404}, {"POST", "/notifications/trades", "text/plain", 415}} {
		r := httptest.NewRequest(tt.method, tt.path, strings.NewReader(legal))
		r.Header.Set("Content-Type", tt.content)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tt.want {
			t.Errorf("%+v: %d", tt, w.Code)
		}
	}
	if len(s.snapshot()) != 0 {
		t.Fatal("invalid request saved")
	}
}

func TestPlatform1MockInvalidConfiguration(t *testing.T) {
	if _, err := platform1.NewHandler(nil, platform1.Accept, 0); err == nil {
		t.Fatal("nil store accepted")
	}
	if _, err := platform1.NewHandler(&controlledStore{}, "bad", 0); err == nil {
		t.Fatal("invalid fault mode accepted")
	}
}
