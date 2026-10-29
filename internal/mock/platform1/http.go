package platform1

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"
	_ "time/tzdata"
	"unicode"
	"unicode/utf8"
)

type Mode string

const (
	Accept            Mode = "accept"
	RejectBusy        Mode = "reject-busy"
	PersistDelay      Mode = "persist-delay"
	PersistDisconnect Mode = "persist-disconnect"
)

func NewHandler(store Store, mode Mode, delay time.Duration) (http.Handler, error) {
	if store == nil || (mode != Accept && mode != RejectBusy && mode != PersistDelay && mode != PersistDisconnect) || delay < 0 || delay > 30*time.Second {
		return nil, errors.New("invalid platform1 mock configuration")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /notifications/trades", func(w http.ResponseWriter, r *http.Request) {
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			http.Error(w, "JSON required", 415)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 65536))
		if err != nil {
			var limit *http.MaxBytesError
			if errors.As(err, &limit) {
				http.Error(w, "payload too large", 413)
			} else {
				http.Error(w, "invalid payload", 400)
			}
			return
		}
		fields, err := validatePayload(body)
		if err != nil {
			http.Error(w, "invalid platform1 payload", 400)
			return
		}
		if mode == RejectBusy {
			writeJSON(w, 429, map[string]any{"code": "BUSY"})
			return
		}
		var id [16]byte
		if _, err = rand.Read(id[:]); err != nil {
			http.Error(w, "receipt unavailable", 503)
			return
		}
		id[6] = (id[6] & 15) | 64
		id[8] = (id[8] & 63) | 128
		receiveID := fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:])
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		err = store.Save(ctx, Receipt{ReceiveID: receiveID, UserUID: fields["userUid"], TraceID: fields["traceId"], Payload: body})
		cancel()
		if err != nil {
			http.Error(w, "receipt persistence unconfirmed", 503)
			return
		}
		if mode == PersistDisconnect {
			conn, _, err := http.NewResponseController(w).Hijack()
			if err == nil {
				conn.Close()
			} else {
				http.Error(w, "receipt confirmation unavailable", 503)
			}
			return
		}
		if mode == PersistDelay {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.Context().Done():
				return
			}
		}
		writeJSON(w, 200, map[string]any{"code": 0, "receiveId": receiveID})
	})
	return mux, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// This validator is independent of the hub adapter: it checks the wire protocol.
func validatePayload(body []byte) (map[string]string, error) {
	bad := errors.New("invalid platform1 payload")
	if !utf8.Valid(body) {
		return nil, bad
	}
	limits := map[string]int{"userUid": 128, "securityCode": 64, "securityName": 255, "direction": 4, "execTime": 19, "market": 2, "tradeType": 12, "traceId": 128}
	d := json.NewDecoder(bytes.NewReader(body))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, bad
	}
	fields := make(map[string]string, 8)
	for d.More() {
		token, err = d.Token()
		if err != nil {
			return nil, bad
		}
		key, ok := token.(string)
		if !ok || limits[key] == 0 {
			return nil, bad
		}
		if _, exists := fields[key]; exists {
			return nil, bad
		}
		token, err = d.Token()
		if err != nil {
			return nil, bad
		}
		value, ok := token.(string)
		if !ok || strings.TrimSpace(value) == "" || utf8.RuneCountInString(value) > limits[key] {
			return nil, bad
		}
		for _, c := range value {
			if unicode.IsControl(c) {
				return nil, bad
			}
		}
		fields[key] = value
	}
	if _, err = d.Token(); err != nil {
		return nil, bad
	}
	if _, err = d.Token(); err != io.EOF || len(fields) != 8 {
		return nil, bad
	}
	if fields["direction"] != "BUY" && fields["direction"] != "SELL" {
		return nil, bad
	}
	if fields["market"] != "SH" && fields["market"] != "SZ" {
		return nil, bad
	}
	switch fields["tradeType"] {
	case "TRADE_NORMAL", "TRADE_IPO", "TRADE_ETF", "TRADE_MARGIN", "TRADE_BLOCK":
	default:
		return nil, bad
	}
	zone, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return nil, bad
	}
	parsed, err := time.ParseInLocation("2006-01-02 15:04:05", fields["execTime"], zone)
	if err != nil || parsed.Format("2006-01-02 15:04:05") != fields["execTime"] {
		return nil, bad
	}
	return fields, nil
}
