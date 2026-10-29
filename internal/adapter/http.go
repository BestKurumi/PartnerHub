package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"partner-hub/internal/model"
	"strings"
	"sync/atomic"
	"time"
	_ "time/tzdata"
	"unicode"
	"unicode/utf8"
)

type HTTP struct {
	endpoint string
	client   *http.Client
}

func NewPlatform1(endpoint string) (*HTTP, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" {
		return nil, errors.New("invalid platform1 endpoint")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// No environment proxy: an unconfigured intermediary must not see the payload.
	transport.Proxy = nil
	return &HTTP{endpoint: endpoint, client: &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

type platform1Payload struct {
	UserUID      string `json:"userUid"`
	SecurityCode string `json:"securityCode"`
	SecurityName string `json:"securityName"`
	Direction    string `json:"direction"`
	ExecTime     string `json:"execTime"`
	Market       string `json:"market"`
	TradeType    string `json:"tradeType"`
	TraceID      string `json:"traceId"`
}

func (a *HTTP) Prepare(n model.TradeNotification, uid string) (model.PreparedRequest, error) {
	types := map[string]string{"NORMAL": "TRADE_NORMAL", "IPO": "TRADE_IPO", "ETF": "TRADE_ETF", "MARGIN": "TRADE_MARGIN", "BLOCK": "TRADE_BLOCK"}
	if types[n.TradeType] == "" || (n.Side != "BUY" && n.Side != "SELL") || (n.Market != "SH" && n.Market != "SZ") || n.ExecTime.IsZero() || !validText(uid, 128) || !validText(n.SecCode, 64) || !validText(n.SecName, 255) || !validText(n.TraceID, 128) {
		return model.PreparedRequest{}, errors.New("invalid platform1 notification")
	}
	zone, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return model.PreparedRequest{}, errors.New("trade timezone unavailable")
	}
	p := platform1Payload{uid, n.SecCode, n.SecName, n.Side, n.ExecTime.In(zone).Format("2006-01-02 15:04:05"), n.Market, types[n.TradeType], n.TraceID}
	body, err := json.Marshal(p)
	if err != nil {
		return model.PreparedRequest{}, err
	}
	return model.PreparedRequest{PlatformID: "PLATFORM_A", ProtocolVersion: "platform1.v1", Payload: body}, nil
}

func validText(s string, max int) bool {
	if !utf8.ValidString(s) || strings.TrimSpace(s) == "" || utf8.RuneCountInString(s) > max {
		return false
	}
	for _, c := range s {
		if unicode.IsControl(c) {
			return false
		}
	}
	return true
}
func (a *HTTP) Send(ctx context.Context, p model.PreparedRequest) (result model.SendResult) {
	result.StartedAt = time.Now().UTC()
	result.Outcome = model.Unknown
	var started atomic.Bool
	defer func() { result.SendStarted = started.Load(); result.FinishedAt = time.Now().UTC() }()
	if p.PlatformID != "PLATFORM_A" || p.ProtocolVersion != "platform1.v1" || len(p.Payload) == 0 {
		result.Outcome = model.PermanentFailure
		result.Summary = "invalid prepared request"
		return
	}
	if ctx.Err() != nil {
		result.Outcome = model.RetryableFailure
		result.Summary = "budget exhausted before send"
		return
	}
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteHeaders: func() { started.Store(true) }})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, bytes.NewReader(p.Payload))
	if err != nil {
		result.Outcome = model.PermanentFailure
		result.Summary = "invalid request configuration"
		return
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := a.client.Do(req)
	if err != nil {
		var op *net.OpError
		if !started.Load() && errors.As(err, &op) && op.Op == "dial" {
			result.Outcome = model.RetryableFailure
		}
		result.Summary = "transport unconfirmed"
		return
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case 429:
		result.Outcome = model.RetryableFailure
	case 400, 422, 401, 403:
		result.Outcome = model.PermanentFailure
	case 200:
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 8193))
		if readErr == nil && len(body) <= 8192 {
			switch responseCode(body) {
			case "0":
				result.Outcome = model.Success
			case "BUSY":
				result.Outcome = model.RetryableFailure
			}
		}
	}
	// Store only allowlisted metadata; arbitrary response bodies can echo secrets.
	result.Summary = fmt.Sprintf("http_status=%d; outcome=%s", response.StatusCode, result.Outcome)
	return
}

func responseCode(body []byte) string {
	if !utf8.Valid(body) {
		return ""
	}
	d := json.NewDecoder(bytes.NewReader(body))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return ""
	}
	seen := map[string]bool{}
	code := ""
	for d.More() {
		token, err = d.Token()
		if err != nil {
			return ""
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return ""
		}
		seen[key] = true
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return ""
		}
		if key == "code" {
			if string(bytes.TrimSpace(value)) == "0" {
				code = "0"
			} else {
				var name string
				if json.Unmarshal(value, &name) == nil && name == "BUSY" {
					code = "BUSY"
				}
			}
		}
	}
	if _, err = d.Token(); err != nil {
		return ""
	}
	if _, err = d.Token(); err != io.EOF {
		return ""
	}
	return code
}
