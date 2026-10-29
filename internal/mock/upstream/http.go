package upstream

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"partner-hub/internal/demo"
	"partner-hub/internal/model"
)

type Config struct{ HubURL, Token string }

const batchBudget = 30 * time.Second

var uuid = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type demoHandler struct {
	hub    string
	token  string
	client *http.Client
	slots  chan struct{}
}
type result struct {
	MsgID      string          `json:"msgId"`
	TraceID    string          `json:"traceId"`
	HTTPStatus int             `json:"httpStatus"`
	Acceptance string          `json:"acceptance"`
	Retryable  bool            `json:"retryable"`
	Sent       bool            `json:"sent"`
	Status     string          `json:"status"`
	Response   json.RawMessage `json:"response,omitempty"`
}

// NewHandler serves the local page and token-protected demonstration APIs.
func NewHandler(cfg Config) (http.Handler, error) {
	u, err := url.Parse(cfg.HubURL)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || strings.TrimSpace(cfg.Token) == "" {
		return nil, errors.New("valid hub origin and nonempty demo token required")
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 2 * time.Second}).DialContext, TLSHandshakeTimeout: 2 * time.Second, ResponseHeaderTimeout: 9 * time.Second, MaxConnsPerHost: 8, MaxIdleConnsPerHost: 4, IdleConnTimeout: 30 * time.Second}
	return &demoHandler{hub: strings.TrimSuffix(cfg.HubURL, "/"), token: cfg.Token, client: &http.Client{Transport: transport, Timeout: 9 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, slots: make(chan struct{}, 4)}, nil
}

func localHost(hostport string) bool {
	host := hostport
	if value, _, err := net.SplitHostPort(hostport); err == nil {
		host = value
	}
	host = strings.Trim(host, "[]")
	return strings.EqualFold(host, "localhost") || net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback()
}

func (h *demoHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if !demo.TrustedPeer(r.RemoteAddr) || !localHost(r.Host) {
		http.Error(w, "local demo authorization required", 403)
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		if err != nil || u.Scheme != scheme || !strings.EqualFold(u.Host, r.Host) || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			http.Error(w, "same origin required", 403)
			return
		}
	}
	if r.Method == "GET" && r.URL.Path == "/" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Write(Page())
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Demo-Token")), []byte(h.token)) != 1 {
		http.Error(w, "local demo authorization required", 403)
		return
	}
	switch {
	case r.Method == "POST" && r.URL.Path == "/demo/trades":
		h.trades(w, r)
	case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/demo/messages/"):
		id := strings.TrimPrefix(r.URL.Path, "/demo/messages/")
		if !uuid.MatchString(id) || r.URL.RawQuery != "" {
			http.Error(w, "invalid msgId", 400)
			return
		}
		h.query(w, r, id)
	default:
		http.NotFound(w, r)
	}
}

func decodeInput(r io.Reader) (string, string, int, int, error) {
	raw, err := io.ReadAll(io.LimitReader(r, 4097))
	if err != nil || len(raw) > 4096 || !utf8.Valid(raw) {
		return "", "", 0, 0, errors.New("invalid body")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	open, err := d.Token()
	if err != nil || open != json.Delim('{') {
		return "", "", 0, 0, errors.New("object required")
	}
	fields := map[string]json.RawMessage{}
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return "", "", 0, 0, err
		}
		k, ok := key.(string)
		if !ok {
			return "", "", 0, 0, errors.New("key required")
		}
		if _, ok := fields[k]; ok {
			return "", "", 0, 0, errors.New("duplicate field")
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return "", "", 0, 0, err
		}
		fields[k] = value
	}
	if _, err := d.Token(); err != nil {
		return "", "", 0, 0, err
	}
	if _, err := d.Token(); err != io.EOF {
		return "", "", 0, 0, errors.New("trailing input")
	}
	client, tradeType := "", ""
	count, qps := 1, 1
	for k, v := range fields {
		switch k {
		case "clientId":
			err = json.Unmarshal(v, &client)
		case "tradeType":
			err = json.Unmarshal(v, &tradeType)
		case "count":
			if string(v) == "null" {
				err = errors.New("null count")
			} else {
				err = json.Unmarshal(v, &count)
			}
		case "qps":
			if string(v) == "null" {
				err = errors.New("null qps")
			} else {
				err = json.Unmarshal(v, &qps)
			}
		default:
			err = errors.New("unknown field")
		}
		if err != nil {
			return "", "", 0, 0, err
		}
	}
	if strings.TrimSpace(client) == "" || utf8.RuneCountInString(client) > 128 || strings.ContainsFunc(client, unicode.IsControl) || count < 1 || count > 100 || qps < 1 || qps > 20 {
		return "", "", 0, 0, errors.New("invalid client or load bounds")
	}
	switch tradeType {
	case "NORMAL", "IPO", "ETF", "MARGIN", "BLOCK":
	default:
		return "", "", 0, 0, errors.New("invalid tradeType")
	}
	return client, tradeType, count, qps, nil
}

func (h *demoHandler) trades(w http.ResponseWriter, r *http.Request) {
	client, tradeType, count, qps, err := decodeInput(r.Body)
	if err != nil {
		http.Error(w, "invalid demo trade request", 400)
		return
	}
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		w.Header().Set("Retry-After", "1")
		http.Error(w, "demo capacity exhausted; no trade sent", 429)
		return
	}
	results := make([]result, count)
	ids := make([]string, count)
	for i := range results {
		ids[i] = model.NewID()
		results[i] = result{MsgID: ids[i], TraceID: "demo-" + ids[i], Acceptance: "REJECTED", Status: "NOT_SENT"}
	}
	// Flush identifiers before a bounded batch: the browser retains them even if
	// the final body is lost. A missing response never licenses resubmission.
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Demo-Msg-Ids", strings.Join(ids, ","))
	w.WriteHeader(200)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	ctx, cancel := context.WithTimeout(r.Context(), batchBudget)
	defer cancel()
	next := time.Now()
	for i := range results {
		delay := time.Until(next)
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
		}
		if ctx.Err() != nil {
			break
		}
		results[i] = h.send(ctx, results[i], client, tradeType)
		next = time.Now().Add(time.Second / time.Duration(qps))
	}
	json.NewEncoder(w).Encode(struct {
		Items []result `json:"items"`
	}{results})
}

func (h *demoHandler) send(ctx context.Context, item result, client, tradeType string) result {
	now := time.Now().In(time.FixedZone("Asia/Shanghai", 8*60*60))
	payload := map[string]any{"msgId": item.MsgID, "eventType": "TRADE_EXECUTION", "produceTime": now.UnixMilli(), "source": "counter_system", "clientId": client, "accountId": "00010001", "orderId": "order-" + item.MsgID, "businessId": "business-" + item.MsgID, "traceId": item.TraceID, "secCode": "600000", "secName": "演示证券", "side": "BUY", "orderType": "LIMIT", "execQuantity": json.Number("100"), "execPrice": json.Number("10.25"), "execAmount": json.Number("1025.00"), "execTime": now.Format("2006-01-02 15:04:05.000"), "orderStatus": "FILLED", "market": "SH", "tradeType": tradeType}
	body, _ := json.Marshal(payload)
	req, _ := http.NewRequestWithContext(ctx, "POST", h.hub+"/api/v1/trades", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	item.Sent = true
	item.Acceptance = "UNCONFIRMED"
	item.Status = "NEEDS_VERIFICATION"
	resp, err := h.client.Do(req)
	if err != nil {
		return item
	}
	defer resp.Body.Close()
	item.HTTPStatus = resp.StatusCode
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil || len(raw) > 65536 {
		return item
	}
	var response struct {
		MsgID      string `json:"msgId"`
		Acceptance string `json:"acceptance"`
		Status     string `json:"status"`
	}
	if json.Unmarshal(raw, &response) == nil {
		item.Response = raw
	}
	if resp.StatusCode == 400 || resp.StatusCode == 413 || resp.StatusCode == 429 {
		item.Acceptance = "REJECTED"
		item.Retryable = true
		item.Status = "PRE_SEND_REJECTED"
		return item
	}
	if response.MsgID != "" && response.MsgID != item.MsgID {
		return item
	}
	confirmed := resp.StatusCode == 200 && response.Acceptance == "COMPLETED" && (response.Status == "SUCCEEDED" || response.Status == "NO_ROUTE") || resp.StatusCode == 202 && response.Acceptance == "ACCEPTED" && response.Status == "RECOVERY_PENDING" || resp.StatusCode == 200 && response.Acceptance == "ACCEPTED" && response.Status == "NEEDS_ATTENTION"
	if confirmed {
		item.Acceptance = response.Acceptance
		item.Status = response.Status
	}
	return item
}

func (h *demoHandler) query(w http.ResponseWriter, r *http.Request, id string) {
	req, _ := http.NewRequestWithContext(r.Context(), "GET", h.hub+"/api/v1/messages/"+id, nil)
	resp, err := h.client.Do(req)
	if err != nil {
		http.Error(w, "authoritative query unavailable; do not resubmit", 503)
		return
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20+1))
	if err != nil || len(raw) > 1<<20 {
		http.Error(w, "authoritative query unavailable; do not resubmit", 503)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(raw)
}
