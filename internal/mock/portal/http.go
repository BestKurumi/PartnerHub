// Package portal serves a fixed platform's local demo controls and actual receipt view.
package portal

import (
	"bytes"
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"
	"partner-hub/internal/binding"
	"partner-hub/internal/demo"
	"partner-hub/internal/model"
	page1 "partner-hub/web/platform1"
	page2 "partner-hub/web/platform2"
)

type Config struct{ PlatformID, Token, HubURL string }
type Handler struct {
	config   Config
	db       *sql.DB
	producer *kgo.Client
	client   *http.Client
	slots    chan struct{}
}

func NewHandler(config Config, db *sql.DB, producer *kgo.Client) (http.Handler, error) {
	u, err := url.Parse(config.HubURL)
	if (config.PlatformID != "PLATFORM_A" && config.PlatformID != "PLATFORM_B") || strings.TrimSpace(config.Token) == "" || db == nil || producer == nil || err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" {
		return nil, errors.New("invalid portal configuration")
	}
	config.HubURL = strings.TrimRight(config.HubURL, "/")
	return &Handler{config: config, db: db, producer: producer, slots: make(chan struct{}, 4), client: &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// Page returns the configured platform's static page. It never contains the demo token.
func (h *Handler) Page() []byte {
	if h.config.PlatformID == "PLATFORM_A" {
		return page1.Page()
	}
	return page2.Page()
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if !demo.TrustedPeer(r.RemoteAddr) {
		writeError(w, 403, "LOCAL_ONLY")
		return
	}
	requestHost := r.Host
	if parsed, _, splitErr := net.SplitHostPort(requestHost); splitErr == nil {
		requestHost = parsed
	}
	requestHost = strings.Trim(requestHost, "[]")
	hostIP := net.ParseIP(requestHost)
	if !strings.EqualFold(requestHost, "localhost") && (hostIP == nil || !hostIP.IsLoopback()) {
		writeError(w, 403, "LOCAL_HOST_REQUIRED")
		return
	}
	origin := r.Header.Get("Origin")
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if origin != "" && origin != scheme+"://"+r.Host {
		writeError(w, 403, "ORIGIN_REJECTED")
		return
	}
	if r.Method == "GET" && r.URL.Path == "/" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(h.Page())
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Demo-Token")), []byte(h.config.Token)) != 1 {
		writeError(w, 401, "DEMO_TOKEN_REQUIRED")
		return
	}
	switch {
	case r.URL.Path == "/demo/received" && r.Method == "GET":
		h.received(w, r)
	case r.URL.Path == "/demo/binding-events" && r.Method == "POST":
		h.publish(w, r)
	case strings.HasPrefix(r.URL.Path, "/demo/binding-events/") && r.Method == "GET":
		h.event(w, r)
	default:
		writeError(w, 404, "NOT_FOUND")
	}
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"code": code})
}
func validText(s string) bool {
	if !utf8.ValidString(s) || strings.TrimSpace(s) == "" || utf8.RuneCountInString(s) > 128 {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
func validUUID(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	return err == nil
}

type receipt struct {
	ReceiveID  string          `json:"receiveId"`
	UserUID    string          `json:"userUid"`
	TraceID    string          `json:"traceId"`
	Payload    json.RawMessage `json:"payload"`
	ReceivedAt time.Time       `json:"receivedAt"`
}
type cursor struct {
	Time time.Time `json:"t"`
	ID   string    `json:"i"`
}

func (h *Handler) received(w http.ResponseWriter, r *http.Request) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeError(w, 400, "INVALID_QUERY")
		return
	}
	for k, v := range q {
		if len(v) != 1 || (k != "uid" && k != "traceId" && k != "limit" && k != "cursor") {
			writeError(w, 400, "INVALID_QUERY")
			return
		}
	}
	limit := 20
	if value, ok := q["limit"]; ok {
		limit, err = strconv.Atoi(value[0])
		if err != nil || limit < 1 || limit > 100 {
			writeError(w, 400, "INVALID_LIMIT")
			return
		}
	}
	query := "SELECT receive_id,user_uid,trace_id,payload,received_at FROM received_notification WHERE 1=1"
	args := []any{}
	for _, filter := range []struct{ key, column string }{{"uid", "user_uid"}, {"traceId", "trace_id"}} {
		if value := q.Get(filter.key); value != "" {
			if !validText(value) {
				writeError(w, 400, "INVALID_FILTER")
				return
			}
			query += " AND " + filter.column + "=?"
			args = append(args, value)
		}
	}
	if encoded := q.Get("cursor"); encoded != "" {
		var c cursor
		raw, decodeErr := base64.RawURLEncoding.DecodeString(encoded)
		if len(encoded) > 256 || decodeErr != nil || json.Unmarshal(raw, &c) != nil || c.Time.IsZero() || !validUUID(c.ID) {
			writeError(w, 400, "INVALID_CURSOR")
			return
		}
		query += " AND (received_at < ? OR (received_at = ? AND receive_id < ?))"
		args = append(args, c.Time, c.Time, c.ID)
	}
	query += " ORDER BY received_at DESC,receive_id DESC LIMIT ?"
	args = append(args, limit+1)
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		writeError(w, 503, "RECEIPTS_UNAVAILABLE")
		return
	}
	defer rows.Close()
	items := make([]receipt, 0, limit+1)
	for rows.Next() {
		var item receipt
		if rows.Scan(&item.ReceiveID, &item.UserUID, &item.TraceID, &item.Payload, &item.ReceivedAt) != nil || !json.Valid(item.Payload) {
			writeError(w, 503, "RECEIPTS_UNAVAILABLE")
			return
		}
		items = append(items, item)
	}
	if rows.Err() != nil {
		writeError(w, 503, "RECEIPTS_UNAVAILABLE")
		return
	}
	next := ""
	if len(items) > limit {
		items = items[:limit]
		last := items[len(items)-1]
		raw, _ := json.Marshal(cursor{last.ReceivedAt, last.ReceiveID})
		next = base64.RawURLEncoding.EncodeToString(raw)
	}
	writeJSON(w, 200, struct {
		Items      []receipt `json:"items"`
		NextCursor string    `json:"nextCursor"`
	}{items, next})
}

func decodeOperation(r io.Reader) (map[string]string, error) {
	body, err := io.ReadAll(io.LimitReader(r, 8193))
	if err != nil || len(body) > 8192 || !utf8.Valid(body) {
		return nil, errors.New("invalid body")
	}
	d := json.NewDecoder(bytes.NewReader(body))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("invalid object")
	}
	result := map[string]string{}
	for d.More() {
		token, err = d.Token()
		key, ok := token.(string)
		if err != nil || !ok || (key != "eventId" && key != "eventType" && key != "clientId" && key != "platformUid" && key != "occurredAt") {
			return nil, errors.New("unknown key")
		}
		if _, seen := result[key]; seen {
			return nil, errors.New("duplicate key")
		}
		var raw json.RawMessage
		if d.Decode(&raw) != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return nil, errors.New("invalid value")
		}
		if key == "occurredAt" {
			var millis int64
			if json.Unmarshal(raw, &millis) != nil {
				return nil, errors.New("invalid timestamp")
			}
			result[key] = strconv.FormatInt(millis, 10)
			continue
		}
		var value string
		if json.Unmarshal(raw, &value) != nil {
			return nil, errors.New("invalid text")
		}
		result[key] = value
	}
	if _, err = d.Token(); err != nil {
		return nil, err
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, errors.New("trailing value")
	}
	return result, nil
}
func (h *Handler) publish(w http.ResponseWriter, r *http.Request) {
	fields, err := decodeOperation(r.Body)
	if err != nil {
		writeError(w, 400, "INVALID_BINDING_OPERATION")
		return
	}
	id, provided := fields["eventId"]
	if !provided {
		id = model.NewID()
	}
	e := model.BindingEvent{SchemaVersion: 1, EventID: id, EventType: fields["eventType"], PlatformID: h.config.PlatformID, ClientID: fields["clientId"], PlatformUID: fields["platformUid"], OccurredAt: time.Now().UnixMilli()}
	if timestamp, ok := fields["occurredAt"]; ok {
		e.OccurredAt, _ = strconv.ParseInt(timestamp, 10, 64)
	}
	if binding.ValidateEvent(e) != nil {
		writeError(w, 400, "INVALID_BINDING_OPERATION")
		return
	}
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		writeError(w, 429, "PUBLISH_BUSY")
		return
	}
	value, _ := json.Marshal(e)
	key, _ := json.Marshal([2]string{e.ClientID, e.PlatformID})
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err = h.producer.ProduceSync(ctx, &kgo.Record{Topic: "customer.binding.events", Key: key, Value: value}).FirstErr(); err != nil {
		writeJSON(w, 503, map[string]any{"eventId": id, "status": "UNCONFIRMED", "code": "PUBLISH_UNCONFIRMED", "event": e})
		return
	}
	writeJSON(w, 202, map[string]any{"eventId": id, "status": "PUBLISHED", "event": e})
}
func (h *Handler) event(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/demo/binding-events/")
	if !validUUID(id) || r.URL.RawQuery != "" {
		writeError(w, 400, "INVALID_EVENT_ID")
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), "GET", h.config.HubURL+"/api/v1/binding-events/"+id, nil)
	if err != nil {
		writeError(w, 503, "RESULT_UNAVAILABLE")
		return
	}
	response, err := h.client.Do(req)
	if err != nil {
		writeError(w, 503, "RESULT_UNAVAILABLE")
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 65537))
	if err != nil || len(body) > 65536 || !json.Valid(body) {
		writeError(w, 503, "RESULT_UNAVAILABLE")
		return
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		writeError(w, 503, "RESULT_UNAVAILABLE")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(response.StatusCode)
	w.Write(body)
}
