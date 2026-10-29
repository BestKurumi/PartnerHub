package query

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"partner-hub/internal/delivery"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

func validID(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	return err == nil
}
func validText(s string) bool {
	if !utf8.ValidString(s) || utf8.RuneCountInString(s) > 128 || strings.TrimSpace(s) == "" {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
func encodeCursor(p position) string {
	body, _ := json.Marshal(p)
	return base64.RawURLEncoding.EncodeToString(body)
}
func scope(kind, id string, f filter) string {
	body, _ := json.Marshal([]string{kind, id, f.platform, f.client, f.msg, f.delivery, f.uid, f.trace, f.status, f.currentState, f.routeStatus, f.from.Format(time.RFC3339Nano), f.to.Format(time.RFC3339Nano)})
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
func parseFilter(q url.Values, kind, id string) (filter, error) {
	f := filter{limit: 20}
	bad := errors.New("invalid query parameters")
	allowed := map[string]bool{"limit": true, "cursor": true, "platformId": true, "from": true, "to": true}
	switch kind {
	case "messages":
		allowed["clientId"] = true
		allowed["msgId"] = true
		allowed["traceId"] = true
		allowed["status"] = true
		allowed["routeStatus"] = true
	case "deliveries":
		allowed["clientId"] = true
		allowed["msgId"] = true
		allowed["deliveryId"] = true
		allowed["platformUid"] = true
		allowed["currentState"] = true
	case "bindings":
		allowed["clientId"] = true
		allowed["platformUid"] = true
	case "message", "event":
		allowed = map[string]bool{}
	}
	for key, v := range q {
		if !allowed[key] || len(v) != 1 || v[0] == "" {
			return f, bad
		}
	}
	if q.Has("limit") {
		n, err := strconv.Atoi(q.Get("limit"))
		if err != nil || n < 1 || n > 100 {
			return f, bad
		}
		f.limit = n
	}
	f.platform = q.Get("platformId")
	if f.platform != "" && f.platform != "PLATFORM_A" && f.platform != "PLATFORM_B" {
		return f, bad
	}
	f.client, f.msg, f.delivery, f.uid = q.Get("clientId"), q.Get("msgId"), q.Get("deliveryId"), q.Get("platformUid")
	f.trace, f.status, f.currentState = q.Get("traceId"), q.Get("status"), q.Get("currentState")
	f.routeStatus = q.Get("routeStatus")
	switch f.routeStatus {
	case "", "ROUTED", "NO_ROUTE", "PENDING", "RETRY_WAIT", "IN_FLIGHT", "NEEDS_ATTENTION":
	default:
		return f, bad
	}
	if f.status != "" && f.status != "SUCCEEDED" && f.status != "RECOVERY_PENDING" && f.status != "NEEDS_ATTENTION" && f.status != "NO_ROUTE" {
		return f, bad
	}
	if f.currentState != "" && f.currentState != "SUCCEEDED" && f.currentState != "NEEDS_ATTENTION" && f.currentState != "RETRY_WAIT" && f.currentState != "IN_FLIGHT" && f.currentState != "PENDING" {
		return f, bad
	}
	for _, v := range []string{f.client, f.uid, f.trace} {
		if v != "" && !validText(v) {
			return f, bad
		}
	}
	for _, v := range []string{f.msg, f.delivery} {
		if v != "" && !validID(v) {
			return f, bad
		}
	}
	for name, dst := range map[string]*time.Time{"from": &f.from, "to": &f.to} {
		if v := q.Get(name); v != "" {
			tm, err := time.Parse(time.RFC3339Nano, v)
			if err != nil || tm.Year() < 1000 || tm.Year() > 9999 {
				return f, bad
			}
			*dst = tm.UTC()
		}
	}
	if !f.from.IsZero() && !f.to.IsZero() && f.from.After(f.to) {
		return f, bad
	}
	if raw := q.Get("cursor"); raw != "" {
		if len(raw) > 2048 {
			return f, bad
		}
		body, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil {
			return f, bad
		}
		var p position
		if json.Unmarshal(body, &p) != nil || p.Time.IsZero() || p.Time.Year() < 1000 || p.Time.Year() > 9999 || p.Scope != scope(kind, id, f) {
			return f, bad
		}
		switch kind {
		case "bindings":
			if !validText(p.ID) || (p.Extra != "PLATFORM_A" && p.Extra != "PLATFORM_B") {
				return f, bad
			}
		case "attempts":
			n, err := strconv.ParseUint(p.ID, 10, 32)
			if err != nil || n == 0 {
				return f, bad
			}
		default:
			if !validID(p.ID) {
				return f, bad
			}
		}
		p.Time = p.Time.UTC()
		f.cursor = &p
	}
	return f, nil
}

type RetryExecutor interface {
	Execute(context.Context, delivery.RetryRequest) (delivery.RetryResult, error)
}

func NewHTTP(s *Service, manual ...RetryExecutor) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		respond := func(code int, value any) { w.WriteHeader(code); _ = json.NewEncoder(w).Encode(value) }
		if len(manual) > 0 && manual[0] != nil && serveRetry(w, r, manual[0]) {
			return
		}
		if r.Method != "GET" {
			w.Header().Set("Allow", "GET")
			respond(405, map[string]string{"code": "METHOD_NOT_ALLOWED"})
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/api/v1/")
		parts := strings.Split(path, "/")
		kind, id := "", ""
		switch {
		case len(parts) == 1 && (path == "messages" || path == "deliveries" || path == "bindings" || path == "anomalies"):
			kind = path
		case len(parts) == 2 && parts[0] == "messages":
			kind, id = "message", parts[1]
		case len(parts) == 2 && parts[0] == "binding-events":
			kind, id = "event", parts[1]
		case len(parts) == 3 && parts[0] == "deliveries" && parts[2] == "attempts":
			kind, id = "attempts", parts[1]
		default:
			respond(404, map[string]string{"code": "NOT_FOUND"})
			return
		}
		if id != "" && !validID(id) {
			respond(400, map[string]string{"code": "INVALID_QUERY"})
			return
		}
		q, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			respond(400, map[string]string{"code": "INVALID_QUERY"})
			return
		}
		f, err := parseFilter(q, kind, id)
		if err != nil {
			respond(400, map[string]string{"code": "INVALID_QUERY"})
			return
		}
		result, err := s.read(r.Context(), kind, id, f)
		if errors.Is(err, sql.ErrNoRows) {
			respond(404, map[string]string{"code": "RECORD_NOT_AVAILABLE", "message": "暂无持久记录，可能尚未同步；不表示未发送，请保留原标识核查。"})
			return
		}
		if err != nil {
			respond(503, map[string]string{"code": "QUERY_UNAVAILABLE"})
			return
		}
		respond(200, result)
	})
}
