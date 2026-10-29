package query

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"

	"partner-hub/internal/delivery"
)

func decodeRetry(w http.ResponseWriter, r *http.Request) (delivery.RetryRequest, error) {
	var request delivery.RetryRequest
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
	if err != nil {
		return request, err
	}
	if !utf8.Valid(body) {
		return request, delivery.ErrInvalidRetry
	}
	d := json.NewDecoder(bytes.NewReader(body))
	tok, err := d.Token()
	if err != nil || tok != json.Delim('{') {
		return request, delivery.ErrInvalidRetry
	}
	seen := map[string]bool{}
	for d.More() {
		k, err := d.Token()
		if err != nil {
			return request, err
		}
		key, ok := k.(string)
		if !ok || seen[key] {
			return request, delivery.ErrInvalidRetry
		}
		seen[key] = true
		var value string
		if err := d.Decode(&value); err != nil {
			return request, err
		}
		switch key {
		case "operationId":
			request.OperationID = value
		case "operator":
			request.Operator = value
		case "reason":
			request.Reason = value
		default:
			return request, delivery.ErrInvalidRetry
		}
	}
	if _, err = d.Token(); err != nil {
		return request, err
	}
	if _, err = d.Token(); err != io.EOF {
		return request, delivery.ErrInvalidRetry
	}
	return request, nil
}

func serveRetry(w http.ResponseWriter, r *http.Request, command RetryExecutor) bool {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/"), "/")
	kind := ""
	if len(parts) == 3 {
		if parts[0] == "deliveries" && parts[2] == "retry" {
			kind = delivery.RetryDelivery
		}
		if parts[0] == "messages" && parts[2] == "retry-routing" {
			kind = delivery.RetryRouting
		}
	}
	if kind == "" {
		return false
	}
	respond := func(code int, value any) { w.WriteHeader(code); _ = json.NewEncoder(w).Encode(value) }
	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		respond(405, map[string]string{"code": "METHOD_NOT_ALLOWED"})
		return true
	}
	if r.URL.RawQuery != "" {
		respond(400, map[string]string{"code": "INVALID_RETRY"})
		return true
	}
	content, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || content != "application/json" {
		respond(415, map[string]string{"code": "JSON_REQUIRED"})
		return true
	}
	request, err := decodeRetry(w, r)
	request.Kind, request.TargetID = kind, parts[1]
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			respond(413, map[string]string{"code": "RETRY_TOO_LARGE"})
		} else {
			respond(400, map[string]string{"code": "INVALID_RETRY"})
		}
		return true
	}
	if err = delivery.ValidateRetryRequest(request); err != nil {
		respond(400, map[string]string{"code": "INVALID_RETRY"})
		return true
	}
	result, err := command.Execute(r.Context(), request)
	switch {
	case errors.Is(err, delivery.ErrInvalidRetry):
		respond(400, map[string]string{"code": "INVALID_RETRY"})
	case errors.Is(err, delivery.ErrRetryNotFound):
		respond(404, map[string]string{"code": "RETRY_TARGET_NOT_FOUND"})
	case errors.Is(err, delivery.ErrRetryConflict):
		respond(409, map[string]string{"code": "RETRY_CONFLICT"})
	case err != nil:
		respond(503, map[string]string{"code": "RETRY_UNCONFIRMED", "message": "操作结果未确认，请保留 operationId 并使用相同参数核查；不要另建命令。"})
	default:
		respond(200, result)
	}
	return true
}
