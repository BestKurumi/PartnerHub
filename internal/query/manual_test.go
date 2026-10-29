package query_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"partner-hub/internal/delivery"
	"partner-hub/internal/query"
)

type manualExecutor struct {
	got   delivery.RetryRequest
	calls int
	err   error
}

func (e *manualExecutor) Execute(ctx context.Context, r delivery.RetryRequest) (delivery.RetryResult, error) {
	e.got = r
	e.calls++
	if err := delivery.ValidateRetryRequest(r); err != nil {
		return delivery.RetryResult{}, err
	}
	return delivery.RetryResult{OperationID: r.OperationID, Kind: r.Kind, TargetID: r.TargetID, State: "PENDING", RetryCycle: 2}, e.err
}

const manualID = "22342b23-e2ce-412a-a9f5-664a7b975578"
const manualBody = `{"operationId":"ab22b1bb-fcc2-41a4-a762-cbcc980ba12a","operator":"演示操作员","reason":"已修复配置"}`

func TestManualHTTPMapsSavedCommand(t *testing.T) {
	for _, v := range []struct{ path, kind string }{{"deliveries/" + manualID + "/retry", delivery.RetryDelivery}, {"messages/" + manualID + "/retry-routing", delivery.RetryRouting}} {
		e := &manualExecutor{}
		h := query.NewHTTP(query.NewService(nil), e)
		r := httptest.NewRequest("POST", "/api/v1/"+v.path, strings.NewReader(manualBody))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		var result delivery.RetryResult
		json.Unmarshal(w.Body.Bytes(), &result)
		if w.Code != 200 || result.State != "PENDING" || result.RetryCycle != 2 || e.got.Kind != v.kind || e.got.TargetID != manualID || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("saved intent: HTTP%d %s %+v", w.Code, w.Body.String(), e.got)
		}
	}
}
func TestManualHTTPErrorsAndStrictInput(t *testing.T) {
	for _, v := range []struct {
		err  error
		code int
		body string
	}{{delivery.ErrRetryNotFound, 404, "RETRY_TARGET_NOT_FOUND"}, {delivery.ErrRetryConflict, 409, "RETRY_CONFLICT"}, {errors.New("secret DB address"), 503, "RETRY_UNCONFIRMED"}} {
		e := &manualExecutor{err: v.err}
		r := httptest.NewRequest("POST", "/api/v1/deliveries/"+manualID+"/retry", strings.NewReader(manualBody))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		query.NewHTTP(nil, e).ServeHTTP(w, r)
		if w.Code != v.code || !strings.Contains(w.Body.String(), v.body) || strings.Contains(w.Body.String(), "secret") {
			t.Fatalf("HTTP%d %s", w.Code, w.Body.String())
		}
	}
	for name, body := range map[string]string{"unknown": strings.TrimSuffix(manualBody, "}") + `,"platformUid":"edited"}`, "case alias": strings.Replace(manualBody, "operator", "Operator", 1), "duplicate": strings.TrimSuffix(manualBody, "}") + `,"reason":"other"}`, "null": `null`, "trailing": manualBody + `{}`, "over size": strings.Repeat(" ", 4097) + manualBody, "empty reason": strings.Replace(manualBody, "已修复配置", "", 1), "bad UUID": strings.Replace(manualBody, "ab22b1bb-fcc2-41a4-a762-cbcc980ba12a", "aa--aaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", 1), "bad utf8": strings.Replace(manualBody, "已修复配置", string([]byte{255}), 1)} {
		t.Run(name, func(t *testing.T) {
			e := &manualExecutor{}
			r := httptest.NewRequest("POST", "/api/v1/deliveries/"+manualID+"/retry", strings.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			query.NewHTTP(nil, e).ServeHTTP(w, r)
			if w.Code != 400 && w.Code != 413 {
				t.Fatalf("HTTP%d %s", w.Code, w.Body.String())
			}
			if e.calls != 0 {
				t.Fatal("invalid body reached command")
			}
		})
	}
	for _, v := range []struct {
		path, method, content string
		code                  int
	}{{"deliveries/" + manualID + "/retry?operator=other", "POST", "application/json", 400}, {"messages/" + manualID + "/retry-routing", "GET", "application/json", 405}, {"deliveries/bad/retry", "POST", "application/json", 400}, {"deliveries/" + manualID + "/retry", "POST", "text/plain", 415}} {
		e := &manualExecutor{}
		r := httptest.NewRequest(v.method, "/api/v1/"+v.path, strings.NewReader(manualBody))
		r.Header.Set("Content-Type", v.content)
		w := httptest.NewRecorder()
		query.NewHTTP(nil, e).ServeHTTP(w, r)
		if w.Code != v.code || e.calls != 0 {
			t.Fatalf("%+v HTTP%d %s", v, w.Code, w.Body.String())
		}
	}
}
