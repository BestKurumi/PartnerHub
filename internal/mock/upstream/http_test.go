package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"partner-hub/internal/ingress"
)

func request(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.RemoteAddr = "127.0.0.1:12345"
	r.Host = "localhost:8083"
	r.Header.Set("Origin", "http://localhost:8083")
	r.Header.Set("X-Demo-Token", "local-test-token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func handler(t *testing.T, url string) http.Handler {
	t.Helper()
	h, err := NewHandler(Config{HubURL: url, Token: "local-test-token"})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// Catches incomplete/invalid generated protocol, reused IDs and ignoring rate limits.
func TestGenerateUniqueProtocolTrades(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]bool{}
	var times []time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/api/v1/trades" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(500)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		trade, err := ingress.Decode(bytes.NewReader(raw))
		if err != nil {
			t.Errorf("invalid generated protocol: %v; %s", err, raw)
			w.WriteHeader(400)
			return
		}
		if trade.ClientID != "000123" || trade.TradeType != "ETF" {
			t.Errorf("lost selected values: %+v", trade)
		}
		mu.Lock()
		if seen[trade.MsgID] {
			t.Error("reused msgId")
		}
		seen[trade.MsgID] = true
		times = append(times, time.Now())
		mu.Unlock()
		fmt.Fprintf(w, `{"msgId":%q,"status":"SUCCEEDED","acceptance":"COMPLETED","recordStatus":"PENDING"}`, trade.MsgID)
	}))
	defer server.Close()
	h := handler(t, server.URL)
	w := request(t, h, "POST", "/demo/trades", `{"clientId":"000123","tradeType":"ETF","count":3,"qps":20}`)
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}
	var out struct {
		Items []struct {
			MsgID, Acceptance string
			HTTPStatus        int
		}
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Items) != 3 || len(seen) != 3 {
		t.Fatalf("items=%d sent=%d", len(out.Items), len(seen))
	}
	for _, item := range out.Items {
		if !seen[item.MsgID] || item.Acceptance != "COMPLETED" || item.HTTPStatus != 200 {
			t.Errorf("unexpected item %+v", item)
		}
	}
	if times[2].Sub(times[0]) < 90*time.Millisecond {
		t.Errorf("qps ignored: %v", times)
	}
	// Default count/qps must still produce a single trade.
	if w := request(t, h, "POST", "/demo/trades", `{"clientId":"000123","tradeType":"ETF"}`); w.Code != 200 || len(seen) != 4 {
		t.Fatalf("default: %d %s sent=%d", w.Code, w.Body, len(seen))
	}
}

// Catches any retry after uncertain delivery, or treating query absence as permission to send.
func TestUnconfirmedResponsesNeverResubmit(t *testing.T) {
	for _, mode := range []string{"lost", "503", "404", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			posts, queries := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					queries++
					w.WriteHeader(404)
					fmt.Fprint(w, `{"status":"RECORD_NOT_AVAILABLE"}`)
					return
				}
				posts++
				switch mode {
				case "lost":
					conn, _, _ := w.(http.Hijacker).Hijack()
					conn.Close()
				case "503":
					w.WriteHeader(503)
					fmt.Fprint(w, `{"acceptance":"UNCONFIRMED"}`)
				case "404":
					w.WriteHeader(404)
				case "redirect":
					w.Header().Set("Location", "/other")
					w.WriteHeader(307)
				}
			}))
			defer server.Close()
			h := handler(t, server.URL)
			w := request(t, h, "POST", "/demo/trades", `{"clientId":"1","tradeType":"NORMAL"}`)
			var out struct {
				Items []struct {
					MsgID, Acceptance string
					Retryable         bool
				}
			}
			if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
				t.Fatal(err)
			}
			if len(out.Items) != 1 || out.Items[0].MsgID == "" || out.Items[0].Acceptance != "UNCONFIRMED" || out.Items[0].Retryable {
				t.Fatalf("uncertain response: %s", w.Body)
			}
			for i := 0; i < 2; i++ {
				q := request(t, h, "GET", "/demo/messages/"+out.Items[0].MsgID, "")
				if q.Code != 404 || !strings.Contains(q.Body.String(), "RECORD_NOT_AVAILABLE") {
					t.Fatalf("query: %d %s", q.Code, q.Body)
				}
			}
			if posts != 1 || queries != 2 {
				t.Fatalf("sent=%d queried=%d", posts, queries)
			}
		})
	}
}

// Catches acceptance inflation and losing the clear pre-send rejection boundary.
func TestConfirmationSemantics(t *testing.T) {
	for _, tc := range []struct {
		code         int
		body, accept string
		retry        bool
	}{
		{202, `{"acceptance":"ACCEPTED","status":"RECOVERY_PENDING"}`, "ACCEPTED", false},
		{200, `{"acceptance":"ACCEPTED","status":"NEEDS_ATTENTION"}`, "ACCEPTED", false},
		{200, `{}`, "UNCONFIRMED", false},
		{202, `{"acceptance":"COMPLETED","status":"SUCCEEDED"}`, "UNCONFIRMED", false},
		{200, `{"acceptance":"COMPLETED","status":"UNKNOWN"}`, "UNCONFIRMED", false},
		{400, `{"acceptance":"REJECTED"}`, "REJECTED", true},
		{413, `{"acceptance":"REJECTED"}`, "REJECTED", true},
		{429, `{"acceptance":"REJECTED"}`, "REJECTED", true},
	} {
		t.Run(fmt.Sprint(tc.code, tc.accept), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(tc.code); fmt.Fprint(w, tc.body) }))
			defer server.Close()
			w := request(t, handler(t, server.URL), "POST", "/demo/trades", `{"clientId":"1","tradeType":"NORMAL"}`)
			var out struct {
				Items []struct {
					Acceptance string
					Retryable  bool
					HTTPStatus int
				}
			}
			json.Unmarshal(w.Body.Bytes(), &out)
			if len(out.Items) != 1 || out.Items[0].Acceptance != tc.accept || out.Items[0].Retryable != tc.retry || out.Items[0].HTTPStatus != tc.code || calls != 1 {
				t.Fatalf("body=%s calls=%d", w.Body, calls)
			}
		})
	}
}

// Catches validation bypasses and allowing unbounded or remote demo traffic.
func TestLimitsAndLocalAuthorization(t *testing.T) {
	h := handler(t, "http://127.0.0.1:1")
	for _, body := range []string{`{}`, `{"clientId":"1","tradeType":"BAD"}`, `{"clientId":"1","tradeType":"NORMAL","count":101}`, `{"clientId":"1","tradeType":"NORMAL","count":-1}`, `{"clientId":"1","tradeType":"NORMAL","qps":21}`, `{"clientId":"1","tradeType":"NORMAL","qps":0}`, `{"clientId":"1","tradeType":"NORMAL","extra":1}`, `{"clientId":"1","clientId":"2","tradeType":"NORMAL"}`, `{"clientId":"1","tradeType":"NORMAL","count":null}`, `{"clientId":"1","tradeType":"NORMAL"} {}`} {
		if w := request(t, h, "POST", "/demo/trades", body); w.Code != 400 {
			t.Errorf("accepted %s: %d", body, w.Code)
		}
	}
	for _, tc := range []struct{ remote, host, origin, token string }{
		{"192.0.2.1:12", "localhost:8083", "http://localhost:8083", "local-test-token"},
		{"127.0.0.1:12", "evil.test", "http://evil.test", "local-test-token"},
		{"127.0.0.1:12", "localhost:8083", "https://evil.test", "local-test-token"},
		{"127.0.0.1:12", "localhost:8083", "http://localhost:8083", "wrong"},
	} {
		r := httptest.NewRequest("GET", "/demo/messages/8a236f08-1bf4-4bb3-80c2-7eb27fe7c63f", nil)
		r.RemoteAddr = tc.remote
		r.Host = tc.host
		r.Header.Set("Origin", tc.origin)
		r.Header.Set("X-Demo-Token", tc.token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 403 {
			t.Errorf("authorization bypass %+v: %d", tc, w.Code)
		}
	}
	if w := request(t, h, "GET", "/demo/messages/not-a-uuid", ""); w.Code != 400 {
		t.Errorf("bad msgId %d", w.Code)
	}
	for _, cfg := range []Config{{}, {HubURL: "file:///tmp/a", Token: "x"}, {HubURL: "http://localhost:8080/unsafe", Token: "x"}, {HubURL: "http://localhost:8080", Token: ""}} {
		if _, err := NewHandler(cfg); err == nil {
			t.Errorf("accepted bad config %+v", cfg)
		}
	}
}

func TestPageIsLocalAndPublicWithoutToken(t *testing.T) {
	h := handler(t, "http://127.0.0.1:1")
	r := httptest.NewRequest("GET", "/", nil)
	r.Host = "localhost:8083"
	r.RemoteAddr = "127.0.0.1:1234"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "上游成交 Mock") {
		t.Fatalf("page: %d %s", w.Code, w.Body)
	}
	if strings.Contains(w.Body.String(), "local-test-token") {
		t.Fatal("token leaked")
	}
	r.Host = "evil.test"
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("nonlocal page: %d", w.Code)
	}
}

// A canceled batch must retain every pre-generated ID and never send the remaining items.
func TestCanceledBatchRetainsIDsAndMarksUnsent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts++
		io.Copy(io.Discard, r.Body)
		cancel()
		w.WriteHeader(503)
	}))
	defer server.Close()
	h := handler(t, server.URL)
	r := httptest.NewRequest("POST", "/demo/trades", strings.NewReader(`{"clientId":"1","tradeType":"NORMAL","count":100,"qps":20}`)).WithContext(ctx)
	r.RemoteAddr = "127.0.0.1:1"
	r.Host = "localhost:8083"
	r.Header.Set("X-Demo-Token", "local-test-token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var out struct {
		Items []struct {
			MsgID, Acceptance, Status string
			Sent                      bool
		}
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	headerIDs := strings.Split(w.Header().Get("X-Demo-Msg-Ids"), ",")
	if posts != 1 || len(out.Items) != 100 || len(headerIDs) != 100 {
		t.Fatalf("posts=%d items=%d header IDs=%d", posts, len(out.Items), len(headerIDs))
	}
	for i, item := range out.Items {
		if item.MsgID != headerIDs[i] {
			t.Fatal("lost pre-generated ID")
		}
		if i == 0 {
			if !item.Sent || item.Acceptance != "UNCONFIRMED" {
				t.Fatalf("sent item: %+v", item)
			}
		} else if item.Sent || item.Status != "NOT_SENT" || item.Acceptance != "REJECTED" {
			t.Fatalf("unsent item: %+v", item)
		}
	}
}

func TestContainerUpstreamPreservesDemoAuthorization(t *testing.T) {
	t.Setenv("DEMO_ALLOW_CONTAINER_LISTEN", "true")
	t.Setenv("DEMO_TRUSTED_CIDRS", "172.30.25.0/24")
	h, err := NewHandler(Config{HubURL: "http://nginx", Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		host, origin, remote string
		want                 int
	}{{"localhost:8080", "http://localhost:8080", "172.30.25.2:1", 200}, {"localhost:8080", "http://evil.test", "172.30.25.2:1", 403}, {"evil.test", "", "172.30.25.2:1", 403}, {"localhost:8080", "", "172.30.26.2:1", 403}} {
		r := httptest.NewRequest("GET", "/", nil)
		r.Host = tc.host
		r.RemoteAddr = tc.remote
		r.Header.Set("Origin", tc.origin)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("upstream %+v HTTP%d", tc, w.Code)
		}
	}
}
