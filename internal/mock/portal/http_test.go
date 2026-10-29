package portal

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func testHandler(t *testing.T, platform, hub string) http.Handler {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	h, err := NewHandler(Config{platform, "local-token", hub}, &sql.DB{}, cl)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
func request(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "http://localhost:8080"+path, strings.NewReader(body))
	r.RemoteAddr = "127.0.0.1:1234"
	r.Header.Set("X-Demo-Token", "local-token")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// Catches missing local, origin or token checks before any side effect.
func TestPortalSecurity(t *testing.T) {
	h := testHandler(t, "PLATFORM_A", "http://127.0.0.1:18080")
	for _, tc := range []struct {
		name, remote, origin, token string
		want                        int
	}{
		{"token", "127.0.0.1:9", "", "", 401}, {"remote", "192.0.2.1:9", "", "local-token", 403}, {"cross-origin", "127.0.0.1:9", "http://localhost:8081", "local-token", 403}, {"null-origin", "127.0.0.1:9", "null", "local-token", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "http://localhost:8080/demo/binding-events", strings.NewReader(`{}`))
			r.RemoteAddr = tc.remote
			r.Header.Set("Origin", tc.origin)
			r.Header.Set("X-Demo-Token", tc.token)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d want=%d", w.Code, tc.want)
			}
		})
	}
}

// Catches platform spoofing, ambiguous JSON, wrong event types and excessive query bounds.
func TestPortalRejectsInvalidInput(t *testing.T) {
	h := testHandler(t, "PLATFORM_A", "http://127.0.0.1:18080")
	for _, body := range []string{`{"eventType":"BIND","clientId":"c","platformUid":"u","platformId":"PLATFORM_B"}`, `{"eventType":"BIND","eventType":"UNBIND","clientId":"c","platformUid":"u"}`, `{"eventType":"OTHER","clientId":"c","platformUid":"u"}`, `{"eventId":null,"eventType":"BIND","clientId":"c","platformUid":"u"}`, `{"eventType":"BIND","clientId":"c","platformUid":"u"} {}`, `{"eventType":"BIND","clientId":"c","platformUid":"u","eventId":"invalid"}`, `{"eventType":"BIND","clientId":" ","platformUid":"u"}`, `[]`, strings.Repeat(" ", 8193)} {
		if w := request(h, "POST", "/demo/binding-events", body); w.Code != 400 && w.Code != 413 {
			t.Errorf("body %.50q status=%d", body, w.Code)
		}
	}
	for _, query := range []string{"limit=0", "limit=101", "limit=-1", "limit=abc", "cursor=bogus", "limit=2&limit=3", "uid=" + strings.Repeat("x", 129)} {
		if w := request(h, "GET", "/demo/received?"+query, ""); w.Code != 400 {
			t.Errorf("query=%s status=%d", query, w.Code)
		}
	}
}

// Catches confusing platform identity or rendering an unavailable page.
func TestPortalPages(t *testing.T) {
	for _, p := range []string{"PLATFORM_A", "PLATFORM_B"} {
		h := testHandler(t, p, "http://127.0.0.1:18080")
		w := request(h, "GET", "/", "")
		if w.Code != 200 || !strings.Contains(w.Body.String(), p) {
			t.Fatalf("%s page status=%d body=%s", p, w.Code, w.Body.String())
		}
		if len(h.(interface{ Page() []byte }).Page()) == 0 {
			t.Fatal("empty Page")
		}
	}
}

// Catches redirect following, losing pending 404 and treating a Hub result as local receipt data.
func TestPortalBindingProxy(t *testing.T) {
	const id = "d34e78a0-d5ad-4a10-b183-6698632b0071"
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/api/v1/binding-events/"+id || r.Header.Get("X-Demo-Token") != "" {
			t.Errorf("unexpected proxy request %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(404)
		io.WriteString(w, `{"code":"RECORD_NOT_AVAILABLE"}`)
	}))
	defer hub.Close()
	h := testHandler(t, "PLATFORM_A", hub.URL)
	w := request(h, "GET", "/demo/binding-events/"+id, "")
	if w.Code != 404 || !strings.Contains(w.Body.String(), "RECORD_NOT_AVAILABLE") {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if w = request(h, "GET", "/demo/binding-events/not-uuid", ""); w.Code != 400 {
		t.Fatalf("invalid id=%d", w.Code)
	}
}

// Catches regenerating event time on explicit retry and losing the full event on uncertain publish.
func TestPortalBindingExplicitOccurredAt(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	cl.Close()
	h, err := NewHandler(Config{"PLATFORM_A", "local-token", "http://127.0.0.1:1"}, &sql.DB{}, cl)
	if err != nil {
		t.Fatal(err)
	}
	w := request(h, "POST", "/demo/binding-events", `{"eventId":"d34e78a0-d5ad-4a10-b183-6698632b0071","eventType":"BIND","clientId":"c","platformUid":"00009","occurredAt":1730000000123}`)
	if w.Code != 503 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var result struct {
		Status string
		Event  struct {
			OccurredAt int64  `json:"occurredAt"`
			EventID    string `json:"eventId"`
			PlatformID string `json:"platformId"`
		}
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "UNCONFIRMED" || result.Event.OccurredAt != 1730000000123 || result.Event.EventID != "d34e78a0-d5ad-4a10-b183-6698632b0071" || result.Event.PlatformID != "PLATFORM_A" {
		t.Fatalf("lost retry content %+v", result)
	}
	for _, value := range []string{`null`, `1.1`, `"1730000000123"`, `9223372036854775808`} {
		w = request(h, "POST", "/demo/binding-events", `{"eventType":"BIND","clientId":"c","platformUid":"u","occurredAt":`+value+`}`)
		if w.Code != 400 {
			t.Fatalf("invalid time=%s status=%d", value, w.Code)
		}
	}
}

func TestPortalProxyFailureBoundaries(t *testing.T) {
	const id = "d34e78a0-d5ad-4a10-b183-6698632b0071"
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   int
	}{{"dependency", 503, `{"status":"DEPENDENCY_UNAVAILABLE"}`, 503}, {"redirect", 302, `{}`, 503}, {"invalid-json", 200, `<html>`, 503}, {"oversized", 200, `"` + strings.Repeat("x", 65536) + `"`, 503}, {"applied", 200, `{"result":"APPLIED","revision":1}`, 200}} {
		t.Run(tc.name, func(t *testing.T) {
			hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "http://127.0.0.1:1")
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer hub.Close()
			w := request(testHandler(t, "PLATFORM_A", hub.URL), "GET", "/demo/binding-events/"+id, "")
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if tc.want == 200 && w.Body.String() != tc.body {
				t.Fatalf("result changed %s", w.Body.String())
			}
		})
	}
}

func TestPortalRejectsForeignHost(t *testing.T) {
	h := testHandler(t, "PLATFORM_A", "http://127.0.0.1:1")
	r := httptest.NewRequest("GET", "http://evil.test/", nil)
	r.RemoteAddr = "127.0.0.1:1"
	r.Header.Set("Origin", "http://evil.test")
	r.Header.Set("X-Demo-Token", "local-token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("foreign Host accepted: %d", w.Code)
	}
}

func TestContainerPortalPreservesDemoAuthorization(t *testing.T) {
	t.Setenv("DEMO_ALLOW_CONTAINER_LISTEN", "true")
	t.Setenv("DEMO_TRUSTED_CIDRS", "172.30.25.0/24")
	h := testHandler(t, "PLATFORM_A", "http://nginx")
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
			t.Fatalf("portal %+v HTTP%d", tc, w.Code)
		}
	}
}
