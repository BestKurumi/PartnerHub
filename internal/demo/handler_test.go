package demo_test

import (
	"net/http"
	"net/http/httptest"
	"partner-hub/internal/demo"
	"testing"
)

func TestLocalGatewayAndPrefixes(t *testing.T) {
	endpoint := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Seen-Host", r.Host)
		w.Write([]byte(r.URL.Path))
	})
	h := demo.NewHubHandler(endpoint, "local-token")
	for _, tt := range []struct {
		path, method, host, origin, token, remote string
		code                                      int
	}{
		{"/hub/", "GET", "127.0.0.1:28080", "", "", "127.0.0.1:1234", 200},
		{"/hub/demo/messages", "GET", "127.0.0.1:28080", "", "local-token", "127.0.0.1:1234", 200},
		{"/hub/demo/messages", "GET", "127.0.0.1:28080", "", "", "127.0.0.1:1234", 401},
		{"/hub/demo/messages", "POST", "127.0.0.1:28080", "", "local-token", "127.0.0.1:1234", 405},
		{"/hub/demo/messages", "GET", "evil.example", "", "local-token", "127.0.0.1:1234", 403},
		{"/hub/demo/messages", "GET", "127.0.0.1:28080", "http://evil.example", "local-token", "127.0.0.1:1234", 403},
		{"/hub/", "GET", "127.0.0.1:28080", "", "", "192.0.2.1:1234", 403},
	} {
		r := httptest.NewRequest(tt.method, tt.path, nil)
		r.Host = tt.host
		r.RemoteAddr = tt.remote
		r.Header.Set("Origin", tt.origin)
		r.Header.Set("X-Demo-Token", tt.token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tt.code {
			t.Fatalf("%+v HTTP%d %s", tt, w.Code, w.Body.String())
		}
		if tt.path == "/hub/demo/messages" && w.Code == 200 && w.Body.String() != "/api/v1/messages" {
			t.Fatalf("wrong query mount: %s", w.Body.String())
		}
	}
}

func TestManualCommandsRequireLocalOriginAndToken(t *testing.T) {
	for _, mount := range []string{"/api/v1/", "/hub/demo/"} {
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(r.URL.Path)) })
		var h http.Handler = demo.NewLocalCommandHandler(next, "local-token")
		if mount == "/hub/demo/" {
			h = demo.NewHubHandler(next, "local-token")
		}
		for _, v := range []struct {
			method, path, host, origin, token, remote string
			code                                      int
		}{
			{"POST", mount + "deliveries/22342b23-e2ce-412a-a9f5-664a7b975578/retry", "127.0.0.1:28080", "http://127.0.0.1:28080", "local-token", "127.0.0.1:1234", 200},
			{"POST", mount + "messages/22342b23-e2ce-412a-a9f5-664a7b975578/retry-routing", "localhost:28080", "", "local-token", "127.0.0.1:1234", 200},
			{"POST", mount + "deliveries/id/retry", "localhost:28080", "", "", "127.0.0.1:1234", 401},
			{"POST", mount + "deliveries/id/retry", "localhost:28080", "http://evil.example", "local-token", "127.0.0.1:1234", 403},
			{"POST", mount + "deliveries/id/retry", "evil.example", "", "local-token", "127.0.0.1:1234", 403},
			{"POST", mount + "deliveries/id/retry", "localhost:28080", "", "local-token", "192.0.2.1:1234", 403},
		} {
			r := httptest.NewRequest(v.method, v.path, nil)
			r.Host = v.host
			r.RemoteAddr = v.remote
			r.Header.Set("Origin", v.origin)
			r.Header.Set("X-Demo-Token", v.token)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != v.code {
				t.Fatalf("%+v HTTP%d %s", v, w.Code, w.Body.String())
			}
			if w.Code == 200 && mount == "/hub/demo/" && w.Body.String() != "/api/v1/"+v.path[len(mount):] {
				t.Fatalf("lost retry path: %s", w.Body.String())
			}
		}
	}
}
