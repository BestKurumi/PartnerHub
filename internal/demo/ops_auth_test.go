package demo_test

import (
	"net/http"
	"net/http/httptest"
	"partner-hub/internal/demo"
	"testing"
)

func TestOpsCanonicalReadsRequireLocalOriginAndToken(t *testing.T) {
	for _, mount := range []string{"/api/v1/ops/", "/hub/demo/ops/"} {
		called := 0
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called++; w.WriteHeader(204) })
		h := demo.NewLocalCommandHandler(next, "local-token")
		if mount == "/hub/demo/ops/" {
			h = demo.NewHubHandler(next, "local-token")
		}
		for _, tc := range []struct {
			host, origin, token, remote string
			code                        int
		}{
			{"localhost:8080", "", "", "127.0.0.1:42", 401},
			{"localhost:8080", "", "wrong", "127.0.0.1:42", 401},
			{"example.com", "", "local-token", "127.0.0.1:42", 403},
			{"localhost:8080", "https://evil.example", "local-token", "127.0.0.1:42", 403},
			{"localhost:8080", "", "local-token", "198.51.100.1:42", 403},
			{"localhost:8080", "http://localhost:8080", "local-token", "127.0.0.1:42", 204},
		} {
			for _, suffix := range []string{"metrics", "diagnostics", "config"} {
				before := called
				r := httptest.NewRequest("GET", mount+suffix, nil)
				r.Host = tc.host
				r.RemoteAddr = tc.remote
				r.Header.Set("Origin", tc.origin)
				r.Header.Set("X-Demo-Token", tc.token)
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				if w.Code != tc.code {
					t.Fatalf("%s %+v -> %d", mount+suffix, tc, w.Code)
				}
				if tc.code != 204 && called != before {
					t.Fatal("unauthorized request reached backend")
				}
			}
		}
	}
}
