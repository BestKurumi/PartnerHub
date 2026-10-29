package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestGatewayRoutesAllFourConsolesAndPreservesQueryAndToken(t *testing.T) {
	for _, tc := range []struct{ path, target string }{{"/hub/demo/messages?traceId=x%2By", "hub"}, {"/mock/p1/demo/received", "p1"}, {"/mock/p2/demo/received", "p2"}, {"/mock/upstream/demo/trades", "upstream"}} {
		t.Run(tc.path, func(t *testing.T) {
			urls := map[string]string{}
			for _, name := range []string{"hub", "p1", "p2", "upstream"} {
				name := name
				s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.RequestURI() != tc.path || r.Header.Get("X-Demo-Token") != "test-token" || r.Method != "POST" {
						t.Error("proxy lost original request facts")
					}
					body, _ := io.ReadAll(r.Body)
					w.Write([]byte(name + ":" + string(body)))
				}))
				defer s.Close()
				urls[name] = s.URL
			}
			h, err := newGateway(urls)
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest("POST", "http://127.0.0.1:28080"+tc.path, strings.NewReader("one-body"))
			r.Header.Set("X-Demo-Token", "test-token")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 200 || w.Body.String() != tc.target+":one-body" {
				t.Fatalf("%d %q", w.Code, w.Body.String())
			}
		})
	}
}

func TestGatewayNeverReplaysUncertainPOSTAndRejectsRemoteTargets(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Close()
	}))
	defer s.Close()
	h, err := newGateway(map[string]string{"hub": s.URL, "p1": s.URL, "p2": s.URL, "upstream": s.URL})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "http://127.0.0.1/hub/demo/test", strings.NewReader("uncertain"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if calls.Load() != 1 || w.Code != 502 {
		t.Fatalf("calls=%d code=%d", calls.Load(), w.Code)
	}
	_, err = newGateway(map[string]string{"hub": "http://example.com", "p1": s.URL, "p2": s.URL, "upstream": s.URL})
	if err == nil {
		t.Fatal("native gateway must use fixed loopback backends")
	}
}
