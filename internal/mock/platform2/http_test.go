package platform2_test

import (
	"net/http/httptest"
	"partner-hub/internal/mock/platform2"
	"strings"
	"testing"
)

type control struct {
	paused bool
	calls  int
}

func (c *control) SetPaused(v bool) { c.calls++; c.paused = v }
func (c *control) Paused() bool     { return c.paused }
func TestPlatform2Control(t *testing.T) {
	for _, tt := range []struct {
		name, remote, token, origin, body string
		code                              int
	}{{"pause", "127.0.0.1:1234", "demo-token", "http://localhost", `{"paused":true}`, 200}, {"wrong token", "127.0.0.1:1234", "bad", "", `{"paused":true}`, 403}, {"cross origin", "127.0.0.1:1234", "demo-token", "http://evil.example", `{"paused":true}`, 403}, {"remote", "192.0.2.1:1234", "demo-token", "", `{"paused":true}`, 403}, {"invalid", "127.0.0.1:1234", "demo-token", "", `{"paused":null}`, 400}} {
		t.Run(tt.name, func(t *testing.T) {
			state := &control{}
			h := platform2.NewControlHandler(state, "demo-token")
			r := httptest.NewRequest("POST", "http://localhost/demo/consumer", strings.NewReader(tt.body))
			r.RemoteAddr = tt.remote
			r.Header.Set("X-Demo-Token", tt.token)
			r.Header.Set("Origin", tt.origin)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tt.code {
				t.Fatalf("HTTP=%d body=%s", w.Code, w.Body.String())
			}
			if tt.code == 200 {
				if !state.paused || state.calls != 1 {
					t.Fatal("pause not applied")
				}
			} else if state.calls != 0 {
				t.Fatal("rejected control changed consumer")
			}
		})
	}
}

func TestPlatform2ControlSameOriginHost(t *testing.T) {
	for _, tc := range []struct{ host, origin string }{{"evil.example", ""}, {"localhost", "https://localhost"}} {
		state := &control{}
		r := httptest.NewRequest("POST", "http://localhost/demo/consumer", strings.NewReader(`{"paused":true}`))
		r.RemoteAddr = "127.0.0.1:1234"
		r.Host = tc.host
		r.Header.Set("Origin", tc.origin)
		r.Header.Set("X-Demo-Token", "demo-token")
		w := httptest.NewRecorder()
		platform2.NewControlHandler(state, "demo-token").ServeHTTP(w, r)
		if w.Code != 403 || state.calls != 0 {
			t.Fatalf("host=%s origin=%s HTTP%d calls=%d", tc.host, tc.origin, w.Code, state.calls)
		}
	}
}

func TestPlatform2ContainerConsumerControl(t *testing.T) {
	t.Setenv("DEMO_ALLOW_CONTAINER_LISTEN", "true")
	t.Setenv("DEMO_TRUSTED_CIDRS", "172.30.41.0/24")
	for _, tc := range []struct {
		remote, host, origin, token string
		code                        int
	}{{"172.30.41.2:1", "localhost:8080", "http://localhost:8080", "demo-token", 200}, {"172.30.42.2:1", "localhost:8080", "", "demo-token", 403}, {"172.30.41.2:1", "evil.test", "", "demo-token", 403}, {"172.30.41.2:1", "localhost:8080", "http://evil.test", "demo-token", 403}, {"172.30.41.2:1", "localhost:8080", "", "wrong", 403}} {
		state := &control{}
		r := httptest.NewRequest("POST", "/demo/consumer", strings.NewReader(`{"paused":true}`))
		r.Host = tc.host
		r.RemoteAddr = tc.remote
		r.Header.Set("Origin", tc.origin)
		r.Header.Set("X-Demo-Token", tc.token)
		w := httptest.NewRecorder()
		platform2.NewControlHandler(state, "demo-token").ServeHTTP(w, r)
		if w.Code != tc.code || (state.calls == 1) != (tc.code == 200) {
			t.Fatalf("container control %+v HTTP%d calls=%d", tc, w.Code, state.calls)
		}
	}
}
