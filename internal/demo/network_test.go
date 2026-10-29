package demo_test

import (
	"net/http"
	"net/http/httptest"
	"partner-hub/internal/demo"
	"testing"
)

func TestContainerDemoPeerRequiresExplicitNetwork(t *testing.T) {
	for _, c := range []struct {
		enabled, cidr, remote string
		want                  bool
	}{
		{"", "172.30.25.0/24", "172.30.25.2:1", false}, {"true", "", "172.30.25.2:1", false}, {"true", "172.30.25.0/24", "172.30.25.2:1", true}, {"true", "172.30.25.0/24", "172.30.26.2:1", false}, {"true", "0.0.0.0/0", "8.8.8.8:1", false}, {"true", "172.30.25.0/24", "8.8.8.8:1", false}, {"true", "bad", "172.30.25.2:1", false}, {"", "", "127.0.0.1:1", true}} {
		t.Setenv("DEMO_ALLOW_CONTAINER_LISTEN", c.enabled)
		t.Setenv("DEMO_TRUSTED_CIDRS", c.cidr)
		if demo.TrustedPeer(c.remote) != c.want {
			t.Fatalf("peer authorization %+v", c)
		}
	}
}
func TestContainerHubStillRequiresLocalHostOriginAndToken(t *testing.T) {
	t.Setenv("DEMO_ALLOW_CONTAINER_LISTEN", "true")
	t.Setenv("DEMO_TRUSTED_CIDRS", "172.30.25.0/24")
	h := demo.NewHubHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }), "token")
	for _, c := range []struct {
		host, origin, token string
		want                int
	}{{"localhost:8080", "http://localhost:8080", "token", 204}, {"localhost:8080", "http://evil.test", "token", 403}, {"evil.test", "", "token", 403}, {"localhost:8080", "", "", 401}} {
		r := httptest.NewRequest("GET", "/hub/demo/messages", nil)
		r.RemoteAddr = "172.30.25.2:123"
		r.Host = c.host
		r.Header.Set("Origin", c.origin)
		r.Header.Set("X-Demo-Token", c.token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != c.want {
			t.Fatalf("container gateway %+v HTTP%d", c, w.Code)
		}
	}
}
