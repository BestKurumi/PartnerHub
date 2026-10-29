package diagnostics_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"partner-hub/internal/diagnostics"
)

func diagRequest(h http.Handler, path, method, token, remote string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, nil)
	r.Header.Set("X-Ops-Token", token)
	r.RemoteAddr = remote
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func diagHandler(t *testing.T, b *diagnostics.Buffer, c diagnostics.Config) http.Handler {
	t.Helper()
	h, err := diagnostics.NewHandler(b, c)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
func decodeDiag(t *testing.T, w *httptest.ResponseRecorder) diagnostics.Snapshot {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	var s diagnostics.Snapshot
	if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil {
		t.Fatal(err)
	}
	return s
}

// Internal authentication is independent of the public demo token/Host and
// cannot be bypassed by alternate methods, remote origins, or unknown params.
func TestInternalDiagnosticsAuthorizationAndStrictParameters(t *testing.T) {
	b := diagnostics.NewBuffer("hub-1")
	b.Record("ingress_invalid", diagnostics.Fields{RequestID: "request-one"})
	h := diagHandler(t, b, diagnostics.Config{Token: "private-internal-token"})
	for _, tt := range []struct {
		method, token, remote string
		code                  int
	}{
		{"GET", "", "127.0.0.1:1", 401}, {"GET", "public-demo-token", "127.0.0.1:1", 401},
		{"GET", "private-internal-token", "8.8.8.8:1", 403}, {"POST", "private-internal-token", "127.0.0.1:1", 405},
		{"GET", "private-internal-token", "10.0.0.2:9999", 200},
	} {
		w := diagRequest(h, "/internal/ops/diagnostics", tt.method, tt.token, tt.remote)
		if w.Code != tt.code {
			t.Fatalf("%+v: %d", tt, w.Code)
		}
	}
	for _, q := range []string{"url=http://169.254.169.254/", "level=WARN&level=ERROR", "limit=0", "limit=101", "limit=1&limit=2", "limit=01", "q=" + strings.Repeat("x", 129), "level=DEBUG", "instance=unknown", "q=%00", "q=%ZZ"} {
		w := diagRequest(h, "/api/v1/ops/diagnostics?"+q, "GET", "", "127.0.0.1:1")
		if w.Code != 400 {
			t.Fatalf("query %s code=%d", q, w.Code)
		}
	}
	s := decodeDiag(t, diagRequest(h, "/internal/ops/diagnostics?q=REQUEST-one&limit=1&level=WARN", "GET", "private-internal-token", "172.20.0.2:8080"))
	if len(s.Items) != 1 || s.Items[0].RequestID != "request-one" || s.Instances[0].Status != "ok" {
		t.Fatalf("filtered=%+v", s)
	}
	if got := diagRequest(h, "/api/v1/ops/diagnostics", "POST", "", "127.0.0.1:1"); got.Code != 405 {
		t.Fatal(got.Code)
	}
	if got := diagRequest(h, "/not-diagnostics", "GET", "", "127.0.0.1:1"); got.Code != 404 {
		t.Fatal(got.Code)
	}
	if _, err := diagnostics.NewHandler(b, diagnostics.Config{}); err == nil {
		t.Fatal("empty internal token accepted")
	}
}

// Aggregation must show partial failures, merge newest-first and cap after
// filtering without ever executing business work or trusting a supplied URL.
func TestDiagnosticsFixedPeersMergeAndPartialFailure(t *testing.T) {
	peerBuffer := diagnostics.NewBuffer("hub-2")
	peer := httptest.NewServer(diagHandler(t, peerBuffer, diagnostics.Config{Token: "shared-internal"}))
	defer peer.Close()
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "secret-backend-error", 503) }))
	defer broken.Close()
	local := diagnostics.NewBuffer("hub-1")
	local.Record("ingress_invalid", diagnostics.Fields{RequestID: "local"})
	time.Sleep(time.Millisecond)
	peerBuffer.Record("ingress_unconfirmed", diagnostics.Fields{RequestID: "peer"})
	h := diagHandler(t, local, diagnostics.Config{Token: "shared-internal", Peers: []diagnostics.Peer{{Instance: "hub-2", URL: peer.URL}, {Instance: "hub-3", URL: broken.URL}}})
	s := decodeDiag(t, diagRequest(h, "/api/v1/ops/diagnostics?limit=1", "GET", "", "127.0.0.1:1"))
	if len(s.Items) != 1 || s.Items[0].RequestID != "peer" || len(s.Instances) != 3 || s.Instances[2].Status != "unavailable" {
		t.Fatalf("merged=%+v", s)
	}
	s = decodeDiag(t, diagRequest(h, "/api/v1/ops/diagnostics?instance=hub-2&level=ERROR", "GET", "", "127.0.0.1:1"))
	if len(s.Items) != 1 || len(s.Instances) != 1 || s.Instances[0].Instance != "hub-2" {
		t.Fatalf("scope=%+v", s)
	}
	s = decodeDiag(t, diagRequest(h, "/api/v1/ops/diagnostics?q=absent", "GET", "", "127.0.0.1:1"))
	if len(s.Items) != 0 || s.Instances[0].Status != "ok" || s.Instances[2].Status != "unavailable" {
		t.Fatal("empty and unavailable conflated")
	}
}

// Oversized, malformed and redirecting responses cannot be accepted as a
// healthy peer or redirect the private token into another network target.
func TestDiagnosticsBoundedPeerResponsesAndNoRedirect(t *testing.T) {
	var redirected atomic.Int32
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer sink.Close()
	for _, mode := range []string{"oversize", "malformed", "redirect", "wrong-instance", "raw-message"} {
		t.Run(mode, func(t *testing.T) {
			peerBuffer := diagnostics.NewBuffer("hub-2")
			peerBuffer.Record("runtime_error", diagnostics.Fields{})
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "oversize":
					fmt.Fprint(w, strings.Repeat(" ", 1024*1024+1))
				case "malformed":
					fmt.Fprint(w, `{"items":[]`)
				case "redirect":
					http.Redirect(w, r, sink.URL, http.StatusFound)
				default:
					s := peerBuffer.Snapshot(diagnostics.Query{})
					if mode == "wrong-instance" {
						s.Instances[0].Instance = "other"
					} else {
						s.Items[0].Message = "secret-peer-error"
					}
					_ = json.NewEncoder(w).Encode(s)
				}
			}))
			defer peer.Close()
			h := diagHandler(t, diagnostics.NewBuffer("hub-1"), diagnostics.Config{Token: "shared", Peers: []diagnostics.Peer{{Instance: "hub-2", URL: peer.URL}}})
			w := diagRequest(h, "/api/v1/ops/diagnostics", "GET", "", "127.0.0.1:1")
			s := decodeDiag(t, w)
			if mode == "raw-message" {
				if len(s.Items) != 1 || strings.Contains(w.Body.String(), "secret-peer-error") {
					t.Fatal("raw peer message leaked")
				}
			} else if s.Instances[1].Status != "unavailable" {
				t.Fatalf("invalid peer accepted: %+v", s)
			}
		})
	}
	if redirected.Load() != 0 {
		t.Fatal("followed redirect with internal token")
	}
}

// One shared request budget and a handler-wide semaphore bound fanout even
// when multiple browser queries arrive together; peer errors stay partial.
func TestDiagnosticsTotalDeadlineAndSharedConcurrencyBound(t *testing.T) {
	var active, maxActive atomic.Int32
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := active.Add(1)
		defer active.Add(-1)
		for {
			old := maxActive.Load()
			if now <= old || maxActive.CompareAndSwap(old, now) {
				break
			}
		}
		<-r.Context().Done()
	}))
	defer slow.Close()
	peers := []diagnostics.Peer{}
	for i := 0; i < 6; i++ {
		peers = append(peers, diagnostics.Peer{Instance: fmt.Sprintf("hub-%d", i+2), URL: slow.URL})
	}
	h := diagHandler(t, diagnostics.NewBuffer("hub-1"), diagnostics.Config{Token: "shared", Peers: peers, Timeout: 100 * time.Millisecond})
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Go(func() {
			s := decodeDiag(t, diagRequest(h, "/api/v1/ops/diagnostics", "GET", "", "127.0.0.1:1"))
			if len(s.Instances) != 7 {
				t.Error("lost failed peers")
			}
		})
	}
	wg.Wait()
	if time.Since(start) > time.Second || maxActive.Load() > 2 {
		t.Fatalf("unbounded elapsed=%s max=%d", time.Since(start), maxActive.Load())
	}
}

func TestDiagnosticsConfigurationCannotTargetPublicOrCredentialURLs(t *testing.T) {
	for _, target := range []string{"https://8.8.8.8", "http://169.254.169.254", "http://user:pass@hub-2:8080", "http://hub-2:8080/custom", "http://hub-2:8080?url=secret", "file:///etc/passwd", "http://hub-2:8080/#secret"} {
		if _, err := diagnostics.NewHandler(diagnostics.NewBuffer("hub-1"), diagnostics.Config{Token: "shared", Peers: []diagnostics.Peer{{Instance: "hub-2", URL: target}}}); err == nil {
			t.Fatalf("unsafe peer accepted %s", target)
		}
	}
}
