package demo

import (
	"crypto/subtle"
	"net"
	"net/http"
	hubpage "partner-hub/web/hub"
	"strings"
)

func localRequest(w http.ResponseWriter, r *http.Request) bool {
	requestHost := r.Host
	if h, _, err := net.SplitHostPort(requestHost); err == nil {
		requestHost = h
	}
	requestHost = strings.Trim(requestHost, "[]")
	hostIP := net.ParseIP(requestHost)
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if !TrustedPeer(r.RemoteAddr) || !(strings.EqualFold(requestHost, "localhost") || hostIP != nil && hostIP.IsLoopback()) || r.Header.Get("Origin") != "" && r.Header.Get("Origin") != scheme+"://"+r.Host {
		http.Error(w, "local same-origin only", 403)
		return false
	}
	return true
}
func demoToken(w http.ResponseWriter, r *http.Request, token string) bool {
	if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(r.Header.Get("X-Demo-Token"))) != 1 {
		http.Error(w, "demo token required", 401)
		return false
	}
	return true
}

// NewLocalCommandHandler keeps canonical write APIs inside the local demo boundary.
// Read-only APIs retain their existing query behavior.
func NewLocalCommandHandler(next http.Handler, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		protectedRead := strings.HasPrefix(r.URL.Path, "/api/v1/ops/") || r.URL.Path == "/api/v1/ops"
		if (r.Method != "GET" || protectedRead) && (!localRequest(w, r) || !demoToken(w, r, token)) {
			return
		}
		next.ServeHTTP(w, r)
	})
}
func retryPath(path string) bool {
	p := strings.Split(strings.TrimPrefix(path, "/hub/demo/"), "/")
	return len(p) == 3 && p[1] != "" && (p[0] == "deliveries" && p[2] == "retry" || p[0] == "messages" && p[2] == "retry-routing")
}

// NewHubHandler provides the local page and token-protected query/command mount.
func NewHubHandler(query http.Handler, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if !localRequest(w, r) {
			return
		}
		if r.Method != "GET" && !(r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/hub/demo/") && retryPath(r.URL.Path)) {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", 405)
			return
		}
		if r.URL.Path == "/hub/" {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write(hubpage.Page())
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/hub/demo/") {
			http.NotFound(w, r)
			return
		}
		if !demoToken(w, r, token) {
			return
		}
		clone := r.Clone(r.Context())
		u := *r.URL
		u.Path = "/api/v1/" + strings.TrimPrefix(u.Path, "/hub/demo/")
		clone.URL = &u
		query.ServeHTTP(w, clone)
	})
}
