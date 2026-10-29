package main

import (
	"errors"
	"net"
	"net/http"
	"os"
)

// Host execution remains loopback-only. Container exposure is an explicit demo
// deployment choice; published ports must still bind to host loopback.
func validateListen(address string) error {
	host, _, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	if err == nil && ip != nil && ip.IsLoopback() {
		return nil
	}
	if err == nil && os.Getenv("DEMO_ALLOW_CONTAINER_LISTEN") == "true" && (host == "" || ip != nil && ip.IsUnspecified()) {
		return nil
	}
	return errors.New("demo must listen on loopback; container wildcard listen requires DEMO_ALLOW_CONTAINER_LISTEN=true")
}

func withHealth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			if r.Method != "GET" {
				w.Header().Set("Allow", "GET")
				http.Error(w, "method not allowed", 405)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		next.ServeHTTP(w, r)
	})
}
