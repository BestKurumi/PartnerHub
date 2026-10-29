// gateway keeps the four native processes under one loopback browser origin.
package main

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"
)

func newGateway(targets map[string]string) (http.Handler, error) {
	proxies := map[string]*httputil.ReverseProxy{}
	for _, name := range []string{"hub", "p1", "p2", "upstream"} {
		u, err := url.Parse(targets[name])
		if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Hostname() != "localhost" && !net.ParseIP(u.Hostname()).IsLoopback()) {
			return nil, errors.New("gateway backends must be fixed loopback HTTP origins")
		}
		proxy := httputil.NewSingleHostReverseProxy(u)
		// A new connection per request prevents transport retries on reused sockets.
		proxy.Transport = &http.Transport{Proxy: nil, DisableKeepAlives: true, MaxConnsPerHost: 32, DialContext: (&net.Dialer{Timeout: time.Second}).DialContext, ResponseHeaderTimeout: 40 * time.Second}
		proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "backend unavailable or result unconfirmed; do not resubmit automatically", http.StatusBadGateway)
		}
		proxies[name] = proxy
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/internal/") {
			http.NotFound(w, r)
			return
		}
		name := "hub"
		for prefix, target := range map[string]string{"/mock/p1/": "p1", "/mock/p2/": "p2", "/mock/upstream/": "upstream"} {
			if strings.HasPrefix(r.URL.Path, prefix) {
				name = target
				break
			}
		}
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		proxies[name].ServeHTTP(w, r)
	}), nil
}

func main() {
	values := map[string]string{}
	for key, env := range map[string]string{"hub": "NATIVE_HUB_URL", "p1": "NATIVE_PLATFORM1_URL", "p2": "NATIVE_PLATFORM2_URL", "upstream": "NATIVE_UPSTREAM_URL"} {
		values[key] = os.Getenv(env)
	}
	h, err := newGateway(values)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	listen := os.Getenv("NATIVE_GATEWAY_LISTEN")
	host, _, err := net.SplitHostPort(listen)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		fmt.Fprintln(os.Stderr, "NATIVE_GATEWAY_LISTEN must be a loopback address:port")
		os.Exit(1)
	}
	server := &http.Server{Addr: listen, Handler: h, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 45 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	fmt.Println("native demo gateway listening on " + listen)
	if err = server.ListenAndServe(); err != nil {
		fmt.Fprintln(os.Stderr, "native gateway stopped")
		os.Exit(1)
	}
}
