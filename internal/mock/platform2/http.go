package platform2

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"partner-hub/internal/demo"
	"strings"
)

type ConsumptionControl interface {
	SetPaused(bool)
	Paused() bool
}

func NewControlHandler(c ConsumptionControl, token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/demo/consumer", func(w http.ResponseWriter, r *http.Request) {
		allowed := demo.TrustedPeer(r.RemoteAddr) && token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Demo-Token")), []byte(token)) == 1
		requestHost := r.Host
		if h, _, e := net.SplitHostPort(requestHost); e == nil {
			requestHost = h
		}
		requestHost = strings.Trim(requestHost, "[]")
		hostIP := net.ParseIP(requestHost)
		allowed = allowed && (strings.EqualFold(requestHost, "localhost") || hostIP != nil && hostIP.IsLoopback())
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			allowed = allowed && origin == scheme+"://"+r.Host
		}
		if !allowed {
			http.Error(w, "forbidden", 403)
			return
		}
		if r.Method == "POST" {
			var body struct {
				Paused *bool `json:"paused"`
			}
			decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
			decoder.DisallowUnknownFields()
			if decoder.Decode(&body) != nil || body.Paused == nil {
				http.Error(w, "invalid control", 400)
				return
			}
			if decoder.Decode(&struct{}{}) != io.EOF {
				http.Error(w, "invalid control", 400)
				return
			}
			c.SetPaused(*body.Paused)
		} else if r.Method != "GET" {
			w.Header().Set("Allow", "GET, POST")
			http.Error(w, "method not allowed", 405)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"paused": c.Paused()})
	})
	return mux
}
