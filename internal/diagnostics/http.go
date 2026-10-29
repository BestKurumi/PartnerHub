package diagnostics

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const InternalPath = "/internal/ops/diagnostics"
const PublicPath = "/api/v1/ops/diagnostics"
const maxPeerBytes = 1024 * 1024

type Peer struct{ Instance, URL string }
type Config struct {
	// Token is a separate internal token, never the public demo token.
	Token string
	Peers []Peer
	// Timeout is one total fanout deadline, clamped to at most two seconds.
	Timeout time.Duration
}
type handler struct {
	buffer  *Buffer
	token   string
	peers   []Peer
	timeout time.Duration
	client  *http.Client
	slots   chan struct{}
}

// NewHandler handles canonical and internal diagnostics. Its canonical mount
// MUST be wrapped by the application's local/same-origin demo authorization.
// The internal mount has its own private-peer/token checks and never fans out.
func NewHandler(b *Buffer, c Config) (http.Handler, error) {
	if b == nil || len(c.Token) < 1 || len(c.Token) > 256 || strings.ContainsFunc(c.Token, unicode.IsControl) {
		return nil, errors.New("diagnostics buffer and internal token required")
	}
	if len(c.Peers) > 16 {
		return nil, errors.New("too many diagnostics peers")
	}
	seen := map[string]bool{b.instance: true}
	peers := append([]Peer{}, c.Peers...)
	for i, p := range peers {
		if p.Instance == "" || p.Instance != cleanText(p.Instance, 64) || seen[p.Instance] {
			return nil, errors.New("invalid or duplicate diagnostics instance")
		}
		seen[p.Instance] = true
		u, err := url.Parse(p.URL)
		if err != nil || u.Opaque != "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" || u.ForceQuery || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || (u.Path != "" && u.Path != "/") || u.RawPath != "" {
			return nil, errors.New("invalid fixed diagnostics peer URL")
		}
		if ip := net.ParseIP(u.Hostname()); ip != nil && !privateIP(ip) {
			return nil, errors.New("diagnostics peer must use a private address")
		}
		if port := u.Port(); port != "" {
			n, e := strconv.Atoi(port)
			if e != nil || n < 1 || n > 65535 {
				return nil, errors.New("invalid diagnostics peer port")
			}
		}
		u.Path = InternalPath
		peers[i].URL = u.String()
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}
	if timeout > 2*time.Second {
		timeout = 2 * time.Second
	}
	transport := &http.Transport{Proxy: nil, DialContext: privateDial, MaxConnsPerHost: 2, MaxIdleConns: 16, IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: timeout, ResponseHeaderTimeout: timeout}
	return &handler{buffer: b, token: c.Token, peers: peers, timeout: timeout, client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, slots: make(chan struct{}, 2)}, nil
}
func privateIP(ip net.IP) bool { return ip.IsLoopback() || ip.IsPrivate() }
func privateDial(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errors.New("invalid diagnostics target")
	}
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil || len(addresses) == 0 {
		return nil, errors.New("diagnostics peer resolution unavailable")
	}
	// Check every resolved address before opening a connection; no environment
	// proxy, redirect, DNS fallback or public/link-local destination is permitted.
	for _, a := range addresses {
		if !privateIP(a.IP) {
			return nil, errors.New("diagnostics peer resolved outside private network")
		}
	}
	dialer := net.Dialer{}
	return dialer.DialContext(ctx, network, net.JoinHostPort(addresses[0].IP.String(), port))
}
func privateRemote(address string) bool {
	host, _, err := net.SplitHostPort(address)
	return err == nil && privateIP(net.ParseIP(host))
}

func (h *handler) parseQuery(r *http.Request, internal bool) (Query, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return Query{}, errors.New("invalid diagnostics query")
	}
	q := Query{Limit: 20}
	for key, v := range values {
		if len(v) != 1 {
			return Query{}, errors.New("duplicate diagnostics parameter")
		}
		value := v[0]
		switch key {
		case "level":
			if value != "" && allowed(value, "ERROR", "WARN", "INFO") == "" {
				return Query{}, errors.New("invalid diagnostics level")
			}
			q.Level = value
		case "instance":
			known := value == "" || value == h.buffer.instance
			if !internal {
				for _, p := range h.peers {
					known = known || value == p.Instance
				}
			}
			if !known {
				return Query{}, errors.New("unknown diagnostics instance")
			}
			q.Instance = value
		case "q":
			if !utf8.ValidString(value) || utf8.RuneCountInString(value) > 128 || strings.ContainsFunc(value, unicode.IsControl) {
				return Query{}, errors.New("invalid diagnostics search")
			}
			q.Q = value
		case "limit":
			n, e := strconv.Atoi(value)
			if e != nil || n < 1 || n > 100 || strconv.Itoa(n) != value {
				return Query{}, errors.New("invalid diagnostics limit")
			}
			q.Limit = n
		default:
			return Query{}, errors.New("unknown diagnostics parameter")
		}
	}
	return q, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	internal := r.URL.Path == InternalPath
	if !internal && r.URL.Path != PublicPath {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", 405)
		return
	}
	if internal {
		if !privateRemote(r.RemoteAddr) {
			http.Error(w, "private peer required", 403)
			return
		}
		if subtle.ConstantTimeCompare([]byte(h.token), []byte(r.Header.Get("X-Ops-Token"))) != 1 {
			http.Error(w, "internal token required", 401)
			return
		}
	}
	q, err := h.parseQuery(r, internal)
	if err != nil {
		http.Error(w, "invalid diagnostics query", 400)
		return
	}
	var s Snapshot
	if internal {
		s = h.buffer.Snapshot(q)
	} else {
		s = h.aggregate(r.Context(), q)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s)
}

func (h *handler) aggregate(parent context.Context, q Query) Snapshot {
	ctx, cancel := context.WithTimeout(parent, h.timeout)
	defer cancel()
	s := newSnapshot()
	if q.Instance == "" || q.Instance == h.buffer.instance {
		s = h.buffer.Snapshot(q)
	}
	type result struct {
		snapshot Snapshot
		info     Instance
	}
	results := make([]result, len(h.peers))
	var wg sync.WaitGroup
	for i, p := range h.peers {
		if q.Instance != "" && q.Instance != p.Instance {
			continue
		}
		results[i].info = Instance{Instance: p.Instance, Status: "unavailable"}
		wg.Go(func() {
			fetched, err := h.fetch(ctx, p, q)
			if err == nil {
				results[i] = result{snapshot: fetched, info: fetched.Instances[0]}
			}
		})
	}
	wg.Wait()
	for _, r := range results {
		if r.info.Instance != "" {
			s.Instances = append(s.Instances, r.info)
			s.Items = append(s.Items, r.snapshot.Items...)
		}
	}
	sortEvents(s.Items)
	if len(s.Items) > q.Limit {
		s.Items = s.Items[:q.Limit]
	}
	return s
}

func (h *handler) fetch(ctx context.Context, p Peer, q Query) (Snapshot, error) {
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	case <-ctx.Done():
		return Snapshot{}, ctx.Err()
	}
	values := url.Values{"limit": {strconv.Itoa(q.Limit)}}
	if q.Level != "" {
		values.Set("level", q.Level)
	}
	if q.Q != "" {
		values.Set("q", q.Q)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL+"?"+values.Encode(), nil)
	if err != nil {
		return Snapshot{}, err
	}
	req.Header.Set("X-Ops-Token", h.token)
	response, err := h.client.Do(req)
	if err != nil {
		return Snapshot{}, errors.New("diagnostics peer unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.ContentLength > maxPeerBytes {
		return Snapshot{}, errors.New("diagnostics peer unavailable")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxPeerBytes+1))
	if err != nil || len(raw) > maxPeerBytes {
		return Snapshot{}, errors.New("diagnostics peer response exceeded budget")
	}
	var s Snapshot
	if err = json.Unmarshal(raw, &s); err != nil {
		return Snapshot{}, errors.New("invalid diagnostics peer response")
	}
	if len(s.Instances) != 1 || len(s.Items) > q.Limit || s.Retention.Persistent || s.Retention.CapacityPerInstance != Capacity {
		return Snapshot{}, errors.New("invalid diagnostics peer scope")
	}
	info := s.Instances[0]
	if info.Instance != p.Instance || info.Status != "ok" || !uuid.MatchString(info.BootID) || info.Capacity != Capacity || info.Retained < 0 || info.Retained > Capacity {
		return Snapshot{}, errors.New("invalid diagnostics peer instance")
	}
	for i, e := range s.Items {
		d, known := events[e.Kind]
		if !known || e.Instance != p.Instance || e.BootID != info.BootID || e.Time.IsZero() || e.ID == "" || len(e.ID) > 100 || !strings.HasPrefix(e.ID, info.BootID+":") || allowed(e.Level, "INFO", "WARN", "ERROR") == "" {
			return Snapshot{}, errors.New("invalid diagnostics peer event")
		}
		f := cleanFields(Fields{RequestID: e.RequestID, MsgID: e.MsgID, TraceID: e.TraceID, DeliveryID: e.DeliveryID, Platform: e.Platform, Outcome: e.Outcome, Status: e.Status})
		e.Message = d.message
		e.RequestID, e.MsgID, e.TraceID, e.DeliveryID, e.Platform, e.Outcome, e.Status = f.RequestID, f.MsgID, f.TraceID, f.DeliveryID, f.Platform, f.Outcome, f.Status
		if !matches(e, q) {
			return Snapshot{}, errors.New("diagnostics peer ignored filter")
		}
		s.Items[i] = e
	}
	return s, nil
}
