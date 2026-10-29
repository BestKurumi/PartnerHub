// Package ops exposes bounded, cached operational observations. The caller must
// wrap Handler in the same authentication and same-origin policy as demo APIs.
package ops

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	PrometheusURL string
	GrafanaURL    string
	Interval      time.Duration
	Timeout       time.Duration
	StaleAfter    time.Duration
	MaxBytes      int64
	MaxSeries     int
}
type Sample struct {
	Labels    map[string]string `json:"labels"`
	Value     float64           `json:"value"`
	SampledAt time.Time         `json:"sampledAt"`
}
type Metric struct {
	Key     string   `json:"key"`
	Source  string   `json:"source"`
	Status  string   `json:"status"`
	Reason  string   `json:"reason,omitempty"`
	Samples []Sample `json:"samples"`
}
type Snapshot struct {
	SampledAt *time.Time `json:"sampledAt"`
	Window    string     `json:"window"`
	Metrics   []Metric   `json:"metrics"`
	Trends    []Trend    `json:"trends"`
}
type definition struct{ key, expression, kind string }
type cached struct {
	samples []Sample
	fetched time.Time
	failed  bool
}
type Service struct {
	config    Config
	client    *http.Client
	endpoint  string
	workers   int
	mu        sync.RWMutex
	cache     map[string]cached
	histories map[string]cachedTrend
	sampledAt *time.Time
	running   atomic.Bool
}

// New validates deployment-owned URLs; browser parameters can never replace them.
func New(c Config) (*Service, error) {
	for _, raw := range []string{c.PrometheusURL, c.GrafanaURL} {
		if raw != "" {
			u, e := url.Parse(raw)
			if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
				return nil, errors.New("ops URL must be http/https without credentials, query or fragment")
			}
		}
	}
	if c.Interval == 0 {
		c.Interval = 15 * time.Second
	}
	if c.Timeout == 0 {
		c.Timeout = 750 * time.Millisecond
	}
	if c.StaleAfter == 0 {
		c.StaleAfter = 2 * time.Minute
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = 1 << 20
	}
	if c.MaxSeries == 0 {
		c.MaxSeries = 128
	}
	if c.Interval < time.Second || c.Interval > time.Hour || c.Timeout <= 0 || c.Timeout > 2*time.Second || c.StaleAfter < c.Interval || c.MaxBytes < 128 || c.MaxBytes > 1<<20 || c.MaxSeries < 1 || c.MaxSeries > 256 {
		return nil, errors.New("ops sampling bounds invalid")
	}
	endpoint := ""
	if c.PrometheusURL != "" {
		endpoint = strings.TrimRight(c.PrometheusURL, "/") + "/api/v1/query"
	}
	return &Service{config: c, endpoint: endpoint, workers: 6, cache: make(map[string]cached), histories: make(map[string]cachedTrend), client: &http.Client{Timeout: c.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func definitions(window string) []definition {
	rate := func(name string) string { return "rate(" + name + "[" + window + "])" }
	return []definition{
		{"ingress_rate", "sum by (status) (" + rate("hub_ingress_requests_total") + ")", ""},
		{"ingress_total", "sum by (status) (hub_ingress_requests_total)", ""},
		{"first_rate", "sum by (platform,outcome) (" + rate("hub_first_delivery_total") + ")", ""},
		{"first_p99", "histogram_quantile(0.99, sum by (platform,outcome,le) (" + rate("hub_first_delivery_duration_seconds_bucket") + "))", ""},
		{"ingress_p99", "histogram_quantile(0.99, sum by (status,le) (" + rate("hub_ingress_duration_seconds_bucket") + "))", ""},
		{"route_rate", "sum by (status) (" + rate("hub_route_results_total") + ")", ""},
		{"archive_pending", "max(hub_archive_pending_count)", ""}, {"archive_bytes", "max(hub_archive_pending_bytes)", ""}, {"archive_oldest", "max(hub_archive_pending_oldest_age_seconds)", ""},
		{"recovery_tasks", "max by (platform,status) (hub_recovery_tasks)", ""}, {"recovery_oldest", "max by (platform,status) (hub_recovery_oldest_age_seconds)", ""},
		{"binding_lag", "max(hub_binding_consumer_lag)", ""},
		{"targets_up", `up{job="hub"}`, ""}, {"observation_up", "hub_observation_up", ""}, {"observation_age", "time() - hub_observation_last_success_timestamp_seconds", ""},
		{"cpu", rate(`process_cpu_seconds_total{job="hub"}`), ""}, {"rss", `process_resident_memory_bytes{job="hub"}`, ""}, {"heap", `go_memstats_heap_alloc_bytes{job="hub"}`, ""}, {"goroutines", `go_goroutines{job="hub"}`, ""},
		{"gc_pause", `go_gc_duration_seconds{job="hub",quantile="1"}`, ""},
		{"db_open", "hub_db_pool_open_connections", ""}, {"db_in_use", "hub_db_pool_in_use_connections", ""}, {"db_max", "hub_db_pool_max_connections", ""},
		{"resource_up", "hub_resource_sample_up", ""}, {"resource_age", "time() - hub_resource_last_success_timestamp_seconds", ""},
		{"mysql_connections", "max(hub_resource_mysql_connections)", "mysql"}, {"mysql_threads_running", "max(hub_resource_mysql_threads_running)", "mysql"}, {"mysql_queries", "max(hub_resource_mysql_queries_total)", "mysql"}, {"mysql_slow_queries", "max(hub_resource_mysql_slow_queries_total)", "mysql"}, {"mysql_buffer_pool_bytes", "max(hub_resource_mysql_buffer_pool_bytes)", "mysql"},
		{"redis_clients", "max(hub_resource_redis_clients)", "redis"}, {"redis_blocked", "max(hub_resource_redis_blocked_clients)", "redis"}, {"redis_evicted", "max(hub_resource_redis_evicted_keys_total)", "redis"}, {"redis_ops", "max(hub_resource_redis_ops_per_second)", "redis"},
		{"redis_used", "max(hub_redis_used_memory_bytes)", ""}, {"redis_max", "max(hub_redis_max_memory_bytes)", ""}, {"redis_durable", "min(hub_redis_persistence_healthy)", ""},
		{"kafka_brokers", "max(hub_resource_kafka_brokers)", "kafka"}, {"kafka_partitions", "max by (topic) (hub_resource_kafka_partitions)", "kafka"}, {"kafka_leaderless", "max by (topic) (hub_resource_kafka_leaderless_partitions)", "kafka"}, {"kafka_under_replicated", "max by (topic) (hub_resource_kafka_under_replicated_partitions)", "kafka"},
	}
}

var windows = []string{"15m", "1h", "6h"}

// Run blocks until cancellation. Sampling cycles cannot overlap, even if called
// twice. Six bounded requests progress through every key; no shared short cycle
// deadline can starve definitions later in the list.
func (s *Service) Run(ctx context.Context) {
	if !s.running.CompareAndSwap(false, true) {
		return
	}
	defer s.running.Store(false)
	for {
		if ctx.Err() != nil {
			return
		}
		s.sampleCycle(ctx)
		timer := time.NewTimer(s.config.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
func (s *Service) sampleCycle(ctx context.Context) {
	if s.endpoint == "" {
		return
	}
	type job struct {
		expression string
		keys       []string
	}
	jobs := make(map[string][]string)
	for _, window := range windows {
		for _, d := range definitions(window) {
			jobs[d.expression] = append(jobs[d.expression], window+"/"+d.key)
		}
	}
	queue := make(chan job)
	var wg sync.WaitGroup
	for range s.workers {
		wg.Go(func() {
			for j := range queue {
				if ctx.Err() != nil {
					return
				}
				samples, err := s.query(ctx, j.expression)
				now := time.Now().UTC()
				s.mu.Lock()
				for _, key := range j.keys {
					old := s.cache[key]
					if err == nil {
						old = cached{samples: samples, fetched: now}
					} else {
						old.failed = true
					}
					s.cache[key] = old
				}
				s.sampledAt = &now
				s.mu.Unlock()
			}
		})
	}
	for expression, keys := range jobs {
		select {
		case queue <- job{expression, keys}:
		case <-ctx.Done():
			close(queue)
			wg.Wait()
			return
		}
	}
	close(queue)
	wg.Wait()
	s.sampleHistory(ctx)
}

func (s *Service) query(ctx context.Context, expression string) ([]Sample, error) {
	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	u, _ := url.Parse(s.endpoint)
	q := u.Query()
	q.Set("query", expression)
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, errors.New("Prometheus unavailable")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, s.config.MaxBytes+1))
	if err != nil || int64(len(body)) > s.config.MaxBytes {
		return nil, errors.New("Prometheus response limit")
	}
	var out struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  []json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &out) != nil || out.Status != "success" || out.Data.ResultType != "vector" || out.Data.Result == nil || len(out.Data.Result) > s.config.MaxSeries {
		return nil, errors.New("invalid Prometheus vector")
	}
	result := make([]Sample, 0, len(out.Data.Result))
	for _, row := range out.Data.Result {
		if len(row.Value) != 2 || len(row.Metric) > 16 {
			return nil, errors.New("invalid Prometheus sample")
		}
		var timestamp float64
		var number string
		if json.Unmarshal(row.Value[0], &timestamp) != nil || json.Unmarshal(row.Value[1], &number) != nil || math.IsNaN(timestamp) || math.IsInf(timestamp, 0) || timestamp < 0 || timestamp > 1e11 {
			return nil, errors.New("invalid Prometheus timestamp")
		}
		value, err := strconv.ParseFloat(number, 64)
		if err != nil {
			return nil, errors.New("invalid Prometheus value")
		}
		// An initialized histogram with no observations legitimately yields NaN.
		// Omit that absent series rather than inventing zero or losing its peers.
		if math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		labels := make(map[string]string)
		for key, label := range row.Metric {
			switch key {
			case "platform", "outcome", "status", "job", "instance", "pool", "kind", "topic":
				if len(label) > 128 {
					return nil, errors.New("Prometheus label limit")
				}
				labels[key] = label
			}
		}
		secs, frac := math.Modf(timestamp)
		result = append(result, Sample{Labels: labels, Value: value, SampledAt: time.Unix(int64(secs), int64(frac*1e9)).UTC()})
	}
	return result, nil
}

func (s *Service) snapshot(window string) Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := Snapshot{Window: window, SampledAt: s.sampledAt, Metrics: make([]Metric, 0), Trends: s.historySnapshotLocked(window)}
	for _, d := range definitions(window) {
		c := s.cache[window+"/"+d.key]
		m := Metric{Key: d.key, Source: "prometheus", Status: "unavailable", Samples: make([]Sample, 0, len(c.samples))}
		switch {
		case c.failed:
			m.Reason = "query_failed"
		case c.fetched.IsZero():
			m.Reason = "not_sampled"
		case time.Since(c.fetched) > s.config.StaleAfter:
			m.Reason = "expired"
		case len(c.samples) == 0:
			m.Reason = "no_samples"
		}
		for _, sample := range c.samples {
			labels := make(map[string]string, len(sample.Labels))
			for k, v := range sample.Labels {
				labels[k] = v
			}
			sample.Labels = labels
			m.Samples = append(m.Samples, sample)
		}
		if len(m.Samples) > 0 {
			m.Status = "ok"
			if c.failed || time.Since(c.fetched) > s.config.StaleAfter {
				m.Status = "stale"
			}
			for _, sample := range m.Samples {
				if time.Since(sample.SampledAt) > s.config.StaleAfter || sample.SampledAt.After(time.Now().Add(s.config.Timeout)) {
					m.Status = "stale"
				}
			}
			health, age, kind := "", "", d.kind
			if kind != "" {
				health, age = "resource_up", "resource_age"
			}
			switch d.key {
			case "archive_pending", "archive_bytes", "archive_oldest", "redis_used", "redis_max", "redis_durable":
				health, age, kind = "observation_up", "observation_age", "archive"
			case "recovery_tasks", "recovery_oldest":
				health, age, kind = "observation_up", "observation_age", "recovery"
			case "binding_lag":
				health, age, kind = "observation_up", "observation_age", "binding_lag"
			}
			if health != "" {
				s.applyDependencyHealth(&m, s.cache[window+"/"+health], s.cache[window+"/"+age], kind)
			}
		}
		out.Metrics = append(out.Metrics, m)
	}
	return out
}

// Dependency up and age belong to a scrape source, not to the whole vector.
// Pair both labels before deciding that a source has never succeeded. Shared
// max series have lost their instance label: retain them if any source has a
// known/possibly historical value, marking partial health conservatively stale.
func (s *Service) applyDependencyHealth(m *Metric, health, age cached, kind string) {
	type source struct{ hasUp, hasAge, up, never, fresh bool }
	sources := make(map[string]source)
	sourceKey := func(labels map[string]string) string { return labels["job"] + "\x00" + labels["instance"] }
	now := time.Now()
	fresh := func(c cached, sample Sample) bool {
		return !c.failed && now.Sub(c.fetched) <= s.config.StaleAfter && now.Sub(sample.SampledAt) <= s.config.StaleAfter && !sample.SampledAt.After(now.Add(s.config.Timeout))
	}
	for _, sample := range health.samples {
		if kind != "" && sample.Labels["kind"] != kind {
			continue
		}
		key := sourceKey(sample.Labels)
		state := sources[key]
		state.hasUp = true
		state.up = sample.Value == 1
		state.fresh = fresh(health, sample)
		sources[key] = state
	}
	for _, sample := range age.samples {
		if kind != "" && sample.Labels["kind"] != kind {
			continue
		}
		key := sourceKey(sample.Labels)
		state := sources[key]
		state.hasAge = true
		ageFresh := fresh(age, sample)
		state.never = ageFresh && sample.Value >= float64(sample.SampledAt.Unix()-1)
		state.fresh = state.fresh && ageFresh && sample.Value >= 0 && sample.Value <= s.config.StaleAfter.Seconds()
		sources[key] = state
	}
	allNever := len(sources) > 0
	allFresh := len(sources) > 0
	for _, state := range sources {
		allNever = allNever && state.never
		allFresh = allFresh && state.hasUp && state.hasAge && state.up && state.fresh
	}
	kept := make([]Sample, 0, len(m.Samples))
	for _, sample := range m.Samples {
		if sample.Labels["job"] != "" || sample.Labels["instance"] != "" {
			state, found := sources[sourceKey(sample.Labels)]
			if found && state.never {
				m.Status = "stale"
				continue
			}
			if !found || !state.hasUp || !state.hasAge || !state.up || !state.fresh {
				m.Status = "stale"
			}
		} else {
			if allNever {
				continue
			}
			if !allFresh {
				m.Status = "stale"
			}
		}
		kept = append(kept, sample)
	}
	m.Samples = kept
	if len(kept) == 0 {
		m.Status = "unavailable"
	}
}
func (s *Service) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		if r.Method != "GET" {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", 405)
			return
		}
		params, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			http.Error(w, "invalid parameters", 400)
			return
		}
		window := "15m"
		switch r.URL.Path {
		case "/api/v1/ops/config":
			if len(params) != 0 {
				http.Error(w, "unknown parameter", 400)
				return
			}
			_ = json.NewEncoder(w).Encode(struct {
				GrafanaURL string `json:"grafanaUrl"`
			}{s.config.GrafanaURL})
		case "/api/v1/ops/metrics":
			for key, values := range params {
				if key != "window" || len(values) != 1 {
					http.Error(w, "unknown or repeated parameter", 400)
					return
				}
				window = values[0]
			}
			if window != "15m" && window != "1h" && window != "6h" {
				http.Error(w, "invalid window", 400)
				return
			}
			if err := json.NewEncoder(w).Encode(s.snapshot(window)); err != nil {
				return
			}
		default:
			http.Error(w, "unknown ops endpoint", 404)
		}
	})
}
