package observability

import (
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"partner-hub/internal/model"
	"strconv"
	"time"
)

// Metrics owns a private registry. Identifiers and error text never become labels.
type Metrics struct {
	routeResults                                            *prometheus.CounterVec
	targetCount                                             prometheus.Histogram
	poolOpen, poolInUse, poolMax, poolWait, poolWaitSeconds *prometheus.GaugeVec
	redisUsed, redisMax, redisDurable                       prometheus.Gauge
	recoverySuccess                                         *prometheus.HistogramVec
	Registry                                                *prometheus.Registry
	first                                                   *prometheus.CounterVec
	firstDuration                                           *prometheus.HistogramVec
	ingress                                                 *prometheus.CounterVec
	ingressDuration                                         *prometheus.HistogramVec
	inFlight                                                prometheus.Gauge
	archiveCount, archiveBytes, archiveAge                  prometheus.Gauge
	archiveFlush                                            *prometheus.CounterVec
	archiveBatch                                            prometheus.Histogram
	archiveVisible                                          prometheus.Histogram
	recovery                                                *prometheus.CounterVec
	leases                                                  *prometheus.CounterVec
	recoveryTasks, recoveryAge                              *prometheus.GaugeVec
	cache, source                                           *prometheus.CounterVec
	bindingLag                                              prometheus.Gauge
	up, lastSuccess                                         *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry()}
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
		m.Registry.MustRegister(v)
		return v
	}
	gauge := func(name, help string) prometheus.Gauge {
		v := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
		m.Registry.MustRegister(v)
		return v
	}
	gauges := func(name, help string, labels ...string) *prometheus.GaugeVec {
		v := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
		m.Registry.MustRegister(v)
		return v
	}
	histogram := func(name, help string, labels ...string) *prometheus.HistogramVec {
		v := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 8, 15, 30, 60}}, labels)
		m.Registry.MustRegister(v)
		return v
	}
	m.routeResults = counter("hub_route_results_total", "Per-message first route results, including no route and lookup failures.", "status")
	m.targetCount = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "hub_first_targets", Help: "Number of first-round targets per routed message, including zero.", Buckets: []float64{0, 1, 2}})
	m.Registry.MustRegister(m.targetCount)
	m.first = counter("hub_first_delivery_total", "First target outcomes, including attempts that never sent.", "platform", "outcome", "send_started")
	m.firstDuration = histogram("hub_first_delivery_duration_seconds", "Ingress arrival to target result; B ends at broker acknowledgement, not consumption.", "platform", "outcome")
	m.ingress = counter("hub_ingress_requests_total", "All trade HTTP responses, including rejected and invalid input.", "status")
	// Keep a bounded zero baseline before the first trade. Without a child,
	// CounterVec exports nothing and an idle, healthy ingress looks missing.
	for _, code := range []string{"200", "202", "400", "404", "413", "429", "500", "503"} {
		m.ingress.WithLabelValues(code).Add(0)
	}
	m.ingressDuration = histogram("hub_ingress_duration_seconds", "Trade HTTP arrival to response.", "status")
	m.inFlight = gauge("hub_ingress_in_flight", "Trade HTTP requests currently executing.")
	m.archiveCount = gauge("hub_archive_pending_count", "Shared Redis pending snapshots; use max across hub instances.")
	m.archiveBytes = gauge("hub_archive_pending_bytes", "Shared Redis pending serialized bytes, conservative for legacy unaccounted records.")
	m.archiveAge = gauge("hub_archive_pending_oldest_age_seconds", "Oldest pending ingress timestamp; only valid when archive observation is healthy.")
	m.archiveFlush = counter("hub_archive_flush_total", "Archive flush outcomes; empty batches count as empty, not successful persistence.", "outcome")
	m.archiveBatch = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "hub_archive_batch_size", Help: "Claimed snapshots per non-empty archive batch.", Buckets: []float64{1, 5, 10, 25, 50, 100}})
	m.archiveVisible = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "hub_archive_visible_duration_seconds", Help: "Ingress to confirmed MySQL archive save; replay may observe a snapshot more than once.", Buckets: []float64{.1, .25, .5, 1, 2, 5, 15, 30, 60, 300}})
	m.Registry.MustRegister(m.archiveBatch, m.archiveVisible)
	m.recovery = counter("hub_recovery_attempts_total", "Persistent recovery completion outcomes; persistence errors counted separately.", "platform", "outcome")
	m.leases = counter("hub_lease_recovered_total", "Expired durable leases recovered, by delivery or route.", "kind")
	m.recoveryTasks = gauges("hub_recovery_tasks", "Shared durable recovery task states; use max across hub instances.", "platform", "status")
	m.recoveryAge = gauges("hub_recovery_oldest_age_seconds", "Oldest ingress age of durable recovery tasks by state.", "platform", "status")
	m.cache = counter("hub_binding_cache_total", "Binding cache read attempts, including singleflight cache rechecks.", "outcome")
	m.source = counter("hub_binding_source_total", "Actual bounded database loads or admission rejections; singleflight waiters are not extra DB loads.", "outcome")
	m.bindingLag = gauge("hub_binding_consumer_lag", "Binding group committed-offset lag in messages, summed over fixed binding topic.")
	m.up = gauges("hub_observation_up", "Last bounded background observation succeeded (1) or failed (0).", "kind")
	m.lastSuccess = gauges("hub_observation_last_success_timestamp_seconds", "Unix timestamp of last successful background observation; zero before first success.", "kind")
	for _, kind := range []string{"archive", "recovery", "binding_lag"} {
		m.up.WithLabelValues(kind).Set(0)
		m.lastSuccess.WithLabelValues(kind).Set(0)
	}
	for _, p := range []string{"PLATFORM_A", "PLATFORM_B"} {
		for _, o := range []model.SendOutcome{model.Success, model.RetryableFailure, model.PermanentFailure, model.Unknown} {
			for _, started := range []string{"false", "true"} {
				m.first.WithLabelValues(p, string(o), started).Add(0)
			}
			m.firstDuration.WithLabelValues(p, string(o))
		}
	}
	m.poolOpen = gauges("hub_db_pool_open_connections", "Open database connections by fixed pool.", "pool")
	m.poolInUse = gauges("hub_db_pool_in_use_connections", "In-use database connections by fixed pool.", "pool")
	m.poolMax = gauges("hub_db_pool_max_connections", "Configured maximum database connections by fixed pool.", "pool")
	m.poolWait = gauges("hub_db_pool_wait_total", "Cumulative connection waits by fixed pool.", "pool")
	m.poolWaitSeconds = gauges("hub_db_pool_wait_seconds_total", "Cumulative connection wait seconds by fixed pool.", "pool")
	m.redisUsed = gauge("hub_redis_used_memory_bytes", "Shared Redis used memory.")
	m.redisMax = gauge("hub_redis_max_memory_bytes", "Shared Redis configured maxmemory.")
	m.redisDurable = gauge("hub_redis_persistence_healthy", "AOF enabled, last write healthy and noeviction; inspect archive observation freshness.")
	m.recoverySuccess = histogram("hub_recovery_success_duration_seconds", "Ingress to confirmed recovery success; separate from first delivery.", "platform")
	m.Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Timeout: time.Second, MaxRequestsInFlight: 2})
}
func platform(p string) string {
	if p == "PLATFORM_A" || p == "PLATFORM_B" {
		return p
	}
	return "OTHER"
}
func outcome(o model.SendOutcome) string {
	switch o {
	case model.Success, model.RetryableFailure, model.PermanentFailure, model.Unknown:
		return string(o)
	}
	return "OTHER"
}
func (m *Metrics) First(s model.FirstRoundSnapshot) {
	status := s.Route.Status
	switch status {
	case "ROUTED", "NO_ROUTE", "RETRY_WAIT":
	default:
		status = "OTHER"
	}
	m.routeResults.WithLabelValues(status).Inc()
	m.targetCount.Observe(float64(min(2, len(s.Route.Targets))))
	for _, a := range s.Attempts {
		p, o := platform(a.Target.PlatformID), outcome(a.Result.Outcome)
		m.first.WithLabelValues(p, o, strconv.FormatBool(a.Result.SendStarted)).Inc()
		if !s.ReceivedAt.IsZero() && !a.Result.FinishedAt.IsZero() {
			d := a.Result.FinishedAt.Sub(s.ReceivedAt).Seconds()
			if d >= 0 {
				m.firstDuration.WithLabelValues(p, o).Observe(d)
			}
		}
	}
}
func (m *Metrics) BindingCache(result string) {
	if result != "hit" && result != "miss" {
		result = "error"
	}
	m.cache.WithLabelValues(result).Inc()
}
func (m *Metrics) BindingSource(result string) {
	if result != "success" && result != "rejected" {
		result = "error"
	}
	m.source.WithLabelValues(result).Inc()
}
func (m *Metrics) Observation(kind string, err error) {
	if kind != "archive" && kind != "recovery" && kind != "binding_lag" {
		return
	}
	if err != nil {
		m.up.WithLabelValues(kind).Set(0)
		return
	}
	m.up.WithLabelValues(kind).Set(1)
	m.lastSuccess.WithLabelValues(kind).Set(float64(time.Now().Unix()))
}
func (m *Metrics) Backlog(count, bytes int64, age time.Duration) {
	m.archiveCount.Set(float64(count))
	m.archiveBytes.Set(float64(bytes))
	m.archiveAge.Set(max(0, age.Seconds()))
}
func (m *Metrics) BindingLag(lag int64) { m.bindingLag.Set(float64(max(0, lag))) }
func (m *Metrics) ArchiveFlush(outcome string, n int) {
	if outcome != "success" && outcome != "empty" && outcome != "lease_lost" {
		outcome = "error"
	}
	m.archiveFlush.WithLabelValues(outcome).Inc()
	if n > 0 {
		m.archiveBatch.Observe(float64(n))
	}
}
func (m *Metrics) ArchiveVisible(received time.Time) {
	if !received.IsZero() {
		m.archiveVisible.Observe(max(0, time.Since(received).Seconds()))
	}
}
func (m *Metrics) Recovery(platformID string, result model.SendResult, err error) {
	o := outcome(result.Outcome)
	if err != nil {
		o = "PERSISTENCE_ERROR"
	}
	m.recovery.WithLabelValues(platform(platformID), o).Inc()
}
func (m *Metrics) LeaseRecovered(kind string, n int) {
	if n > 0 && (kind == "delivery" || kind == "route") {
		m.leases.WithLabelValues(kind).Add(float64(n))
	}
}
func (m *Metrics) RecoveryState(p, status string, count int64, age time.Duration) {
	if status != "PENDING" && status != "RETRY_WAIT" && status != "IN_FLIGHT" && status != "NEEDS_ATTENTION" {
		return
	}
	if p != "ROUTE" {
		p = platform(p)
	}
	m.recoveryTasks.WithLabelValues(p, status).Set(float64(count))
	m.recoveryAge.WithLabelValues(p, status).Set(max(0, age.Seconds()))
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func (m *Metrics) HTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		m.inFlight.Inc()
		defer m.inFlight.Dec()
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		status := sw.status
		if status == 0 {
			status = 200
		}
		label := strconv.Itoa(status)
		if status < 100 || status > 599 {
			label = "other"
		}
		m.ingress.WithLabelValues(label).Inc()
		m.ingressDuration.WithLabelValues(label).Observe(time.Since(start).Seconds())
	})
}

func (m *Metrics) Pool(name string, s sql.DBStats) {
	switch name {
	case "bindings", "handoff", "archive", "recovery", "query":
	default:
		return
	}
	m.poolOpen.WithLabelValues(name).Set(float64(s.OpenConnections))
	m.poolInUse.WithLabelValues(name).Set(float64(s.InUse))
	m.poolMax.WithLabelValues(name).Set(float64(s.MaxOpenConnections))
	m.poolWait.WithLabelValues(name).Set(float64(s.WaitCount))
	m.poolWaitSeconds.WithLabelValues(name).Set(s.WaitDuration.Seconds())
}
func (m *Metrics) Redis(used, maxBytes int64, healthy bool) {
	m.redisUsed.Set(float64(used))
	m.redisMax.Set(float64(maxBytes))
	v := 0.
	if healthy {
		v = 1
	}
	m.redisDurable.Set(v)
}
func (m *Metrics) RecoverySuccess(p string, received time.Time) {
	if !received.IsZero() {
		m.recoverySuccess.WithLabelValues(platform(p)).Observe(max(0, time.Since(received).Seconds()))
	}
}
