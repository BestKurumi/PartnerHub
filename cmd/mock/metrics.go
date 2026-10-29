package main

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type metricSample func(context.Context) (int64, error)
type sampledGauge struct {
	read                        metricSample
	value, success, lastSuccess prometheus.Gauge
	timeout                     time.Duration
}
type mockMetrics struct {
	handler http.Handler
	samples []sampledGauge
}

func newMockMetrics(receipts, lag metricSample, paused func() bool) *mockMetrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := &mockMetrics{handler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{MaxRequestsInFlight: 2, Timeout: time.Second})}
	add := func(read metricSample, name, help, health, last string, timeout time.Duration) {
		if read == nil {
			return
		}
		sample := sampledGauge{read: read, value: prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help}), success: prometheus.NewGauge(prometheus.GaugeOpts{Name: health, Help: "Whether the last background sample succeeded (0 before first success)."}), lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{Name: last, Help: "Unix timestamp of the last successful background sample; zero before first success."}), timeout: timeout}
		registry.MustRegister(sample.value, sample.success, sample.lastSuccess)
		m.samples = append(m.samples, sample)
	}
	add(receipts, "mock_received_rows", "Durable valid receipt rows in this Mock's independent database, including pre-process receipts. Gauge may decrease after explicit data reset; Kafka offset replays do not add rows.", "mock_receipt_sample_success", "mock_receipt_last_success_timestamp_seconds", time.Second)
	add(lag, "mock_platform2_consumer_lag_records", "Sum of fixed platform2 topic log end offset minus group committed next offset (or log start before first commit); includes valid and invalid uncommitted records. Not a Broker ACK count.", "mock_platform2_lag_sample_success", "mock_platform2_lag_last_success_timestamp_seconds", 1500*time.Millisecond)
	if paused != nil {
		registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "mock_platform2_consumer_paused", Help: "Whether platform2 notification consumption is administratively paused."}, func() float64 {
			if paused() {
				return 1
			}
			return 0
		}))
	}
	return m
}
func (m *mockMetrics) withHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		if r.Method != "GET" {
			w.Header().Set("Allow", "GET")
			http.Error(w, "method not allowed", 405)
			return
		}
		m.handler.ServeHTTP(w, r)
	})
}
func (m *mockMetrics) sample(ctx context.Context) {
	for _, s := range m.samples {
		if ctx.Err() != nil {
			return
		}
		budget, cancel := context.WithTimeout(ctx, s.timeout)
		value, err := s.read(budget)
		cancel()
		if err != nil {
			s.success.Set(0)
			continue
		}
		s.value.Set(float64(value))
		s.success.Set(1)
		s.lastSuccess.SetToCurrentTime()
	}
}
func (m *mockMetrics) run(ctx context.Context, interval time.Duration) {
	m.sample(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sample(ctx)
		}
	}
}
func receiptSampler(db *sql.DB) metricSample {
	return func(ctx context.Context) (int64, error) {
		var rows int64
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM received_notification").Scan(&rows)
		return rows, err
	}
}
