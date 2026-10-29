package observability

import (
	"context"
	"database/sql"
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ResourceConfig struct {
	DB      *sql.DB
	Redis   *redis.Options
	Brokers []string
}
type resourceValue struct {
	name, topic string
	value       float64
}
type resourceSnapshot struct {
	up     bool
	last   time.Time
	values []resourceValue
}

// Resources owns only its Redis/Kafka clients. MySQL uses the existing service
// identity/pool. Scrapes never perform network IO or request new privileges.
type Resources struct {
	db      *sql.DB
	redis   *redis.Client
	kafka   *kgo.Client
	mu      sync.RWMutex
	state   map[string]resourceSnapshot
	descs   map[string]*prometheus.Desc
	running atomic.Bool
}

var resourceNames = map[string]string{
	"mysql_connections":        "Current MySQL global Threads_connected; may include other service identities.",
	"mysql_threads_running":    "MySQL global active threads.",
	"mysql_queries_total":      "MySQL global Queries since server start.",
	"mysql_slow_queries_total": "MySQL global Slow_queries since server start.",
	"mysql_buffer_pool_bytes":  "MySQL InnoDB buffer pool bytes occupied by data; not a container memory limit.",
	"redis_clients":            "Redis connected clients.", "redis_blocked_clients": "Redis blocked clients.", "redis_evicted_keys_total": "Redis evicted keys since server start.", "redis_ops_per_second": "Redis instantaneous operations per second.",
	"kafka_brokers": "Brokers reported by fixed-topic Kafka metadata.", "kafka_partitions": "Partitions in a fixed application topic.", "kafka_leaderless_partitions": "Fixed-topic partitions without a leader.", "kafka_under_replicated_partitions": "Fixed-topic partitions with fewer ISR than replicas.",
}

func NewResources(c ResourceConfig, registry *prometheus.Registry) (*Resources, error) {
	if registry == nil {
		return nil, errors.New("resource registry required")
	}
	r := &Resources{db: c.DB, state: make(map[string]resourceSnapshot), descs: make(map[string]*prometheus.Desc)}
	for name, help := range resourceNames {
		var labels []string
		if strings.HasPrefix(name, "kafka_") && name != "kafka_brokers" {
			labels = []string{"topic"}
		}
		r.descs[name] = prometheus.NewDesc("hub_resource_"+name, help, labels, nil)
	}
	r.descs["up"] = prometheus.NewDesc("hub_resource_sample_up", "Last resource observation succeeded (1) or failed/unavailable (0).", []string{"kind"}, nil)
	r.descs["last"] = prometheus.NewDesc("hub_resource_last_success_timestamp_seconds", "Last successful resource sample; zero before first observation.", []string{"kind"}, nil)
	for _, kind := range []string{"mysql", "redis", "kafka"} {
		r.state[kind] = resourceSnapshot{}
	}
	if c.Redis != nil {
		opts := *c.Redis
		opts.ContextTimeoutEnabled = true
		opts.DialTimeout = 200 * time.Millisecond
		opts.ReadTimeout = 300 * time.Millisecond
		opts.WriteTimeout = 200 * time.Millisecond
		opts.PoolTimeout = 100 * time.Millisecond
		opts.MaxRetries = -1
		opts.PoolSize = 1
		opts.MinIdleConns = 0
		r.redis = redis.NewClient(&opts)
	}
	if len(c.Brokers) > 0 {
		if len(c.Brokers) > 8 {
			_ = r.Close()
			return nil, errors.New("resource broker limit")
		}
		client, err := kgo.NewClient(kgo.SeedBrokers(c.Brokers...), kgo.RequestRetries(0), kgo.DialTimeout(300*time.Millisecond), kgo.RequestTimeoutOverhead(time.Second), kgo.BrokerMaxReadBytes(1<<20), kgo.FetchMaxBytes(512<<10))
		if err != nil {
			_ = r.Close()
			return nil, err
		}
		r.kafka = client
	}
	if err := registry.Register(r); err != nil {
		_ = r.Close()
		return nil, err
	}
	return r, nil
}
func (r *Resources) Close() error {
	if r.kafka != nil {
		r.kafka.Close()
	}
	if r.redis != nil {
		return r.redis.Close()
	}
	return nil
}
func (r *Resources) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range r.descs {
		ch <- desc
	}
}
func (r *Resources) Collect(ch chan<- prometheus.Metric) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for kind, state := range r.state {
		up := 0.
		if state.up {
			up = 1
		}
		last := 0.
		if !state.last.IsZero() {
			last = float64(state.last.UnixNano()) / 1e9
		}
		ch <- prometheus.MustNewConstMetric(r.descs["up"], prometheus.GaugeValue, up, kind)
		ch <- prometheus.MustNewConstMetric(r.descs["last"], prometheus.GaugeValue, last, kind)
		for _, value := range state.values {
			labels := []string{}
			if value.topic != "" {
				labels = append(labels, value.topic)
			}
			typ := prometheus.GaugeValue
			if strings.HasSuffix(value.name, "_total") {
				typ = prometheus.CounterValue
			}
			ch <- prometheus.MustNewConstMetric(r.descs[value.name], typ, value.value, labels...)
		}
	}
}
func (r *Resources) Run(ctx context.Context, interval time.Duration) {
	if !r.running.CompareAndSwap(false, true) {
		return
	}
	defer r.running.Store(false)
	if interval < time.Second {
		interval = time.Second
	}
	for {
		if ctx.Err() != nil {
			return
		}
		r.sampleCycle(ctx)
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
func (r *Resources) sampleCycle(ctx context.Context) {
	for _, kind := range []string{"mysql", "redis", "kafka"} {
		if ctx.Err() != nil {
			return
		}
		one, cancel := context.WithTimeout(ctx, 600*time.Millisecond)
		var values []resourceValue
		var err error
		switch kind {
		case "mysql":
			values, err = r.mysqlValues(one)
		case "redis":
			if r.redis == nil {
				err = errors.New("Redis unavailable")
			} else {
				var info string
				info, err = r.redis.Info(one, "clients", "stats").Result()
				if err == nil {
					var parsed map[string]float64
					parsed, err = redisResourceValues(info)
					for name, value := range parsed {
						values = append(values, resourceValue{name: name, value: value})
					}
				}
			}
		case "kafka":
			if r.kafka == nil {
				err = errors.New("Kafka unavailable")
			} else {
				var md kadm.Metadata
				md, err = kadm.NewClient(r.kafka).Metadata(one, "customer.binding.events", "platform2.trade.notifications")
				if err == nil {
					values, err = kafkaResourceValues(md)
				}
			}
		}
		cancel()
		r.mu.Lock()
		state := r.state[kind]
		state.up = err == nil
		if err == nil {
			state.values = values
			state.last = time.Now().UTC()
		}
		r.state[kind] = state
		r.mu.Unlock()
	}
}
func (r *Resources) mysqlValues(ctx context.Context) ([]resourceValue, error) {
	if r.db == nil {
		return nil, errors.New("MySQL unavailable")
	}
	const query = "SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_connected','Threads_running','Queries','Slow_queries','Innodb_buffer_pool_bytes_data')"
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names := map[string]string{"Threads_connected": "mysql_connections", "Threads_running": "mysql_threads_running", "Queries": "mysql_queries_total", "Slow_queries": "mysql_slow_queries_total", "Innodb_buffer_pool_bytes_data": "mysql_buffer_pool_bytes"}
	result := make([]resourceValue, 0, 5)
	seen := make(map[string]bool)
	for rows.Next() {
		var name, text string
		if err := rows.Scan(&name, &text); err != nil {
			return nil, err
		}
		key, ok := names[name]
		if !ok || seen[name] {
			return nil, errors.New("invalid MySQL resource status")
		}
		seen[name] = true
		value, err := resourceNumber(text)
		if err != nil {
			return nil, err
		}
		result = append(result, resourceValue{name: key, value: value})
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	if len(result) != 5 {
		return nil, errors.New("incomplete MySQL resource status")
	}
	return result, nil
}
func resourceNumber(text string) (float64, error) {
	v, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0, errors.New("invalid resource number")
	}
	return v, nil
}
func redisResourceValues(info string) (map[string]float64, error) {
	if len(info) > 64<<10 {
		return nil, errors.New("Redis INFO limit")
	}
	names := map[string]string{"connected_clients": "redis_clients", "blocked_clients": "redis_blocked_clients", "evicted_keys": "redis_evicted_keys_total", "instantaneous_ops_per_sec": "redis_ops_per_second"}
	result := make(map[string]float64)
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSuffix(line, "\r")
		name, text, ok := strings.Cut(line, ":")
		key, wanted := names[name]
		if !ok || !wanted {
			continue
		}
		if _, duplicate := result[key]; duplicate {
			return nil, errors.New("duplicate Redis resource field")
		}
		value, err := resourceNumber(text)
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	if len(result) != len(names) {
		return nil, errors.New("incomplete Redis INFO")
	}
	return result, nil
}
func kafkaResourceValues(md kadm.Metadata) ([]resourceValue, error) {
	if len(md.Brokers) == 0 || len(md.Brokers) > 256 {
		return nil, errors.New("Kafka brokers unavailable")
	}
	result := []resourceValue{{name: "kafka_brokers", value: float64(len(md.Brokers))}}
	for _, topic := range []string{"customer.binding.events", "platform2.trade.notifications"} {
		detail, ok := md.Topics[topic]
		if !ok || detail.Err != nil || len(detail.Partitions) == 0 || len(detail.Partitions) > 4096 {
			return nil, errors.New("Kafka fixed topic unavailable")
		}
		var leaderless, under int
		for _, p := range detail.Partitions {
			if p.Err != nil {
				return nil, errors.New("Kafka partition metadata unavailable")
			}
			if p.Leader < 0 {
				leaderless++
			}
			if len(p.ISR) < len(p.Replicas) {
				under++
			}
		}
		result = append(result, resourceValue{name: "kafka_partitions", topic: topic, value: float64(len(detail.Partitions))}, resourceValue{name: "kafka_leaderless_partitions", topic: topic, value: float64(leaderless)}, resourceValue{name: "kafka_under_replicated_partitions", topic: topic, value: float64(under)})
	}
	return result, nil
}
