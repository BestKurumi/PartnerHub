package observability

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
)

// Catch missing/invalid INFO fields masquerading as healthy zero observations.
func TestResourceRedisRequiresCompleteFiniteNonnegativeFields(t *testing.T) {
	info := "# Clients\r\nconnected_clients:3\r\nblocked_clients:1\r\n# Stats\r\nevicted_keys:9\r\ninstantaneous_ops_per_sec:12\r\n"
	values, err := redisResourceValues(info)
	if err != nil || values["redis_clients"] != 3 || values["redis_blocked_clients"] != 1 || values["redis_evicted_keys_total"] != 9 || values["redis_ops_per_second"] != 12 {
		t.Fatalf("values %v, err %v", values, err)
	}
	for _, bad := range []string{strings.Replace(info, "blocked_clients:1\r\n", "", 1), strings.Replace(info, "evicted_keys:9", "evicted_keys:NaN", 1), strings.Replace(info, "connected_clients:3", "connected_clients:-1", 1), info + "blocked_clients:4\r\n"} {
		if _, err := redisResourceValues(bad); err == nil {
			t.Fatalf("accepted invalid INFO %q", bad)
		}
	}
}

// Catch unseen/unauthorized topic metadata becoming healthy counts, and wrong
// leader/ISR calculations. Only the two application topics are expected.
func TestResourceKafkaTopology(t *testing.T) {
	md := kadm.Metadata{Brokers: kadm.BrokerDetails{{NodeID: 1}}, Topics: kadm.TopicDetails{
		"customer.binding.events":       {Topic: "customer.binding.events", Partitions: map[int32]kadm.PartitionDetail{0: {Leader: 1, Replicas: []int32{1}, ISR: []int32{1}}}},
		"platform2.trade.notifications": {Topic: "platform2.trade.notifications", Partitions: map[int32]kadm.PartitionDetail{0: {Leader: -1, Replicas: []int32{1, 2}, ISR: []int32{1}}}},
	}}
	values, err := kafkaResourceValues(md)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, v := range values {
		got[v.name+"/"+v.topic] = v.value
	}
	if got["kafka_brokers/"] != 1 || got["kafka_partitions/customer.binding.events"] != 1 || got["kafka_leaderless_partitions/platform2.trade.notifications"] != 1 || got["kafka_under_replicated_partitions/platform2.trade.notifications"] != 1 {
		t.Fatal(got)
	}
	delete(md.Topics, "customer.binding.events")
	if _, err := kafkaResourceValues(md); err == nil {
		t.Fatal("missing fixed topic became healthy")
	}
}

type statusDriver struct{}
type statusConn struct{}
type statusRows struct{ index int }

var failStatus bool

func (statusDriver) Open(string) (driver.Conn, error)  { return statusConn{}, nil }
func (statusConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unexpected prepare") }
func (statusConn) Close() error                        { return nil }
func (statusConn) Begin() (driver.Tx, error)           { return nil, errors.New("unexpected write") }
func (statusConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if failStatus {
		return nil, errors.New("access denied")
	}
	if !strings.HasPrefix(query, "SHOW GLOBAL STATUS WHERE Variable_name IN (") {
		return nil, errors.New("unexpected query")
	}
	return &statusRows{}, nil
}
func (*statusRows) Columns() []string { return []string{"Variable_name", "Value"} }
func (*statusRows) Close() error      { return nil }
func (r *statusRows) Next(dest []driver.Value) error {
	rows := [][2]string{{"Threads_connected", "5"}, {"Threads_running", "2"}, {"Queries", "120"}, {"Slow_queries", "1"}, {"Innodb_buffer_pool_bytes_data", "16384"}}
	if r.index == len(rows) {
		return io.EOF
	}
	dest[0], dest[1] = rows[r.index][0], rows[r.index][1]
	r.index++
	return nil
}

func TestResourceCollectorCachesNoProbeAndPreservesLastSuccess(t *testing.T) {
	sql.Register("resource-test", statusDriver{})
	db, _ := sql.Open("resource-test", "")
	defer db.Close()
	registry := prometheus.NewRegistry()
	resources, err := NewResources(ResourceConfig{DB: db}, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Close()
	resources.sampleCycle(context.Background())
	first := resourceGauge(t, registry, "hub_resource_mysql_connections", "")
	if first != 5 {
		t.Fatal(first)
	}
	stamp := resourceGauge(t, registry, "hub_resource_last_success_timestamp_seconds", "mysql")
	if stamp == 0 {
		t.Fatal("missing success timestamp")
	}
	failStatus = true
	defer func() { failStatus = false }() // Gather must read cache even while DB would reject a query.
	if resourceGauge(t, registry, "hub_resource_mysql_connections", "") != 5 || resourceGauge(t, registry, "hub_resource_sample_up", "mysql") != 1 {
		t.Fatal("scrape probed dependency")
	}
	resources.sampleCycle(context.Background())
	if resourceGauge(t, registry, "hub_resource_mysql_connections", "") != 5 || resourceGauge(t, registry, "hub_resource_sample_up", "mysql") != 0 || resourceGauge(t, registry, "hub_resource_last_success_timestamp_seconds", "mysql") != stamp {
		t.Fatal("failure discarded/advanced last successful observation")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { resources.Run(ctx, time.Second); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("resource sampler did not exit")
	}
}
func resourceGauge(t *testing.T, registry *prometheus.Registry, name, kind string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name {
			for _, m := range family.Metric {
				if kind == "" {
					return m.GetGauge().GetValue()
				}
				for _, label := range m.Label {
					if label.GetName() == "kind" && label.GetValue() == kind {
						return m.GetGauge().GetValue()
					}
				}
			}
		}
	}
	t.Fatalf("missing %s/%s", name, kind)
	return 0
}
