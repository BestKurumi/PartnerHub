//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"partner-hub/internal/binding"
	hubkafka "partner-hub/internal/infra/kafka"
	hubmysql "partner-hub/internal/infra/mysql"
	"partner-hub/internal/model"
)

func kafkaTestClient(t *testing.T) (*kgo.Client, *kadm.Client, string) {
	t.Helper()
	broker := os.Getenv("KAFKA_TEST_BROKERS")
	if broker == "" {
		t.Fatal("KAFKA_TEST_BROKERS required")
	}
	options := []kgo.Opt{kgo.SeedBrokers(broker), kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.AllowAutoTopicCreation()}
	if os.Getenv("BINDING_TEST_KAFKA_DEBUG") == "1" {
		options = append(options, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	}
	cl, err := kgo.NewClient(options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl, kadm.NewClient(cl), broker
}
func createBindingTopic(t *testing.T, admin *kadm.Client) string {
	t.Helper()
	topic := fmt.Sprintf("it.bindings.%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	responses, err := admin.CreateTopics(ctx, 3, 1, map[string]*string{"retention.ms": kadm.StringPtr("604800000")}, topic)
	if err != nil {
		t.Fatal(err)
	}
	for _, response := range responses {
		if response.Err != nil {
			t.Fatal(response.Err)
		}
	}
	// KRaft's controller can acknowledge creation and advertise a leader before
	// that broker has activated its replicas. Probe the data plane, not merely
	// metadata, before starting a test's five-second produce budget. Otherwise
	// NOT_LEADER_FOR_PARTITION can consume that whole budget waiting for the
	// producer's normal five-second metadata refresh.
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		offsets, err := admin.ListEndOffsets(ctx, topic)
		ready := err == nil && len(offsets[topic]) == 3
		for partition := int32(0); partition < 3; partition++ {
			offset, exists := offsets.Lookup(topic, partition)
			ready = ready && exists && offset.Err == nil && offset.Offset >= 0
		}
		if ready {
			return topic
		}
		select {
		case <-ctx.Done():
			t.Fatalf("binding fixture replicas not ready: %v", ctx.Err())
		case <-tick.C:
		}
	}
}
func produceBinding(t *testing.T, cl *kgo.Client, topic string, partition int32, e model.BindingEvent) *kgo.Record {
	t.Helper()
	payload, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	record := &kgo.Record{Topic: topic, Partition: partition, Key: hubkafka.BindingKey(e.ClientID, e.PlatformID), Value: payload}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = cl.ProduceSync(ctx, record).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return record
}
func await(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("timed out waiting for observable binding state")
		case <-tick.C:
		}
	}
}
func committed(admin *kadm.Client, group, topic string, partition int32) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	offsets, err := admin.FetchOffsets(ctx, group)
	if err != nil {
		return -1
	}
	if ps, ok := offsets[topic]; ok {
		if offset, ok := ps[partition]; ok && offset.Err == nil {
			return offset.At
		}
	}
	return -1
}

type gatedBindingRepository struct {
	binding.Repository
	failApply, failAnomaly atomic.Bool
	calls                  atomic.Int32
}

func (r *gatedBindingRepository) Apply(ctx context.Context, e model.BindingEvent) (model.BindingResult, error) {
	r.calls.Add(1)
	if r.failApply.Load() {
		return model.BindingResult{}, errors.New("injected DB unavailable")
	}
	return r.Repository.Apply(ctx, e)
}
func (r *gatedBindingRepository) SaveAnomaly(ctx context.Context, a model.BindingAnomaly) error {
	r.calls.Add(1)
	if r.failAnomaly.Load() {
		return errors.New("injected anomaly persistence unavailable")
	}
	return r.Repository.SaveAnomaly(ctx, a)
}
func startBindingConsumer(t *testing.T, broker, group, topic string, repo binding.Repository) (*hubkafka.BindingConsumer, context.CancelFunc, <-chan error) {
	t.Helper()
	consumer, err := hubkafka.NewBindingConsumer(hubkafka.BindingConfig{Brokers: []string{broker}, Group: group, Topic: topic}, binding.NewService(repo, nil), repo)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	t.Cleanup(func() { cancel(); consumer.Close() })
	return consumer, cancel, done
}

// Shutdown can arrive from cancellation and more than one owner. Closing the
// underlying client concurrently must not race with Run's deferred shutdown.
func TestBindingsKafkaConcurrentClose(t *testing.T) {
	db := bindingDB(t)
	cl, admin, broker := kafkaTestClient(t)
	topic := createBindingTopic(t, admin)
	produceBinding(t, cl, topic, 0, bindingEvent("it-close", 801, "BIND", "PLATFORM_A", "close"))
	repo := &gatedBindingRepository{Repository: hubmysql.NewBindings(db)}
	repo.failApply.Store(true)
	consumer, cancel, done := startBindingConsumer(t, broker, "it-close-"+topic, topic, repo)
	await(t, func() bool { return repo.calls.Load() > 0 })
	var callers sync.WaitGroup
	for i := 0; i < 4; i++ {
		callers.Add(1)
		go func() { defer callers.Done(); consumer.Close() }()
	}
	cancel()
	callers.Wait()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled consumer did not stop")
	}
}

// Catches skipping a DB-failed record, poison skipping before durable anomaly,
// wrong partition keys, and committing beyond the contiguous processed prefix.
func TestBindingsKafkaDurableOffsets(t *testing.T) {
	db := bindingDB(t)
	client := "it-kafka-durable"
	cleanBindingClient(t, db, client)
	cl, admin, broker := kafkaTestClient(t)
	topic := createBindingTopic(t, admin)
	group := "it-binding-" + topic
	e := bindingEvent(client, 501, "BIND", "PLATFORM_A", "U501")
	first := produceBinding(t, cl, topic, 0, e)
	if string(first.Key) != `["it-kafka-durable","PLATFORM_A"]` {
		t.Fatalf("ambiguous/nonprotocol key: %s", first.Key)
	}
	repo := &gatedBindingRepository{Repository: hubmysql.NewBindings(db)}
	repo.failApply.Store(true)
	repo.failAnomaly.Store(true)
	_, _, done := startBindingConsumer(t, broker, group, topic, repo)
	await(t, func() bool {
		select {
		case err := <-done:
			t.Fatalf("consumer stopped: %v", err)
		default:
		}
		return repo.calls.Load() > 0
	})
	if at := committed(admin, group, topic, 0); at > first.Offset {
		t.Fatalf("DB-failed record committed: %d", at)
	}
	repo.failApply.Store(false)
	await(t, func() bool { return committed(admin, group, topic, 0) == first.Offset+1 })
	poison := &kgo.Record{Topic: topic, Partition: 0, Key: first.Key, Value: []byte(`{"poison":true}`)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, poison).FirstErr(); err != nil {
		t.Fatal(err)
	}
	produceBinding(t, cl, topic, 0, bindingEvent(client, 502, "UNBIND", "PLATFORM_A", "U501"))
	before := repo.calls.Load()
	await(t, func() bool { return repo.calls.Load() > before })
	if at := committed(admin, group, topic, 0); at != poison.Offset {
		t.Fatalf("unpersisted poison skipped: %d, want%d", at, poison.Offset)
	}
	snapshot, err := repo.Load(context.Background(), client)
	if err != nil || len(snapshot.Targets) != 1 {
		t.Fatalf("later record overtook poison: %+v %v", snapshot, err)
	}
	repo.failAnomaly.Store(false)
	await(t, func() bool { return committed(admin, group, topic, 0) == poison.Offset+2 })
	snapshot, err = repo.Load(context.Background(), client)
	if err != nil || len(snapshot.Targets) != 0 {
		t.Fatalf("valid following event not applied: %+v %v", snapshot, err)
	}
	var n int
	if err = db.QueryRow("SELECT COUNT(*) FROM ingress_anomaly WHERE source_topic=? AND source_partition=0 AND source_offset=?", topic, poison.Offset).Scan(&n); err != nil || n != 1 {
		t.Fatalf("poison anomaly count=%d err=%v", n, err)
	}
}

// Catches silently resetting an existing committed offset past retained data.
func TestBindingsKafkaRetentionGapStops(t *testing.T) {
	db := bindingDB(t)
	cl, admin, broker := kafkaTestClient(t)
	topic := createBindingTopic(t, admin)
	group := "it-gap-" + topic
	for i := 0; i < 3; i++ {
		produceBinding(t, cl, topic, 0, bindingEvent("it-gap", 601+i, "BIND", "PLATFORM_A", "U"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	offsets := kadm.Offsets{topic: {0: {Topic: topic, Partition: 0, At: 0, LeaderEpoch: -1}}}
	results, err := admin.CommitOffsets(ctx, group, offsets)
	if err != nil {
		t.Fatal(err)
	}
	if err = results.Error(); err != nil {
		t.Fatal(err)
	}
	deleted, err := admin.DeleteRecords(ctx, kadm.Offsets{topic: {0: {Topic: topic, Partition: 0, At: 2, LeaderEpoch: -1}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, ps := range deleted {
		for _, p := range ps {
			if p.Err != nil {
				t.Fatal(p.Err)
			}
		}
	}
	_, _, done := startBindingConsumer(t, broker, group, topic, hubmysql.NewBindings(db))
	select {
	case err = <-done:
		if !errors.Is(err, hubkafka.ErrBindingGap) {
			t.Fatalf("gap error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("consumer failed to stop on retained gap")
	}
	if at := committed(admin, group, topic, 0); at != 0 {
		t.Fatalf("retention gap silently skipped: %d", at)
	}
}

// Catches committing a DB-failed record from an obsolete assignment after a
// second instance joins. Both workers use the real transactional repository.
func TestBindingsKafkaRebalance(t *testing.T) {
	db := bindingDB(t)
	client := "it-kafka-rebalance"
	cleanBindingClient(t, db, client)
	cl, admin, broker := kafkaTestClient(t)
	topic := createBindingTopic(t, admin)
	group := "it-rebalance-" + topic
	for i := 0; i < 3; i++ {
		produceBinding(t, cl, topic, int32(i), bindingEvent(client, 701+i, "BIND", []string{"PLATFORM_A", "PLATFORM_B", "PLATFORM_A"}[i], []string{"A", "B", "A"}[i]))
	}
	repo := &gatedBindingRepository{Repository: hubmysql.NewBindings(db)}
	repo.failApply.Store(true)
	_, cancel1, done1 := startBindingConsumer(t, broker, group, topic, repo)
	await(t, func() bool {
		select {
		case err := <-done1:
			t.Fatalf("first stopped: %v", err)
		default:
		}
		return repo.calls.Load() > 0
	})
	_, _, done2 := startBindingConsumer(t, broker, group, topic, repo)
	// Force the original owner to leave during failed processing/backoff.
	cancel1()
	select {
	case err := <-done1:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("old worker exit: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old worker did not stop within budget")
	}
	for i := 0; i < 3; i++ {
		if at := committed(admin, group, topic, int32(i)); at > 0 {
			t.Fatalf("failed former owner committed partition%d offset%d", i, at)
		}
	}
	repo.failApply.Store(false)
	await(t, func() bool {
		select {
		case err := <-done2:
			t.Fatalf("successor stopped: %v", err)
		default:
		}
		for i := 0; i < 3; i++ {
			if committed(admin, group, topic, int32(i)) != 1 {
				return false
			}
		}
		return true
	})
	snapshot, err := repo.Load(context.Background(), client)
	if err != nil || len(snapshot.Targets) != 2 {
		t.Fatalf("rebalance lost targets: %+v %v", snapshot, err)
	}
	var count int
	if err = db.QueryRow("SELECT COUNT(*) FROM binding_event WHERE client_id=?", client).Scan(&count); err != nil || count != 3 {
		t.Fatalf("event responsibility lost or duplicated: %d %v", count, err)
	}
}
