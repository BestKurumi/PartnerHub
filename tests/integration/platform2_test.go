//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"partner-hub/internal/adapter"
	"partner-hub/internal/mock/platform2"
	"partner-hub/internal/model"
)

func platform2DB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("PLATFORM2_TEST_DSN")
	if dsn == "" {
		t.Fatal("PLATFORM2_TEST_DSN required")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ddl, err := os.ReadFile("../../deploy/mysql/mock-platform2/001_received.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range strings.Split(string(ddl), ";") {
		if strings.TrimSpace(stmt) != "" {
			if _, err = db.Exec(stmt); err != nil {
				t.Fatal(err)
			}
		}
	}
	return db
}
func TestPlatform2StoreReplay(t *testing.T) {
	db := platform2DB(t)
	store := platform2.NewMySQLStore(db)
	ctx := context.Background()
	topic := "it.platform2.store." + model.NewID()
	r := platform2.Receipt{ReceiveID: model.NewID(), Topic: topic, Partition: 0, Offset: 0, UserUID: "00009", TraceID: "same-trace", Payload: []byte(`{"extUserId":"00009"}`)}
	if err := store.Save(ctx, r); err != nil {
		t.Fatal(err)
	}
	replay := r
	replay.ReceiveID = model.NewID()
	if err := store.Save(ctx, replay); err != nil {
		t.Fatal(err)
	}
	second := r
	second.ReceiveID = model.NewID()
	second.Offset = 1
	if err := store.Save(ctx, second); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM received_notification WHERE source_topic=?", topic).Scan(&n); err != nil || n != 2 {
		t.Fatalf("business dedup or replay duplication: count=%d err=%v", n, err)
	}
}

type gatedPlatform2Store struct {
	store       platform2.Store
	fail        atomic.Bool
	failedSaves atomic.Int32
}

func (s *gatedPlatform2Store) Save(ctx context.Context, r platform2.Receipt) error {
	if s.fail.Load() {
		s.failedSaves.Add(1)
		return errors.New("injected store failure")
	}
	return s.store.Save(ctx, r)
}
func (s *gatedPlatform2Store) SaveInvalid(ctx context.Context, r platform2.InvalidRecord) error {
	if s.fail.Load() {
		return errors.New("injected store failure")
	}
	return s.store.SaveInvalid(ctx, r)
}
func TestPlatform2BrokerAndDurableOffsets(t *testing.T) {
	db := platform2DB(t)
	producer, admin, broker := kafkaTestClient(t)
	topic := fmt.Sprintf("it.platform2.%d", time.Now().UnixNano())
	group := topic + ".receive"
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	responses, err := admin.CreateTopics(ctx, 1, 1, map[string]*string{"retention.ms": kadm.StringPtr("604800000")}, topic)
	if err != nil || responses.Error() != nil {
		t.Fatalf("create topic: %v %v", err, responses.Error())
	}
	store := &gatedPlatform2Store{store: platform2.NewMySQLStore(db)}
	store.fail.Store(true)
	consumer, err := platform2.NewConsumer(platform2.ConsumerConfig{Brokers: []string{broker}, Group: group, Topic: topic, Paused: true}, store)
	if err != nil {
		t.Fatal(err)
	}
	run, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(run) }()
	t.Cleanup(func() { stop(); consumer.Close(); <-done })
	a, err := adapter.NewPlatform2([]string{broker}, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	n := model.TradeNotification{SecCode: "000001", SecName: "平安银行", Side: "BUY", Market: "SZ", TradeType: "NORMAL", TraceID: model.NewID(), ExecTime: time.Date(2026, 10, 22, 9, 30, 0, 123000000, time.FixedZone("Shanghai", 8*3600))}
	p, err := a.Prepare(n, "uid-"+topic)
	if err != nil {
		t.Fatal(err)
	}
	result := a.Send(ctx, p)
	if result.Outcome != model.Success || !result.SendStarted {
		t.Fatalf("broker result=%+v", result)
	}
	var count int
	if err = db.QueryRow("SELECT COUNT(*) FROM received_notification WHERE source_topic=?", topic).Scan(&count); err != nil || count != 0 {
		t.Fatalf("paused consumer fabricated receipt=%d %v", count, err)
	}
	// A non-empty broker offset proves the success belongs to transport, not Mock.
	ends, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	end, ok := ends.Lookup(topic, 0)
	if !ok || end.Offset != 1 {
		t.Fatalf("broker end=%+v", ends)
	}
	consumer.SetPaused(false)
	// Store is failing; no record may become durable or committed.
	await(t, func() bool { return store.failedSaves.Load() > 0 })
	if at := committed(admin, group, topic, 0); at > 0 {
		t.Fatalf("offset advanced before durable Save: %d", at)
	}
	store.fail.Store(false)
	await(t, func() bool {
		_ = db.QueryRow("SELECT COUNT(*) FROM received_notification WHERE source_topic=?", topic).Scan(&count)
		return count == 1 && committed(admin, group, topic, 0) == 1
	})
	// A new Kafka record with identical business payload is a real second receipt.
	if result = a.Send(ctx, p); result.Outcome != model.Success {
		t.Fatalf("second attempt=%+v", result)
	}
	await(t, func() bool {
		_ = db.QueryRow("SELECT COUNT(*) FROM received_notification WHERE source_topic=?", topic).Scan(&count)
		return count == 2 && committed(admin, group, topic, 0) == 2
	})
	poison := &kgo.Record{Topic: topic, Partition: 0, Key: []byte("bad"), Value: []byte(`{"msgId":"must-not-be-a-notification"}`)}
	if err = producer.ProduceSync(ctx, poison).FirstErr(); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		var n int
		_ = db.QueryRow("SELECT COUNT(*) FROM invalid_notification WHERE source_topic=?", topic).Scan(&n)
		return n == 1 && committed(admin, group, topic, 0) == 3
	})
}

type ackLostPlatform2Store struct {
	store platform2.Store
	saved atomic.Int32
}

func (s *ackLostPlatform2Store) Save(ctx context.Context, r platform2.Receipt) error {
	if err := s.store.Save(ctx, r); err != nil {
		return err
	}
	s.saved.Add(1)
	return errors.New("injected Save acknowledgement lost")
}
func (s *ackLostPlatform2Store) SaveInvalid(ctx context.Context, r platform2.InvalidRecord) error {
	return s.store.SaveInvalid(ctx, r)
}
func TestPlatform2RebalanceReplay(t *testing.T) {
	db := platform2DB(t)
	producer, admin, broker := kafkaTestClient(t)
	topic := fmt.Sprintf("it.platform2.rebalance.%d", time.Now().UnixNano())
	group := topic + ".receive"
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	responses, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	if err != nil || responses.Error() != nil {
		t.Fatalf("create topic: %v %v", err, responses.Error())
	}
	actual := platform2.NewMySQLStore(db)
	uncertain := &ackLostPlatform2Store{store: actual}
	first, err := platform2.NewConsumer(platform2.ConsumerConfig{Brokers: []string{broker}, Group: group, Topic: topic}, uncertain)
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.Run(firstCtx) }()
	t.Cleanup(func() { stopFirst(); first.Close() })
	payload := []byte(`{"extUserId":"rebalance-uid","stockCode":"000001","stockName":"平安银行","operateType":"BUY","tradeDateTime":"2026-10-22 09:30:00.123","marketNo":"SZ","bizType":1,"traceId":"rebalance-trace"}`)
	if err = producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Key: []byte("rebalance-uid"), Value: payload}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return uncertain.saved.Load() > 0 })
	if at := committed(admin, group, topic, 0); at > 0 {
		t.Fatalf("uncertain Save committed offset %d", at)
	}
	second, err := platform2.NewConsumer(platform2.ConsumerConfig{Brokers: []string{broker}, Group: group, Topic: topic}, actual)
	if err != nil {
		t.Fatal(err)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Run(secondCtx) }()
	t.Cleanup(func() { stopSecond(); second.Close(); <-secondDone })
	stopFirst()
	first.Close()
	<-firstDone
	await(t, func() bool { return committed(admin, group, topic, 0) == 1 })
	var count int
	if err = db.QueryRow("SELECT COUNT(*) FROM received_notification WHERE source_topic=?", topic).Scan(&count); err != nil || count != 1 {
		t.Fatalf("rebalance duplicated persisted receipt: %d %v", count, err)
	}
}
