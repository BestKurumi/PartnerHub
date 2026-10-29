//go:build integration

package platform2_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"partner-hub/internal/mock/platform2"
	"partner-hub/internal/model"
)

func TestConsumerLagPausedAndCommitted(t *testing.T) {
	broker, dsn := os.Getenv("KAFKA_TEST_BROKERS"), os.Getenv("PLATFORM2_TEST_DSN")
	if broker == "" || dsn == "" {
		t.Fatal("Kafka and independent platform2 DSN required")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal("open independent DB failed")
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(broker, ",")...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	topic := "mock-metrics-" + model.NewID()
	group := topic + "-group"
	trace := model.NewID()
	admin := kadm.NewClient(client)
	created, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	if err != nil {
		t.Fatal("create metrics fixture topic failed")
	}
	if created[topic].Err != nil {
		t.Fatal("create metrics fixture topic returned error")
	}
	// Keep this uniquely named short-lived test topic for evidence; no shared topic is removed.
	defer db.Exec("DELETE FROM received_notification WHERE trace_id=?", trace)
	consumer, err := platform2.NewConsumer(platform2.ConsumerConfig{Brokers: strings.Split(broker, ","), Group: group, Topic: topic, Paused: true}, platform2.NewMySQLStore(db))
	if err != nil {
		t.Fatal(err)
	}
	work, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(work) }()
	defer func() {
		stop()
		consumer.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("consumer did not stop")
		}
	}()
	for i := 0; i < 3; i++ {
		body := strings.Replace(validPayload, `"traceId":"trace"`, `"traceId":"`+trace+`"`, 1)
		if err = client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("00009"), Value: []byte(body)}).FirstErr(); err != nil {
			t.Fatal("fixture produce failed")
		}
	}
	waitLag := func(want int64) {
		t.Helper()
		for ctx.Err() == nil {
			got, e := consumer.Lag(ctx)
			if e == nil && got == want {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("consumer lag did not reach %d", want)
	}
	waitLag(3)
	var rows int
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM received_notification WHERE trace_id=?", trace).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("paused consumer received=%d err=%v", rows, err)
	}
	consumer.SetPaused(false)
	waitLag(0)
	if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM received_notification WHERE trace_id=?", trace).Scan(&rows); err != nil || rows != 3 {
		t.Fatalf("resumed durable receives=%d err=%v", rows, err)
	}
	t.Log("Paused Kafka group lag 3 with 0 durable receipts; resume committed lag 0 with 3 durable receipts")
}
