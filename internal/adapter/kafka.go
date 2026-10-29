package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"partner-hub/internal/model"
	"strings"
	"time"
)

type Kafka struct {
	client *kgo.Client
	topic  string
}

func NewPlatform2(brokers []string, topic string) (*Kafka, error) {
	if len(brokers) == 0 || strings.TrimSpace(topic) == "" {
		return nil, errors.New("invalid platform2 configuration")
	}
	for _, b := range brokers {
		if strings.TrimSpace(b) == "" {
			return nil, errors.New("empty Kafka broker")
		}
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.MaxBufferedRecords(64), kgo.MaxBufferedBytes(512*1024), kgo.ProducerBatchMaxBytes(64*1024), kgo.RecordDeliveryTimeout(2*time.Second), kgo.RequestTimeoutOverhead(time.Second), kgo.ProduceRequestTimeout(time.Second), kgo.RetryBackoffFn(func(int) time.Duration { return 50 * time.Millisecond }))
	if err != nil {
		return nil, err
	}
	return &Kafka{cl, topic}, nil
}
func (k *Kafka) Close() { k.client.Close() }

type platform2Payload struct {
	ExtUserID     string `json:"extUserId"`
	StockCode     string `json:"stockCode"`
	StockName     string `json:"stockName"`
	OperateType   string `json:"operateType"`
	TradeDateTime string `json:"tradeDateTime"`
	MarketNo      string `json:"marketNo"`
	BizType       int    `json:"bizType"`
	TraceID       string `json:"traceId"`
}

func (k *Kafka) Prepare(n model.TradeNotification, uid string) (model.PreparedRequest, error) {
	types := map[string]int{"NORMAL": 1, "IPO": 2, "ETF": 3, "MARGIN": 4, "BLOCK": 5}
	if types[n.TradeType] == 0 || (n.Side != "BUY" && n.Side != "SELL") || (n.Market != "SH" && n.Market != "SZ") || n.ExecTime.IsZero() || !validText(uid, 128) || !validText(n.SecCode, 64) || !validText(n.SecName, 255) || !validText(n.TraceID, 128) {
		return model.PreparedRequest{}, errors.New("invalid platform2 notification")
	}
	zone, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return model.PreparedRequest{}, errors.New("trade timezone unavailable")
	}
	body, err := json.Marshal(platform2Payload{uid, n.SecCode, n.SecName, n.Side, n.ExecTime.In(zone).Format("2006-01-02 15:04:05.000"), n.Market, types[n.TradeType], n.TraceID})
	return model.PreparedRequest{PlatformID: "PLATFORM_B", ProtocolVersion: "platform2.v1", Payload: body}, err
}
func (k *Kafka) Send(ctx context.Context, p model.PreparedRequest) (r model.SendResult) {
	r.StartedAt = time.Now().UTC()
	r.Outcome = model.PermanentFailure
	r.Summary = "invalid prepared request"
	defer func() { r.FinishedAt = time.Now().UTC() }()
	var payload platform2Payload
	if p.PlatformID != "PLATFORM_B" || p.ProtocolVersion != "platform2.v1" || len(p.Payload) == 0 || len(p.Payload) > 8192 || json.Unmarshal(p.Payload, &payload) != nil || !validText(payload.ExtUserID, 128) {
		return
	}
	if ctx.Err() != nil {
		r.Outcome = model.RetryableFailure
		r.Summary = "budget exhausted before send"
		return
	}
	send, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	k.client.TryProduce(send, &kgo.Record{Topic: k.topic, Key: []byte(payload.ExtUserID), Value: p.Payload}, func(_ *kgo.Record, err error) { done <- err })
	// Once handed to the client, lack of broker confirmation is conservatively
	// UNKNOWN. The callback may arrive later; it can never change this result.
	r.SendStarted = true
	r.Outcome = model.Unknown
	r.Summary = "broker confirmation unconfirmed"
	select {
	case err := <-done:
		if err == nil {
			r.Outcome = model.Success
			r.Summary = "broker write confirmed"
		} else if errors.Is(err, kgo.ErrMaxBuffered) {
			r.SendStarted = false
			r.Outcome = model.RetryableFailure
			r.Summary = "producer capacity unavailable"
		}
	case <-send.Done():
	}
	return
}
